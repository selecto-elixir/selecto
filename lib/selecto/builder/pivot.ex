defmodule Selecto.Builder.Pivot do
  @moduledoc """
  SQL generation logic for Pivot functionality.

  This module handles the construction of SQL queries that pivot from the source table
  to a target table while preserving existing filters through subqueries.
  """

  alias Selecto.Types
  import Selecto.Builder.Sql.Helpers

  @spec build_pivot_query(Types.t(), keyword()) :: Types.builder_result()
  def build_pivot_query(selecto, opts \\ []) do
    pivot_config = Selecto.Pivot.get_pivot_config(selecto)

    if pivot_config do
      case pivot_config.subquery_strategy do
        :in -> build_in_subquery(selecto, pivot_config, opts)
        :exists -> build_exists_subquery(selecto, pivot_config, opts)
        :join -> build_join_strategy(selecto, pivot_config, opts)
        :cte -> build_cte_strategy(selecto, pivot_config, opts)
      end
    else
      # No pivot configuration, return standard FROM clause
      build_standard_from(selecto, opts)
    end
  end

  @doc """
  Extract pivot conditions from existing filters to construct the subquery.
  """
  @spec extract_pivot_conditions(Types.t(), Types.pivot_config(), String.t()) ::
          {Types.iodata_with_markers(), Types.sql_params()}
  def extract_pivot_conditions(selecto, pivot_config, source_alias) do
    if pivot_config.preserve_filters do
      # Build WHERE conditions from the original query context
      # Only use pre-pivot filters for the subquery
      pre_pivot_filters = Map.get(selecto.set, :filtered, [])
      build_filter_conditions(selecto, pre_pivot_filters, source_alias)
    else
      {[], []}
    end
  end

  @doc """
  Build the join chain subquery for connecting source to target.
  """
  @spec build_join_chain_subquery(Types.t(), Types.pivot_config(), [atom()]) ::
          {Types.iodata_with_markers(), Types.sql_params()}
  def build_join_chain_subquery(selecto, pivot_config, join_path) do
    # Get the actual source table name from domain
    source_table = get_source_table_name(selecto)

    # For pivots, we need to handle the hierarchical structure properly
    case join_path do
      [] ->
        # No path means we're already at the target
        {[], []}

      [single_assoc] ->
        # Single association - might be direct or need traversal
        build_single_hop_subquery(selecto, pivot_config, single_assoc, source_table)

      multiple_assocs ->
        # Multiple associations in path - need to follow the chain
        build_multi_hop_subquery(selecto, pivot_config, multiple_assocs, source_table)
    end
  end

  defp get_source_table_name(selecto) do
    # Get the actual table name from the domain source
    case selecto.domain.source do
      %{source_table: table} when is_binary(table) -> table
      %{source_table: table} when is_atom(table) -> to_string(table)
      _ -> raise ArgumentError, "No source_table found in domain"
    end
  end

  defp build_single_hop_subquery(selecto, pivot_config, assoc_name, source_table) do
    # For a single hop, we need to build the proper join path
    # Get the association from the source
    assoc_config = get_association_from_position(selecto, assoc_name, :source)

    if assoc_config == nil do
      raise ArgumentError, "Association #{assoc_name} not found in source"
    end

    # Get the target schema from the association
    target_schema = Map.get(assoc_config, :queryable, assoc_name)

    # Look up the target schema configuration to get its table
    target_config = Map.get(selecto.domain.schemas, target_schema)

    if target_config == nil do
      raise ArgumentError, "Schema #{target_schema} not found in domain"
    end

    target_table = Map.get(target_config, :source_table) || to_string(target_schema)
    target_pk = Map.get(target_config, :primary_key, :id)

    # Build the subquery with proper joins
    {where_clause, where_params} = extract_pivot_conditions(selecto, pivot_config, "s")

    owner_key = Map.get(assoc_config, :owner_key, :"#{assoc_name}_id")
    related_key = Map.get(assoc_config, :related_key, target_pk)

    # Build subquery: SELECT target.pk FROM source JOIN target WHERE filters
    # Note: for single hop, we don't need the intermediate join, just directly get the FK
    subquery_iodata = [
      "SELECT DISTINCT t.",
      escape_identifier(to_string(target_pk)),
      " FROM ",
      source_table,
      " s",
      " JOIN ",
      target_table,
      " t ON s.",
      escape_identifier(to_string(owner_key)),
      " = t.",
      escape_identifier(to_string(related_key))
    ]

    subquery_iodata =
      if where_clause != [] do
        subquery_iodata ++ [" WHERE ", where_clause]
      else
        subquery_iodata
      end

    {subquery_iodata, where_params}
  end

  defp build_multi_hop_subquery(selecto, pivot_config, join_path, source_table) do
    # For multi-hop pivots, we need to walk the hierarchical structure
    # Example path: [:film_actors, :film] means actor -> film_actors -> film

    {where_clause, where_params} = extract_pivot_conditions(selecto, pivot_config, "s")

    # Build the join chain by walking the path
    {join_clauses, join_params, final_table_info} =
      build_hierarchical_join_chain(selecto, join_path)

    # Extract final table info
    {final_alias, final_pk} = final_table_info

    # Build the subquery - use the final alias to qualify the field
    subquery_iodata = [
      "SELECT DISTINCT ",
      final_alias,
      ".",
      escape_identifier(to_string(final_pk)),
      " FROM ",
      source_table,
      " s",
      join_clauses
    ]

    subquery_iodata =
      if where_clause != [] do
        subquery_iodata ++ [" WHERE ", where_clause]
      else
        subquery_iodata
      end

    {subquery_iodata, join_params ++ where_params}
  end

  defp build_hierarchical_join_chain(selecto, join_path) do
    # Walk the hierarchical path building proper joins
    {join_clauses, params, current_position, _counter} =
      Enum.reduce(join_path, {[], [], :source, 1}, fn assoc_name,
                                                      {acc_joins, acc_params, current_pos,
                                                       counter} ->
        # Get the association from the current position
        assoc_config = get_association_from_position(selecto, assoc_name, current_pos)

        if assoc_config == nil do
          raise ArgumentError, "Association #{assoc_name} not found at position #{current_pos}"
        end

        # Get the target schema for this association
        target_schema = Map.get(assoc_config, :queryable, assoc_name)
        target_config = Map.get(selecto.domain.schemas, target_schema)

        if target_config == nil do
          raise ArgumentError, "Schema #{target_schema} not found"
        end

        target_table = Map.get(target_config, :source_table) || to_string(target_schema)
        target_pk = Map.get(target_config, :primary_key, :id)

        # Build the join clause
        current_alias = if current_pos == :source, do: "s", else: "j#{counter - 1}"
        next_alias = "j#{counter}"

        owner_key = Map.get(assoc_config, :owner_key, :"#{assoc_name}_id")
        related_key = Map.get(assoc_config, :related_key, target_pk)

        join_clause = [
          " JOIN ",
          target_table,
          " ",
          next_alias,
          " ON ",
          current_alias,
          ".",
          escape_identifier(to_string(owner_key)),
          " = ",
          next_alias,
          ".",
          escape_identifier(to_string(related_key))
        ]

        {acc_joins ++ join_clause, acc_params, target_schema, counter + 1}
      end)

    # Get the final table's primary key
    _last_schema = List.last(join_path)
    last_position = current_position

    # Look up the final schema to get its primary key
    final_schema_config =
      if last_position == :source do
        selecto.domain.source
      else
        Map.get(selecto.domain.schemas, last_position)
      end

    final_pk = Map.get(final_schema_config, :primary_key, :id)
    final_alias = "j#{length(join_path)}"

    {join_clauses, params, {final_alias, final_pk}}
  end

  # Private implementation functions

  defp build_in_subquery(selecto, pivot_config, _opts) do
    # Get the target table from the hierarchical structure
    target_schema = pivot_config.target_schema
    target_table = get_target_table_from_schema(selecto, target_schema)
    target_alias = get_target_alias()

    # Build the subquery that will select IDs from the original query context
    {subquery_iodata, subquery_params} =
      build_join_chain_subquery(
        selecto,
        pivot_config,
        pivot_config.join_path
      )

    # Get the primary key of the target table
    connection_field = get_target_primary_key(selecto, target_schema)

    # Build the IN condition
    in_condition =
      if subquery_iodata != [] do
        [target_alias, ".", escape_identifier(connection_field), " IN (", subquery_iodata, ")"]
      else
        # No subquery needed - we're already at the target
        []
      end

    # Get post-pivot filters and build additional conditions
    post_pivot_filters = Map.get(selecto.set, :post_pivot_filters, [])

    {post_pivot_conditions, post_pivot_params} =
      if post_pivot_filters != [] do
        conditions =
          Enum.map(post_pivot_filters, fn {field, value} ->
            field_name = escape_identifier(to_string(field))
            [target_alias, ".", field_name, " = ", {:param, value}]
          end)

        where_clause =
          case conditions do
            [single] -> single
            multiple -> Enum.intersperse(multiple, [" AND "])
          end

        {where_clause, Enum.map(post_pivot_filters, fn {_field, value} -> value end)}
      else
        {[], []}
      end

    # Combine IN condition with post-pivot filters
    where_conditions =
      cond do
        in_condition == [] and post_pivot_conditions != [] ->
          post_pivot_conditions

        in_condition != [] and post_pivot_conditions != [] ->
          [in_condition, " AND ", post_pivot_conditions]

        in_condition != [] ->
          in_condition

        true ->
          []
      end

    # Return FROM clause, WHERE conditions, and params
    from_iodata = [target_table, " ", target_alias]

    {from_iodata, where_conditions, subquery_params ++ post_pivot_params, []}
  end

  defp get_target_table_from_schema(selecto, target_schema) do
    # Navigate the hierarchical structure to find the target table
    case Map.get(selecto.domain.schemas, target_schema) do
      %{source_table: table} when not is_nil(table) ->
        to_string(table)

      _ ->
        # Fallback to using the schema name as table name
        to_string(target_schema)
    end
  end

  defp get_target_primary_key(selecto, target_schema) do
    # Get the primary key of the target schema
    case Map.get(selecto.domain.schemas, target_schema) do
      %{primary_key: pk} when not is_nil(pk) ->
        to_string(pk)

      _ ->
        # Default to "id" if not specified
        "id"
    end
  end

  defp build_exists_subquery(selecto, pivot_config, _opts) do
    target_table = get_target_table(selecto, pivot_config.target_schema)
    target_alias = get_target_alias()

    {subquery_iodata, subquery_params} =
      build_correlation_subquery(
        selecto,
        pivot_config,
        target_alias
      )

    # Build the EXISTS condition
    exists_condition = ["EXISTS (", subquery_iodata, ")"]

    # Get post-pivot filters and build additional conditions
    post_pivot_filters = Map.get(selecto.set, :post_pivot_filters, [])

    {post_pivot_conditions, post_pivot_params} =
      if post_pivot_filters != [] do
        conditions =
          Enum.map(post_pivot_filters, fn {field, value} ->
            field_name = escape_identifier(to_string(field))
            [target_alias, ".", field_name, " = ", {:param, value}]
          end)

        where_clause =
          case conditions do
            [single] -> single
            multiple -> Enum.intersperse(multiple, [" AND "])
          end

        {where_clause, Enum.map(post_pivot_filters, fn {_field, value} -> value end)}
      else
        {[], []}
      end

    # Combine EXISTS condition with post-pivot filters
    where_conditions =
      if post_pivot_conditions != [] do
        [exists_condition, " AND ", post_pivot_conditions]
      else
        exists_condition
      end

    # Return FROM clause, WHERE conditions, and params
    from_iodata = [target_table, " ", target_alias]

    {from_iodata, where_conditions, subquery_params ++ post_pivot_params, []}
  end

  defp build_join_strategy(selecto, pivot_config, _opts) do
    # For complex cases, build a series of JOINs instead of subqueries
    target_table = get_target_table(selecto, pivot_config.target_schema)
    target_alias = get_target_alias()

    {join_clauses, join_params} = build_explicit_joins(selecto, pivot_config)

    {filter_conditions, filter_params} =
      extract_pivot_conditions(selecto, pivot_config, get_source_alias())

    from_iodata = [target_table, " ", target_alias, join_clauses]

    # Return FROM clause, WHERE conditions, and params
    {from_iodata, filter_conditions, join_params ++ filter_params, []}
  end

  defp build_cte_strategy(selecto, pivot_config, _opts) do
    # Build a CTE-based pivot query for better performance
    # This returns special markers that the SQL builder will use to construct the CTE

    target_table = get_target_table_from_schema(selecto, pivot_config.target_schema)
    target_alias = get_target_alias()

    # Build the CTE query that filters the original data
    {cte_query, cte_params} = build_cte_filter_query(selecto, pivot_config)

    # Mark this as needing CTE construction
    cte_spec = %{
      name: "pivot_source",
      query: cte_query,
      params: cte_params,
      columns: [
        maybe_quote_identifier(
          to_string(get_target_primary_key(selecto, pivot_config.target_schema))
        )
      ]
    }

    # Build the main FROM clause that joins with the CTE
    from_iodata = [
      target_table,
      " ",
      target_alias,
      " INNER JOIN pivot_source ps ON ",
      target_alias,
      ".",
      maybe_quote_identifier(
        to_string(get_target_primary_key(selecto, pivot_config.target_schema))
      ),
      " = ps.",
      maybe_quote_identifier(
        to_string(get_target_primary_key(selecto, pivot_config.target_schema))
      )
    ]

    # Return FROM clause, empty WHERE (filtering is in CTE), params, and CTE spec
    {from_iodata, [], cte_params, [{:cte, cte_spec}]}
  end

  defp build_cte_filter_query(selecto, pivot_config) do
    # Build the query that goes inside the CTE
    source_table = get_source_table_name(selecto)

    # Build the join chain if needed
    case pivot_config.join_path do
      [] ->
        # Direct pivot, no joins needed
        {where_clause, where_params} = extract_pivot_conditions(selecto, pivot_config, "s")

        query_iodata = [
          "SELECT DISTINCT s.",
          maybe_quote_identifier(to_string(pivot_config.target_schema)),
          "_id",
          " FROM ",
          source_table,
          " s"
        ]

        query_iodata =
          if where_clause != [] do
            query_iodata ++ [" WHERE ", where_clause]
          else
            query_iodata
          end

        {query_iodata, where_params}

      join_path ->
        # Need to walk the join path
        {join_clauses, join_params, final_table_info} =
          build_hierarchical_join_chain(selecto, join_path)

        {where_clause, where_params} = extract_pivot_conditions(selecto, pivot_config, "s")

        # Extract final table info
        {final_alias, final_pk} = final_table_info

        query_iodata = [
          "SELECT DISTINCT ",
          final_alias,
          ".",
          maybe_quote_identifier(to_string(final_pk)),
          " FROM ",
          source_table,
          " s",
          join_clauses
        ]

        query_iodata =
          if where_clause != [] do
            query_iodata ++ [" WHERE ", where_clause]
          else
            query_iodata
          end

        {query_iodata, join_params ++ where_params}
    end
  end

  defp build_standard_from(selecto, _opts) do
    source_table = get_source_table_name(selecto)
    source_alias = get_source_alias()

    from_iodata = [source_table, " ", source_alias]
    # Return FROM clause, empty WHERE conditions, and no params
    {from_iodata, [], [], []}
  end

  defp build_single_join(selecto, join_name, current_alias, current_position) do
    association = get_association_from_position(selecto, join_name, current_position)

    if association == nil do
      raise ArgumentError, "Association #{join_name} not found at position #{current_position}"
    end

    join_config = ensure_association_fields(association, join_name)
    next_alias = generate_join_alias(join_name)

    join_type = Map.get(join_config, :type, :inner)
    join_table = get_join_table(selecto, join_name, current_position)

    # Build ON clause based on association configuration
    {on_clause, on_params} =
      build_join_condition(selecto, join_name, current_alias, next_alias, current_position)

    join_clause = [
      " ",
      sql_join_type(join_type),
      " JOIN ",
      join_table,
      " ",
      next_alias,
      " ON ",
      on_clause
    ]

    {join_clause, on_params, next_alias, Map.get(association, :queryable, join_name)}
  end

  defp build_correlation_subquery(selecto, pivot_config, target_alias) do
    # Build a correlated subquery that connects target to source
    source_table = get_source_table_name(selecto)
    source_alias = "sub_" <> get_source_alias()

    {join_clauses, join_params} =
      build_reverse_joins(selecto, pivot_config.join_path, source_alias, target_alias)

    {where_clause, where_params} = extract_pivot_conditions(selecto, pivot_config, source_alias)

    _correlation_field = get_target_connection_field(selecto, pivot_config)

    subquery_iodata = [
      "SELECT 1 FROM ",
      source_table,
      " ",
      source_alias,
      join_clauses
    ]

    # Add correlation condition
    correlation_condition =
      get_correlation_condition(selecto, pivot_config, source_alias, target_alias)

    subquery_iodata = subquery_iodata ++ [" WHERE ", correlation_condition]

    # Add additional filters
    subquery_iodata =
      if where_clause != [] do
        subquery_iodata ++ [" AND ", where_clause]
      else
        subquery_iodata
      end

    {subquery_iodata, join_params ++ where_params}
  end

  defp build_explicit_joins(selecto, pivot_config) do
    # Build explicit JOIN clauses from source to target
    source_alias = get_source_alias()

    Enum.reduce(pivot_config.join_path, {[], [], source_alias, :source}, fn join_name,
                                                                            {acc_clauses,
                                                                             acc_params,
                                                                             current_alias,
                                                                             current_position} ->
      {join_clause, join_params, next_alias, next_position} =
        build_single_join(selecto, join_name, current_alias, current_position)

      {acc_clauses ++ [join_clause], acc_params ++ join_params, next_alias, next_position}
    end)
    |> then(fn {clauses, params, _alias, _position} -> {clauses, params} end)
  end

  # Helper functions for table and field resolution

  defp get_target_table(selecto, target_schema) do
    case Map.get(selecto.domain.schemas, target_schema) do
      nil -> raise ArgumentError, "Target schema #{target_schema} not found"
      schema_config -> schema_config.source_table
    end
  end

  defp get_source_alias, do: "s"
  defp get_target_alias, do: "t"

  defp generate_join_alias(join_name) do
    "j_" <> to_string(join_name)
  end

  defp get_join_table(selecto, join_name, current_position) do
    # Get the association to find the target schema
    association = get_association_from_position(selecto, join_name, current_position)
    target_schema = association.queryable

    case Map.get(selecto.domain.schemas, target_schema) do
      nil -> raise ArgumentError, "Schema #{target_schema} not found for join #{join_name}"
      schema_config -> schema_config.source_table
    end
  end

  defp build_join_condition(selecto, join_name, current_alias, next_alias, current_position) do
    # Get association configuration to build ON clause
    association = get_association_from_position(selecto, join_name, current_position)

    # Infer the join keys based on naming conventions
    owner_key = Map.get(association, :owner_key)
    related_key = Map.get(association, :related_key)

    on_clause = [
      current_alias,
      ".",
      to_string(owner_key),
      " = ",
      next_alias,
      ".",
      to_string(related_key)
    ]

    {on_clause, []}
  end

  defp get_association_from_position(selecto, target_name, current_position) do
    # Navigate the hierarchical domain structure based on current position
    current_schema =
      case current_position do
        :source -> selecto.domain.source
        schema_name -> Map.get(selecto.domain.schemas, schema_name)
      end

    if current_schema do
      # Look for the association in the current schema
      associations = Map.get(current_schema, :associations, %{})

      case Map.get(associations, target_name) do
        nil ->
          # Not found at this level, need to look in joins structure if available
          joins = Map.get(selecto.domain, :joins, %{})
          find_in_joins_structure(joins, target_name)

        assoc_config ->
          assoc_config
      end
    else
      nil
    end
  end

  defp find_in_joins_structure(joins, target_name) when is_map(joins) do
    # Search through the joins structure recursively
    Enum.find_value(joins, fn {join_name, join_config} ->
      if join_name == target_name do
        # Found it - ensure it has the expected fields
        ensure_association_fields(join_config, join_name)
      else
        # Look in nested joins
        case Map.get(join_config, :joins) do
          nil -> nil
          nested -> find_in_joins_structure(nested, target_name)
        end
      end
    end)
  end

  defp find_in_joins_structure(_, _), do: nil

  defp ensure_association_fields(config, join_name) do
    config
    |> Map.put_new(:queryable, join_name)
    |> Map.put_new(:field, join_name)
  end

  defp build_filter_conditions(_selecto, [], _source_alias), do: {[], []}

  defp build_filter_conditions(_selecto, filters, source_alias) do
    # Build simple WHERE conditions for pivot subqueries
    if length(filters) == 0 do
      {[], []}
    else
      conditions =
        Enum.map(filters, fn {field, value} ->
          field_name = escape_identifier(to_string(field))
          [source_alias, ".", field_name, " = ", {:param, value}]
        end)

      where_clause =
        case conditions do
          [single] -> single
          multiple -> Enum.intersperse(multiple, [" AND "])
        end

      # Extract parameters
      params = Enum.map(filters, fn {_field, value} -> value end)

      {where_clause, params}
    end
  end

  defp get_target_connection_field(selecto, pivot_config) do
    # Return the field on the target table that connects back to the source
    target_config = Map.get(selecto.domain.schemas, pivot_config.target_schema)

    if target_config do
      to_string(target_config.primary_key || :id)
    else
      "id"
    end
  end

  defp build_reverse_joins(selecto, join_path, source_alias, _target_alias) do
    # Build joins from source to target for correlation subquery
    {join_clauses, params, _last_alias, _last_position} =
      Enum.reduce(join_path, {[], [], source_alias, :source}, fn join_name,
                                                                 {acc_clauses, acc_params,
                                                                  current_alias, current_position} ->
        {join_clause, join_params, next_alias, next_position} =
          build_single_join(selecto, join_name, current_alias, current_position)

        {acc_clauses ++ [join_clause], acc_params ++ join_params, next_alias, next_position}
      end)

    {join_clauses, params}
  end

  defp get_correlation_condition(selecto, pivot_config, source_alias, target_alias) do
    # Build correlation condition between the final joined table and the target
    target_schema = pivot_config.target_schema
    target_config = Map.get(selecto.domain.schemas, target_schema)

    if target_config do
      target_pk = to_string(target_config.primary_key || :id)

      # Find the final join alias
      final_alias =
        case pivot_config.join_path do
          [] ->
            source_alias

          joins ->
            Enum.reduce(joins, source_alias, fn join_name, _ ->
              generate_join_alias(join_name)
            end)
        end

      [final_alias, ".", target_pk, " = ", target_alias, ".", target_pk]
    else
      [source_alias, ".id = ", target_alias, ".id"]
    end
  end

  defp sql_join_type(:left), do: "LEFT"
  defp sql_join_type(:right), do: "RIGHT"
  defp sql_join_type(:inner), do: "INNER"
  defp sql_join_type(:full), do: "FULL"
  # Default
  defp sql_join_type(_), do: "LEFT"

  # Use escape_identifier as alias for maybe_quote_identifier
  defp escape_identifier(identifier) do
    maybe_quote_identifier(identifier)
  end
end
