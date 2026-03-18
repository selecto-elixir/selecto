defmodule Selecto.Builder.Retarget do
  @moduledoc """
  SQL generation logic for Retarget functionality.

  This module handles the construction of SQL queries that retarget from the source table
  to a target table while preserving existing filters through subqueries.
  """

  alias Selecto.Types
  import Selecto.Builder.Sql.Helpers

  @comparison_operators [:=, :!=, :<, :>, :<=, :>=, :gt, :lt, :gte, :lte, :eq, :ne]

  @spec build_retarget_query(Types.t(), keyword()) :: Types.builder_result()
  def build_retarget_query(selecto, opts \\ []) do
    retarget_config = Selecto.Retarget.get_retarget_config(selecto)

    if retarget_config do
      case retarget_config.subquery_strategy do
        :in -> build_in_subquery(selecto, retarget_config, opts)
        :exists -> build_exists_subquery(selecto, retarget_config, opts)
        :join -> build_join_strategy(selecto, retarget_config, opts)
        :cte -> build_cte_strategy(selecto, retarget_config, opts)
      end
    else
      # No retarget configuration, return standard FROM clause
      build_standard_from(selecto, opts)
    end
  end

  @doc """
  Extract retarget conditions from existing filters to construct the subquery.
  """
  @spec extract_retarget_conditions(Types.t(), Types.retarget_config(), String.t()) ::
          {Types.iodata_with_markers(), Types.sql_params()}
  def extract_retarget_conditions(selecto, retarget_config, source_alias) do
    if retarget_config.preserve_filters do
      # Build WHERE conditions from the original query context
      # Only use pre-retarget filters for the subquery
      pre_retarget_filters = Map.get(selecto.set, :filtered, [])
      build_filter_conditions(selecto, pre_retarget_filters, source_alias)
    else
      {[], []}
    end
  end

  @doc """
  Build the join chain subquery for connecting source to target.
  """
  @spec build_join_chain_subquery(Types.t(), Types.retarget_config(), [atom()]) ::
          {Types.iodata_with_markers(), Types.sql_params()}
  def build_join_chain_subquery(selecto, retarget_config, join_path) do
    # Get the actual source table name from domain
    source_table = get_source_table_name(selecto)

    # For retargets, we need to handle the hierarchical structure properly
    case join_path do
      [] ->
        # No path means we're already at the target
        {[], []}

      [single_assoc] ->
        # Single association - might be direct or need traversal
        build_single_hop_subquery(selecto, retarget_config, single_assoc, source_table)

      multiple_assocs ->
        # Multiple associations in path - need to follow the chain
        build_multi_hop_subquery(selecto, retarget_config, multiple_assocs, source_table)
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

  defp build_single_hop_subquery(selecto, retarget_config, assoc_name, source_table) do
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
    {where_clause, where_params} = extract_retarget_conditions(selecto, retarget_config, "s")

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

  defp build_multi_hop_subquery(selecto, retarget_config, join_path, source_table) do
    # For multi-hop retargets, we need to walk the hierarchical structure
    # Example path: [:film_actors, :film] means actor -> film_actors -> film

    {where_clause, where_params} = extract_retarget_conditions(selecto, retarget_config, "s")

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

  defp build_in_subquery(selecto, retarget_config, _opts) do
    # Get the target table from the hierarchical structure
    target_schema = retarget_config.target_schema
    target_table = get_target_table_from_schema(selecto, target_schema)
    target_alias = get_target_alias()

    # Build the subquery that will select IDs from the original query context
    {subquery_iodata, subquery_params} =
      build_join_chain_subquery(
        selecto,
        retarget_config,
        retarget_config.join_path
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

    {post_pivot_conditions, post_pivot_params} =
      build_post_retarget_conditions(selecto, retarget_config, target_alias)

    # Combine IN condition with post-retarget filters
    where_conditions = combine_where_conditions(in_condition, post_pivot_conditions)

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

  defp build_exists_subquery(selecto, retarget_config, _opts) do
    target_table = get_target_table(selecto, retarget_config.target_schema)
    target_alias = get_target_alias()

    {subquery_iodata, subquery_params} =
      build_correlation_subquery(
        selecto,
        retarget_config,
        target_alias
      )

    # Build the EXISTS condition
    exists_condition = ["EXISTS (", subquery_iodata, ")"]

    {post_pivot_conditions, post_pivot_params} =
      build_post_retarget_conditions(selecto, retarget_config, target_alias)

    # Combine EXISTS condition with post-retarget filters
    where_conditions = combine_where_conditions(exists_condition, post_pivot_conditions)

    # Return FROM clause, WHERE conditions, and params
    from_iodata = [target_table, " ", target_alias]

    {from_iodata, where_conditions, subquery_params ++ post_pivot_params, []}
  end

  defp build_join_strategy(selecto, retarget_config, _opts) do
    # For complex cases, build a series of JOINs instead of subqueries
    target_table = get_target_table(selecto, retarget_config.target_schema)
    target_alias = get_target_alias()

    {join_clauses, join_params} = build_explicit_joins(selecto, retarget_config)

    {filter_conditions, filter_params} =
      extract_retarget_conditions(selecto, retarget_config, get_source_alias())

    {post_pivot_conditions, post_pivot_params} =
      build_post_retarget_conditions(selecto, retarget_config, target_alias)

    from_iodata = [target_table, " ", target_alias, join_clauses]
    where_conditions = combine_where_conditions(filter_conditions, post_pivot_conditions)

    # Return FROM clause, WHERE conditions, and params
    {from_iodata, where_conditions, join_params ++ filter_params ++ post_pivot_params, []}
  end

  defp build_cte_strategy(selecto, retarget_config, _opts) do
    # Build a CTE-based retarget query for better performance
    # This returns special markers that the SQL builder will use to construct the CTE

    target_table = get_target_table_from_schema(selecto, retarget_config.target_schema)
    target_alias = get_target_alias()

    # Build the CTE query that filters the original data
    {cte_query, cte_params} = build_cte_filter_query(selecto, retarget_config)

    # Mark this as needing CTE construction
    cte_spec = %{
      name: "pivot_source",
      query: cte_query,
      params: cte_params,
      columns: [
        maybe_quote_identifier(
          to_string(get_target_primary_key(selecto, retarget_config.target_schema))
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
        to_string(get_target_primary_key(selecto, retarget_config.target_schema))
      ),
      " = ps.",
      maybe_quote_identifier(
        to_string(get_target_primary_key(selecto, retarget_config.target_schema))
      )
    ]

    {post_pivot_conditions, post_pivot_params} =
      build_post_retarget_conditions(selecto, retarget_config, target_alias)

    # Return FROM clause, outer WHERE for post-retarget filters, params, and CTE spec
    {from_iodata, post_pivot_conditions, cte_params ++ post_pivot_params, [{:cte, cte_spec}]}
  end

  defp build_cte_filter_query(selecto, retarget_config) do
    # Build the query that goes inside the CTE
    source_table = get_source_table_name(selecto)

    # Build the join chain if needed
    case retarget_config.join_path do
      [] ->
        # Direct retarget, no joins needed
        {where_clause, where_params} = extract_retarget_conditions(selecto, retarget_config, "s")

        query_iodata = [
          "SELECT DISTINCT s.",
          maybe_quote_identifier(to_string(retarget_config.target_schema)),
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

        {where_clause, where_params} = extract_retarget_conditions(selecto, retarget_config, "s")

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

  defp build_correlation_subquery(selecto, retarget_config, target_alias) do
    # Build a correlated subquery that connects target to source
    source_table = get_source_table_name(selecto)
    source_alias = "sub_" <> get_source_alias()

    {join_clauses, join_params} =
      build_reverse_joins(selecto, retarget_config.join_path, source_alias, target_alias)

    {where_clause, where_params} =
      extract_retarget_conditions(selecto, retarget_config, source_alias)

    _correlation_field = get_target_connection_field(selecto, retarget_config)

    subquery_iodata = [
      "SELECT 1 FROM ",
      source_table,
      " ",
      source_alias,
      join_clauses
    ]

    # Add correlation condition
    correlation_condition =
      get_correlation_condition(selecto, retarget_config, source_alias, target_alias)

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

  defp build_explicit_joins(selecto, retarget_config) do
    # Build explicit JOIN clauses from source to target
    source_alias = get_source_alias()

    Enum.reduce(retarget_config.join_path, {[], [], source_alias, :source}, fn join_name,
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
    # Build simple WHERE conditions for retarget subqueries
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

  defp get_target_connection_field(selecto, retarget_config) do
    # Return the field on the target table that connects back to the source
    target_config = Map.get(selecto.domain.schemas, retarget_config.target_schema)

    if target_config do
      to_string(target_config.primary_key || :id)
    else
      "id"
    end
  end

  defp build_reverse_joins(selecto, join_path, source_alias, _target_alias) do
    # Build joins from source to target for correlation subquery
    {join_clauses, params, _last_alias, _last_position} =
      Enum.reduce(join_path, {[], [], source_alias, :source}, fn join_name, join_state ->
        {acc_clauses, acc_params, current_alias, current_position} = join_state

        {join_clause, join_params, next_alias, next_position} =
          build_single_join(selecto, join_name, current_alias, current_position)

        {acc_clauses ++ [join_clause], acc_params ++ join_params, next_alias, next_position}
      end)

    {join_clauses, params}
  end

  defp get_correlation_condition(selecto, retarget_config, source_alias, target_alias) do
    # Build correlation condition between the final joined table and the target
    target_schema = retarget_config.target_schema
    target_config = Map.get(selecto.domain.schemas, target_schema)

    if target_config do
      target_pk = to_string(target_config.primary_key || :id)

      # Find the final join alias
      final_alias =
        case retarget_config.join_path do
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

  defp build_post_retarget_conditions(selecto, retarget_config, target_alias) do
    filters =
      Map.get(selecto.set, :post_retarget_filters) ||
        Map.get(selecto.set, :post_pivot_filters, []) || []

    build_pivot_filter_group(selecto, retarget_config.target_schema, target_alias, filters)
  end

  defp build_pivot_filter_group(_selecto, _target_schema, _target_alias, []), do: {[], []}

  defp build_pivot_filter_group(selecto, target_schema, target_alias, filters)
       when is_list(filters) do
    {clauses, params} =
      Enum.reduce(filters, {[], []}, fn filter, {clauses, params} ->
        {clause, clause_params} =
          build_pivot_filter(selecto, target_schema, target_alias, filter)

        {clauses ++ [clause], params ++ clause_params}
      end)

    where_clause =
      case clauses do
        [single] ->
          single

        multiple ->
          multiple
          |> Enum.map(&["(", &1, ")"])
          |> Enum.intersperse(" AND ")
      end

    {where_clause, params}
  end

  defp build_pivot_filter(selecto, target_schema, target_alias, {:not, filter}) do
    {clause, params} = build_pivot_filter(selecto, target_schema, target_alias, filter)
    {["NOT (", clause, ")"], params}
  end

  defp build_pivot_filter(selecto, target_schema, target_alias, {conj, filters})
       when conj in [:and, :or] do
    {clauses, params} =
      Enum.reduce(filters, {[], []}, fn filter, {clauses, params} ->
        {clause, clause_params} =
          build_pivot_filter(selecto, target_schema, target_alias, filter)

        {clauses ++ [clause], params ++ clause_params}
      end)

    joined_clause =
      case clauses do
        [single] ->
          single

        multiple ->
          multiple
          |> Enum.map(&["(", &1, ")"])
          |> Enum.intersperse(" #{conj} ")
      end

    {joined_clause, params}
  end

  defp build_pivot_filter(selecto, target_schema, target_alias, {field, {:between, [min, max]}}) do
    build_pivot_filter(selecto, target_schema, target_alias, {field, {:between, min, max}})
  end

  defp build_pivot_filter(_selecto, target_schema, target_alias, {field, {:between, min, max}}) do
    selector = pivot_target_selector(target_schema, target_alias, field)
    {[selector, " BETWEEN ", {:param, min}, " AND ", {:param, max}], [min, max]}
  end

  defp build_pivot_filter(_selecto, target_schema, target_alias, {field, {comp, value}})
       when comp in [:like, :ilike] do
    selector = pivot_target_selector(target_schema, target_alias, field)
    {[selector, " ", to_string(comp), " ", {:param, value}], [value]}
  end

  defp build_pivot_filter(_selecto, target_schema, target_alias, {field, {:not_like, value}}) do
    selector = pivot_target_selector(target_schema, target_alias, field)
    {[selector, " NOT LIKE ", {:param, value}], [value]}
  end

  defp build_pivot_filter(_selecto, target_schema, target_alias, {field, {comp, value}})
       when comp in @comparison_operators do
    selector = pivot_target_selector(target_schema, target_alias, field)
    {[selector, " ", sql_operator(comp), " ", {:param, value}], [value]}
  end

  defp build_pivot_filter(_selecto, target_schema, target_alias, {field, {:in, values}})
       when is_list(values) do
    selector = pivot_target_selector(target_schema, target_alias, field)
    {[selector, " = ANY(", {:param, values}, ")"], [values]}
  end

  defp build_pivot_filter(_selecto, target_schema, target_alias, {field, {:not_in, values}})
       when is_list(values) do
    selector = pivot_target_selector(target_schema, target_alias, field)
    {["NOT (", selector, " = ANY(", {:param, values}, "))"], [values]}
  end

  defp build_pivot_filter(_selecto, target_schema, target_alias, {field, :not_null}) do
    selector = pivot_target_selector(target_schema, target_alias, field)
    {[selector, " IS NOT NULL"], []}
  end

  defp build_pivot_filter(_selecto, target_schema, target_alias, {field, nil}) do
    selector = pivot_target_selector(target_schema, target_alias, field)
    {[selector, " IS NULL"], []}
  end

  defp build_pivot_filter(selecto, target_schema, target_alias, {field, values})
       when is_list(values) do
    build_pivot_filter(selecto, target_schema, target_alias, {field, {:in, values}})
  end

  defp build_pivot_filter(_selecto, target_schema, target_alias, {field, value}) do
    selector = pivot_target_selector(target_schema, target_alias, field)
    {[selector, " = ", {:param, value}], [value]}
  end

  defp pivot_target_selector(target_schema, target_alias, field) do
    field_name = normalize_pivot_field(target_schema, field)
    [target_alias, ".", escape_identifier(field_name)]
  end

  defp normalize_pivot_field(target_schema, field) do
    field_str = to_string(field)
    target_prefix = "#{target_schema}."

    if String.starts_with?(field_str, target_prefix) do
      String.replace_prefix(field_str, target_prefix, "")
    else
      field_str
    end
  end

  defp combine_where_conditions([], []), do: []
  defp combine_where_conditions(base, []), do: base
  defp combine_where_conditions([], extra), do: extra
  defp combine_where_conditions(base, extra), do: ["(", base, ") AND (", extra, ")"]

  defp sql_operator(:gt), do: ">"
  defp sql_operator(:lt), do: "<"
  defp sql_operator(:gte), do: ">="
  defp sql_operator(:lte), do: "<="
  defp sql_operator(:eq), do: "="
  defp sql_operator(:ne), do: "!="
  defp sql_operator(operator), do: to_string(operator)

  # Use escape_identifier as alias for maybe_quote_identifier
  defp escape_identifier(identifier) do
    maybe_quote_identifier(identifier)
  end
end
