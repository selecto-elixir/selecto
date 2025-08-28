defmodule Selecto.Builder.Pivot do
  @moduledoc """
  SQL generation logic for Pivot functionality.

  This module handles the construction of SQL queries that pivot from the source table
  to a target table while preserving existing filters through subqueries.
  """

  import Selecto.Builder.Sql.Helpers
  alias Selecto.SQL.Params
  alias Selecto.Types

  @spec build_pivot_query(Types.t(), keyword()) :: Types.builder_result()
  def build_pivot_query(selecto, opts \\ []) do
    pivot_config = Selecto.Pivot.get_pivot_config(selecto)
    
    if pivot_config do
      case pivot_config.subquery_strategy do
        :in -> build_in_subquery(selecto, pivot_config, opts)
        :exists -> build_exists_subquery(selecto, pivot_config, opts)
        :join -> build_join_strategy(selecto, pivot_config, opts)
      end
    else
      # No pivot configuration, return standard FROM clause
      build_standard_from(selecto, opts)
    end
  end

  @doc """
  Extract pivot conditions from existing filters to construct the subquery.
  """
  @spec extract_pivot_conditions(Types.t(), Types.pivot_config(), String.t()) :: {Types.iodata_with_markers(), Types.sql_params()}
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
  @spec build_join_chain_subquery(Types.t(), Types.pivot_config(), [atom()]) :: {Types.iodata_with_markers(), Types.sql_params()}
  def build_join_chain_subquery(selecto, pivot_config, join_path) do
    source_table = selecto.config.source_table
    target_schema = pivot_config.target_schema

    {_source_alias, join_clauses, join_params} = build_join_sequence(selecto, join_path)
    {where_clause, where_params} = extract_pivot_conditions(selecto, pivot_config, get_source_alias())

    # Get the final connection field for the subquery result
    connection_field = get_connection_field(selecto, target_schema, join_path)

    # Get the final alias in the join chain to qualify the connection field
    final_alias = get_final_join_alias(join_path)

    subquery_iodata = [
      "SELECT ", "subq.", connection_field, " FROM (SELECT DISTINCT ", final_alias, ".", connection_field, " AS ", connection_field,
      " FROM ", source_table, " ", get_source_alias(),
      join_clauses
    ]

    subquery_iodata = if where_clause != [] do
      subquery_iodata ++ [" WHERE ", where_clause]
    else
      subquery_iodata
    end

    # Close the inner subquery and add alias
    subquery_iodata = subquery_iodata ++ [") AS subq"]

    {subquery_iodata, join_params ++ where_params}
  end

  # Private implementation functions

  defp build_in_subquery(selecto, pivot_config, _opts) do
    target_table = get_target_table(selecto, pivot_config.target_schema)
    target_alias = get_target_alias()
    
    {subquery_iodata, subquery_params} = build_join_chain_subquery(
      selecto, 
      pivot_config, 
      pivot_config.join_path
    )
    
    connection_field = get_target_connection_field(selecto, pivot_config)
    
    # Build the IN condition
    in_condition = [target_alias, ".", connection_field, " IN (", subquery_iodata, ")"]
    
    # Get post-pivot filters and build additional conditions
    post_pivot_filters = Map.get(selecto.set, :post_pivot_filters, [])
    {post_pivot_conditions, post_pivot_params} = if post_pivot_filters != [] do
      conditions = Enum.map(post_pivot_filters, fn {field, value} ->
        field_name = escape_identifier(to_string(field))
        [target_alias, ".", field_name, " = ", {:param, value}]
      end)
      
      where_clause = case conditions do
        [single] -> single
        multiple -> Enum.intersperse(multiple, [" AND "])
      end
      
      {where_clause, Enum.map(post_pivot_filters, fn {_field, value} -> value end)}
    else
      {[], []}
    end
    
    # Combine IN condition with post-pivot filters
    where_conditions = if post_pivot_conditions != [] do
      [in_condition, " AND ", post_pivot_conditions]
    else
      in_condition
    end
    
    # Return FROM clause, WHERE conditions, and params
    from_iodata = [target_table, " ", target_alias]
    
    {from_iodata, where_conditions, subquery_params ++ post_pivot_params, []}
  end

  defp build_exists_subquery(selecto, pivot_config, _opts) do
    target_table = get_target_table(selecto, pivot_config.target_schema)
    target_alias = get_target_alias()
    
    {subquery_iodata, subquery_params} = build_correlation_subquery(
      selecto, 
      pivot_config, 
      target_alias
    )
    
    # Build the EXISTS condition
    exists_condition = ["EXISTS (", subquery_iodata, ")"]
    
    # Get post-pivot filters and build additional conditions
    post_pivot_filters = Map.get(selecto.set, :post_pivot_filters, [])
    {post_pivot_conditions, post_pivot_params} = if post_pivot_filters != [] do
      conditions = Enum.map(post_pivot_filters, fn {field, value} ->
        field_name = escape_identifier(to_string(field))
        [target_alias, ".", field_name, " = ", {:param, value}]
      end)
      
      where_clause = case conditions do
        [single] -> single
        multiple -> Enum.intersperse(multiple, [" AND "])
      end
      
      {where_clause, Enum.map(post_pivot_filters, fn {_field, value} -> value end)}
    else
      {[], []}
    end
    
    # Combine EXISTS condition with post-pivot filters
    where_conditions = if post_pivot_conditions != [] do
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
    {filter_conditions, filter_params} = extract_pivot_conditions(selecto, pivot_config, get_source_alias())
    
    from_iodata = [target_table, " ", target_alias, join_clauses]
    
    # Return FROM clause, WHERE conditions, and params
    {from_iodata, filter_conditions, join_params ++ filter_params, []}
  end

  defp build_standard_from(selecto, _opts) do
    source_table = selecto.config.source_table
    source_alias = get_source_alias()
    
    from_iodata = [source_table, " ", source_alias]
    # Return FROM clause, empty WHERE conditions, and no params
    {from_iodata, [], [], []}
  end

  defp build_join_sequence(selecto, join_path) do
    source_alias = get_source_alias()
    
    {join_clauses, params, _current_alias} = 
      Enum.reduce(join_path, {[], [], source_alias}, fn join_name, {acc_clauses, acc_params, current_alias} ->
        {join_clause, join_params, next_alias} = build_single_join(selecto, join_name, current_alias)
        {acc_clauses ++ [join_clause], acc_params ++ join_params, next_alias}
      end)
    
    {get_source_alias(), join_clauses, params}
  end

  defp build_single_join(selecto, join_name, current_alias) do
    join_config = get_join_config(selecto, join_name)
    next_alias = generate_join_alias(join_name)
    
    join_type = Map.get(join_config, :type, :inner)
    join_table = get_join_table(selecto, join_name)
    
    # Build ON clause based on association configuration
    {on_clause, on_params} = build_join_condition(selecto, join_name, current_alias, next_alias)
    
    join_clause = [
      " ", sql_join_type(join_type), " JOIN ", join_table, " ", next_alias,
      " ON ", on_clause
    ]
    
    {join_clause, on_params, next_alias}
  end

  defp build_correlation_subquery(selecto, pivot_config, target_alias) do
    # Build a correlated subquery that connects target to source
    source_table = selecto.config.source_table
    source_alias = "sub_" <> get_source_alias()
    
    {join_clauses, join_params} = build_reverse_joins(selecto, pivot_config.join_path, source_alias, target_alias)
    {where_clause, where_params} = extract_pivot_conditions(selecto, pivot_config, source_alias)
    
    correlation_field = get_target_connection_field(selecto, pivot_config)
    
    subquery_iodata = [
      "SELECT 1 FROM ", source_table, " ", source_alias,
      join_clauses
    ]
    
    # Add correlation condition
    correlation_condition = get_correlation_condition(selecto, pivot_config, source_alias, target_alias)
    subquery_iodata = subquery_iodata ++ [" WHERE ", correlation_condition]
    
    # Add additional filters
    subquery_iodata = if where_clause != [] do
      subquery_iodata ++ [" AND ", where_clause]
    else
      subquery_iodata
    end
    
    {subquery_iodata, join_params ++ where_params}
  end

  defp build_explicit_joins(selecto, pivot_config) do
    # Build explicit JOIN clauses from source to target
    source_alias = get_source_alias()
    
    Enum.reduce(pivot_config.join_path, {[], []}, fn join_name, {acc_clauses, acc_params} ->
      {join_clause, join_params, _} = build_single_join(selecto, join_name, source_alias)
      {acc_clauses ++ [join_clause], acc_params ++ join_params}
    end)
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
  defp get_final_join_alias([]), do: get_source_alias()
  defp get_final_join_alias(join_path) do
    # The final alias is the alias of the last join in the path
    join_name = List.last(join_path)
    generate_join_alias(join_name)
  end
  
  defp generate_join_alias(join_name) do
    "j_" <> to_string(join_name)
  end

  defp get_join_config(selecto, join_name) do
    case Map.get(selecto.config.joins, join_name) do
      nil -> raise ArgumentError, "Join #{join_name} not found in configuration"
      config -> config
    end
  end

  defp get_join_table(selecto, join_name) do
    # Get the association to find the target schema
    association = get_association_for_join(selecto, join_name)
    target_schema = association.queryable
    
    case Map.get(selecto.domain.schemas, target_schema) do
      nil -> raise ArgumentError, "Schema #{target_schema} not found for join #{join_name}"
      schema_config -> schema_config.source_table
    end
  end

  defp build_join_condition(selecto, join_name, current_alias, next_alias) do
    # Get association configuration to build ON clause
    association = get_association_for_join(selecto, join_name)
    
    owner_key = association.owner_key
    related_key = association.related_key
    
    on_clause = [current_alias, ".", to_string(owner_key), " = ", next_alias, ".", to_string(related_key)]
    {on_clause, []}
  end

  defp get_association_for_join(selecto, join_name) do
    # Navigate through domain configuration to find the association
    # This is a simplified version - may need refinement based on actual structure
    case Map.get(selecto.domain.source.associations, join_name) do
      nil -> 
        # Look in schemas
        Enum.find_value(selecto.domain.schemas, fn {_name, schema} ->
          Map.get(schema.associations, join_name)
        end) || raise ArgumentError, "Association #{join_name} not found"
      assoc -> assoc
    end
  end

  defp get_applicable_filters(selecto) do
    # Return filters that should be preserved in the pivot subquery
    selecto.set.filtered
  end

  defp build_filter_conditions(_selecto, [], _source_alias), do: {[], []}
  defp build_filter_conditions(_selecto, filters, source_alias) do
    # Build simple WHERE conditions for pivot subqueries
    if length(filters) == 0 do
      {[], []}
    else
      conditions = Enum.map(filters, fn {field, value} ->
        field_name = escape_identifier(to_string(field))
        [source_alias, ".", field_name, " = ", {:param, value}]
      end)
      
      where_clause = case conditions do
        [single] -> single
        multiple -> Enum.intersperse(multiple, [" AND "])
      end
      
      # Extract parameters
      params = Enum.map(filters, fn {_field, value} -> value end)
      
      {where_clause, params}
    end
  end

  defp get_connection_field(selecto, target_schema, join_path) do
    # Return the field that connects the final join to the target
    target_config = Map.get(selecto.domain.schemas, target_schema)
    if target_config do
      to_string(target_config.primary_key || :id)
    else
      "id"
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

  defp get_correlation_field(selecto, pivot_config) do
    # Return the field used for correlation in EXISTS subqueries
    # This should be the source table's primary key
    source_config = selecto.domain.source
    if source_config do
      to_string(source_config.primary_key || :id)
    else
      "id"
    end
  end

  defp build_reverse_joins(selecto, join_path, source_alias, target_alias) do
    # Build joins from source to target for correlation subquery
    {join_clauses, params, _} = 
      Enum.reduce(join_path, {[], [], source_alias}, fn join_name, {acc_clauses, acc_params, current_alias} ->
        {join_clause, join_params, next_alias} = build_single_join(selecto, join_name, current_alias)
        {acc_clauses ++ [join_clause], acc_params ++ join_params, next_alias}
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
      final_alias = case pivot_config.join_path do
        [] -> source_alias
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
  defp sql_join_type(_), do: "LEFT"  # Default

  defp escape_identifier(identifier) do
    # Escape SQL identifiers - simplified implementation
    "\"#{identifier}\""
  end
end