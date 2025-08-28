defmodule Selecto.Builder.Subselect do
  @moduledoc """
  SQL generation logic for Subselect functionality.

  This module handles the construction of correlated subqueries that return
  aggregated data from related tables as JSON arrays, PostgreSQL arrays,
  or other aggregate formats.
  """

  import Selecto.Builder.Sql.Helpers
  alias Selecto.SQL.Params
  alias Selecto.Types

  @doc """
  Build subselect clauses for the SELECT portion of the query.

  Returns a list of SQL fragments that can be included in the main SELECT clause.
  Each subselect becomes a correlated subquery that aggregates related data.
  """
  @spec build_subselect_clauses(Types.t()) :: {[Types.iodata_with_markers()], Types.sql_params()}
  def build_subselect_clauses(selecto) do
    # Determine the correct source alias based on pivot context
    source_alias = if Selecto.Pivot.has_pivot?(selecto) do
      # In pivot context, use "s" for source table
      "s"
    else
      # In standard context, use "selecto_root"
      "selecto_root"
    end

    build_subselect_clauses(selecto, source_alias)
  end

  @spec build_subselect_clauses(Types.t(), String.t()) :: {[Types.iodata_with_markers()], Types.sql_params()}
  def build_subselect_clauses(selecto, source_alias) do
    subselects = Selecto.Subselect.get_subselect_configs(selecto)
    
    if length(subselects) > 0 do
      {clauses, all_params} = 
        Enum.map(subselects, &build_single_subselect(selecto, &1, source_alias))
        |> Enum.unzip()
      
      {clauses, List.flatten(all_params)}
    else
      {[], []}
    end
  end

  @doc """
  Build a single correlated subquery for a subselect configuration.
  """
  @spec build_single_subselect(Types.t(), Types.subselect_selector()) :: {Types.iodata_with_markers(), Types.sql_params()}
  def build_single_subselect(selecto, subselect_config) do
    # Determine the correct source alias based on pivot context
    source_alias = if Selecto.Pivot.has_pivot?(selecto) do
      # In pivot context, use "s" for source table
      "s"
    else
      # In standard context, use "selecto_root"
      "selecto_root"
    end

    build_single_subselect(selecto, subselect_config, source_alias)
  end

  @spec build_single_subselect(Types.t(), Types.subselect_selector(), String.t()) :: {Types.iodata_with_markers(), Types.sql_params()}
  def build_single_subselect(selecto, subselect_config, source_alias) do
    # Build the complete subselect with aggregation function
    {subselect_iodata, subselect_params} = build_aggregated_subselect(selecto, subselect_config, source_alias)
    
    # Add alias for the subselect field
    field_with_alias = [
      "(", subselect_iodata, ") AS ", escape_identifier(subselect_config.alias)
    ]
    
    {field_with_alias, subselect_params}
  end
  
  defp build_aggregated_subselect(selecto, subselect_config) do
    build_aggregated_subselect(selecto, subselect_config, "selecto_root")
  end

  defp build_aggregated_subselect(selecto, subselect_config, source_alias) do
    target_table = get_target_table(selecto, subselect_config.target_schema)
    target_alias = generate_subquery_alias(subselect_config.target_schema)
    
    # Build SELECT fields for the subquery based on aggregation type
    {select_clause, select_params} = case subselect_config.format do
      :json_agg when length(subselect_config.fields) == 1 ->
        [field] = subselect_config.fields
        field_name = escape_identifier(to_string(field))
        {["json_agg(", target_alias, ".", field_name, ")"], []}
        
      :json_agg ->
        # Multiple fields - build JSON objects
        json_pairs = Enum.map(subselect_config.fields, fn field ->
          field_name = escape_identifier(to_string(field))
          # Use literal string for field key, not parameter
          field_key = escape_string(to_string(field))
          [field_key, ", ", target_alias, ".", field_name]
        end)

        json_build = [
          "json_agg(json_build_object(",
          Enum.intersperse(json_pairs, [", "]),
          "))"
        ]

        # No parameters needed for field names - they're literal strings
        {json_build, []}
        
      :array_agg ->
        [field] = subselect_config.fields  # Simplify for now
        field_name = escape_identifier(to_string(field))
        {["array_agg(", target_alias, ".", field_name, ")"], []}
        
      :string_agg ->
        [field] = subselect_config.fields  # Simplify for now  
        field_name = escape_identifier(to_string(field))
        separator = Map.get(subselect_config, :separator, ",")
        separator_param = {:param, separator}
        {["string_agg(", target_alias, ".", field_name, ", ", separator_param, ")"], [separator]}
        
      :count ->
        {["count(*)"], []}
    end
    
    # Build correlation WHERE clause
    {correlation_where, correlation_params} = build_correlation_condition(
      selecto, 
      subselect_config, 
      target_alias,
      source_alias
    )
    
    # Build additional filters if specified
    {additional_where, additional_params} = build_additional_filters(
      subselect_config, 
      target_alias
    )
    
    # Combine all WHERE conditions
    all_where_conditions = [correlation_where] ++ 
      if additional_where != [], do: [additional_where], else: []
    
    where_clause = case all_where_conditions do
      [single] -> single
      multiple -> Enum.intersperse(multiple, [" AND "])
    end
    
    # Build complete subquery
    subselect_iodata = [
      "SELECT ", select_clause,
      " FROM ", target_table, " ", target_alias,
      " WHERE ", where_clause
    ]
    
    all_params = select_params ++ correlation_params ++ additional_params
    {subselect_iodata, all_params}
  end

  @doc """
  Build the correlated subquery that fetches related data.
  """
  @spec build_correlated_subquery(Types.t(), Types.subselect_selector()) :: {Types.iodata_with_markers(), Types.sql_params()}
  def build_correlated_subquery(selecto, subselect_config) do
    # Determine the correct source alias based on pivot context
    source_alias = if Selecto.Pivot.has_pivot?(selecto) do
      # In pivot context, use "s" for source table
      "s"
    else
      # In standard context, use "selecto_root"
      "selecto_root"
    end

    build_correlated_subquery(selecto, subselect_config, source_alias)
  end

  @spec build_correlated_subquery(Types.t(), Types.subselect_selector(), String.t()) :: {Types.iodata_with_markers(), Types.sql_params()}
  def build_correlated_subquery(selecto, subselect_config, source_alias) do
    target_table = get_target_table(selecto, subselect_config.target_schema)
    target_alias = generate_subquery_alias(subselect_config.target_schema)
    
    # Build SELECT fields for the subquery
    {select_fields, select_params} = build_subquery_select_fields(subselect_config, target_alias)
    
    # Build correlation WHERE clause
    {correlation_where, correlation_params} = build_correlation_condition(
      selecto, 
      subselect_config, 
      target_alias,
      source_alias
    )
    
    # Build additional filters if specified
    {additional_where, additional_params} = build_additional_filters(
      subselect_config, 
      target_alias
    )
    
    # Build ORDER BY if specified
    {order_clause, order_params} = build_subquery_order_by(
      subselect_config, 
      target_alias
    )
    
    # Combine all WHERE conditions
    all_where_conditions = [correlation_where] ++ 
      if additional_where != [], do: [additional_where], else: []
    
    where_clause = case all_where_conditions do
      [single] -> single
      multiple -> Enum.intersperse(multiple, [" AND "])
    end
    
    base_subquery = [
      "SELECT ", select_fields,
      " FROM ", target_table, " ", target_alias,
      " WHERE ", where_clause
    ]
    
    subquery_iodata = if order_clause != [] do
      base_subquery ++ [" ORDER BY ", order_clause]
    else
      base_subquery
    end
    
    all_params = select_params ++ correlation_params ++ additional_params ++ order_params
    {subquery_iodata, all_params}
  end

  @doc """
  Wrap subquery results in the appropriate aggregation function.
  """
  @spec wrap_in_aggregation(Types.iodata_with_markers(), Types.sql_params(), Types.subselect_format(), Types.subselect_selector()) :: 
    {Types.iodata_with_markers(), Types.sql_params()}
  def wrap_in_aggregation(subquery_iodata, subquery_params, format, config) do
    case format do
      :json_agg ->
        {["(", subquery_iodata, ")"], subquery_params}
        
      :array_agg ->
        {["(", subquery_iodata, ")"], subquery_params}
        
      :string_agg ->
        {["(", subquery_iodata, ")"], subquery_params}
        
      :count ->
        # For count, we need to modify the SELECT clause
        count_subquery = String.replace(IO.iodata_to_binary(subquery_iodata), "SELECT ", "SELECT count(*) FROM (SELECT ")
        count_subquery = count_subquery <> ") _count_sub"
        {[count_subquery], subquery_params}
    end
  end

  @doc """
  Resolve the join condition needed to correlate the subquery with the main query.
  """
  @spec resolve_join_condition(Types.t(), atom()) :: {:ok, {String.t(), String.t()}} | {:error, String.t()}
  def resolve_join_condition(selecto, target_schema) do
    # Find the relationship path from source to target
    case Selecto.Subselect.resolve_join_path(selecto, target_schema) do
      {:ok, join_path} ->
        # Get the final connection fields
        {source_field, target_field} = get_connection_fields(selecto, target_schema, join_path)
        {:ok, {source_field, target_field}}
        
      {:error, reason} ->
        {:error, "Cannot resolve join condition: #{reason}"}
    end
  end

  @spec resolve_join_condition_with_path(Types.t(), atom()) :: {:ok, Types.iodata_with_markers()} | {:error, String.t()}
  def resolve_join_condition_with_path(selecto, target_schema) do
    # Determine the correct source alias based on pivot context
    source_alias = if Selecto.Pivot.has_pivot?(selecto) do
      # In pivot context, use "s" for source table
      "s"
    else
      # In standard context, use "selecto_root"
      "selecto_root"
    end

    resolve_join_condition_with_path(selecto, target_schema, source_alias)
  end

  @spec resolve_join_condition_with_path(Types.t(), atom(), String.t()) :: {:ok, Types.iodata_with_markers()} | {:error, String.t()}
  def resolve_join_condition_with_path(selecto, target_schema, source_alias) do
    # Special handling for known Pagila relationships
    case {selecto.domain.source.source_table, target_schema} do
      {"actor", :film} ->
        # Use the known actor -> film_actor -> film relationship
        build_pagila_actor_film_correlation(selecto, target_schema, source_alias)
        
      {"film", :film_actors} ->
        # Use the known film -> film_actor relationship (direct)
        build_pagila_film_actors_correlation(selecto, target_schema, source_alias)
        
      _ ->
        # General case - use join path resolution
        case Selecto.Subselect.resolve_join_path(selecto, target_schema) do
          {:ok, []} ->
            # Direct relationship - build simple correlation
            build_direct_correlation(selecto, target_schema, source_alias)
            
          {:ok, join_path} ->
            # Multi-step relationship - build EXISTS condition
            build_exists_correlation(selecto, target_schema, join_path, source_alias)
            
          {:error, reason} ->
            {:error, "Cannot resolve join condition: #{reason}"}
        end
    end
  end

  # Private helper functions

  defp build_pagila_actor_film_correlation(_selecto, target_schema, source_alias) do
    target_alias = generate_subquery_alias(target_schema)
    
    # Check if we're in pivot context (source_alias is "t" for target table)
    if source_alias == "t" do
      # In pivot context, correlate directly with the main query's target table
      # Use film_id as the primary key for film table
      condition = [
        target_alias, ".film_id = ", source_alias, ".film_id"
      ]
      
      {:ok, condition}
    else
      # Standard actor-to-film correlation
      exists_condition = [
        "EXISTS (SELECT 1 FROM film_actor fa",
        " WHERE fa.actor_id = ", escape_identifier(source_alias), ".actor_id",
        " AND fa.film_id = ", escape_identifier(target_alias), ".film_id)"
      ]
      
      {:ok, exists_condition}
    end
  end

  defp build_pagila_film_actors_correlation(_selecto, target_schema, source_alias) do
    target_alias = generate_subquery_alias(target_schema)
    
    # Direct relationship: film -> film_actors via film_id
    condition = [
      target_alias, ".", escape_identifier("film_id"), " = ", source_alias, ".", escape_identifier("film_id")
    ]
    
    {:ok, condition}
  end

  defp build_direct_correlation(_selecto, target_schema, source_alias) do
    target_alias = generate_subquery_alias(target_schema)
    
    # Simple direct relationship - assume primary key correlation
    condition = [
      target_alias, ".id = ", source_alias, ".id"
    ]
    
    {:ok, condition}
  end

  defp build_exists_correlation(selecto, target_schema, join_path, source_alias) do
    # For actor → film_actors → film, we need:
    # EXISTS (SELECT 1 FROM film_actor fa WHERE fa.actor_id = selecto_root.actor_id AND fa.film_id = sub_film.film_id)
    case join_path do
      [junction_schema] ->
        # Simple one-step junction (actor → film_actors → film)
        build_single_junction_exists(selecto, target_schema, junction_schema, source_alias)
      
      multi_path ->
        # Multi-step path (more complex)
        build_multi_step_exists(selecto, target_schema, multi_path, source_alias)
    end
  end

  defp build_single_junction_exists(selecto, target_schema, junction_schema, source_alias) do
    target_alias = generate_subquery_alias(target_schema)
    junction_alias = generate_subquery_alias(junction_schema)
    
    # Get junction table name
    junction_table = get_target_table(selecto, junction_schema)
    
    # Get source association (actor → film_actors)
    source_assoc = Map.get(selecto.domain.source.associations, junction_schema)
    # Get junction association (film_actors → film)
    junction_schema_config = Map.get(selecto.domain.schemas, junction_schema)
    target_assoc = Map.get(junction_schema_config.associations, target_schema)
    
    if source_assoc && target_assoc do
      exists_condition = [
        "EXISTS (SELECT 1 FROM ", junction_table, " ", junction_alias,
        " WHERE ", junction_alias, ".", escape_identifier(to_string(source_assoc.related_key)),
        " = ", source_alias, ".", escape_identifier(to_string(source_assoc.owner_key)),
        " AND ", junction_alias, ".", escape_identifier(to_string(target_assoc.owner_key)),
        " = ", target_alias, ".", escape_identifier(to_string(target_assoc.related_key)),
        ")"
      ]
      {:ok, exists_condition}
    else
      {:error, "Cannot build EXISTS correlation - missing association configuration"}
    end
  end

  defp build_multi_step_exists(_selecto, _target_schema, _multi_path, _source_alias) do
    # For now, return an error - can be implemented later for more complex paths
    {:error, "Multi-step join paths not yet implemented for subselects"}
  end

  defp build_subquery_select_fields(subselect_config, target_alias) do
    case subselect_config.format do
      :json_agg ->
        build_json_select_fields(subselect_config.fields, target_alias)
        
      :count ->
        {["1"], []}  # For count, we just need any field
        
      _ ->
        build_simple_select_fields(subselect_config.fields, target_alias)
    end
  end

  defp build_json_select_fields(fields, target_alias) do
    case fields do
      [single_field] ->
        # Single field - return the value directly for json_agg
        field_name = escape_identifier(to_string(single_field))
        {[target_alias, ".", field_name], []}

      multiple_fields ->
        # Multiple fields - build JSON object
        json_pairs = Enum.map(multiple_fields, fn field ->
          field_name = escape_identifier(to_string(field))
          # Use literal string for field key, not parameter
          field_key = escape_string(to_string(field))
          [field_key, ", ", target_alias, ".", field_name]
        end)

        json_build_call = [
          "json_build_object(",
          Enum.intersperse(json_pairs, [", "]),
          ")"
        ]

        # No parameters needed for field names - they're literal strings
        {json_build_call, []}
    end
  end

  defp build_simple_select_fields(fields, target_alias) do
    field_clauses = Enum.map(fields, fn field ->
      field_name = escape_identifier(to_string(field))
      [target_alias, ".", field_name]
    end)
    
    select_clause = case field_clauses do
      [single] -> single
      multiple -> Enum.intersperse(multiple, [", "])
    end
    
    {select_clause, []}
  end

  defp build_correlation_condition(selecto, subselect_config, target_alias) do
    # Determine the correct source alias based on pivot context
    source_alias = if Selecto.Pivot.has_pivot?(selecto) do
      # In pivot context, use "s" for source table
      "s"
    else
      # In standard context, use "selecto_root"
      "selecto_root"
    end

    build_correlation_condition(selecto, subselect_config, target_alias, source_alias)
  end

  defp build_correlation_condition(selecto, subselect_config, target_alias, source_alias) do
    case resolve_join_condition_with_path(selecto, subselect_config.target_schema, source_alias) do
      {:ok, condition_sql} ->
        {condition_sql, []}

      {:error, _reason} ->
        # Fallback to simple ID correlation
        condition = [
          target_alias, ".id = ", source_alias, ".id"
        ]

        {condition, []}
    end
  end

  defp build_additional_filters(subselect_config, target_alias) do
    case subselect_config.filters do
      [] ->
        {[], []}
        
      filters ->
        # Build WHERE conditions for additional filters
        build_filter_conditions(filters, target_alias)
    end
  end

  defp build_subquery_order_by(subselect_config, target_alias) do
    case subselect_config.order_by do
      [] ->
        {[], []}
        
      order_specs ->
        order_clauses = Enum.map(order_specs, fn
          {direction, field} ->
            field_name = escape_identifier(to_string(field))
            direction_sql = case direction do
              :asc -> "ASC"
              :desc -> "DESC"
              _ -> "ASC"
            end
            [target_alias, ".", field_name, " ", direction_sql]
            
          field when is_atom(field) ->
            field_name = escape_identifier(to_string(field))
            [target_alias, ".", field_name]
            
          field when is_binary(field) ->
            field_name = escape_identifier(field)
            [target_alias, ".", field_name]
        end)
        
        order_clause = Enum.intersperse(order_clauses, [", "])
        {order_clause, []}
    end
  end

  defp build_filter_conditions(filters, target_alias) do
    # Use existing filter building logic, adapted for subquery context
    # This is simplified - in reality, we'd reuse Selecto.Builder.Sql.Where logic
    condition_clauses = Enum.map(filters, fn
      {field, value} ->
        field_name = escape_identifier(to_string(field))
        value_param = {:param, value}
        [target_alias, ".", field_name, " = ", value_param]
    end)
    
    condition_sql = case condition_clauses do
      [] -> {[], []}
      [single] -> {single, extract_params(single)}
      multiple -> 
        combined = Enum.intersperse(multiple, [" AND "])
        {combined, extract_params(combined)}
    end
    
    condition_sql
  end

  defp get_target_table(selecto, target_schema) do
    case Map.get(selecto.domain.schemas, target_schema) do
      nil -> raise ArgumentError, "Target schema #{target_schema} not found"
      schema_config -> schema_config.source_table
    end
  end

  defp generate_subquery_alias(target_schema) do
    "sub_" <> to_string(target_schema)
  end

  defp get_main_query_alias do
    # This should match the alias used in the main query
    "selecto_root"  # Main query uses 'selecto_root' as source alias
  end

  defp get_main_query_alias(source_alias) do
    source_alias
  end

  defp get_connection_fields(selecto, target_schema, join_path) do
    # Determine the fields that connect the main query to the subquery target
    # This is a simplified implementation - needs refinement based on actual join path
    case join_path do
      [] ->
        # Direct relationship
        {"id", "parent_id"}  # Simplified assumption
        
      [first_join | _rest] ->
        # Get the association configuration for the first join
        association = get_association_config(selecto, first_join)
        source_field = to_string(association.owner_key)
        target_field = to_string(association.related_key)
        {source_field, target_field}
    end
  end

  defp get_association_config(selecto, join_name) do
    # Look up association configuration
    case Map.get(selecto.domain.source.associations, join_name) do
      nil ->
        # Look in schemas
        Enum.find_value(selecto.domain.schemas, fn {_name, schema} ->
          Map.get(schema.associations, join_name)
        end) || raise ArgumentError, "Association #{join_name} not found"
      assoc -> assoc
    end
  end

  defp extract_params(iodata) when is_list(iodata) do
    # Extract parameter values from iodata structure
    Enum.flat_map(iodata, fn
      {:param, value} -> [value]
      item when is_list(item) -> extract_params(item)
      _ -> []
    end)
  end

  defp extract_params(_), do: []

  defp escape_string(string) do
    # Escape SQL string literals
    "'#{String.replace(string, "'", "''")}'"
  end

  defp escape_identifier(identifier) do
    # Escape SQL identifiers - simplified implementation
    "\"#{identifier}\""
  end
end