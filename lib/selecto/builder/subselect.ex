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
    subselects = Selecto.Subselect.get_subselect_configs(selecto)
    
    if length(subselects) > 0 do
      {clauses, all_params} = 
        Enum.map(subselects, &build_single_subselect(selecto, &1))
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
    {subquery_iodata, subquery_params} = build_correlated_subquery(selecto, subselect_config)
    
    # Wrap in aggregation function
    {aggregation_iodata, aggregation_params} = wrap_in_aggregation(
      subquery_iodata, 
      subquery_params,
      subselect_config.format,
      subselect_config
    )
    
    # Add alias for the subselect field
    field_with_alias = [
      "(", aggregation_iodata, ") AS ", escape_identifier(subselect_config.alias)
    ]
    
    {field_with_alias, aggregation_params}
  end

  @doc """
  Build the correlated subquery that fetches related data.
  """
  @spec build_correlated_subquery(Types.t(), Types.subselect_selector()) :: {Types.iodata_with_markers(), Types.sql_params()}
  def build_correlated_subquery(selecto, subselect_config) do
    target_table = get_target_table(selecto, subselect_config.target_schema)
    target_alias = generate_subquery_alias(subselect_config.target_schema)
    
    # Build SELECT fields for the subquery
    {select_fields, select_params} = build_subquery_select_fields(subselect_config, target_alias)
    
    # Build correlation WHERE clause
    {correlation_where, correlation_params} = build_correlation_condition(
      selecto, 
      subselect_config, 
      target_alias
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
    
    subquery_iodata = [
      "SELECT ", select_fields,
      " FROM ", target_table, " ", target_alias,
      " WHERE ", where_clause
    ]
    
    subquery_iodata = if order_clause != [] do
      subquery_iodata ++ [" ORDER BY ", order_clause]
    else
      subquery_iodata
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
        {["json_agg(", subquery_iodata, ")"], subquery_params}
        
      :array_agg ->
        if length(config.fields) == 1 do
          # Single field - simple array_agg
          {["array_agg(", subquery_iodata, ")"], subquery_params}
        else
          # Multiple fields - array of ROW types
          {["array_agg(ROW(", subquery_iodata, "))"], subquery_params}
        end
        
      :string_agg ->
        separator = Map.get(config, :separator, ",")
        separator_param = {:param, separator}
        
        if length(config.fields) == 1 do
          {["string_agg(", subquery_iodata, ", ", separator_param, ")"], subquery_params ++ [separator]}
        else
          # Multiple fields - concatenate them first
          {["string_agg(", subquery_iodata, ", ", separator_param, ")"], subquery_params ++ [separator]}
        end
        
      :count ->
        {["(SELECT count(*) FROM (", subquery_iodata, ") _count_sub)"], subquery_params}
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

  # Private helper functions

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
        field_name = escape_identifier(Atom.to_string(single_field))
        {[target_alias, ".", field_name], []}
        
      multiple_fields ->
        # Multiple fields - build JSON object
        json_pairs = Enum.map(multiple_fields, fn field ->
          field_name = escape_identifier(Atom.to_string(field))
          field_key = {:param, Atom.to_string(field)}
          [field_key, ", ", target_alias, ".", field_name]
        end)
        
        json_build_call = [
          "json_build_object(", 
          Enum.intersperse(json_pairs, [", "]), 
          ")"
        ]
        
        field_names = Enum.map(multiple_fields, &Atom.to_string/1)
        {json_build_call, field_names}
    end
  end

  defp build_simple_select_fields(fields, target_alias) do
    field_clauses = Enum.map(fields, fn field ->
      field_name = escape_identifier(Atom.to_string(field))
      [target_alias, ".", field_name]
    end)
    
    select_clause = case field_clauses do
      [single] -> single
      multiple -> Enum.intersperse(multiple, [", "])
    end
    
    {select_clause, []}
  end

  defp build_correlation_condition(selecto, subselect_config, target_alias) do
    case resolve_join_condition(selecto, subselect_config.target_schema) do
      {:ok, {source_field, target_field}} ->
        source_alias = get_main_query_alias()
        
        condition = [
          target_alias, ".", escape_identifier(target_field), 
          " = ", 
          source_alias, ".", escape_identifier(source_field)
        ]
        
        {condition, []}
        
      {:error, _reason} ->
        # Fallback to simple ID correlation
        condition = [
          target_alias, ".id = ", get_main_query_alias(), ".id"
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
            field_name = escape_identifier(Atom.to_string(field))
            direction_sql = case direction do
              :asc -> "ASC"
              :desc -> "DESC"
              _ -> "ASC"
            end
            [target_alias, ".", field_name, " ", direction_sql]
            
          field when is_atom(field) ->
            field_name = escape_identifier(Atom.to_string(field))
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
        field_name = escape_identifier(Atom.to_string(field))
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
    "sub_" <> Atom.to_string(target_schema)
  end

  defp get_main_query_alias do
    # This should match the alias used in the main query
    "s"  # Assuming main query uses 's' as source alias
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
        source_field = Atom.to_string(association.owner_key)
        target_field = Atom.to_string(association.related_key)
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

  defp escape_identifier(identifier) do
    # Escape SQL identifiers - simplified implementation
    "\"#{identifier}\""
  end
end