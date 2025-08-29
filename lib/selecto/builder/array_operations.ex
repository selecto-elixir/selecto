defmodule Selecto.Builder.ArrayOperations do
  @moduledoc """
  SQL builder for PostgreSQL array operations.
  
  Generates SQL for array aggregation, manipulation, testing, and unnest operations.
  Handles proper parameter binding and escaping for safe SQL generation.
  """
  
  alias Selecto.Advanced.ArrayOperations.Spec
  
  @doc """
  Build SQL for an array operation.
  """
  def build_array_sql(%Spec{} = spec, params_list) do
    case spec.operation do
      # Aggregation operations
      op when op in [:array_agg, :array_agg_distinct] ->
        build_array_agg_sql(spec, params_list)
        
      :string_agg ->
        build_string_agg_sql(spec, params_list)
        
      # Testing operations
      op when op in [:array_contains, :array_contained, :array_overlap, :array_eq] ->
        build_array_test_sql(spec, params_list)
        
      # Size operations
      op when op in [:array_length, :cardinality, :array_ndims, :array_dims] ->
        build_array_size_sql(spec, params_list)
        
      # Construction operations
      op when op in [:array, :array_fill, :array_append, :array_prepend, :array_cat] ->
        build_array_construct_sql(spec, params_list)
        
      # Element operations
      op when op in [:array_position, :array_positions, :array_remove, :array_replace] ->
        build_array_element_sql(spec, params_list)
        
      # Transformation operations
      :unnest ->
        build_unnest_sql(spec, params_list)
        
      op when op in [:array_to_string, :string_to_array] ->
        build_array_transform_sql(spec, params_list)
        
      # Set operations
      op when op in [:array_union, :array_intersect, :array_except] ->
        build_array_set_sql(spec, params_list)
        
      _ ->
        raise "Unsupported array operation: #{spec.operation}"
    end
  end
  
  # Array aggregation operations
  defp build_array_agg_sql(%Spec{operation: op} = spec, params_list) do
    distinct = if op == :array_agg_distinct or spec.distinct, do: "DISTINCT ", else: ""
    column_sql = build_column_reference(spec.column)
    
    # Build ORDER BY clause if present
    order_clause = if spec.order_by do
      order_parts = Enum.map(spec.order_by, fn
        {col, dir} -> "#{col} #{String.upcase(to_string(dir))}"
        col -> col
      end)
      " ORDER BY #{Enum.join(order_parts, ", ")}"
    else
      ""
    end
    
    sql = "ARRAY_AGG(#{distinct}#{column_sql}#{order_clause})"
    
    # Add alias if present
    sql = if spec.alias, do: "#{sql} AS #{spec.alias}", else: sql
    
    {sql, params_list}
  end
  
  defp build_string_agg_sql(%Spec{} = spec, params_list) do
    column_sql = build_column_reference(spec.column)
    
    # Get delimiter from options or use default
    delimiter = spec.options[:delimiter] || ","
    {delimiter_param, params_list} = add_param(delimiter, params_list)
    
    # Build ORDER BY clause if present
    order_clause = if spec.order_by do
      order_parts = Enum.map(spec.order_by, fn
        {col, dir} -> "#{col} #{String.upcase(to_string(dir))}"
        col -> col
      end)
      " ORDER BY #{Enum.join(order_parts, ", ")}"
    else
      ""
    end
    
    sql = "STRING_AGG(#{column_sql}, #{delimiter_param}#{order_clause})"
    
    # Add alias if present
    sql = if spec.alias, do: "#{sql} AS #{spec.alias}", else: sql
    
    {sql, params_list}
  end
  
  # Array testing operations
  defp build_array_test_sql(%Spec{} = spec, params_list) do
    column_sql = build_column_reference(spec.column)
    {value_param, params_list} = format_array_value(spec.value, params_list)
    
    operator = case spec.operation do
      :array_contains -> "@>"
      :array_contained -> "<@"
      :array_overlap -> "&&"
      :array_eq -> "="
    end
    
    sql = "#{column_sql} #{operator} #{value_param}"
    {sql, params_list}
  end
  
  # Array size operations
  defp build_array_size_sql(%Spec{operation: :array_length} = spec, params_list) do
    column_sql = build_column_reference(spec.column)
    sql = "ARRAY_LENGTH(#{column_sql}, #{spec.dimension})"
    
    # Add alias if present
    sql = if spec.alias, do: "#{sql} AS #{spec.alias}", else: sql
    
    {sql, params_list}
  end
  
  defp build_array_size_sql(%Spec{operation: :cardinality} = spec, params_list) do
    column_sql = build_column_reference(spec.column)
    sql = "CARDINALITY(#{column_sql})"
    
    # Add alias if present
    sql = if spec.alias, do: "#{sql} AS #{spec.alias}", else: sql
    
    {sql, params_list}
  end
  
  defp build_array_size_sql(%Spec{operation: op} = spec, params_list) when op in [:array_ndims, :array_dims] do
    column_sql = build_column_reference(spec.column)
    func_name = String.upcase(to_string(op))
    sql = "#{func_name}(#{column_sql})"
    
    # Add alias if present
    sql = if spec.alias, do: "#{sql} AS #{spec.alias}", else: sql
    
    {sql, params_list}
  end
  
  # Array construction operations
  defp build_array_construct_sql(%Spec{operation: :array} = spec, params_list) do
    # spec.value should contain the array elements
    {array_sql, params_list} = format_array_literal(spec.value, params_list)
    
    sql = "ARRAY[#{array_sql}]"
    sql = if spec.alias, do: "#{sql} AS #{spec.alias}", else: sql
    
    {sql, params_list}
  end
  
  defp build_array_construct_sql(%Spec{operation: :array_append} = spec, params_list) do
    column_sql = build_column_reference(spec.column)
    {value_param, params_list} = add_param(spec.value, params_list)
    
    sql = "ARRAY_APPEND(#{column_sql}, #{value_param})"
    sql = if spec.alias, do: "#{sql} AS #{spec.alias}", else: sql
    
    {sql, params_list}
  end
  
  defp build_array_construct_sql(%Spec{operation: :array_prepend} = spec, params_list) do
    {value_param, params_list} = add_param(spec.value, params_list)
    column_sql = build_column_reference(spec.column)
    
    sql = "ARRAY_PREPEND(#{value_param}, #{column_sql})"
    sql = if spec.alias, do: "#{sql} AS #{spec.alias}", else: sql
    
    {sql, params_list}
  end
  
  defp build_array_construct_sql(%Spec{operation: :array_cat} = spec, params_list) do
    column_sql = build_column_reference(spec.column)
    {value_param, params_list} = format_array_value(spec.value, params_list)
    
    sql = "ARRAY_CAT(#{column_sql}, #{value_param})"
    sql = if spec.alias, do: "#{sql} AS #{spec.alias}", else: sql
    
    {sql, params_list}
  end
  
  defp build_array_construct_sql(%Spec{operation: :array_fill} = spec, params_list) do
    {value_param, params_list} = add_param(spec.value, params_list)
    {dims_param, params_list} = format_array_value(spec.options[:dimensions], params_list)
    
    sql = "ARRAY_FILL(#{value_param}, #{dims_param})"
    sql = if spec.alias, do: "#{sql} AS #{spec.alias}", else: sql
    
    {sql, params_list}
  end
  
  # Array element operations
  defp build_array_element_sql(%Spec{operation: op} = spec, params_list) when op in [:array_position, :array_positions] do
    column_sql = build_column_reference(spec.column)
    {value_param, params_list} = add_param(spec.value, params_list)
    
    func_name = String.upcase(to_string(op))
    sql = "#{func_name}(#{column_sql}, #{value_param})"
    sql = if spec.alias, do: "#{sql} AS #{spec.alias}", else: sql
    
    {sql, params_list}
  end
  
  defp build_array_element_sql(%Spec{operation: :array_remove} = spec, params_list) do
    column_sql = build_column_reference(spec.column)
    {value_param, params_list} = add_param(spec.value, params_list)
    
    sql = "ARRAY_REMOVE(#{column_sql}, #{value_param})"
    sql = if spec.alias, do: "#{sql} AS #{spec.alias}", else: sql
    
    {sql, params_list}
  end
  
  defp build_array_element_sql(%Spec{operation: :array_replace} = spec, params_list) do
    column_sql = build_column_reference(spec.column)
    {old_param, params_list} = add_param(spec.value, params_list)
    {new_param, params_list} = add_param(spec.options[:new_value], params_list)
    
    sql = "ARRAY_REPLACE(#{column_sql}, #{old_param}, #{new_param})"
    sql = if spec.alias, do: "#{sql} AS #{spec.alias}", else: sql
    
    {sql, params_list}
  end
  
  # Unnest operation
  defp build_unnest_sql(%Spec{operation: :unnest} = spec, params_list) do
    column_sql = build_column_reference(spec.column)
    
    sql = if spec.options[:with_ordinality] do
      "UNNEST(#{column_sql}) WITH ORDINALITY"
    else
      "UNNEST(#{column_sql})"
    end
    
    # Add alias if present
    sql = if spec.alias do
      if spec.options[:with_ordinality] do
        "#{sql} AS #{spec.alias}(value, ordinality)"
      else
        "#{sql} AS #{spec.alias}"
      end
    else
      sql
    end
    
    {sql, params_list}
  end
  
  # Array transformation operations
  defp build_array_transform_sql(%Spec{operation: :array_to_string} = spec, params_list) do
    column_sql = build_column_reference(spec.column)
    delimiter = spec.value || ","
    {delimiter_param, params_list} = add_param(delimiter, params_list)
    
    sql = if spec.options[:null_string] do
      {null_param, params_list} = add_param(spec.options[:null_string], params_list)
      "ARRAY_TO_STRING(#{column_sql}, #{delimiter_param}, #{null_param})"
    else
      "ARRAY_TO_STRING(#{column_sql}, #{delimiter_param})"
    end
    
    sql = if spec.alias, do: "#{sql} AS #{spec.alias}", else: sql
    {sql, params_list}
  end
  
  defp build_array_transform_sql(%Spec{operation: :string_to_array} = spec, params_list) do
    column_sql = build_column_reference(spec.column)
    delimiter = spec.value || ","
    {delimiter_param, params_list} = add_param(delimiter, params_list)
    
    sql = if spec.options[:null_string] do
      {null_param, params_list} = add_param(spec.options[:null_string], params_list)
      "STRING_TO_ARRAY(#{column_sql}, #{delimiter_param}, #{null_param})"
    else
      "STRING_TO_ARRAY(#{column_sql}, #{delimiter_param})"
    end
    
    sql = if spec.alias, do: "#{sql} AS #{spec.alias}", else: sql
    {sql, params_list}
  end
  
  # Array set operations (PostgreSQL 14+)
  defp build_array_set_sql(%Spec{operation: op} = spec, params_list) do
    column_sql = build_column_reference(spec.column)
    {value_param, params_list} = format_array_value(spec.value, params_list)
    
    func_name = case op do
      :array_union -> "ARRAY_UNION"
      :array_intersect -> "ARRAY_INTERSECT"
      :array_except -> "ARRAY_EXCEPT"
    end
    
    sql = "#{func_name}(#{column_sql}, #{value_param})"
    sql = if spec.alias, do: "#{sql} AS #{spec.alias}", else: sql
    
    {sql, params_list}
  end
  
  # Helper functions
  
  defp add_param(value, params_list) do
    param_num = length(params_list) + 1
    {"$#{param_num}", params_list ++ [value]}
  end
  
  defp build_column_reference(column) when is_binary(column), do: column
  
  defp build_column_reference({:array_agg, column}) do
    "ARRAY_AGG(#{column})"
  end
  
  defp build_column_reference(column) when is_tuple(column) do
    # Handle nested operations
    Tuple.to_list(column) |> Enum.join(".")
  end
  
  defp format_array_value(value, params_list) when is_list(value) do
    # Convert Elixir list to PostgreSQL array format
    {param, params_list} = add_param(value, params_list)
    {param, params_list}
  end
  
  defp format_array_value(value, params_list) when is_binary(value) do
    # Already a string reference (like column name)
    {value, params_list}
  end
  
  defp format_array_value(value, params_list) do
    add_param(value, params_list)
  end
  
  defp format_array_literal(elements, params_list) when is_list(elements) do
    {element_params, params_list} = 
      Enum.reduce(elements, {[], params_list}, fn elem, {acc, params} ->
        {param, new_params} = add_param(elem, params)
        {acc ++ [param], new_params}
      end)
    
    {Enum.join(element_params, ", "), params_list}
  end
  
  defp format_array_literal(elements, params_list) do
    format_array_value(elements, params_list)
  end
end