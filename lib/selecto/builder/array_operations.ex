defmodule Selecto.Builder.ArrayOperations do
  @moduledoc """
  SQL builder for PostgreSQL array operations.
  
  Generates SQL for array aggregation, manipulation, testing, and unnest operations.
  Handles proper parameter binding and escaping for safe SQL generation.
  """
  
  alias Selecto.Advanced.ArrayOperations.Spec
  import Selecto.Builder.Sql.Helpers

  # Helper to build properly quoted column references
  defp build_column_reference(column, selecto) when is_binary(column) do
    # Parse qualified column name (e.g., "selecto_root.field" or "field")
    case String.split(column, ".", parts: 2) do
      [table, field] when selecto != nil ->
        # Qualified column with selecto context - quote both parts
        quote_char = get_quote_char(selecto)
        "#{quote_char}#{table}#{quote_char}.#{quote_char}#{field}#{quote_char}"

      [field] when selecto != nil ->
        # Unqualified field with selecto context - add source table alias with quotes
        quote_char = get_quote_char(selecto)
        "#{quote_char}selecto_root#{quote_char}.#{quote_char}#{field}#{quote_char}"

      [table, field] ->
        # Qualified column without selecto (unit test) - use as-is
        "#{table}.#{field}"

      [field] ->
        # Unqualified field without selecto (unit test) - use as-is
        field
    end
  end

  defp build_column_reference(column, selecto) when is_atom(column) do
    build_column_reference(Atom.to_string(column), selecto)
  end

  defp build_column_reference({:field, field}, selecto) do
    build_column_reference(field, selecto)
  end

  # Handle nested array operations (e.g., {:array_agg, "film_id"})
  defp build_column_reference({op, column}, selecto) when is_atom(op) do
    # This is a nested array operation - build it recursively
    spec = %Spec{
      id: "nested_#{op}",
      operation: op,
      column: column,
      validated: true
    }
    {iodata, _params} = build_array_sql(spec, [], selecto)
    # Convert iodata to string for embedding
    IO.iodata_to_binary(iodata)
  end

  defp build_column_reference(column, _selecto) do
    # Fallback for other types
    to_string(column)
  end

  @doc """
  Build SQL for an array operation.
  Returns iodata with parameter markers instead of SQL strings.
  """
  def build_array_sql(%Spec{} = spec, params_list, selecto \\ nil) do
    case spec.operation do
      # Aggregation operations
      op when op in [:array_agg, :array_agg_distinct] ->
        build_array_agg_sql(spec, params_list, selecto)
        
      :string_agg ->
        build_string_agg_sql(spec, params_list, selecto)
        
      # Testing operations
      op when op in [:array_contains, :array_contained, :array_overlap, :array_eq] ->
        build_array_test_sql(spec, params_list, selecto)
        
      # Size operations
      op when op in [:array_length, :cardinality, :array_ndims, :array_dims] ->
        build_array_size_sql(spec, params_list, selecto)
        
      # Construction operations
      op when op in [:array, :array_fill, :array_append, :array_prepend, :array_cat] ->
        build_array_construct_sql(spec, params_list, selecto)
        
      # Element operations
      op when op in [:array_position, :array_positions, :array_remove, :array_replace] ->
        build_array_element_sql(spec, params_list, selecto)
        
      # Transformation operations
      :unnest ->
        build_unnest_sql(spec, params_list, selecto)
        
      op when op in [:array_to_string, :string_to_array] ->
        build_array_transform_sql(spec, params_list, selecto)
        
      # Set operations
      op when op in [:array_union, :array_intersect, :array_except] ->
        build_array_set_sql(spec, params_list, selecto)
        
      _ ->
        raise "Unsupported array operation: #{spec.operation}"
    end
  end
  
  # Array aggregation operations
  defp build_array_agg_sql(%Spec{operation: op} = spec, params_list, selecto) do
    distinct = if op == :array_agg_distinct or spec.distinct, do: "DISTINCT ", else: ""
    column_sql = build_column_reference(spec.column, selecto)
    
    # Build ORDER BY clause if present
    order_clause = if spec.order_by do
      order_parts = Enum.map(spec.order_by, fn
        {col, dir} -> 
          col_ref = build_column_reference(col, selecto)
          "#{col_ref} #{String.upcase(to_string(dir))}"
        col -> 
          build_column_reference(col, selecto)
      end)
      " ORDER BY #{Enum.join(order_parts, ", ")}"
    else
      ""
    end
    
    iodata = ["ARRAY_AGG(", distinct, column_sql, order_clause, ")"]
    
    # Add alias if present
    iodata = if spec.alias, do: iodata ++ [" AS ", spec.alias], else: iodata
    
    {iodata, params_list}
  end
  
  defp build_string_agg_sql(%Spec{} = spec, params_list, selecto) do
    column_sql = build_column_reference(spec.column, selecto)
    
    # Get delimiter from options or use default
    delimiter = spec.options[:delimiter] || ","
    
    # Build ORDER BY clause if present
    order_clause = if spec.order_by do
      order_parts = Enum.map(spec.order_by, fn
        {col, dir} -> 
          col_ref = build_column_reference(col, selecto)
          "#{col_ref} #{String.upcase(to_string(dir))}"
        col -> 
          build_column_reference(col, selecto)
      end)
      " ORDER BY #{Enum.join(order_parts, ", ")}"
    else
      ""
    end
    
    iodata = ["STRING_AGG(", column_sql, ", ", {:param, delimiter}, order_clause, ")"]
    
    # Add alias if present
    iodata = if spec.alias, do: iodata ++ [" AS ", spec.alias], else: iodata
    
    # Return with parameters appended
    {iodata, params_list ++ [delimiter]}
  end
  
  # Array testing operations
  defp build_array_test_sql(%Spec{} = spec, params_list, selecto) do
    column_sql = build_column_reference(spec.column, selecto)
    
    operator = case spec.operation do
      :array_contains -> "@>"
      :array_contained -> "<@"
      :array_overlap -> "&&"
      :array_eq -> "="
    end
    
    iodata = [column_sql, " ", operator, " ", {:param, spec.value}]
    {iodata, params_list ++ [spec.value]}
  end
  
  # Array size operations
  defp build_array_size_sql(%Spec{operation: :array_length} = spec, params_list, selecto) do
    column_sql = build_column_reference(spec.column, selecto)
    iodata = ["ARRAY_LENGTH(", column_sql, ", ", Integer.to_string(spec.dimension), ")"]
    
    # Add alias if present
    iodata = if spec.alias, do: iodata ++ [" AS ", spec.alias], else: iodata
    
    {iodata, params_list}
  end
  
  defp build_array_size_sql(%Spec{operation: :cardinality} = spec, params_list, selecto) do
    column_sql = build_column_reference(spec.column, selecto)
    iodata = ["CARDINALITY(", column_sql, ")"]
    
    # Add alias if present
    iodata = if spec.alias, do: iodata ++ [" AS ", spec.alias], else: iodata
    
    {iodata, params_list}
  end
  
  defp build_array_size_sql(%Spec{operation: op} = spec, params_list, selecto) when op in [:array_ndims, :array_dims] do
    column_sql = build_column_reference(spec.column, selecto)
    func_name = String.upcase(to_string(op))
    iodata = [func_name, "(", column_sql, ")"]
    
    # Add alias if present
    iodata = if spec.alias, do: iodata ++ [" AS ", spec.alias], else: iodata
    
    {iodata, params_list}
  end
  
  # Array construction operations
  defp build_array_construct_sql(%Spec{operation: :array} = spec, params_list, _selecto) do
    # spec.value should contain the array elements
    elements = spec.value || []
    element_parts = elements |> Enum.map(fn elem -> {:param, elem} end) |> Enum.intersperse(", ")
    
    iodata = ["ARRAY["] ++ element_parts ++ ["]"]
    iodata = if spec.alias, do: iodata ++ [" AS ", spec.alias], else: iodata
    
    {iodata, params_list ++ elements}
  end
  
  defp build_array_construct_sql(%Spec{operation: :array_append} = spec, params_list, selecto) do
    column_sql = build_column_reference(spec.column, selecto)
    
    iodata = ["ARRAY_APPEND(", column_sql, ", ", {:param, spec.value}, ")"]
    iodata = if spec.alias, do: iodata ++ [" AS ", spec.alias], else: iodata
    
    {iodata, params_list ++ [spec.value]}
  end
  
  defp build_array_construct_sql(%Spec{operation: :array_prepend} = spec, params_list, selecto) do
    column_sql = build_column_reference(spec.column, selecto)
    
    iodata = ["ARRAY_PREPEND(", {:param, spec.value}, ", ", column_sql, ")"]
    iodata = if spec.alias, do: iodata ++ [" AS ", spec.alias], else: iodata
    
    {iodata, params_list ++ [spec.value]}
  end
  
  defp build_array_construct_sql(%Spec{operation: :array_cat} = spec, params_list, selecto) do
    column_sql = build_column_reference(spec.column, selecto)
    
    iodata = ["ARRAY_CAT(", column_sql, ", ", {:param, spec.value}, ")"]
    iodata = if spec.alias, do: iodata ++ [" AS ", spec.alias], else: iodata
    
    {iodata, params_list ++ [spec.value]}
  end
  
  defp build_array_construct_sql(%Spec{operation: :array_fill} = spec, params_list, _selecto) do
    dims = spec.options[:dimensions]
    
    iodata = ["ARRAY_FILL(", {:param, spec.value}, ", ", {:param, dims}, ")"]
    iodata = if spec.alias, do: iodata ++ [" AS ", spec.alias], else: iodata
    
    {iodata, params_list ++ [spec.value, dims]}
  end
  
  # Array element operations
  defp build_array_element_sql(%Spec{operation: op} = spec, params_list, selecto) when op in [:array_position, :array_positions] do
    column_sql = build_column_reference(spec.column, selecto)
    func_name = String.upcase(to_string(op))
    
    iodata = [func_name, "(", column_sql, ", ", {:param, spec.value}, ")"]
    iodata = if spec.alias, do: iodata ++ [" AS ", spec.alias], else: iodata
    
    {iodata, params_list ++ [spec.value]}
  end
  
  defp build_array_element_sql(%Spec{operation: :array_remove} = spec, params_list, selecto) do
    column_sql = build_column_reference(spec.column, selecto)
    
    iodata = ["ARRAY_REMOVE(", column_sql, ", ", {:param, spec.value}, ")"]
    iodata = if spec.alias, do: iodata ++ [" AS ", spec.alias], else: iodata
    
    {iodata, params_list ++ [spec.value]}
  end
  
  defp build_array_element_sql(%Spec{operation: :array_replace} = spec, params_list, selecto) do
    column_sql = build_column_reference(spec.column, selecto)
    new_value = spec.options[:new_value]
    
    iodata = ["ARRAY_REPLACE(", column_sql, ", ", {:param, spec.value}, ", ", {:param, new_value}, ")"]
    iodata = if spec.alias, do: iodata ++ [" AS ", spec.alias], else: iodata
    
    {iodata, params_list ++ [spec.value, new_value]}
  end
  
  # Unnest operation
  defp build_unnest_sql(%Spec{operation: :unnest} = spec, params_list, selecto) do
    column_sql = build_column_reference(spec.column, selecto)
    
    iodata = if spec.options[:with_ordinality] do
      ["UNNEST(", column_sql, ") WITH ORDINALITY"]
    else
      ["UNNEST(", column_sql, ")"]
    end
    
    # Add alias if present
    iodata = if spec.alias do
      if spec.options[:with_ordinality] do
        iodata ++ [" AS ", spec.alias, "(value, ordinality)"]
      else
        iodata ++ [" AS ", spec.alias]
      end
    else
      iodata
    end
    
    {iodata, params_list}
  end
  
  # Array transformation operations
  defp build_array_transform_sql(%Spec{operation: :array_to_string} = spec, params_list, selecto) do
    column_sql = build_column_reference(spec.column, selecto)
    delimiter = spec.value || ","
    
    iodata = if spec.options[:null_string] do
      null_string = spec.options[:null_string]
      ["ARRAY_TO_STRING(", column_sql, ", ", {:param, delimiter}, ", ", {:param, null_string}, ")"]
    else
      ["ARRAY_TO_STRING(", column_sql, ", ", {:param, delimiter}, ")"]
    end
    
    iodata = if spec.alias, do: iodata ++ [" AS ", spec.alias], else: iodata
    
    params = if spec.options[:null_string] do
      params_list ++ [delimiter, spec.options[:null_string]]
    else
      params_list ++ [delimiter]
    end
    
    {iodata, params}
  end
  
  defp build_array_transform_sql(%Spec{operation: :string_to_array} = spec, params_list, selecto) do
    column_sql = build_column_reference(spec.column, selecto)
    delimiter = spec.value || ","
    
    iodata = if spec.options[:null_string] do
      null_string = spec.options[:null_string]
      ["STRING_TO_ARRAY(", column_sql, ", ", {:param, delimiter}, ", ", {:param, null_string}, ")"]
    else
      ["STRING_TO_ARRAY(", column_sql, ", ", {:param, delimiter}, ")"]
    end
    
    iodata = if spec.alias, do: iodata ++ [" AS ", spec.alias], else: iodata
    
    params = if spec.options[:null_string] do
      params_list ++ [delimiter, spec.options[:null_string]]
    else
      params_list ++ [delimiter]
    end
    
    {iodata, params}
  end
  
  # Array set operations (PostgreSQL 14+)
  defp build_array_set_sql(%Spec{operation: op} = spec, params_list, selecto) do
    column_sql = build_column_reference(spec.column, selecto)
    
    func_name = case op do
      :array_union -> "ARRAY_UNION"
      :array_intersect -> "ARRAY_INTERSECT"
      :array_except -> "ARRAY_EXCEPT"
    end
    
    iodata = [func_name, "(", column_sql, ", ", {:param, spec.value}, ")"]
    iodata = if spec.alias, do: iodata ++ [" AS ", spec.alias], else: iodata
    
    {iodata, params_list ++ [spec.value]}
  end
end