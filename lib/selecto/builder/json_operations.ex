defmodule Selecto.Builder.JsonOperations do
  @moduledoc """
  SQL generation for PostgreSQL JSON operations.
  
  Generates SQL for JSON path queries, aggregation, manipulation, and testing
  functions. Supports both JSON and JSONB column types with proper parameter
  binding and PostgreSQL-specific syntax.
  """
  
  alias Selecto.Advanced.JsonOperations.Spec
  alias Selecto.SQL.Params
  
  @doc """
  Generate SQL for a JSON operation in SELECT clauses.
  
  Returns SQL iodata with proper function calls and parameter binding.
  """
  def build_json_select(%Spec{} = spec) do
    case spec.validated do
      false ->
        raise ArgumentError, "JSON operation specification must be validated before SQL generation"
      true ->
        generate_select_sql(spec)
    end
  end
  
  @doc """
  Generate SQL for a JSON operation in WHERE clauses.
  
  Returns SQL iodata suitable for filtering conditions.
  """
  def build_json_filter(%Spec{} = spec) do
    case spec.validated do
      false ->
        raise ArgumentError, "JSON operation specification must be validated before SQL generation"
      true ->
        generate_filter_sql(spec)
    end
  end
  
  @doc """
  Generate SQL for multiple JSON operations.
  
  Returns {sql_iodata, parameters} tuple for batch operations.
  """
  def build_json_operations(specs) when is_list(specs) do
    sql_parts = Enum.map(specs, &build_json_select/1)
    combined_sql = Enum.intersperse(sql_parts, ", ")
    {combined_sql, []}  # Parameters handled individually by each operation
  end
  
  # Generate SELECT clause SQL for JSON operations
  defp generate_select_sql(%Spec{operation: operation} = spec) do
    case operation do
      # Extraction operations
      :json_extract -> build_json_extract(spec)
      :json_extract_text -> build_json_extract_text(spec)
      :json_extract_path -> build_json_extract_path(spec)
      :json_extract_path_text -> build_json_extract_path_text(spec)
      
      # Aggregation operations
      :json_agg -> build_json_agg(spec)
      :json_object_agg -> build_json_object_agg(spec)
      :jsonb_agg -> build_jsonb_agg(spec)
      :jsonb_object_agg -> build_jsonb_object_agg(spec)
      
      # Construction operations
      :json_build_object -> build_json_build_object(spec)
      :json_build_array -> build_json_build_array(spec)
      :jsonb_build_object -> build_jsonb_build_object(spec)
      :jsonb_build_array -> build_jsonb_build_array(spec)
      
      # Manipulation operations
      :json_set -> build_json_set(spec)
      :jsonb_set -> build_jsonb_set(spec)
      :json_insert -> build_json_insert(spec)
      :jsonb_insert -> build_jsonb_insert(spec)
      
      # Type operations
      :json_typeof -> build_json_typeof(spec)
      :jsonb_typeof -> build_jsonb_typeof(spec)
      :json_array_length -> build_json_array_length(spec)
      :jsonb_array_length -> build_jsonb_array_length(spec)
      
      _ ->
        raise ArgumentError, "Unsupported JSON operation for SELECT: #{operation}"
    end
  end
  
  # Generate WHERE clause SQL for JSON operations
  defp generate_filter_sql(%Spec{operation: operation} = spec) do
    case operation do
      # Containment operations
      :json_contains -> build_json_contains(spec)
      :json_contained -> build_json_contained(spec)
      
      # Existence operations
      :json_exists -> build_json_exists(spec)
      :json_path_exists -> build_json_path_exists(spec)
      
      # Extraction operations (for comparison)
      :json_extract -> build_json_extract(spec)
      :json_extract_text -> build_json_extract_text(spec)
      
      _ ->
        raise ArgumentError, "Unsupported JSON operation for WHERE: #{operation}"
    end
  end
  
  # JSON extraction using -> operator (returns JSON)
  defp build_json_extract(%Spec{column: column, path: path} = spec) do
    sql_parts = [
      column,
      build_json_path_operator(path, :json)
    ]
    
    add_alias(sql_parts, spec.alias)
  end
  
  # JSON extraction using ->> operator (returns text)
  defp build_json_extract_text(%Spec{column: column, path: path} = spec) do
    sql_parts = [
      column,
      build_json_path_operator(path, :text)
    ]
    
    add_alias(sql_parts, spec.alias)
  end
  
  # JSON path extraction using json_extract_path()
  defp build_json_extract_path(%Spec{column: column, path: path} = spec) do
    path_elements = parse_json_path(path)
    
    sql_parts = [
      "json_extract_path(",
      column,
      ", ",
      format_path_elements(path_elements),
      ")"
    ]
    
    add_alias(sql_parts, spec.alias)
  end
  
  # JSON path extraction using json_extract_path_text()
  defp build_json_extract_path_text(%Spec{column: column, path: path} = spec) do
    path_elements = parse_json_path(path)
    
    sql_parts = [
      "json_extract_path_text(",
      column,
      ", ",
      format_path_elements(path_elements),
      ")"
    ]
    
    add_alias(sql_parts, spec.alias)
  end
  
  # JSON aggregation
  defp build_json_agg(%Spec{column: column} = spec) do
    sql_parts = [
      "JSON_AGG(",
      column,
      ")"
    ]
    
    add_alias(sql_parts, spec.alias)
  end
  
  # JSON object aggregation
  defp build_json_object_agg(%Spec{key_field: key_field, value_field: value_field} = spec) do
    sql_parts = [
      "JSON_OBJECT_AGG(",
      key_field,
      ", ",
      value_field,
      ")"
    ]
    
    add_alias(sql_parts, spec.alias)
  end
  
  # JSONB aggregation
  defp build_jsonb_agg(%Spec{column: column} = spec) do
    sql_parts = [
      "JSONB_AGG(",
      column,
      ")"
    ]
    
    add_alias(sql_parts, spec.alias)
  end
  
  # JSONB object aggregation
  defp build_jsonb_object_agg(%Spec{key_field: key_field, value_field: value_field} = spec) do
    sql_parts = [
      "JSONB_OBJECT_AGG(",
      key_field,
      ", ",
      value_field,
      ")"
    ]
    
    add_alias(sql_parts, spec.alias)
  end
  
  # JSON object construction
  defp build_json_build_object(%Spec{value: pairs} = spec) when is_list(pairs) do
    formatted_pairs = 
      pairs
      |> Enum.map(fn {key, value} -> ["'#{key}'", ", ", format_json_value(value)] end)
      |> Enum.intersperse(", ")
    
    sql_parts = [
      "JSON_BUILD_OBJECT(",
      formatted_pairs,
      ")"
    ]
    
    add_alias(sql_parts, spec.alias)
  end
  
  # JSON array construction
  defp build_json_build_array(%Spec{value: elements} = spec) when is_list(elements) do
    formatted_elements = 
      elements
      |> Enum.map(&format_json_value/1)
      |> Enum.intersperse(", ")
    
    sql_parts = [
      "JSON_BUILD_ARRAY(",
      formatted_elements,
      ")"
    ]
    
    add_alias(sql_parts, spec.alias)
  end
  
  # JSONB object construction
  defp build_jsonb_build_object(%Spec{value: pairs} = spec) when is_list(pairs) do
    formatted_pairs = 
      pairs
      |> Enum.map(fn {key, value} -> ["'#{key}'", ", ", format_json_value(value)] end)
      |> Enum.intersperse(", ")
    
    sql_parts = [
      "JSONB_BUILD_OBJECT(",
      formatted_pairs,
      ")"
    ]
    
    add_alias(sql_parts, spec.alias)
  end
  
  # JSONB array construction
  defp build_jsonb_build_array(%Spec{value: elements} = spec) when is_list(elements) do
    formatted_elements = 
      elements
      |> Enum.map(&format_json_value/1)
      |> Enum.intersperse(", ")
    
    sql_parts = [
      "JSONB_BUILD_ARRAY(",
      formatted_elements,
      ")"
    ]
    
    add_alias(sql_parts, spec.alias)
  end
  
  # JSON set operation
  defp build_json_set(%Spec{column: column, path: path, value: value} = spec) do
    path_array = format_json_path_array(path)
    
    sql_parts = [
      "JSON_SET(",
      column,
      ", ",
      path_array,
      ", ",
      format_json_value(value),
      ")"
    ]
    
    add_alias(sql_parts, spec.alias)
  end
  
  # JSONB set operation
  defp build_jsonb_set(%Spec{column: column, path: path, value: value} = spec) do
    path_array = format_json_path_array(path)
    
    sql_parts = [
      "JSONB_SET(",
      column,
      ", ",
      path_array,
      ", ",
      format_json_value(value),
      ")"
    ]
    
    add_alias(sql_parts, spec.alias)
  end
  
  # JSON insert operation
  defp build_json_insert(%Spec{column: column, path: path, value: value} = spec) do
    path_array = format_json_path_array(path)
    
    sql_parts = [
      "JSON_INSERT(",
      column,
      ", ",
      path_array,
      ", ",
      format_json_value(value),
      ")"
    ]
    
    add_alias(sql_parts, spec.alias)
  end
  
  # JSONB insert operation
  defp build_jsonb_insert(%Spec{column: column, path: path, value: value} = spec) do
    path_array = format_json_path_array(path)
    
    sql_parts = [
      "JSONB_INSERT(",
      column,
      ", ",
      path_array,
      ", ",
      format_json_value(value),
      ")"
    ]
    
    add_alias(sql_parts, spec.alias)
  end
  
  # JSON containment (@> operator)
  defp build_json_contains(%Spec{column: column, value: value}) do
    [
      column,
      " @> ",
      format_json_value(value)
    ]
  end
  
  # JSON contained (<@ operator)  
  defp build_json_contained(%Spec{column: column, value: value}) do
    [
      column,
      " <@ ",
      format_json_value(value)
    ]
  end
  
  # JSON exists (? operator)
  defp build_json_exists(%Spec{column: column, path: path}) do
    [
      column,
      " ? ",
      "'#{path}'"
    ]
  end
  
  # JSON path exists
  defp build_json_path_exists(%Spec{column: column, path: path}) do
    [
      "JSONB_PATH_EXISTS(",
      column,
      ", '",
      path,
      "')"
    ]
  end
  
  # JSON typeof
  defp build_json_typeof(%Spec{column: column} = spec) do
    sql_parts = [
      "JSON_TYPEOF(",
      column,
      ")"
    ]
    
    add_alias(sql_parts, spec.alias)
  end
  
  # JSONB typeof
  defp build_jsonb_typeof(%Spec{column: column} = spec) do
    sql_parts = [
      "JSONB_TYPEOF(",
      column,
      ")"
    ]
    
    add_alias(sql_parts, spec.alias)
  end
  
  # JSON array length
  defp build_json_array_length(%Spec{column: column} = spec) do
    sql_parts = [
      "JSON_ARRAY_LENGTH(",
      column,
      ")"
    ]
    
    add_alias(sql_parts, spec.alias)
  end
  
  # JSONB array length
  defp build_jsonb_array_length(%Spec{column: column} = spec) do
    sql_parts = [
      "JSONB_ARRAY_LENGTH(",
      column,
      ")"
    ]
    
    add_alias(sql_parts, spec.alias)
  end
  
  # Build JSON path operator (-> or ->>)
  defp build_json_path_operator(path, :json) do
    path_parts = parse_simple_path(path)
    
    path_parts
    |> Enum.map(fn part ->
      case part do
        {:key, key} -> [" -> '", key, "'"]
        {:index, idx} -> [" -> ", Integer.to_string(idx)]
      end
    end)
  end
  
  defp build_json_path_operator(path, :text) do
    path_parts = parse_simple_path(path)
    
    # All parts except the last use ->, the last uses ->>
    {init_parts, [last_part]} = Enum.split(path_parts, -1)
    
    init_sql = 
      init_parts
      |> Enum.map(fn part ->
        case part do
          {:key, key} -> [" -> '", key, "'"]
          {:index, idx} -> [" -> ", Integer.to_string(idx)]
        end
      end)
    
    last_sql = case last_part do
      {:key, key} -> [" ->> '", key, "'"]
      {:index, idx} -> [" ->> ", Integer.to_string(idx)]
    end
    
    init_sql ++ [last_sql]
  end
  
  # Parse simple JSON path ($.key[0].subkey format)
  defp parse_simple_path(path) do
    path
    |> String.replace_prefix("$.", "")
    |> String.split(~r/[\.\[\]]/, trim: true)
    |> Enum.map(fn part ->
      case Integer.parse(part) do
        {idx, ""} -> {:index, idx}
        _ -> {:key, part}
      end
    end)
  end
  
  # Parse JSON path into elements for json_extract_path functions
  defp parse_json_path(path) do
    path
    |> String.replace_prefix("$.", "")
    |> String.split(".")
  end
  
  # Format path elements for function calls
  defp format_path_elements(elements) do
    elements
    |> Enum.map(fn element -> "'#{element}'" end)
    |> Enum.intersperse(", ")
  end
  
  # Format JSON path as PostgreSQL array literal
  defp format_json_path_array(path) do
    elements = parse_json_path(path)
    formatted_elements = Enum.map(elements, fn el -> "'#{el}'" end)
    
    [
      "'{",
      Enum.intersperse(formatted_elements, ", "),
      "}'"
    ]
  end
  
  # Format various JSON values for SQL
  defp format_json_value(value) when is_binary(value), do: "'#{String.replace(value, "'", "''")}'"
  defp format_json_value(value) when is_integer(value), do: Integer.to_string(value)
  defp format_json_value(value) when is_float(value), do: Float.to_string(value)
  defp format_json_value(true), do: "true"
  defp format_json_value(false), do: "false"
  defp format_json_value(nil), do: "null"
  defp format_json_value(value) when is_map(value) do
    json_string = Jason.encode!(value)
    "'#{String.replace(json_string, "'", "''")}'"
  end
  defp format_json_value(value) when is_list(value) do
    json_string = Jason.encode!(value)
    "'#{String.replace(json_string, "'", "''")}'"
  end
  defp format_json_value(value), do: "'#{inspect(value)}'"
  
  # Add alias to SQL parts if present
  defp add_alias(sql_parts, nil), do: sql_parts
  defp add_alias(sql_parts, alias_name) do
    sql_parts ++ [" AS ", "\"#{alias_name}\""]
  end
end