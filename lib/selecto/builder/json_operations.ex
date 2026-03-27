defmodule Selecto.Builder.JsonOperations do
  @moduledoc """
  SQL generation for PostgreSQL JSON operations.

  Generates SQL for JSON path queries, aggregation, manipulation, and testing
  functions. Supports both JSON and JSONB column types with proper parameter
  binding and PostgreSQL-specific syntax.
  """

  alias Selecto.Advanced.JsonOperations.Spec
  alias Selecto.AdapterSupport
  alias Selecto.Error
  alias Selecto.Jsonb

  @doc """
  Generate SQL for a JSON operation in SELECT clauses.

  Returns SQL iodata with proper function calls and parameter binding.
  """
  def build_json_select(%Spec{} = spec, opts \\ []) do
    case spec.validated do
      false ->
        raise ArgumentError,
              "JSON operation specification must be validated before SQL generation"

      true ->
        generate_select_sql(spec, opts)
    end
  end

  @doc """
  Generate SQL for a JSON operation in WHERE clauses.

  Returns SQL iodata suitable for filtering conditions.
  """
  def build_json_filter(%Spec{} = spec, opts \\ []) do
    case spec.validated do
      false ->
        raise ArgumentError,
              "JSON operation specification must be validated before SQL generation"

      true ->
        generate_filter_sql(spec, opts)
    end
  end

  @doc """
  Generate SQL for multiple JSON operations.

  Returns {sql_iodata, parameters} tuple for batch operations.
  """
  def build_json_operations(specs, opts \\ []) when is_list(specs) do
    sql_parts = Enum.map(specs, &build_json_select(&1, opts))
    combined_sql = Enum.intersperse(sql_parts, ", ")
    # Parameters handled individually by each operation
    {combined_sql, []}
  end

  # Generate SELECT clause SQL for JSON operations
  defp generate_select_sql(%Spec{operation: operation} = spec, opts) do
    ensure_operation_supported!(operation, Keyword.get(opts, :adapter), :select)

    case operation do
      # Extraction operations
      :json_extract ->
        build_json_extract(spec, opts)

      :json_extract_text ->
        build_json_extract_text(spec, opts)

      :json_extract_path ->
        build_json_extract_path(spec, opts)

      :json_extract_path_text ->
        build_json_extract_path_text(spec, opts)

      # Aggregation operations
      :json_agg ->
        build_json_agg(spec)

      :json_object_agg ->
        build_json_object_agg(spec)

      :jsonb_agg ->
        build_jsonb_agg(spec)

      :jsonb_object_agg ->
        build_jsonb_object_agg(spec)

      # Construction operations
      :json_build_object ->
        build_json_build_object(spec)

      :json_build_array ->
        build_json_build_array(spec)

      :jsonb_build_object ->
        build_jsonb_build_object(spec)

      :jsonb_build_array ->
        build_jsonb_build_array(spec)

      # Manipulation operations
      :json_set ->
        build_json_set(spec)

      :jsonb_set ->
        build_jsonb_set(spec)

      :json_insert ->
        build_json_insert(spec)

      :jsonb_insert ->
        build_jsonb_insert(spec)

      # Type operations
      :json_typeof ->
        build_json_typeof(spec, opts)

      :jsonb_typeof ->
        build_jsonb_typeof(spec)

      :json_array_length ->
        build_json_array_length(spec, opts)

      :jsonb_array_length ->
        build_jsonb_array_length(spec)

      _ ->
        raise ArgumentError, "Unsupported JSON operation for SELECT: #{operation}"
    end
  end

  # Generate WHERE clause SQL for JSON operations
  defp generate_filter_sql(%Spec{operation: operation} = spec, opts) do
    ensure_operation_supported!(operation, Keyword.get(opts, :adapter), :filter)

    case operation do
      # Containment operations
      :json_contains ->
        build_json_contains(spec, opts)

      :json_contained ->
        build_json_contained(spec)

      # Existence operations
      :json_exists ->
        build_json_exists(spec, opts)

      :json_path_exists ->
        build_json_path_exists(spec, opts)

      # Extraction operations (for comparison)
      :json_extract ->
        build_json_extract(spec, opts)

      :json_extract_text ->
        build_json_extract_text(spec, opts)

      _ ->
        raise ArgumentError, "Unsupported JSON operation for WHERE: #{operation}"
    end
  end

  # JSON extraction using -> operator (returns JSON)
  defp build_json_extract(%Spec{column: column, path: path} = spec, opts) do
    sql_parts = extraction_sql(column, path, :json, opts)

    add_alias(sql_parts, spec.alias, opts)
  end

  # JSON extraction using ->> operator (returns text)
  defp build_json_extract_text(%Spec{column: column, path: path} = spec, opts) do
    sql_parts = extraction_sql(column, path, :text, opts)

    add_alias(sql_parts, spec.alias, opts)
  end

  # JSON path extraction using json_extract_path()
  defp build_json_extract_path(%Spec{column: column, path: path} = spec, opts) do
    sql_parts = extraction_sql(column, path, :json, opts)

    add_alias(sql_parts, spec.alias, opts)
  end

  # JSON path extraction using json_extract_path_text()
  defp build_json_extract_path_text(%Spec{column: column, path: path} = spec, opts) do
    sql_parts = extraction_sql(column, path, :text, opts)

    add_alias(sql_parts, spec.alias, opts)
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
  defp build_json_contains(%Spec{column: column, value: value}, opts) do
    adapter = Keyword.get(opts, :adapter)
    table_alias = Keyword.get(opts, :table_alias)

    Jsonb.build_contains(column, value, adapter: adapter, table_alias: table_alias)
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
  defp build_json_exists(%Spec{column: column, path: path}, opts) do
    adapter = Keyword.get(opts, :adapter)
    table_alias = Keyword.get(opts, :table_alias)

    Jsonb.build_key_exists(column, path, adapter: adapter, table_alias: table_alias)
  end

  # JSON path exists
  defp build_json_path_exists(%Spec{column: column, path: path}, opts) do
    adapter = Keyword.get(opts, :adapter)
    table_alias = Keyword.get(opts, :table_alias)

    Jsonb.build_key_exists(column, path_to_list(path), adapter: adapter, table_alias: table_alias)
  end

  # JSON typeof
  defp build_json_typeof(%Spec{column: column, path: path} = spec, opts) do
    adapter = Keyword.get(opts, :adapter)
    table_alias = Keyword.get(opts, :table_alias)

    sql_parts =
      case AdapterSupport.adapter_name(adapter) do
        :sqlite ->
          sqlite_json_function("json_type", column, path, table_alias)

        _ ->
          [
            "JSON_TYPEOF(",
            column,
            ")"
          ]
      end

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
  defp build_json_array_length(%Spec{column: column, path: path} = spec, opts) do
    adapter = Keyword.get(opts, :adapter)
    table_alias = Keyword.get(opts, :table_alias)

    sql_parts =
      case AdapterSupport.adapter_name(adapter) do
        :sqlite ->
          sqlite_json_function("json_array_length", column, path, table_alias)

        _ ->
          [
            "JSON_ARRAY_LENGTH(",
            column,
            ")"
          ]
      end

    add_alias(sql_parts, spec.alias)
  end

  defp sqlite_json_function(function_name, column, nil, table_alias) do
    [function_name, "(", sqlite_json_column_ref(column, table_alias), ")"]
  end

  defp sqlite_json_function(function_name, column, path, table_alias) do
    [
      function_name,
      "(",
      sqlite_json_column_ref(column, table_alias),
      ", '",
      path,
      "')"
    ]
  end

  defp sqlite_json_column_ref(column, nil), do: ~s("#{column}")
  defp sqlite_json_column_ref(column, table_alias), do: ~s("#{table_alias}"."#{column}")

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

    last_sql =
      case last_part do
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
  defp extraction_sql(column, path, kind, opts) do
    adapter = Keyword.get(opts, :adapter)
    table_alias = Keyword.get(opts, :table_alias)

    if AdapterSupport.adapter_name(adapter) in [:mssql, :mysql, :mariadb, :sqlite] do
      Jsonb.build_extraction(column, path_to_list(path),
        as_text: kind == :text,
        adapter: adapter,
        table_alias: table_alias
      )
    else
      case kind do
        :json -> [column, build_json_path_operator(path, :json)]
        :text -> [column, build_json_path_operator(path, :text)]
      end
    end
  end

  defp path_to_list(path) do
    path
    |> String.replace_prefix("$.", "")
    |> String.split(~r/[\.\[\]]/, trim: true)
  end

  defp add_alias(sql_parts, alias_name), do: add_alias(sql_parts, alias_name, [])

  defp add_alias(sql_parts, nil, _opts), do: sql_parts

  defp add_alias(sql_parts, alias_name, opts) do
    adapter = Keyword.get(opts, :adapter)

    quoted_alias =
      if AdapterSupport.callback_available?(adapter, :quote_identifier, 1) do
        adapter.quote_identifier(alias_name)
      else
        "\"#{alias_name}\""
      end

    [sql_parts, " AS ", quoted_alias]
  end

  defp ensure_operation_supported!(operation, adapter, clause_type) do
    case {AdapterSupport.adapter_name(adapter), operation} do
      {:mssql, op}
      when op in [
             :json_agg,
             :json_object_agg,
             :jsonb_agg,
             :jsonb_object_agg,
             :json_build_object,
             :json_build_array,
             :jsonb_build_object,
             :jsonb_build_array,
             :json_set,
             :jsonb_set,
             :json_insert,
             :jsonb_insert,
             :json_typeof,
             :jsonb_typeof,
             :json_array_length,
             :jsonb_array_length,
             :json_contained
           ] ->
        error =
          Error.validation_error("Adapter does not support this JSON operation", %{
            adapter: :mssql,
            clause_type: clause_type,
            operation: operation,
            unsupported_feature: :json_operation
          })

        raise Error.to_exception(error)

      {adapter_name, op}
      when adapter_name == :sqlite and
             op in [
               :json_contained,
               :json_agg,
               :json_object_agg,
               :jsonb_agg,
               :jsonb_object_agg,
               :json_build_object,
               :json_build_array,
               :jsonb_build_object,
               :jsonb_build_array,
               :json_set,
               :jsonb_set,
               :json_insert,
               :jsonb_insert,
               :jsonb_typeof,
               :jsonb_array_length
             ] ->
        error =
          Error.validation_error("Adapter does not support this JSON operation", %{
            adapter: adapter_name,
            clause_type: clause_type,
            operation: operation,
            unsupported_feature: :json_operation
          })

        raise Error.to_exception(error)

      {adapter_name, op}
      when adapter_name in [:mysql, :mariadb] and
             op in [
               :json_contained,
               :jsonb_agg,
               :jsonb_object_agg,
               :jsonb_build_object,
               :jsonb_build_array,
               :jsonb_set,
               :jsonb_insert,
               :jsonb_typeof,
               :jsonb_array_length
             ] ->
        error =
          Error.validation_error("Adapter does not support this JSON operation", %{
            adapter: adapter_name,
            clause_type: clause_type,
            operation: operation,
            unsupported_feature: :json_operation
          })

        raise Error.to_exception(error)

      _ ->
        :ok
    end
  end
end
