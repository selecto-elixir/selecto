defmodule Selecto.Jsonb do
  @moduledoc """
  JSONB column support with dot notation for filtering and selection.

  Provides structured JSONB column definitions with schemas that enable:
  - Dot notation access: `"attributes.color"` → `attributes->>'color'`
  - Type-aware comparisons with automatic casting
  - Contains/exists operations for JSONB-specific queries
  - Nested object and array support

  ## Domain Configuration

      columns: %{
        "attributes" => %{
          type: :jsonb,
          schema: %{
            "color" => %{type: :string, required: true},
            "size" => %{type: :string, enum: ["small", "medium", "large"]},
            "weight" => %{type: :decimal},
            "dimensions" => %{
              type: :object,
              schema: %{
                "length" => %{type: :decimal},
                "width" => %{type: :decimal}
              }
            },
            "tags" => %{type: :array, items: %{type: :string}}
          }
        }
      }

  ## Filtering Examples

      # Equality on JSONB path
      Selecto.filter(selecto, {"attributes.color", "red"})

      # Comparison operators
      Selecto.filter(selecto, {"attributes.weight", {:gt, 10.0}})

      # JSONB contains
      Selecto.filter(selecto, {"attributes", {:jsonb_contains, %{"color" => "red"}}})

      # Array contains
      Selecto.filter(selecto, {"attributes.tags", {:contains, "featured"}})

      # Key exists
      Selecto.filter(selecto, {"attributes.warranty", :exists})
  """

  @doc """
  Parse a field reference that may contain JSONB dot notation.

  Returns `{:jsonb, column, path}` if it's a JSONB path, or `{:regular, field}` otherwise.

  ## Examples

      iex> parse_field_reference("attributes.color", domain)
      {:jsonb, "attributes", ["color"]}

      iex> parse_field_reference("attributes.dimensions.length", domain)
      {:jsonb, "attributes", ["dimensions", "length"]}

      iex> parse_field_reference("name", domain)
      {:regular, "name"}
  """
  def parse_field_reference(field, domain) when is_binary(field) do
    columns = Map.get(domain, :columns, %{})

    if String.contains?(field, ".") do
      parts = String.split(field, ".")
      [first | rest] = parts

      # Check if first part is a JSONB column
      case Map.get(columns, first) || Map.get(columns, safe_existing_atom(first)) do
        %{type: type} when type in [:jsonb, :json] ->
          {:jsonb, first, rest}

        _ ->
          # Not a JSONB column, treat as regular qualified field
          {:regular, field}
      end
    else
      {:regular, field}
    end
  end

  def parse_field_reference(field, _domain), do: {:regular, field}

  defp safe_existing_atom(value) when is_binary(value) do
    try do
      String.to_existing_atom(value)
    rescue
      ArgumentError -> nil
    end
  end

  defp safe_existing_atom(_value), do: nil

  @doc """
  Get the JSONB schema for a path within a column.

  Returns the schema definition for the nested field, or nil if not defined.
  """
  def get_path_schema(domain, column, path) when is_list(path) do
    columns = Map.get(domain, :columns, %{})

    case Map.get(columns, column) do
      %{type: type, schema: schema} when type in [:jsonb, :json] ->
        traverse_schema(schema, path)

      _ ->
        nil
    end
  end

  defp traverse_schema(schema, []) when is_map(schema), do: schema

  defp traverse_schema(schema, [key | rest]) when is_map(schema) do
    case Map.get(schema, key) do
      %{type: :object, schema: nested_schema} ->
        traverse_schema(nested_schema, rest)

      %{type: :array, items: %{type: :object, schema: nested_schema}} when rest != [] ->
        traverse_schema(nested_schema, rest)

      field_def when rest == [] ->
        field_def

      _ ->
        nil
    end
  end

  defp traverse_schema(_, _), do: nil

  @doc """
  Build a PostgreSQL JSONB extraction expression.

  ## Options
    - `:as_text` - Use `->>` for text extraction (default: true for leaf values)
    - `:cast` - Cast to specific type after extraction

  ## Examples

      iex> build_extraction("attributes", ["color"], as_text: true)
      ~s("attributes"->>'color')

      iex> build_extraction("attributes", ["dimensions", "length"], cast: :decimal)
      ~s(("attributes"#>>'{dimensions,length}')::numeric)
  """
  def build_extraction(column, path, opts \\ []) do
    as_text = Keyword.get(opts, :as_text, true)
    cast = Keyword.get(opts, :cast)
    alias_name = Keyword.get(opts, :table_alias)
    adapter = Keyword.get(opts, :adapter)

    case Selecto.AdapterSupport.adapter_name(adapter) do
      :mssql ->
        build_mssql_extraction(column, path,
          as_text: as_text,
          cast: cast,
          table_alias: alias_name
        )

      adapter_name when adapter_name in [:mysql, :mariadb] ->
        build_mysql_extraction(column, path,
          as_text: as_text,
          cast: cast,
          table_alias: alias_name
        )

      _ ->
        build_postgres_extraction(column, path,
          as_text: as_text,
          cast: cast,
          table_alias: alias_name
        )
    end
  end

  defp build_postgres_extraction(column, path, opts) do
    as_text = Keyword.get(opts, :as_text, true)
    cast = Keyword.get(opts, :cast)
    alias_name = Keyword.get(opts, :table_alias)

    column_ref =
      if alias_name do
        ~s("#{alias_name}"."#{column}")
      else
        ~s("#{column}")
      end

    extraction =
      case path do
        [single_key] ->
          operator = if as_text, do: "->>", else: "->"
          ~s(#{column_ref}#{operator}'#{single_key}')

        keys when is_list(keys) ->
          path_str = Enum.join(keys, ",")
          operator = if as_text, do: "#>>", else: "#>"
          ~s(#{column_ref}#{operator}'{#{path_str}}')
      end

    # Apply type cast if needed
    case cast do
      nil -> extraction
      :integer -> "(#{extraction})::integer"
      :decimal -> "(#{extraction})::numeric"
      :float -> "(#{extraction})::double precision"
      :boolean -> "(#{extraction})::boolean"
      :date -> "(#{extraction})::date"
      :datetime -> "(#{extraction})::timestamp"
      :utc_datetime -> "(#{extraction})::timestamptz"
      other -> "(#{extraction})::#{other}"
    end
  end

  defp build_mssql_extraction(column, path, opts) do
    as_text = Keyword.get(opts, :as_text, true)
    cast = Keyword.get(opts, :cast)
    alias_name = Keyword.get(opts, :table_alias)

    column_ref = mssql_column_ref(column, alias_name)
    json_path = to_json_path(path)
    extractor = if as_text, do: "JSON_VALUE", else: "JSON_QUERY"
    extraction = "#{extractor}(#{column_ref}, '#{json_path}')"

    case cast do
      nil -> extraction
      :integer -> "CAST(#{extraction} AS int)"
      :decimal -> "CAST(#{extraction} AS decimal(38, 10))"
      :float -> "CAST(#{extraction} AS float)"
      :boolean -> "CAST(#{extraction} AS bit)"
      :date -> "CAST(#{extraction} AS date)"
      :datetime -> "CAST(#{extraction} AS datetime2)"
      :utc_datetime -> "CAST(#{extraction} AS datetimeoffset)"
      other -> "CAST(#{extraction} AS #{other})"
    end
  end

  defp build_mysql_extraction(column, path, opts) do
    as_text = Keyword.get(opts, :as_text, true)
    cast = Keyword.get(opts, :cast)
    alias_name = Keyword.get(opts, :table_alias)

    column_ref = mysql_column_ref(column, alias_name)
    json_path = to_json_path(path)
    extraction = "JSON_EXTRACT(#{column_ref}, '#{json_path}')"
    extraction = if as_text, do: "JSON_UNQUOTE(#{extraction})", else: extraction

    case cast do
      nil -> extraction
      :integer -> "CAST(#{extraction} AS SIGNED)"
      :decimal -> "CAST(#{extraction} AS DECIMAL(38, 10))"
      :float -> "CAST(#{extraction} AS DOUBLE)"
      :boolean -> "CAST(#{extraction} AS UNSIGNED)"
      :date -> "CAST(#{extraction} AS DATE)"
      :datetime -> "CAST(#{extraction} AS DATETIME)"
      :utc_datetime -> "CAST(#{extraction} AS DATETIME)"
      other -> "CAST(#{extraction} AS #{other})"
    end
  end

  @doc """
  Build a JSONB containment check expression.

  ## Examples

      iex> build_contains("attributes", %{"color" => "red"})
      ~s("attributes" @> '{"color":"red"}'::jsonb)
  """
  def build_contains(column, value, opts \\ []) do
    alias_name = Keyword.get(opts, :table_alias)
    adapter = Keyword.get(opts, :adapter)

    case Selecto.AdapterSupport.adapter_name(adapter) do
      :mssql ->
        build_mssql_contains(column, value, alias_name)

      adapter_name when adapter_name in [:mysql, :mariadb] ->
        column_ref = mysql_column_ref(column, alias_name)
        json_value = Jason.encode!(value) |> escape_sql_literal()
        "JSON_CONTAINS(#{column_ref}, '#{json_value}')"

      _ ->
        json_value = Jason.encode!(value)

        column_ref =
          if alias_name do
            ~s("#{alias_name}"."#{column}")
          else
            ~s("#{column}")
          end

        ~s(#{column_ref} @> '#{json_value}'::jsonb)
    end
  end

  @doc """
  Build a JSONB key exists check expression.

  ## Examples

      iex> build_key_exists("attributes", "color")
      ~s("attributes" ? 'color')

      iex> build_key_exists("attributes", ["dimensions", "length"])
      ~s("attributes"->'dimensions' ? 'length')
  """
  def build_key_exists(column, key_or_path, opts \\ []) do
    alias_name = Keyword.get(opts, :table_alias)
    adapter = Keyword.get(opts, :adapter)

    case Selecto.AdapterSupport.adapter_name(adapter) do
      :mssql ->
        json_path = key_or_path |> normalize_path_segments() |> to_json_path()
        column_ref = mssql_column_ref(column, alias_name)

        "(JSON_QUERY(#{column_ref}, '#{json_path}') IS NOT NULL OR JSON_VALUE(#{column_ref}, '#{json_path}') IS NOT NULL)"

      adapter_name when adapter_name in [:mysql, :mariadb] ->
        json_path = key_or_path |> normalize_path_segments() |> to_json_path()
        column_ref = mysql_column_ref(column, alias_name)
        "JSON_CONTAINS_PATH(#{column_ref}, 'one', '#{json_path}')"

      _ ->
        column_ref =
          if alias_name do
            ~s("#{alias_name}"."#{column}")
          else
            ~s("#{column}")
          end

        case key_or_path do
          key when is_binary(key) ->
            ~s(#{column_ref} ? '#{key}')

          [single_key] ->
            ~s(#{column_ref} ? '#{single_key}')

          keys when is_list(keys) ->
            {parent_path, [last_key]} = Enum.split(keys, -1)

            parent_expr =
              build_extraction(column, parent_path, as_text: false, table_alias: alias_name)

            ~s(#{parent_expr} ? '#{last_key}')
        end
    end
  end

  @doc """
  Build a JSONB array contains check expression.

  ## Examples

      iex> build_array_contains("attributes", ["tags"], "featured")
      ~s("attributes"->'tags' ? 'featured')

      iex> build_array_contains("attributes", ["tags"], ["featured", "new"])
      ~s("attributes"->'tags' ?| array['featured','new'])
  """
  def build_array_contains(column, path, value, opts \\ []) do
    alias_name = Keyword.get(opts, :table_alias)
    adapter = Keyword.get(opts, :adapter)

    case Selecto.AdapterSupport.adapter_name(adapter) do
      :mssql ->
        build_mssql_array_contains(column, path, value, alias_name)

      adapter_name when adapter_name in [:mysql, :mariadb] ->
        build_mysql_array_contains(column, path, value, alias_name)

      _ ->
        array_expr = build_extraction(column, path, as_text: false, table_alias: alias_name)

        case value do
          v when is_binary(v) ->
            ~s(#{array_expr} ? '#{v}')

          values when is_list(values) ->
            escaped = Enum.map(values, fn v -> "'#{v}'" end) |> Enum.join(",")
            ~s(#{array_expr} ?| array[#{escaped}])
        end
    end
  end

  @doc """
  Build a JSONB array contains all check expression.

  ## Examples

      iex> build_array_contains_all("attributes", ["tags"], ["featured", "new"])
      ~s("attributes"->'tags' ?& array['featured','new'])
  """
  def build_array_contains_all(column, path, values, opts \\ []) when is_list(values) do
    alias_name = Keyword.get(opts, :table_alias)
    adapter = Keyword.get(opts, :adapter)

    case Selecto.AdapterSupport.adapter_name(adapter) do
      :mssql ->
        values
        |> Enum.map(&build_mssql_array_contains(column, path, &1, alias_name))
        |> Enum.intersperse(" AND ")

      adapter_name when adapter_name in [:mysql, :mariadb] ->
        values
        |> Enum.map(&build_mysql_array_contains(column, path, &1, alias_name))
        |> Enum.intersperse(" AND ")

      _ ->
        array_expr = build_extraction(column, path, as_text: false, table_alias: alias_name)
        escaped = Enum.map(values, fn v -> "'#{v}'" end) |> Enum.join(",")
        ~s(#{array_expr} ?& array[#{escaped}])
    end
  end

  defp build_mssql_contains(column, value, alias_name) when is_map(value) do
    value
    |> flatten_mssql_contains([])
    |> Enum.map(fn {path, path_value} ->
      extraction = build_mssql_extraction(column, path, as_text: true, table_alias: alias_name)
      [extraction, " = '", escape_sql_literal(path_value), "'"]
    end)
    |> Enum.intersperse(" AND ")
  end

  defp flatten_mssql_contains(map, prefix) do
    Enum.flat_map(map, fn
      {key, nested} when is_map(nested) ->
        flatten_mssql_contains(nested, prefix ++ [to_string(key)])

      {key, value} when is_list(value) ->
        raise ArgumentError,
              "MSSQL JSON containment for arrays is not implemented yet: #{inspect(prefix ++ [to_string(key)])}"

      {key, value} ->
        [{prefix ++ [to_string(key)], value}]
    end)
  end

  defp build_mssql_array_contains(column, path, value, alias_name) do
    column_ref = mssql_column_ref(column, alias_name)
    json_path = to_json_path(path)

    [
      "EXISTS (SELECT 1 FROM OPENJSON(",
      column_ref,
      ", '",
      json_path,
      "') WHERE value = '",
      escape_sql_literal(value),
      "')"
    ]
  end

  defp build_mysql_array_contains(column, path, value, alias_name) when is_list(value) do
    value
    |> Enum.map(&build_mysql_array_contains(column, path, &1, alias_name))
    |> Enum.intersperse(" OR ")
  end

  defp build_mysql_array_contains(column, path, value, alias_name) do
    column_ref = mysql_column_ref(column, alias_name)
    json_path = to_json_path(path)
    candidate = mysql_json_literal(value)

    ["JSON_CONTAINS(", column_ref, ", '", candidate, "', '", json_path, "')"]
  end

  defp normalize_path_segments(path) when is_binary(path) do
    path
    |> String.replace_prefix("$.", "")
    |> String.split(~r/[\.\[\]]/, trim: true)
  end

  defp normalize_path_segments(path) when is_list(path), do: path

  defp to_json_path(path) when is_list(path) do
    Enum.reduce(path, "$", fn segment, acc ->
      case Integer.parse(to_string(segment)) do
        {index, ""} -> acc <> "[#{index}]"
        _ -> acc <> "." <> to_string(segment)
      end
    end)
  end

  defp mssql_column_ref(column, nil), do: column
  defp mssql_column_ref(column, alias_name), do: "#{alias_name}.#{column}"

  defp mysql_column_ref(column, nil), do: "`#{escape_mysql_identifier(column)}`"

  defp mysql_column_ref(column, alias_name) do
    "`#{escape_mysql_identifier(alias_name)}`.`#{escape_mysql_identifier(column)}`"
  end

  defp escape_mysql_identifier(identifier) do
    identifier
    |> to_string()
    |> String.replace("`", "``")
  end

  defp mysql_json_literal(value) do
    value
    |> Jason.encode!()
    |> escape_sql_literal()
  end

  defp escape_sql_literal(value) when is_binary(value), do: String.replace(value, "'", "''")
  defp escape_sql_literal(value) when is_boolean(value), do: if(value, do: "true", else: "false")
  defp escape_sql_literal(nil), do: "null"
  defp escape_sql_literal(value), do: to_string(value)

  @doc """
  Determine the PostgreSQL cast type for a JSONB schema type.
  """
  def pg_cast_for_type(nil), do: nil
  # No cast needed, ->> returns text
  def pg_cast_for_type(:string), do: nil
  def pg_cast_for_type(:integer), do: :integer
  def pg_cast_for_type(:decimal), do: :decimal
  def pg_cast_for_type(:float), do: :float
  def pg_cast_for_type(:boolean), do: :boolean
  def pg_cast_for_type(:date), do: :date
  def pg_cast_for_type(:datetime), do: :datetime
  def pg_cast_for_type(:utc_datetime), do: :utc_datetime
  def pg_cast_for_type(:naive_datetime), do: :datetime
  def pg_cast_for_type(_), do: nil

  @doc """
  Check if a column is a JSONB type in the domain.
  """
  def jsonb_column?(domain, column) do
    columns = Map.get(domain, :columns, %{})

    case Map.get(columns, column) do
      %{type: type} when type in [:jsonb, :json] -> true
      _ -> false
    end
  end
end
