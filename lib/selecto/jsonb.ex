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
      case Map.get(columns, first) do
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

    column_ref = if alias_name do
      ~s("#{alias_name}"."#{column}")
    else
      ~s("#{column}")
    end

    extraction = case path do
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

  @doc """
  Build a JSONB containment check expression.

  ## Examples

      iex> build_contains("attributes", %{"color" => "red"})
      ~s("attributes" @> '{"color":"red"}'::jsonb)
  """
  def build_contains(column, value, opts \\ []) do
    alias_name = Keyword.get(opts, :table_alias)
    json_value = Jason.encode!(value)

    column_ref = if alias_name do
      ~s("#{alias_name}"."#{column}")
    else
      ~s("#{column}")
    end

    ~s(#{column_ref} @> '#{json_value}'::jsonb)
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

    column_ref = if alias_name do
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
        # Navigate to parent, then check for last key
        {parent_path, [last_key]} = Enum.split(keys, -1)
        parent_expr = build_extraction(column, parent_path, as_text: false, table_alias: alias_name)
        ~s(#{parent_expr} ? '#{last_key}')
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
    array_expr = build_extraction(column, path, as_text: false, table_alias: alias_name)

    case value do
      v when is_binary(v) ->
        ~s(#{array_expr} ? '#{v}')

      values when is_list(values) ->
        escaped = Enum.map(values, fn v -> "'#{v}'" end) |> Enum.join(",")
        ~s(#{array_expr} ?| array[#{escaped}])
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
    array_expr = build_extraction(column, path, as_text: false, table_alias: alias_name)
    escaped = Enum.map(values, fn v -> "'#{v}'" end) |> Enum.join(",")
    ~s(#{array_expr} ?& array[#{escaped}])
  end

  @doc """
  Determine the PostgreSQL cast type for a JSONB schema type.
  """
  def pg_cast_for_type(nil), do: nil
  def pg_cast_for_type(:string), do: nil  # No cast needed, ->> returns text
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
