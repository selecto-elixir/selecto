defmodule Selecto.Output.Transformers.Maps do
  @moduledoc """
  Transforms query results to list of maps format.

  Supports various key types (strings, atoms, existing atoms) and key transformations
  (camelCase, snake_case). Also supports type coercion from database types to Elixir types.
  """

  @doc """
  Transform rows, columns, and aliases to list of maps.

  ## Parameters

  - `rows` - List of row data (list of lists)
  - `columns` - List of column names
  - `aliases` - Map of column aliases
  - `options` - Transformation options

  ## Options

  - `:keys` - Key type (:strings, :atoms, :existing_atoms). Default: :strings
  - `:transform` - Key name transformation (:none, :camelCase, :snake_case). Default: :none
  - `:coerce_types` - Whether to coerce database types to Elixir types. Default: false
  - `:null_handling` - How to handle NULL values (:preserve, :remove). Default: :preserve

  ## Examples

      # String keys (default)
      {:ok, maps} = transform(rows, columns, aliases, [])
      # => [%{"name" => "John", "age" => 25}, ...]

      # Atom keys
      {:ok, maps} = transform(rows, columns, aliases, keys: :atoms)
      # => [%{name: "John", age: 25}, ...]

      # CamelCase transformation
      {:ok, maps} = transform(rows, columns, aliases, keys: :atoms, transform: :camelCase)
      # => [%{firstName: "John", userAge: 25}, ...]
  """
  @spec transform(list(), list(), map(), keyword()) :: {:ok, list(map())} | {:error, term()}
  def transform(rows, columns, aliases, options \\ []) do
    try do
      # Validate and set defaults for options
      opts = Keyword.validate!(options,
        keys: :strings,
        transform: :none,
        coerce_types: false,
        null_handling: :preserve
      )

      # Transform column names based on options
      transformed_columns = transform_column_names(columns, aliases, opts)

      # Convert rows to maps
      maps = Enum.map(rows, fn row ->
        transformed_columns
        |> Enum.zip(row)
        |> handle_null_values(opts[:null_handling])
        |> Enum.into(%{})
      end)

      {:ok, maps}
    rescue
      error -> {:error, {:transformation_failed, error}}
    end
  end

  @doc """
  Transform rows with type coercion based on database column types.

  This is a more advanced version that can coerce database types to proper Elixir types
  based on PostgreSQL column type information.
  """
  @spec transform_with_types(list(), list(), map(), keyword()) :: {:ok, list(map())} | {:error, term()}
  def transform_with_types(rows, columns, aliases, options \\ []) do
    try do
      # For now, we'll implement basic type coercion
      # In a full implementation, this would use column type metadata
      # from the database query result
      opts = Keyword.validate!(options,
        keys: :strings,
        transform: :none,
        coerce: :safe,
        preserve: [],
        custom_coercions: %{}
      )

      # Transform column names
      transformed_columns = transform_column_names(columns, aliases, opts)

      # Convert rows with type coercion
      maps = Enum.map(rows, fn row ->
        transformed_columns
        |> Enum.zip(row)
        |> Enum.map(fn {key, value} -> {key, coerce_value(value, opts)} end)
        |> Enum.into(%{})
      end)

      {:ok, maps}
    rescue
      error -> {:error, {:type_coercion_failed, error}}
    end
  end

  # Private helper functions

  defp transform_column_names(columns, aliases, opts) do
    columns
    |> Enum.map(fn col ->
      # Use alias if available, otherwise use column name
      display_name = Map.get(aliases, col, col)
      # Apply transformations
      display_name
      |> apply_name_transform(opts[:transform])
      |> convert_to_key_type(opts[:keys])
    end)
  end

  defp apply_name_transform(name, :none), do: name
  defp apply_name_transform(name, :camelCase) do
    name
    |> String.split(~r/[_\s-]+/)
    |> Enum.with_index()
    |> Enum.map(fn
      {part, 0} -> String.downcase(part)
      {part, _} -> String.capitalize(part)
    end)
    |> Enum.join("")
  end
  defp apply_name_transform(name, :snake_case) do
    name
    |> String.replace(~r/([A-Z])/, "_\\1")
    |> String.downcase()
    |> String.replace(~r/^_/, "")
  end

  defp convert_to_key_type(name, :strings), do: name
  defp convert_to_key_type(name, :atoms), do: String.to_atom(name)
  defp convert_to_key_type(name, :existing_atoms) do
    try do
      String.to_existing_atom(name)
    rescue
      ArgumentError -> name  # Fallback to string if atom doesn't exist
    end
  end

  defp handle_null_values(key_value_pairs, :preserve), do: key_value_pairs
  defp handle_null_values(key_value_pairs, :remove) do
    Enum.reject(key_value_pairs, fn {_key, value} -> is_nil(value) end)
  end

  # Basic type coercion - this would be expanded with proper PostgreSQL type mapping
  defp coerce_value(value, opts) when is_nil(value), do: value
  defp coerce_value(value, opts) do
    case opts[:coerce] do
      :none -> value
      :safe -> safe_coerce(value)
      :all -> aggressive_coerce(value)
      custom_func when is_function(custom_func, 1) -> custom_func.(value)
    end
  end

  # Safe coercion - only coerce obvious cases
  defp safe_coerce(value) when is_binary(value) do
    cond do
      # Try to parse as integer
      Regex.match?(~r/^\-?\d+$/, value) ->
        case Integer.parse(value) do
          {int, ""} -> int
          _ -> value
        end

      # Try to parse as float
      Regex.match?(~r/^\-?\d+\.\d+$/, value) ->
        case Float.parse(value) do
          {float, ""} -> float
          _ -> value
        end

      # Try to parse boolean-like strings
      String.downcase(value) in ["true", "t", "yes", "y", "1"] -> true
      String.downcase(value) in ["false", "f", "no", "n", "0"] -> false

      # Try to parse ISO datetime strings
      String.match?(value, ~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/) ->
        case DateTime.from_iso8601(value) do
          {:ok, dt, _} -> dt
          _ -> value
        end

      # Leave other strings as-is
      true -> value
    end
  end
  defp safe_coerce(value), do: value

  # Aggressive coercion - attempt more transformations
  defp aggressive_coerce(value) do
    # This would include more aggressive type coercion
    # For now, delegate to safe coercion
    safe_coerce(value)
  end

  @doc """
  Stream transformation for large datasets.

  Returns a stream that transforms rows in batches to avoid loading
  all data into memory at once.
  """
  def stream_transform(rows_stream, columns, aliases, options \\ []) do
    opts = Keyword.validate!(options,
      keys: :strings,
      transform: :none,
      coerce_types: false,
      batch_size: 1000
    )

    transformed_columns = transform_column_names(columns, aliases, opts)

    rows_stream
    |> Stream.chunk_every(opts[:batch_size])
    |> Stream.map(fn batch ->
      Enum.map(batch, fn row ->
        transformed_columns
        |> Enum.zip(row)
        |> Enum.into(%{})
      end)
    end)
    |> Stream.concat()
  end
end
