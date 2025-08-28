defmodule Selecto.Output.Transformers.Json do
  @moduledoc """
  JSON transformer for Selecto query results.

  Provides JSON serialization with configurable options for null handling,
  metadata inclusion, pretty printing, and streaming support for large datasets.

  ## Options

  - `:include_meta` - Include query metadata in response (default: false)
  - `:pretty` - Pretty print JSON output (default: false)
  - `:null_handling` - How to handle nil values: `:null`, `:omit`, `:empty_string` (default: `:null`)
  - `:keys` - Key format: `:strings`, `:atoms` (default: `:strings`)
  - `:coerce_types` - Enable type coercion (default: false)
  - `:date_format` - Date serialization format: `:iso8601`, `:unix`, `:string` (default: `:iso8601`)
  - `:decimal_format` - Decimal format: `:string`, `:float` (default: `:string`)

  ## Examples

      # Basic JSON serialization
      {:ok, json} = Json.transform(rows, columns, %{}, [])

      # JSON with metadata
      {:ok, json} = Json.transform(rows, columns, %{}, [include_meta: true])

      # Pretty printed JSON with null omission
      {:ok, json} = Json.transform(rows, columns, %{}, [
        pretty: true,
        null_handling: :omit
      ])

      # Streaming for large datasets
      stream = Json.stream_transform(rows, columns, %{}, [pretty: true])
  """

  alias Selecto.Error
  alias Selecto.Output.TypeCoercion

  @type json_option ::
          {:include_meta, boolean()}
          | {:pretty, boolean()}
          | {:null_handling, :null | :omit | :empty_string}
          | {:keys, :strings | :atoms}
          | {:coerce_types, boolean()}
          | {:date_format, :iso8601 | :unix | :string}
          | {:decimal_format, :string | :float}

  @type json_options :: [json_option()]

  @doc """
  Transform query results to JSON string.

  Returns a JSON string representation of the query results with configurable
  serialization options.
  """
  @spec transform(list(list()), list(String.t()), map(), json_options()) ::
          {:ok, String.t()} | {:error, Error.t()}
  def transform(rows, columns, aliases, options \\ []) do
    try do
      # Parse and validate options
      opts = parse_options(options)

      # First convert to maps format
      case convert_to_maps(rows, columns, aliases, opts) do
        {:ok, maps} ->
          # Apply null handling
          processed_maps = apply_null_handling(maps, opts.null_handling)

          # Create final JSON structure
          json_data = if opts.include_meta do
            %{
              "data" => processed_maps,
              "meta" => build_metadata(rows, columns, aliases)
            }
          else
            processed_maps
          end

          # Serialize to JSON
          case serialize_json(json_data, opts) do
            {:ok, json_string} -> {:ok, json_string}
            {:error, reason} ->
              {:error, Error.transformation_error("JSON serialization failed: #{inspect(reason)}")}
          end

        {:error, reason} ->
          {:error, Error.transformation_error("Failed to convert to maps: #{inspect(reason)}")}
      end
    rescue
      error ->
        {:error, Error.transformation_error("JSON transform error: #{inspect(error)}")}
    end
  end

  @doc """
  Transform query results to a JSON stream for large datasets.

  Returns a stream that yields JSON strings, useful for processing
  large result sets without loading everything into memory.
  """
  @spec stream_transform(list(list()) | Enumerable.t(), list(String.t()), map(), json_options()) ::
          Enumerable.t()
  def stream_transform(rows, columns, aliases, options \\ []) do
    opts = parse_options(options)

    Stream.resource(
      fn ->
        # Initialize streaming state
        {rows, 0, opts.include_meta}
      end,
      fn
        {remaining_rows, index, needs_meta} ->
          case remaining_rows do
            [] ->
              if needs_meta and index == 0 do
                # If no data but metadata requested, return empty data with meta
                json_data = %{
                  "data" => [],
                  "meta" => build_metadata([], columns, aliases)
                }
                case serialize_json(json_data, opts) do
                  {:ok, json} -> {[json], {[], 1, false}}
                  {:error, _} -> {:halt, {[], 1, false}}
                end
              else
                {:halt, {[], index, false}}
              end

            [row | rest] ->
              case convert_single_row_to_map(row, columns, aliases, opts) do
                {:ok, map} ->
                  processed_map = apply_null_handling([map], opts.null_handling) |> List.first()

                  json_item = if needs_meta and index == 0 do
                    # First item includes metadata wrapper
                    %{
                      "data" => [processed_map],
                      "meta" => build_metadata([row | rest], columns, aliases)
                    }
                  else
                    processed_map
                  end

                  case serialize_json(json_item, opts) do
                    {:ok, json} -> {[json], {rest, index + 1, false}}
                    {:error, _} -> {:halt, {rest, index + 1, false}}
                  end

                {:error, _} -> {:halt, {rest, index + 1, false}}
              end
          end

        :done ->
          {:halt, :done}
      end,
      fn _acc -> :ok end
    )
  end

  # Private functions

  defp parse_options(options) do
    %{
      include_meta: Keyword.get(options, :include_meta, false),
      pretty: Keyword.get(options, :pretty, false),
      null_handling: Keyword.get(options, :null_handling, :null),
      keys: Keyword.get(options, :keys, :strings),
      coerce_types: Keyword.get(options, :coerce_types, false),
      date_format: Keyword.get(options, :date_format, :iso8601),
      decimal_format: Keyword.get(options, :decimal_format, :string)
    }
  end

  defp convert_to_maps(rows, columns, aliases, opts) do
    # Use the Maps transformer logic to convert to maps first
    maps_options = [
      keys: opts.keys,
      coerce_types: opts.coerce_types
    ]

    # Convert each row to a map
    try do
      maps = Enum.map(rows, fn row ->
        case convert_single_row_to_map(row, columns, aliases, opts) do
          {:ok, map} -> map
          {:error, _} -> %{}  # Return empty map on error
        end
      end)

      {:ok, maps}
    rescue
      error -> {:error, error}
    end
  end

  defp convert_single_row_to_map(row, columns, aliases, opts) do
    try do
      # Create column name -> value mapping
      map_data = columns
      |> Enum.with_index()
      |> Enum.map(fn {column, index} ->
        value = Enum.at(row, index)

        # Get the effective column name (use alias if available)
        effective_name = Map.get(aliases, column, column)

        # Apply key transformation
        final_name = case opts.keys do
          :atoms -> ensure_atom(effective_name)
          :strings -> to_string(effective_name)
        end

        # Apply type coercion if enabled
        coerced_value = if opts.coerce_types do
          # For JSON, we need to be more careful about type coercion
          # to ensure JSON serializability
          coerce_for_json(value, opts)
        else
          value
        end

        {final_name, coerced_value}
      end)
      |> Enum.into(%{})

      {:ok, map_data}
    rescue
      error -> {:error, error}
    end
  end

  defp coerce_for_json(value, opts) when is_nil(value), do: nil

  defp coerce_for_json(%Decimal{} = decimal, opts) do
    case opts.decimal_format do
      :string -> Decimal.to_string(decimal)
      :float -> Decimal.to_float(decimal)
    end
  end

  defp coerce_for_json(%DateTime{} = dt, opts) do
    case opts.date_format do
      :iso8601 -> DateTime.to_iso8601(dt)
      :unix -> DateTime.to_unix(dt)
      :string -> to_string(dt)
    end
  end

  defp coerce_for_json(%Date{} = date, opts) do
    case opts.date_format do
      :iso8601 -> Date.to_iso8601(date)
      :unix -> date |> Date.to_erl() |> :calendar.datetime_to_gregorian_seconds() |> Kernel.-(62167219200)
      :string -> to_string(date)
    end
  end

  defp coerce_for_json(%Time{} = time, _opts) do
    Time.to_iso8601(time)
  end

  defp coerce_for_json(value, _opts) when is_binary(value), do: value
  defp coerce_for_json(value, _opts) when is_number(value), do: value
  defp coerce_for_json(value, _opts) when is_boolean(value), do: value
  defp coerce_for_json(value, _opts) when is_list(value), do: value
  defp coerce_for_json(value, _opts) when is_map(value), do: value
  defp coerce_for_json(value, _opts), do: to_string(value)

  defp apply_null_handling(maps, :null), do: maps

  defp apply_null_handling(maps, :omit) do
    Enum.map(maps, fn map ->
      map
      |> Enum.reject(fn {_k, v} -> is_nil(v) end)
      |> Enum.into(%{})
    end)
  end

  defp apply_null_handling(maps, :empty_string) do
    Enum.map(maps, fn map ->
      map
      |> Enum.map(fn {k, v} -> {k, if(is_nil(v), do: "", else: v)} end)
      |> Enum.into(%{})
    end)
  end

  defp build_metadata(rows, columns, aliases) do
    %{
      "total_rows" => length(rows),
      "columns" => columns,
      "aliases" => aliases,
      "query_time_ms" => nil,  # Could be populated by caller
      "generated_at" => DateTime.utc_now() |> DateTime.to_iso8601()
    }
  end

  defp serialize_json(data, opts) do
    jason_options = if opts.pretty do
      [pretty: true]
    else
      []
    end

    case Jason.encode(data, jason_options) do
      {:ok, json} -> {:ok, json}
      {:error, reason} -> {:error, reason}
    end
  end

  defp ensure_atom(name) when is_atom(name), do: name
  defp ensure_atom(name) when is_binary(name) do
    try do
      String.to_existing_atom(name)
    rescue
      ArgumentError -> String.to_atom(name)
    end
  end
  defp ensure_atom(name), do: name |> to_string() |> ensure_atom()
end
