defmodule Selecto.Output.Formats do
  @moduledoc """
  Format registry and configuration system for Selecto output formats.

  This module manages the registration and configuration of different output formats
  and provides the main interface for transforming query results from the default
  {rows, columns, aliases} format to other formats like maps, structs, JSON, CSV, etc.
  """

  @type format_spec :: :raw | :maps | :json | :csv |
                   {:maps, keyword()} |
                   {:structs, module()} |
                   {:json, keyword()} |
                   {:csv, keyword()} |
                   {:stream, format_spec()} |
                   {:typed_maps, keyword()}

  @doc """
  Transform query results from the default format to the specified output format.

  ## Parameters

  - `result` - The query result tuple `{rows, columns, aliases}`
  - `format` - The output format specification
  - `options` - Additional options for the transformation

  ## Format Specifications

  - `:raw` - Return the original {rows, columns, aliases} format (no transformation)
  - `:maps` - Transform to list of maps with string keys
  - `{:maps, options}` - Transform to maps with additional options
  - `{:structs, struct_module}` - Transform to list of structs
  - `:json` - Transform to JSON string
  - `{:json, options}` - Transform to JSON with options
  - `:csv` - Transform to CSV string
  - `{:csv, options}` - Transform to CSV with options
  - `{:stream, format}` - Transform to stream (for large datasets)

  ## Examples

      # Original format (no transformation)
      {:ok, {rows, columns, aliases}} = transform(result, :raw)

      # Maps with string keys
      {:ok, maps} = transform(result, :maps)

      # Maps with atom keys
      {:ok, maps} = transform(result, {:maps, keys: :atoms})

      # Custom struct
      {:ok, structs} = transform(result, {:structs, Customer})

      # JSON string
      {:ok, json_string} = transform(result, :json)

      # CSV with headers
      {:ok, csv_string} = transform(result, {:csv, headers: true})
  """
  @spec transform({list(), list(), map()}, format_spec(), keyword()) ::
    {:ok, term()} | {:error, term()}

  def transform({rows, columns, aliases} = result, format, options \\ []) do
    case format do
      :raw ->
        {:ok, result}

      :maps ->
        Selecto.Output.Transformers.Maps.transform(rows, columns, aliases, [])

      {:maps, map_options} ->
        Selecto.Output.Transformers.Maps.transform(rows, columns, aliases, map_options)

      {:structs, struct_module} ->
        Selecto.Output.Transformers.Structs.transform(rows, columns, aliases, struct_module, options)

      :json ->
        Selecto.Output.Transformers.Json.transform(rows, columns, aliases, [])

      {:json, json_options} ->
        Selecto.Output.Transformers.Json.transform(rows, columns, aliases, json_options)

      :csv ->
        Selecto.Output.Transformers.Csv.transform(rows, columns, aliases, [])

      {:csv, csv_options} ->
        Selecto.Output.Transformers.Csv.transform(rows, columns, aliases, csv_options)

      {:stream, inner_format} ->
        # For streaming, we'll delegate to the streaming transformer
        Selecto.Output.Transformers.Stream.transform(rows, columns, aliases, inner_format, options)

      {:typed_maps, type_options} ->
        # For typed maps with type coercion
        Selecto.Output.Transformers.Maps.transform_with_types(rows, columns, aliases, type_options)

      unknown_format ->
        {:error, {:unknown_format, unknown_format}}
    end
  end

  @doc """
  Get available format types and their descriptions.

  Returns a list of available formats with their capabilities and options.
  """
  def available_formats() do
    [
      %{
        format: :raw,
        description: "Original {rows, columns, aliases} format",
        supports_streaming: false,
        memory_efficient: true
      },
      %{
        format: :maps,
        description: "List of maps with configurable key types",
        supports_streaming: true,
        memory_efficient: false,
        options: [
          keys: [:strings, :atoms, :existing_atoms],
          transform: [:none, :camelCase, :snake_case]
        ]
      },
      %{
        format: :structs,
        description: "List of structs with compile-time field validation",
        supports_streaming: true,
        memory_efficient: false,
        options: [
          auto_generate: :boolean,
          strict_fields: :boolean
        ]
      },
      %{
        format: :json,
        description: "JSON string with configurable serialization",
        supports_streaming: true,
        memory_efficient: false,
        options: [
          include_meta: :boolean,
          pretty: :boolean,
          null_handling: [:preserve, :remove]
        ]
      },
      %{
        format: :csv,
        description: "CSV string with configurable formatting",
        supports_streaming: true,
        memory_efficient: true,
        options: [
          headers: :boolean,
          delimiter: :string,
          quote_char: :string,
          escape_char: :string
        ]
      }
    ]
  end

  @doc """
  Validate format specification and options.

  Returns `:ok` if the format is valid, `{:error, reason}` otherwise.
  """
  def validate_format(format, options \\ []) do
    case format do
      :raw -> :ok
      :maps -> :ok
      :structs -> :ok
      :json -> :ok
      :csv -> :ok
      {:maps, map_options} -> validate_maps_options(map_options)
      {:structs, struct_module} -> validate_struct_module(struct_module)
      {:json, json_options} -> validate_json_options(json_options)
      {:csv, csv_options} -> validate_csv_options(csv_options)
      {:stream, inner_format} -> validate_format(inner_format, options)
      {:typed_maps, type_options} -> validate_typed_maps_options(type_options)
      _ -> {:error, {:invalid_format, format}}
    end
  end

  # Private validation functions
  defp validate_maps_options(options) do
    valid_keys = [:keys, :transform, :coerce_types]
    valid_key_types = [:strings, :atoms, :existing_atoms]
    valid_transforms = [:none, :camelCase, :snake_case]

    case Keyword.validate(options, keys: :strings, transform: :none, coerce_types: false) do
      {:ok, validated} ->
        with :ok <- validate_option_value(validated[:keys], valid_key_types, :keys),
             :ok <- validate_option_value(validated[:transform], valid_transforms, :transform) do
          :ok
        end
      {:error, invalid_keys} ->
        {:error, {:invalid_map_options, invalid_keys}}
    end
  end

  defp validate_struct_module(module) when is_atom(module) do
    if Code.ensure_loaded?(module) do
      :ok
    else
      {:error, {:struct_module_not_found, module}}
    end
  end
  defp validate_struct_module(module), do: {:error, {:invalid_struct_module, module}}

  defp validate_json_options(options) do
    valid_keys = [:include_meta, :pretty, :null_handling]

    case Keyword.validate(options, include_meta: false, pretty: false, null_handling: :preserve) do
      {:ok, _} -> :ok
      {:error, invalid_keys} -> {:error, {:invalid_json_options, invalid_keys}}
    end
  end

  defp validate_csv_options(options) do
    valid_keys = [:headers, :delimiter, :quote_char, :escape_char]

    case Keyword.validate(options, headers: true, delimiter: ",", quote_char: "\"", escape_char: "\\") do
      {:ok, _} -> :ok
      {:error, invalid_keys} -> {:error, {:invalid_csv_options, invalid_keys}}
    end
  end

  defp validate_typed_maps_options(options) do
    valid_keys = [:coerce, :preserve, :custom_coercions]
    valid_coerce_values = [:all, :safe, :none]

    case Keyword.validate(options, coerce: :safe, preserve: [], custom_coercions: %{}) do
      {:ok, validated} ->
        validate_option_value(validated[:coerce], valid_coerce_values, :coerce)
      {:error, invalid_keys} ->
        {:error, {:invalid_typed_maps_options, invalid_keys}}
    end
  end

  defp validate_option_value(value, valid_values, option_name) do
    if value in valid_values do
      :ok
    else
      {:error, {:invalid_option_value, option_name, value, valid_values}}
    end
  end

  @doc """
  Get performance characteristics for a given format.

  Returns information about memory usage, processing time, and scalability.
  """
  def performance_info(format) do
    base_format = case format do
      {format_type, _options} -> format_type
      format_type -> format_type
    end

    case base_format do
      :raw ->
        %{memory_overhead: 0, processing_time: 0, streaming_capable: false, recommended_max_rows: :unlimited}
      :maps ->
        %{memory_overhead: 25, processing_time: 15, streaming_capable: true, recommended_max_rows: 100_000}
      :structs ->
        %{memory_overhead: 15, processing_time: 5, streaming_capable: true, recommended_max_rows: 500_000}
      :json ->
        %{memory_overhead: 10, processing_time: 30, streaming_capable: true, recommended_max_rows: 50_000}
      :csv ->
        %{memory_overhead: 5, processing_time: 20, streaming_capable: true, recommended_max_rows: 1_000_000}
      :stream ->
        %{memory_overhead: -80, processing_time: 10, streaming_capable: true, recommended_max_rows: :unlimited}
      _ ->
        %{memory_overhead: :unknown, processing_time: :unknown, streaming_capable: :unknown, recommended_max_rows: :unknown}
    end
  end
end
