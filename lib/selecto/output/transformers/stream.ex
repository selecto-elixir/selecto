defmodule Selecto.Output.Transformers.Stream do
  @moduledoc """
  Transforms query results to streaming format for memory-efficient processing of large datasets.

  This module provides lazy evaluation of result transformations, allowing processing
  of large result sets without loading all data into memory at once.

  ## Features

  - Lazy evaluation via Elixir Streams
  - Configurable batch sizes for optimization
  - Composition with other transformers (maps, JSON, CSV)
  - Memory-efficient processing for large datasets
  - Backpressure support for downstream consumers

  ## Examples

      # Stream rows as maps
      {:ok, stream} = transform(rows, columns, aliases, :maps)
      Enum.each(stream, &process_row/1)

      # Stream to JSON Lines format
      {:ok, stream} = transform(rows, columns, aliases, {:json, format: :lines})
      Enum.into(stream, file_stream)

      # Stream CSV rows
      {:ok, stream} = transform(rows, columns, aliases, :csv)
      Enum.into(stream, File.stream!("output.csv"))
  """

  @default_batch_size 1000

  @doc """
  Transform rows to a stream with the specified inner format.

  ## Parameters

  - `rows` - List of row data or stream of rows
  - `columns` - List of column names
  - `aliases` - Map of column aliases
  - `inner_format` - The format to transform each row/batch to
  - `options` - Transformation options

  ## Options

  - `:batch_size` - Number of rows to process in each batch. Default: 1000
  - `:parallel` - Whether to process batches in parallel. Default: false
  - Other options are passed to the inner transformer

  ## Inner Format Support

  - `:maps` - Stream of maps
  - `{:maps, opts}` - Stream of maps with options
  - `:json` - Stream of JSON strings (JSON Lines format by default)
  - `{:json, opts}` - Stream of JSON with options
  - `:csv` - Stream of CSV lines
  - `{:csv, opts}` - Stream of CSV with options
  - `:raw` - Stream of raw row lists

  ## Examples

      # Basic streaming to maps
      {:ok, stream} = transform(rows, columns, aliases, :maps)

      # Streaming with custom batch size
      {:ok, stream} = transform(rows, columns, aliases, :maps, batch_size: 500)

      # Streaming to JSON Lines
      {:ok, stream} = transform(rows, columns, aliases, {:json, format: :lines})
  """
  @spec transform(list() | Enumerable.t(), list(), map(), atom() | tuple(), keyword()) ::
    {:ok, Enumerable.t()} | {:error, term()}
  def transform(rows, columns, aliases, inner_format, options \\ []) do
    try do
      batch_size = Keyword.get(options, :batch_size, @default_batch_size)
      parallel = Keyword.get(options, :parallel, false)

      # Create a stream from rows if not already a stream
      row_stream = ensure_stream(rows)

      # Build the transformation pipeline
      result_stream = row_stream
      |> chunk_rows(batch_size)
      |> transform_batches(columns, aliases, inner_format, options, parallel)
      |> Stream.concat()

      {:ok, result_stream}
    rescue
      error -> {:error, {:stream_transform_failed, error}}
    end
  end

  @doc """
  Create a streaming transformation that processes rows one at a time.

  This is more memory efficient for simple transformations but may be slower
  for complex transformations that benefit from batching.
  """
  @spec transform_single(Enumerable.t(), list(), map(), atom() | tuple(), keyword()) ::
    {:ok, Enumerable.t()} | {:error, term()}
  def transform_single(rows, columns, aliases, inner_format, options \\ []) do
    try do
      row_stream = ensure_stream(rows)

      # Prepare column names once
      transformed_columns = prepare_columns(columns, aliases, inner_format, options)

      result_stream = row_stream
      |> Stream.map(fn row ->
        transform_single_row(row, transformed_columns, inner_format, options)
      end)

      {:ok, result_stream}
    rescue
      error -> {:error, {:single_transform_failed, error}}
    end
  end

  @doc """
  Create a stream that yields results in chunks, useful for pagination or batch processing.

  ## Examples

      {:ok, chunked_stream} = transform_chunked(rows, columns, aliases, :maps, chunk_size: 100)
      Enum.each(chunked_stream, fn chunk ->
        # Process 100 maps at a time
        process_batch(chunk)
      end)
  """
  @spec transform_chunked(Enumerable.t(), list(), map(), atom() | tuple(), keyword()) ::
    {:ok, Enumerable.t()} | {:error, term()}
  def transform_chunked(rows, columns, aliases, inner_format, options \\ []) do
    try do
      chunk_size = Keyword.get(options, :chunk_size, @default_batch_size)

      row_stream = ensure_stream(rows)

      # Prepare column names once
      transformed_columns = prepare_columns(columns, aliases, inner_format, options)

      result_stream = row_stream
      |> Stream.chunk_every(chunk_size)
      |> Stream.map(fn batch ->
        Enum.map(batch, fn row ->
          transform_single_row(row, transformed_columns, inner_format, options)
        end)
      end)

      {:ok, result_stream}
    rescue
      error -> {:error, {:chunked_transform_failed, error}}
    end
  end

  @doc """
  Transform and write results directly to an IO device or file stream.

  This is useful for writing large result sets directly to files without
  buffering all data in memory.

  ## Examples

      # Write to file
      File.open!("output.csv", [:write], fn file ->
        transform_to_io(rows, columns, aliases, :csv, file)
      end)
  """
  @spec transform_to_io(Enumerable.t(), list(), map(), atom() | tuple(), IO.device(), keyword()) ::
    :ok | {:error, term()}
  def transform_to_io(rows, columns, aliases, inner_format, io_device, options \\ []) do
    case transform(rows, columns, aliases, inner_format, options) do
      {:ok, stream} ->
        # Write headers if applicable
        maybe_write_header(io_device, columns, aliases, inner_format, options)

        # Stream each item to the IO device
        Enum.each(stream, fn item ->
          IO.write(io_device, format_for_io(item, inner_format))
        end)

        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Private helpers

  defp ensure_stream(rows) when is_list(rows), do: Stream.resource(fn -> rows end, &stream_list/1, fn _ -> :ok end)
  defp ensure_stream(stream), do: stream

  defp stream_list([]), do: {:halt, []}
  defp stream_list([head | tail]), do: {[head], tail}

  defp chunk_rows(stream, batch_size), do: Stream.chunk_every(stream, batch_size)

  defp transform_batches(chunked_stream, columns, aliases, inner_format, options, false = _parallel) do
    Stream.map(chunked_stream, fn batch ->
      transform_batch(batch, columns, aliases, inner_format, options)
    end)
  end

  defp transform_batches(chunked_stream, columns, aliases, inner_format, options, true = _parallel) do
    # Parallel processing using Task.async_stream
    chunked_stream
    |> Task.async_stream(fn batch ->
      transform_batch(batch, columns, aliases, inner_format, options)
    end, max_concurrency: System.schedulers_online())
    |> Stream.map(fn {:ok, result} -> result end)
  end

  defp transform_batch(batch, columns, aliases, inner_format, options) do
    case inner_format do
      :raw ->
        batch

      :maps ->
        transform_batch_to_maps(batch, columns, aliases, options)

      {:maps, map_opts} ->
        transform_batch_to_maps(batch, columns, aliases, Keyword.merge(options, map_opts))

      :json ->
        transform_batch_to_json(batch, columns, aliases, options)

      {:json, json_opts} ->
        transform_batch_to_json(batch, columns, aliases, Keyword.merge(options, json_opts))

      :csv ->
        transform_batch_to_csv(batch, columns, aliases, options)

      {:csv, csv_opts} ->
        transform_batch_to_csv(batch, columns, aliases, Keyword.merge(options, csv_opts))

      _ ->
        batch
    end
  end

  defp transform_batch_to_maps(batch, columns, aliases, _options) do
    column_names = Enum.map(columns, fn col -> Map.get(aliases, col, col) end)

    Enum.map(batch, fn row ->
      column_names
      |> Enum.zip(row)
      |> Enum.into(%{})
    end)
  end

  defp transform_batch_to_json(batch, columns, aliases, options) do
    format = Keyword.get(options, :format, :lines)
    maps = transform_batch_to_maps(batch, columns, aliases, options)

    case format do
      :lines ->
        # JSON Lines format - one JSON object per line
        Enum.map(maps, &Jason.encode!/1)

      :array ->
        # Single JSON array
        [Jason.encode!(maps)]
    end
  end

  defp transform_batch_to_csv(batch, columns, aliases, options) do
    delimiter = Keyword.get(options, :delimiter, ",")
    quote_char = Keyword.get(options, :quote_char, "\"")

    _column_names = Enum.map(columns, fn col -> Map.get(aliases, col, col) end)

    Enum.map(batch, fn row ->
      row
      |> Enum.map(&csv_escape(&1, quote_char))
      |> Enum.join(delimiter)
    end)
  end

  defp csv_escape(nil, _quote_char), do: ""
  defp csv_escape(value, quote_char) when is_binary(value) do
    if String.contains?(value, [",", "\n", quote_char]) do
      escaped = String.replace(value, quote_char, quote_char <> quote_char)
      "#{quote_char}#{escaped}#{quote_char}"
    else
      value
    end
  end
  defp csv_escape(value, _quote_char), do: to_string(value)

  defp prepare_columns(columns, aliases, inner_format, options) do
    key_type = case inner_format do
      {:maps, opts} -> Keyword.get(opts, :keys, :strings)
      _ -> Keyword.get(options, :keys, :strings)
    end

    Enum.map(columns, fn col ->
      name = Map.get(aliases, col, col)
      case key_type do
        :atoms -> String.to_atom(name)
        :existing_atoms ->
          try do
            String.to_existing_atom(name)
          rescue
            ArgumentError -> name
          end
        _ -> name
      end
    end)
  end

  defp transform_single_row(row, _columns, :raw, _options), do: row
  defp transform_single_row(row, columns, :maps, _options) do
    columns |> Enum.zip(row) |> Enum.into(%{})
  end
  defp transform_single_row(row, columns, {:maps, _opts}, _options) do
    columns |> Enum.zip(row) |> Enum.into(%{})
  end
  defp transform_single_row(row, columns, :json, _options) do
    map = columns |> Enum.zip(row) |> Enum.into(%{})
    Jason.encode!(map)
  end
  defp transform_single_row(row, columns, {:json, _opts}, _options) do
    map = columns |> Enum.zip(row) |> Enum.into(%{})
    Jason.encode!(map)
  end
  defp transform_single_row(row, _columns, :csv, options) do
    delimiter = Keyword.get(options, :delimiter, ",")
    row |> Enum.map(&csv_escape(&1, "\"")) |> Enum.join(delimiter)
  end
  defp transform_single_row(row, _columns, {:csv, csv_opts}, _options) do
    delimiter = Keyword.get(csv_opts, :delimiter, ",")
    quote_char = Keyword.get(csv_opts, :quote_char, "\"")
    row |> Enum.map(&csv_escape(&1, quote_char)) |> Enum.join(delimiter)
  end
  defp transform_single_row(row, _columns, _format, _options), do: row

  defp maybe_write_header(io_device, columns, aliases, :csv, options) do
    if Keyword.get(options, :headers, true) do
      delimiter = Keyword.get(options, :delimiter, ",")
      header = columns |> Enum.map(fn col -> Map.get(aliases, col, col) end) |> Enum.join(delimiter)
      IO.puts(io_device, header)
    end
  end
  defp maybe_write_header(io_device, columns, aliases, {:csv, csv_opts}, _options) do
    if Keyword.get(csv_opts, :headers, true) do
      delimiter = Keyword.get(csv_opts, :delimiter, ",")
      header = columns |> Enum.map(fn col -> Map.get(aliases, col, col) end) |> Enum.join(delimiter)
      IO.puts(io_device, header)
    end
  end
  defp maybe_write_header(_io_device, _columns, _aliases, _format, _options), do: :ok

  defp format_for_io(item, :csv), do: item <> "\n"
  defp format_for_io(item, {:csv, _}), do: item <> "\n"
  defp format_for_io(item, :json), do: item <> "\n"
  defp format_for_io(item, {:json, opts}) do
    if Keyword.get(opts, :format) == :lines, do: item <> "\n", else: item
  end
  defp format_for_io(item, _format) when is_binary(item), do: item <> "\n"
  defp format_for_io(item, _format), do: inspect(item) <> "\n"
end
