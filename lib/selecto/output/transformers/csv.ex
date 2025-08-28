defmodule Selecto.Output.Transformers.CSV do
  @moduledoc """
  CSV transformer for Selecto query results.
  
  Converts query results to CSV format with configurable options:
  - Headers: Include/exclude column headers
  - Delimiter: Custom field separators (comma, tab, semicolon, etc.)
  - Quoting: Proper escaping for special characters
  - Null handling: Custom null value representation
  - Streaming: Efficient processing for large datasets
  
  ## Options
  
  - `:headers` - Include headers (default: true)
  - `:delimiter` - Field separator (default: ",")
  - `:quote_char` - Quote character (default: "\"")
  - `:null_value` - Null representation (default: "")
  - `:force_quotes` - Quote all fields (default: false)
  - `:line_ending` - Line ending style (default: "\\n")
  
  ## Examples
  
      iex> Selecto.Output.Transformers.CSV.transform(
      ...>   [["Alice", 25], ["Bob", 30]], 
      ...>   ["name", "age"], 
      ...>   ["name", "age"]
      ...> )
      {:ok, "name,age\\nAlice,25\\nBob,30\\n"}
      
      iex> Selecto.Output.Transformers.CSV.transform(
      ...>   [["John, Jr.", nil]], 
      ...>   ["full_name", "age"], 
      ...>   ["full_name", "age"],
      ...>   delimiter: ";", null_value: "N/A"
      ...> )
      {:ok, "full_name;age\\n\\"John, Jr.\\";N/A\\n"}
  """
  
  alias Selecto.Output.TypeCoercion
  alias Selecto.Error
  
  @behaviour Selecto.Output.Formats
  
  @default_options [
    headers: true,
    delimiter: ",",
    quote_char: "\"",
    null_value: "",
    force_quotes: false,
    line_ending: "\n"
  ]
  
  @doc """
  Transform query results to CSV format.
  
  Returns the complete CSV as a string with optional headers and properly escaped values.
  """
  def transform(rows, columns, aliases, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)
    
    case validate_options(opts, columns) do
      :ok ->
        case coerce_rows(rows, columns) do
          {:ok, coerced_rows} ->
            csv_content = build_csv(coerced_rows, aliases, opts)
            {:ok, csv_content}
          {:error, error} ->
            {:error, error}
        end
      {:error, error} ->
        {:error, error}
    end
  rescue
    error ->
      {:error, Error.transformation_error(
        "CSV transformation failed: #{Exception.message(error)}",
        %{rows: length(rows), columns: length(columns)}
      )}
  end
  
  @doc """
  Transform query results to CSV format with streaming support.
  
  Returns a stream that yields CSV lines (including header if configured).
  Efficient for large datasets as it processes rows one at a time.
  """
  def stream_transform(rows, columns, aliases, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)
    
    case validate_options(opts, columns) do
      :ok ->
        csv_stream = build_csv_stream(rows, columns, aliases, opts)
        {:ok, csv_stream}
        
      {:error, error} ->
        {:error, error}
    end
  rescue
    error ->
      {:error, Error.transformation_error(
        "CSV stream transformation failed: #{Exception.message(error)}",
        %{columns: length(columns)}
      )}
  end
  
  # Build complete CSV content
  defp build_csv(rows, aliases, opts) do
    lines = []
    
    # Add header if requested
    lines = if opts[:headers] do
      header_line = build_csv_line(aliases, opts)
      [header_line | lines]
    else
      lines
    end
    
    # Add data rows
    lines = Enum.reduce(rows, lines, fn row, acc ->
      row_line = build_csv_line(row, opts)
      [row_line | acc]
    end)
    
    # Join all lines with line ending
    result = lines
    |> Enum.reverse()
    |> Enum.join(opts[:line_ending])
    
    # Add final line ending only if there's content
    if result == "" do
      ""
    else
      result <> opts[:line_ending]
    end
  end
  
  # Build CSV stream
  defp build_csv_stream(rows, columns, aliases, opts) do
    Stream.concat([
      # Header stream
      if opts[:headers] do
        [build_csv_line(aliases, opts)]
      else
        []
      end,
      
      # Data rows stream
      Stream.map(rows, fn row ->
        case coerce_row(row, columns) do
          {:ok, coerced_row} ->
            build_csv_line(coerced_row, opts)
            
          {:error, _} ->
            # For streaming, we'll convert problematic values to strings
            string_row = Enum.map(row, &safe_to_string/1)
            build_csv_line(string_row, opts)
        end
      end)
    ])
    |> Stream.map(&(&1 <> opts[:line_ending]))
  end
  
  # Build a single CSV line from values
  defp build_csv_line(values, opts) do
    values
    |> Enum.map(&format_csv_field(&1, opts))
    |> Enum.join(opts[:delimiter])
  end
  
  # Format a single CSV field with proper escaping
  defp format_csv_field(value, opts) do
    cond do
      is_nil(value) ->
        opts[:null_value]
        
      opts[:force_quotes] ->
        quote_field(format_value_as_string(value), opts)
        
      true ->
        string_value = format_value_as_string(value)
        
        if needs_quoting?(string_value, opts) do
          quote_field(string_value, opts)
        else
          string_value
        end
    end
  end
  
  # Convert various data types to string representation
  defp format_value_as_string(value) when is_map(value) and not is_struct(value) do
    # Convert complex data types (maps, but not structs) to JSON string
    case Jason.encode(value) do
      {:ok, json_string} -> json_string
      {:error, _reason} -> inspect(value)
    end
  end
  
  defp format_value_as_string(value) when is_list(value) do
    # Convert lists to JSON string
    case Jason.encode(value) do
      {:ok, json_string} -> json_string
      {:error, _reason} -> inspect(value)
    end
  end
  
  defp format_value_as_string(value) do
    # For all other types (including structs like Date, DateTime, Decimal), use to_string/1
    to_string(value)
  end
  
  # Check if a field needs quoting
  defp needs_quoting?(value, opts) do
    delimiter = opts[:delimiter]
    quote_char = opts[:quote_char]
    line_ending = opts[:line_ending]
    
    # Quote if contains delimiter, quote char, line endings, or common CSV problematic characters
    String.contains?(value, [delimiter, quote_char, line_ending, "\r", "\n", ","])
  end
  
  # Quote and escape a field
  defp quote_field(value, opts) do
    quote_char = opts[:quote_char]
    escaped_value = String.replace(value, quote_char, quote_char <> quote_char)
    quote_char <> escaped_value <> quote_char
  end
  
  # Coerce all rows
  defp coerce_rows(rows, columns) do
    try do
      coerced_rows = Enum.map(rows, fn row ->
        case coerce_row(row, columns) do
          {:ok, coerced_row} -> coerced_row
          {:error, reason} -> raise RuntimeError, message: "Row coercion failed: #{reason}"
        end
      end)
      
      {:ok, coerced_rows}
    rescue
      error ->
        {:error, Error.transformation_error("Row coercion failed: #{Exception.message(error)}")}
    end
  end
  
  # Coerce a single row
  defp coerce_row(row, columns) when length(row) == length(columns) do
    try do
      coerced_values = 
        Enum.zip(row, columns)
        |> Enum.map(fn {value, column_info} ->
          column_type = Map.get(column_info, :type) || Map.get(column_info, "type")
          TypeCoercion.coerce_value(value, column_type, :safe, %{})
        end)
      
      {:ok, coerced_values}
    rescue
      error ->
        {:error, "Type coercion failed: #{Exception.message(error)}"}
    end
  end
  
  defp coerce_row(row, columns) do
    {:error, "Row length #{length(row)} does not match columns length #{length(columns)}"}
  end
  
  # Safe string conversion for streaming
  defp safe_to_string(value) when is_nil(value), do: nil
  defp safe_to_string(value) when is_binary(value), do: value
  defp safe_to_string(value), do: inspect(value)
  
  # Validate transformation options
  defp validate_options(opts, columns \\ []) do
    cond do
      not is_boolean(opts[:headers]) ->
        {:error, Error.transformation_error("headers option must be a boolean", %{columns: length(columns)})}
        
      not is_binary(opts[:delimiter]) or String.length(opts[:delimiter]) != 1 ->
        {:error, Error.transformation_error("delimiter must be a single character string", %{columns: length(columns)})}
        
      not is_binary(opts[:quote_char]) or String.length(opts[:quote_char]) != 1 ->
        {:error, Error.transformation_error("quote_char must be a single character string", %{columns: length(columns)})}
        
      not is_binary(opts[:null_value]) ->
        {:error, Error.transformation_error("null_value must be a string", %{columns: length(columns)})}
        
      not is_boolean(opts[:force_quotes]) ->
        {:error, Error.transformation_error("force_quotes option must be a boolean", %{columns: length(columns)})}
        
      not is_binary(opts[:line_ending]) ->
        {:error, Error.transformation_error("line_ending must be a string", %{columns: length(columns)})}
        
      opts[:delimiter] == opts[:quote_char] ->
        {:error, Error.transformation_error("delimiter and quote_char cannot be the same", %{columns: length(columns)})}
        
      true ->
        :ok
    end
  end
end