defmodule Selecto.Advanced.ValuesClause do
  @moduledoc """
  VALUES clause support for inline table generation and data transformations.
  
  VALUES clauses allow creating inline tables from literal data, useful for
  lookup tables, data transformations, and testing scenarios.
  
  ## Examples
  
      # Basic VALUES table
      selecto
      |> Selecto.with_values([
          ["PG", "Family Friendly", 1],
          ["PG-13", "Teen", 2],
          ["R", "Adult", 3]
        ], 
        columns: ["rating_code", "description", "sort_order"],
        as: "rating_lookup"
      )
      
      # Map-based VALUES
      selecto
      |> Selecto.with_values([
          %{month: 1, name: "January", days: 31},
          %{month: 2, name: "February", days: 28},
          %{month: 3, name: "March", days: 31}
        ], as: "months")
  """
  
  defmodule Spec do
    @moduledoc """
    Specification for a VALUES clause operation.
    """
    defstruct [
      :id,                    # Unique identifier for the VALUES clause
      :data,                  # List of data rows (lists or maps)
      :columns,               # Column names (inferred from data or explicit)
      :alias,                 # Alias for the VALUES table
      :data_type,             # :list_of_lists, :list_of_maps
      :column_types,          # Inferred data types for each column
      :validated              # Boolean indicating if data has been validated
    ]
    
    @type data_row :: [term()] | map()
    @type column_spec :: String.t() | atom()
    
    @type t :: %__MODULE__{
      id: String.t(),
      data: [data_row()],
      columns: [column_spec()],
      alias: String.t(),
      data_type: :list_of_lists | :list_of_maps,
      column_types: map(),
      validated: boolean()
    }
  end
  
  defmodule ValidationError do
    @moduledoc """
    Error raised when VALUES clause data is invalid.
    """
    defexception [:type, :message, :details]
    
    @type t :: %__MODULE__{
      type: :inconsistent_columns | :empty_data | :type_mismatch | :validation_error,
      message: String.t(),
      details: map()
    }
  end
  
  @doc """
  Create a VALUES clause specification.
  
  ## Parameters
  
  - `data` - List of data rows (lists or maps)
  - `opts` - Options including :columns, :as (alias)
  
  ## Examples
  
      # List of lists format
      ValuesClause.create_values_clause([
        ["PG", "Family", 1],
        ["R", "Adult", 3]
      ], columns: ["code", "desc", "order"], as: "ratings")
      
      # Map format (columns inferred)
      ValuesClause.create_values_clause([
        %{id: 1, name: "Alice"},
        %{id: 2, name: "Bob"}
      ], as: "users")
  """
  def create_values_clause(data, opts \\ []) do
    alias_name = Keyword.get(opts, :as, "values_table")
    explicit_columns = Keyword.get(opts, :columns, [])
    
    spec = %Spec{
      id: generate_values_id(alias_name),
      data: data,
      columns: [],
      alias: alias_name,
      data_type: detect_data_type(data),
      column_types: %{},
      validated: false
    }
    
    # Validate and process the data
    case validate_and_process_data(spec, explicit_columns) do
      {:ok, processed_spec} -> processed_spec
      {:error, validation_error} -> raise validation_error
    end
  end
  
  @doc """
  Validate VALUES clause data and infer column information.
  
  Ensures data consistency and infers column names and types.
  """
  def validate_and_process_data(%Spec{} = spec, explicit_columns) do
    with :ok <- validate_data_not_empty(spec.data),
         {:ok, data_type} <- validate_data_consistency(spec.data),
         {:ok, columns} <- determine_columns(spec.data, data_type, explicit_columns),
         {:ok, column_types} <- infer_column_types(spec.data, data_type, columns),
         :ok <- validate_data_completeness(spec.data, data_type, columns) do
      
      processed_spec = %{spec |
        data_type: data_type,
        columns: columns,
        column_types: column_types,
        validated: true
      }
      
      {:ok, processed_spec}
    else
      {:error, reason} -> {:error, reason}
    end
  end
  
  # Validate that data is not empty
  defp validate_data_not_empty([]), do: {:error, %ValidationError{
    type: :empty_data,
    message: "VALUES clause cannot have empty data",
    details: %{}
  }}
  defp validate_data_not_empty(_data), do: :ok
  
  # Validate that all data rows have consistent structure
  defp validate_data_consistency([first_row | rest]) do
    data_type = detect_data_type([first_row])
    
    case data_type do
      :list_of_lists ->
        validate_list_consistency([first_row | rest])
        
      :list_of_maps ->
        validate_map_consistency([first_row | rest])
        
      _ -> {:error, %ValidationError{
        type: :validation_error,
        message: "Unsupported data format for VALUES clause",
        details: %{data_type: data_type}
      }}
    end
  end
  
  # Validate that all rows are lists with same length
  defp validate_list_consistency([first_row | rest]) when is_list(first_row) do
    expected_length = length(first_row)
    
    invalid_rows = 
      rest
      |> Enum.with_index(1)
      |> Enum.filter(fn {row, _index} -> 
        not is_list(row) or (is_list(row) and length(row) != expected_length)
      end)
    
    if Enum.empty?(invalid_rows) do
      {:ok, :list_of_lists}
    else
      [{invalid_row, index}] = Enum.take(invalid_rows, 1)
      actual_length = if is_list(invalid_row), do: length(invalid_row), else: 0
      {:error, %ValidationError{
        type: :inconsistent_columns,
        message: "Row #{index + 1} has #{actual_length} columns, expected #{expected_length}",
        details: %{expected_length: expected_length, actual_length: actual_length, row_index: index + 1}
      }}
    end
  end
  
  # Validate that all rows are maps with consistent keys
  defp validate_map_consistency([first_row | rest]) when is_map(first_row) do
    expected_keys = Map.keys(first_row) |> Enum.map(&to_string/1) |> Enum.sort()
    
    invalid_rows = 
      rest
      |> Enum.with_index(1)
      |> Enum.filter(fn {row, _index} ->
        not is_map(row) or (Map.keys(row) |> Enum.map(&to_string/1) |> Enum.sort()) != expected_keys
      end)
    
    if Enum.empty?(invalid_rows) do
      {:ok, :list_of_maps}
    else
      [{invalid_row, index}] = Enum.take(invalid_rows, 1)
      actual_keys = if is_map(invalid_row), do: Map.keys(invalid_row) |> Enum.map(&to_string/1) |> Enum.sort(), else: []
      
      {:error, %ValidationError{
        type: :inconsistent_columns,
        message: "Row #{index + 1} has different keys than first row",
        details: %{
          expected_keys: expected_keys,
          actual_keys: actual_keys,
          row_index: index + 1
        }
      }}
    end
  end
  
  # Determine column names from data and explicit columns
  defp determine_columns(data, :list_of_lists, explicit_columns) do
    case explicit_columns do
      [] ->
        # Generate default column names
        [first_row | _] = data
        column_count = length(first_row)
        columns = Enum.map(1..column_count, &"column#{&1}")
        {:ok, columns}
        
      columns when is_list(columns) ->
        [first_row | _] = data
        if length(columns) == length(first_row) do
          {:ok, Enum.map(columns, &to_string/1)}
        else
          {:error, %ValidationError{
            type: :inconsistent_columns,
            message: "Number of explicit columns (#{length(columns)}) doesn't match data columns (#{length(first_row)})",
            details: %{explicit_count: length(columns), data_count: length(first_row)}
          }}
        end
    end
  end
  
  defp determine_columns(data, :list_of_maps, explicit_columns) do
    [first_row | _] = data
    inferred_columns = Map.keys(first_row) |> Enum.map(&to_string/1) |> Enum.sort()
    
    case explicit_columns do
      [] -> {:ok, inferred_columns}
      columns when is_list(columns) ->
        explicit_strings = Enum.map(columns, &to_string/1) |> Enum.sort()
        if explicit_strings == inferred_columns do
          {:ok, explicit_strings}
        else
          {:error, %ValidationError{
            type: :inconsistent_columns,
            message: "Explicit columns don't match map keys",
            details: %{explicit_columns: explicit_strings, map_keys: inferred_columns}
          }}
        end
    end
  end
  
  # Infer column types from data
  defp infer_column_types(data, data_type, columns) do
    column_types = case data_type do
      :list_of_lists ->
        infer_types_from_lists(data, columns)
        
      :list_of_maps ->
        infer_types_from_maps(data, columns)
    end
    
    {:ok, column_types}
  end
  
  # Infer types from list of lists format
  defp infer_types_from_lists(data, columns) do
    columns
    |> Enum.with_index()
    |> Enum.into(%{}, fn {column, index} ->
      # Sample values from this column position
      sample_values = 
        data
        |> Enum.take(10)  # Sample first 10 rows
        |> Enum.map(&Enum.at(&1, index))
        |> Enum.reject(&is_nil/1)
      
      inferred_type = infer_type_from_values(sample_values)
      {column, inferred_type}
    end)
  end
  
  # Infer types from list of maps format
  defp infer_types_from_maps(data, columns) do
    columns
    |> Enum.into(%{}, fn column ->
      column_key = String.to_existing_atom(column)
      
      # Sample values from this column
      sample_values = 
        data
        |> Enum.take(10)  # Sample first 10 rows
        |> Enum.map(&Map.get(&1, column_key))
        |> Enum.reject(&is_nil/1)
      
      inferred_type = infer_type_from_values(sample_values)
      {column, inferred_type}
    end)
  rescue
    ArgumentError ->
      # Fallback if string to atom conversion fails
      columns
      |> Enum.into(%{}, fn column ->
        sample_values = 
          data
          |> Enum.take(10)
          |> Enum.map(&Map.get(&1, column))
          |> Enum.reject(&is_nil/1)
        
        inferred_type = infer_type_from_values(sample_values)
        {column, inferred_type}
      end)
  end
  
  # Infer data type from sample values
  defp infer_type_from_values([]), do: :unknown
  defp infer_type_from_values(values) do
    # Check the first non-nil value's type
    case Enum.find(values, &(not is_nil(&1))) do
      val when is_integer(val) -> :integer
      val when is_float(val) -> :decimal
      val when is_boolean(val) -> :boolean
      val when is_binary(val) -> :string
      %Date{} -> :date
      %DateTime{} -> :utc_datetime
      %NaiveDateTime{} -> :naive_datetime
      _ -> :unknown
    end
  end
  
  # Validate that all data rows have complete data for all columns
  defp validate_data_completeness(data, :list_of_lists, columns) do
    expected_length = length(columns)
    
    incomplete_rows = 
      data
      |> Enum.with_index()
      |> Enum.filter(fn {row, _index} -> length(row) != expected_length end)
    
    if Enum.empty?(incomplete_rows) do
      :ok
    else
      [{row, index}] = Enum.take(incomplete_rows, 1)
      {:error, %ValidationError{
        type: :inconsistent_columns,
        message: "Row #{index + 1} has #{length(row)} values, expected #{expected_length}",
        details: %{expected: expected_length, actual: length(row), row_index: index + 1}
      }}
    end
  end
  
  defp validate_data_completeness(data, :list_of_maps, columns) do
    required_keys = Enum.map(columns, fn col ->
      try do
        String.to_existing_atom(col)
      rescue
        ArgumentError -> col
      end
    end)
    
    incomplete_rows = 
      data
      |> Enum.with_index()
      |> Enum.filter(fn {row, _index} ->
        row_keys = Map.keys(row) |> Enum.sort()
        Enum.sort(required_keys) != row_keys
      end)
    
    if Enum.empty?(incomplete_rows) do
      :ok
    else
      [{row, index}] = Enum.take(incomplete_rows, 1)
      {:error, %ValidationError{
        type: :inconsistent_columns,
        message: "Row #{index + 1} missing or has extra keys",
        details: %{required_keys: required_keys, row_keys: Map.keys(row), row_index: index + 1}
      }}
    end
  end
  
  # Detect data type from first element
  defp detect_data_type([]), do: :empty
  defp detect_data_type([first | _]) when is_list(first), do: :list_of_lists
  defp detect_data_type([first | _]) when is_map(first), do: :list_of_maps
  defp detect_data_type(_), do: :unknown
  
  # Generate unique ID for VALUES clause
  defp generate_values_id(alias_name) do
    unique = :erlang.unique_integer([:positive])
    "values_#{alias_name}_#{unique}"
  end
end