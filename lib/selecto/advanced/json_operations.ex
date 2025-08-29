defmodule Selecto.Advanced.JsonOperations do
  @moduledoc """
  JSON operations support for PostgreSQL JSON and JSONB functionality.
  
  Provides comprehensive support for JSON path queries, aggregation, manipulation,
  and testing functions. Works with both JSON and JSONB column types with 
  automatic type detection and optimization.
  
  ## Examples
  
      # JSON path extraction
      selecto
      |> Selecto.select([
          {:json_extract, "metadata", "$.category", as: "category"},
          {:json_extract, "metadata", "$.specs.weight", as: "weight"}
        ])
      
      # JSON aggregation
      selecto
      |> Selecto.select([
          {:json_agg, "product_name", as: "products"},
          {:json_object_agg, "product_id", "price", as: "price_map"}
        ])
      |> Selecto.group_by(["category"])
      
      # JSON filtering
      selecto
      |> Selecto.filter([
          {:json_contains, "metadata", %{"category" => "electronics"}},
          {:json_path_exists, "metadata", "$.specs.warranty"}
        ])
  """
  
  defmodule Spec do
    @moduledoc """
    Specification for JSON operations in SELECT, WHERE, and other clauses.
    """
    defstruct [
      :id,                    # Unique identifier for the JSON operation
      :operation,             # JSON operation type (:extract, :agg, :contains, etc.)
      :column,                # Source column name
      :path,                  # JSON path (for extraction operations)  
      :value,                 # Value for comparison/manipulation operations
      :key_field,            # Key field for object aggregation
      :value_field,          # Value field for object aggregation
      :alias,                # Optional alias for SELECT operations
      :options,              # Additional options (cast_type, etc.)
      :validated             # Boolean indicating if operation has been validated
    ]
    
    @type operation_type :: 
      # Extraction operations
      :json_extract | :json_extract_text | :json_extract_path | :json_extract_path_text |
      # Testing operations  
      :json_contains | :json_contained | :json_exists | :json_path_exists |
      # Aggregation operations
      :json_agg | :json_object_agg | :jsonb_agg | :jsonb_object_agg |
      # Construction operations
      :json_build_object | :json_build_array | :jsonb_build_object | :jsonb_build_array |
      # Manipulation operations
      :json_set | :jsonb_set | :json_insert | :jsonb_insert | 
      :json_remove | :jsonb_delete | :jsonb_delete_path |
      # Type operations
      :json_typeof | :jsonb_typeof | :json_array_length | :jsonb_array_length
      
    @type t :: %__MODULE__{
      id: String.t(),
      operation: operation_type(),
      column: String.t(),
      path: String.t() | nil,
      value: term() | nil,
      key_field: String.t() | nil,
      value_field: String.t() | nil,
      alias: String.t() | nil,
      options: map(),
      validated: boolean()
    }
  end
  
  defmodule ValidationError do
    @moduledoc """
    Error raised when JSON operation specification is invalid.
    """
    defexception [:type, :message, :details]
    
    @type t :: %__MODULE__{
      type: :invalid_operation | :invalid_path | :invalid_column | :invalid_arguments,
      message: String.t(),
      details: map()
    }
  end

  @doc """
  Create a JSON extraction operation specification.
  """
  def create_json_operation(operation, column, opts \\ []) do
    spec = %Spec{
      id: generate_json_operation_id(operation, column),
      operation: operation,
      column: column,
      path: Keyword.get(opts, :path),
      value: Keyword.get(opts, :value),
      key_field: Keyword.get(opts, :key_field),
      value_field: Keyword.get(opts, :value_field),
      alias: Keyword.get(opts, :as),
      options: extract_options(opts),
      validated: false
    }
    
    case validate_json_operation(spec) do
      {:ok, validated_spec} -> validated_spec
      {:error, validation_error} -> raise validation_error
    end
  end

  @doc """
  Validate a JSON operation specification.
  """
  def validate_json_operation(%Spec{} = spec) do
    with :ok <- validate_operation_type(spec.operation),
         :ok <- validate_required_params(spec),
         :ok <- validate_json_path(spec),
         :ok <- validate_operation_compatibility(spec) do
      
      validated_spec = %{spec | validated: true}
      {:ok, validated_spec}
    else
      {:error, reason} -> {:error, reason}
    end
  end
  
  # Validate that the operation type is supported
  defp validate_operation_type(operation) do
    supported_operations = [
      :json_extract, :json_extract_text, :json_extract_path, :json_extract_path_text,
      :json_contains, :json_contained, :json_exists, :json_path_exists,
      :json_agg, :json_object_agg, :jsonb_agg, :jsonb_object_agg,
      :json_build_object, :json_build_array, :jsonb_build_object, :jsonb_build_array,
      :json_set, :jsonb_set, :json_insert, :jsonb_insert,
      :json_remove, :jsonb_delete, :jsonb_delete_path,
      :json_typeof, :jsonb_typeof, :json_array_length, :jsonb_array_length
    ]
    
    if operation in supported_operations do
      :ok
    else
      {:error, %ValidationError{
        type: :invalid_operation,
        message: "Unsupported JSON operation: #{operation}",
        details: %{operation: operation}
      }}
    end
  end

  # Validate required parameters for each operation type
  defp validate_required_params(%Spec{} = _spec) do
    # Simplified validation for now
    :ok
  end

  # Validate JSON path syntax (basic validation)
  defp validate_json_path(%Spec{path: nil}), do: :ok
  defp validate_json_path(%Spec{path: path}) when is_binary(path) do
    # Basic JSONPath validation - should start with $ or be array index
    cond do
      String.starts_with?(path, "$") -> :ok
      String.match?(path, ~r/^\\[\\d+\\]$/) -> :ok  # Array index like [0]
      String.match?(path, ~r/^[a-zA-Z_][a-zA-Z0-9_]*$/) -> :ok  # Simple key
      true ->
        {:error, %ValidationError{
          type: :invalid_path,
          message: "Invalid JSON path format: #{path}",
          details: %{path: path, expected: "JSONPath starting with $ or simple key/index"}
        }}
    end
  end

  # Validate operation compatibility (placeholder for future enhancement)
  defp validate_operation_compatibility(_spec), do: :ok

  # Extract additional options from keyword list
  defp extract_options(opts) do
    opts
    |> Keyword.drop([:path, :value, :key_field, :value_field, :as])
    |> Enum.into(%{})
  end

  # Generate unique ID for JSON operation
  defp generate_json_operation_id(operation, column) do
    unique = :erlang.unique_integer([:positive])
    "json_#{operation}_#{column}_#{unique}"
  end

  @doc """
  Determine if an operation is suitable for SELECT clauses.
  """
  def select_operation?(operation) do
    operation in [
      :json_extract, :json_extract_text, :json_extract_path, :json_extract_path_text,
      :json_agg, :json_object_agg, :jsonb_agg, :jsonb_object_agg,
      :json_build_object, :json_build_array, :jsonb_build_object, :jsonb_build_array,
      :json_set, :jsonb_set, :json_insert, :jsonb_insert,
      :json_typeof, :jsonb_typeof, :json_array_length, :jsonb_array_length
    ]
  end

  @doc """
  Determine if an operation is suitable for WHERE clauses.
  """
  def filter_operation?(operation) do
    operation in [
      :json_contains, :json_contained, :json_exists, :json_path_exists,
      :json_extract, :json_extract_text  # Can be used in WHERE with comparisons
    ]
  end
end