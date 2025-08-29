defmodule Selecto.Advanced.ArrayOperations do
  @moduledoc """
  Array operations support for PostgreSQL array functionality.
  
  Provides comprehensive support for array construction, aggregation, manipulation,
  testing, and unnesting operations. Works with PostgreSQL native array types
  and provides type-safe operations for array columns.
  
  ## Examples
  
      # Array aggregation
      selecto
      |> Selecto.select([
          "category.name",
          {:array_agg, "film.title", as: "films"},
          {:array_length, {:array_agg, "film.film_id"}, 1, as: "film_count"}
        ])
      |> Selecto.group_by(["category.category_id", "category.name"])
      
      # Array filtering
      selecto
      |> Selecto.filter([
          {:array_contains, "film.special_features", ["Trailers"]},
          {:array_overlap, "film.special_features", ["Deleted Scenes", "Behind the Scenes"]}
        ])
      
      # Array unnesting
      selecto
      |> Selecto.select(["film.title", "feature"])
      |> Selecto.unnest("film.special_features", as: "feature")
  """
  
  defmodule Spec do
    @moduledoc """
    Specification for array operations in SELECT, WHERE, and other clauses.
    """
    defstruct [
      :id,                    # Unique identifier for the array operation
      :operation,             # Array operation type (:array_agg, :array_contains, etc.)
      :column,                # Source column name or expression
      :dimension,             # Array dimension for length/cardinality operations
      :value,                 # Value for comparison/containment operations
      :distinct,              # Whether to use DISTINCT in aggregation
      :order_by,              # ORDER BY clause for array_agg
      :alias,                 # Optional alias for SELECT operations
      :options,               # Additional options (null handling, etc.)
      :validated              # Boolean indicating if operation has been validated
    ]
    
    @type operation_type :: 
      # Aggregation operations
      :array_agg | :array_agg_distinct | :string_agg |
      # Testing operations  
      :array_contains | :array_contained | :array_overlap | :array_eq |
      # Size operations
      :array_length | :cardinality | :array_ndims | :array_dims |
      # Construction operations
      :array | :array_fill | :array_append | :array_prepend | :array_cat |
      # Element operations
      :array_position | :array_positions | :array_remove | :array_replace |
      # Transformation operations
      :unnest | :array_to_string | :string_to_array |
      # Set operations
      :array_union | :array_intersect | :array_except
      
    @type t :: %__MODULE__{
      id: String.t(),
      operation: operation_type(),
      column: String.t() | tuple(),
      dimension: integer() | nil,
      value: term() | nil,
      distinct: boolean(),
      order_by: list() | nil,
      alias: String.t() | nil,
      options: map(),
      validated: boolean()
    }
  end
  
  defmodule ValidationError do
    @moduledoc """
    Error raised when array operation specification is invalid.
    """
    defexception [:type, :message, :details]
    
    @type t :: %__MODULE__{
      type: :invalid_operation | :invalid_column | :invalid_arguments | :invalid_dimension,
      message: String.t(),
      details: map()
    }
  end

  @doc """
  Create an array aggregation operation specification.
  
  ## Examples
  
      # Simple array aggregation
      create_array_operation(:array_agg, "film.title", as: "film_titles")
      
      # Array aggregation with DISTINCT
      create_array_operation(:array_agg, "actor.name", distinct: true, as: "unique_actors")
      
      # Array aggregation with ORDER BY
      create_array_operation(:array_agg, "film.title", 
        order_by: [{"film.release_year", :desc}], 
        as: "films_by_year")
  """
  def create_array_operation(operation, column, opts \\ []) do
    spec = %Spec{
      id: generate_array_operation_id(operation, column),
      operation: operation,
      column: column,
      dimension: opts[:dimension],
      value: opts[:value],
      distinct: opts[:distinct] || false,
      order_by: opts[:order_by],
      alias: opts[:as],
      options: Map.new(Keyword.drop(opts, [:dimension, :value, :distinct, :order_by, :as])),
      validated: false
    }
    
    validate_array_operation!(spec)
  end
  
  @doc """
  Create an array containment/testing operation for filters.
  
  ## Examples
  
      # Array contains
      create_array_filter(:array_contains, "tags", ["featured", "new"])
      
      # Array overlap
      create_array_filter(:array_overlap, "categories", ["electronics", "computers"])
  """
  def create_array_filter(operation, column, value) do
    spec = %Spec{
      id: generate_array_operation_id(operation, column),
      operation: operation,
      column: column,
      value: value,
      validated: false
    }
    
    validate_array_operation!(spec)
  end
  
  @doc """
  Create an array length/dimension operation.
  
  ## Examples
  
      # Get array length at dimension 1
      create_array_size(:array_length, "tags", 1, as: "tag_count")
      
      # Get array cardinality (total number of elements)
      create_array_size(:cardinality, "matrix", as: "total_elements")
  """
  def create_array_size(operation, column, dimension \\ nil, opts \\ []) do
    spec = %Spec{
      id: generate_array_operation_id(operation, column),
      operation: operation,
      column: column,
      dimension: dimension,
      alias: opts[:as],
      options: Map.new(Keyword.drop(opts, [:as])),
      validated: false
    }
    
    validate_array_operation!(spec)
  end
  
  @doc """
  Create an unnest operation for array expansion.
  
  ## Examples
  
      # Unnest array column
      create_unnest("special_features", as: "feature")
      
      # Unnest with ordinality
      create_unnest("tags", with_ordinality: true, as: "tag")
  """
  def create_unnest(column, opts \\ []) do
    spec = %Spec{
      id: generate_array_operation_id(:unnest, column),
      operation: :unnest,
      column: column,
      alias: opts[:as],
      options: Map.new(Keyword.drop(opts, [:as])),
      validated: false
    }
    
    validate_array_operation!(spec)
  end
  
  @doc """
  Validate an array operation specification.
  """
  def validate_array_operation!(%Spec{validated: true} = spec), do: spec
  
  def validate_array_operation!(%Spec{} = spec) do
    spec
    |> validate_operation_type!()
    |> validate_column!()
    |> validate_arguments!()
    |> Map.put(:validated, true)
  end
  
  defp validate_operation_type!(%Spec{operation: op} = spec) when is_atom(op) do
    valid_operations = [
      :array_agg, :array_agg_distinct, :string_agg,
      :array_contains, :array_contained, :array_overlap, :array_eq,
      :array_length, :cardinality, :array_ndims, :array_dims,
      :array, :array_fill, :array_append, :array_prepend, :array_cat,
      :array_position, :array_positions, :array_remove, :array_replace,
      :unnest, :array_to_string, :string_to_array,
      :array_union, :array_intersect, :array_except
    ]
    
    unless op in valid_operations do
      raise ValidationError, 
        type: :invalid_operation,
        message: "Invalid array operation: #{inspect(op)}",
        details: %{operation: op, valid_operations: valid_operations}
    end
    
    spec
  end
  
  defp validate_column!(%Spec{column: nil} = spec) do
    raise ValidationError,
      type: :invalid_column,
      message: "Column is required for array operation",
      details: %{operation: spec.operation}
  end
  
  defp validate_column!(%Spec{column: column} = spec) when is_binary(column) do
    spec
  end
  
  defp validate_column!(%Spec{column: column} = spec) when is_tuple(column) do
    # Allow tuples for nested operations like {:array_agg, "column"}
    spec
  end
  
  defp validate_column!(%Spec{column: column} = spec) do
    raise ValidationError,
      type: :invalid_column,
      message: "Invalid column type: #{inspect(column)}",
      details: %{column: column}
  end
  
  defp validate_arguments!(%Spec{operation: op} = spec) when op in [:array_length] do
    unless is_integer(spec.dimension) and spec.dimension > 0 do
      raise ValidationError,
        type: :invalid_dimension,
        message: "Array dimension must be a positive integer for #{op}",
        details: %{operation: op, dimension: spec.dimension}
    end
    spec
  end
  
  defp validate_arguments!(%Spec{operation: op} = spec) 
       when op in [:array_contains, :array_contained, :array_overlap] do
    unless spec.value != nil do
      raise ValidationError,
        type: :invalid_arguments,
        message: "Value is required for #{op} operation",
        details: %{operation: op}
    end
    spec
  end
  
  defp validate_arguments!(spec), do: spec
  
  @doc """
  Generate SQL for an array operation.
  """
  def to_sql(%Spec{} = spec, params_list) do
    Selecto.Builder.ArrayOperations.build_array_sql(spec, params_list)
  end
  
  @doc """
  Check if an operation is an aggregation function.
  """
  def is_aggregate?(%Spec{operation: op}) do
    op in [:array_agg, :array_agg_distinct, :string_agg]
  end
  
  @doc """
  Check if an operation is a filter/WHERE clause operation.
  """
  def is_filter?(%Spec{operation: op}) do
    op in [:array_contains, :array_contained, :array_overlap, :array_eq]
  end
  
  @doc """
  Check if an operation is an unnest operation.
  """
  def is_unnest?(%Spec{operation: :unnest}), do: true
  def is_unnest?(_), do: false
  
  # Private helpers
  
  defp generate_array_operation_id(operation, column) when is_binary(column) do
    "array_#{operation}_#{String.replace(column, ".", "_")}_#{:erlang.unique_integer([:positive])}"
  end
  
  defp generate_array_operation_id(operation, column) do
    "array_#{operation}_#{inspect(column)}_#{:erlang.unique_integer([:positive])}"
  end
end