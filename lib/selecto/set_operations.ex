defmodule Selecto.SetOperations do
  @moduledoc """
  Set operations for combining query results using UNION, INTERSECT, and EXCEPT.
  
  Set operations allow combining results from multiple Selecto queries using
  standard SQL set operations. All participating queries must have compatible
  column counts and types.
  
  ## Examples
  
      # Basic UNION - combine results from two queries
      query1 = Selecto.configure(users_domain, connection)
        |> Selecto.select(["name", "email"])
        |> Selecto.filter([{"active", true}])
        
      query2 = Selecto.configure(contacts_domain, connection)  
        |> Selecto.select(["full_name", "email_address"])
        |> Selecto.filter([{"status", "active"}])
        
      combined = Selecto.union(query1, query2, all: true)
      
      # INTERSECT - find common records
      premium_active = Selecto.intersect(premium_users, active_users)
      
      # EXCEPT - find differences
      free_users = Selecto.except(all_users, premium_users)
      
      # Chained set operations
      result = query1
        |> Selecto.union(query2) 
        |> Selecto.intersect(query3)
        |> Selecto.except(query4)
  """

  alias Selecto.SetOperations.{Spec, Validation}

  defmodule Spec do
    @moduledoc """
    Specification for a set operation between two or more queries.
    """
    defstruct [
      :id,                    # Unique identifier for the set operation
      :operation,             # :union, :intersect, or :except
      :left_query,            # Left side query (Selecto struct)
      :right_query,           # Right side query (Selecto struct) 
      :options,               # Operation options (all: true/false, etc.)
      :column_mapping,        # Optional column mapping for incompatible schemas
      :validated              # Boolean indicating if schemas have been validated
    ]

    @type set_operation :: :union | :intersect | :except
    @type operation_options :: %{
      all: boolean(),
      column_mapping: [{String.t(), String.t()}] | nil
    }

    @type t :: %__MODULE__{
      id: String.t(),
      operation: set_operation(),
      left_query: Selecto.t(),
      right_query: Selecto.t(),
      options: operation_options(),
      column_mapping: [{String.t(), String.t()}] | nil,
      validated: boolean()
    }
  end

  defmodule Validation do
    @moduledoc """
    Schema validation for set operations between queries.
    """
    
    defmodule SchemaError do
      defexception [:type, :message, :query1_info, :query2_info]
      
      @type t :: %__MODULE__{
        type: :column_count_mismatch | :type_incompatibility | :mapping_error,
        message: String.t(),
        query1_info: map(),
        query2_info: map()
      }
    end

    @doc """
    Validate that two queries are compatible for set operations.
    
    Returns {:ok, validated_spec} or {:error, validation_error}.
    """
    def validate_compatibility(spec) do
      with {:ok, left_columns} <- extract_query_columns(spec.left_query),
           {:ok, right_columns} <- extract_query_columns(spec.right_query),
           :ok <- validate_column_count(left_columns, right_columns),
           :ok <- validate_column_types(left_columns, right_columns, spec.column_mapping) do
        {:ok, %{spec | validated: true}}
      else
        {:error, reason} -> {:error, reason}
      end
    end

    # Extract column information from a Selecto query
    defp extract_query_columns(selecto) do
      selected = Map.get(selecto.set, :selected, [])
      
      if Enum.empty?(selected) do
        {:error, %SchemaError{
          type: :validation_error,
          message: "Query has no selected columns",
          query1_info: %{selected: selected},
          query2_info: %{}
        }}
      else
        columns = Enum.map(selected, &normalize_column_info(selecto, &1))
        {:ok, columns}
      end
    end

    # Normalize column information for comparison
    defp normalize_column_info(selecto, column_spec) do
      case column_spec do
        column when is_binary(column) ->
          # Basic column name - resolve type from domain
          case Selecto.resolve_field(selecto, column) do
            {:ok, field_info} ->
              %{
                name: column,
                type: Map.get(field_info, :type, :unknown),
                source: :field
              }
            {:error, _} ->
              %{
                name: column,
                type: :unknown,
                source: :field
              }
          end
          
        {:as, expression, alias_name} ->
          # Aliased expression - try to infer type
          %{
            name: alias_name,
            type: infer_expression_type(expression),
            source: :expression
          }
          
        {:func, func_name, _args} ->
          # Function call - infer return type
          %{
            name: func_name,
            type: infer_function_return_type(func_name),
            source: :function
          }
          
        _ ->
          # Complex expression - treat as unknown type
          %{
            name: inspect(column_spec),
            type: :unknown,
            source: :complex
          }
      end
    end

    # Infer type from expression (simplified)
    defp infer_expression_type({:literal, value}) when is_binary(value), do: :string
    defp infer_expression_type({:literal, value}) when is_integer(value), do: :integer
    defp infer_expression_type({:literal, value}) when is_float(value), do: :decimal
    defp infer_expression_type({:literal, value}) when is_boolean(value), do: :boolean
    defp infer_expression_type(_), do: :unknown

    # Infer function return type (simplified mapping)
    defp infer_function_return_type("COUNT"), do: :integer
    defp infer_function_return_type("SUM"), do: :decimal
    defp infer_function_return_type("AVG"), do: :decimal
    defp infer_function_return_type("MIN"), do: :unknown  # Depends on input
    defp infer_function_return_type("MAX"), do: :unknown  # Depends on input
    defp infer_function_return_type("CONCAT"), do: :string
    defp infer_function_return_type(_), do: :unknown

    # Validate column counts match
    defp validate_column_count(left_columns, right_columns) do
      left_count = length(left_columns)
      right_count = length(right_columns)
      
      if left_count == right_count do
        :ok
      else
        {:error, %SchemaError{
          type: :column_count_mismatch,
          message: "Query 1 has #{left_count} columns, Query 2 has #{right_count} columns",
          query1_info: %{column_count: left_count, columns: Enum.map(left_columns, & &1.name)},
          query2_info: %{column_count: right_count, columns: Enum.map(right_columns, & &1.name)}
        }}
      end
    end

    # Validate column types are compatible
    defp validate_column_types(left_columns, right_columns, column_mapping) do
      paired_columns = apply_column_mapping(left_columns, right_columns, column_mapping)
      
      incompatible = 
        paired_columns
        |> Enum.with_index()
        |> Enum.filter(fn {{left_col, right_col}, _index} ->
          not types_compatible?(left_col.type, right_col.type)
        end)
      
      if Enum.empty?(incompatible) do
        :ok
      else
        [{_columns, index}] = Enum.take(incompatible, 1)  # Show first incompatible pair
        {{left_col, right_col}, _} = Enum.at(paired_columns, index)
        
        {:error, %SchemaError{
          type: :type_incompatibility,
          message: "Column #{index + 1}: '#{left_col.name}' (#{left_col.type}) incompatible with '#{right_col.name}' (#{right_col.type})",
          query1_info: %{column: left_col.name, type: left_col.type},
          query2_info: %{column: right_col.name, type: right_col.type}
        }}
      end
    end

    # Apply column mapping if provided, otherwise pair by position
    defp apply_column_mapping(left_columns, right_columns, nil) do
      Enum.zip(left_columns, right_columns)
    end
    
    defp apply_column_mapping(left_columns, right_columns, mapping) do
      # TODO: Implement column mapping logic
      # For now, fall back to position-based pairing
      Enum.zip(left_columns, right_columns)
    end

    # Check if two types are compatible for set operations
    defp types_compatible?(:unknown, _), do: true
    defp types_compatible?(_, :unknown), do: true
    defp types_compatible?(type, type), do: true
    
    # String-like types
    defp types_compatible?(type1, type2) when type1 in [:string, :text] and type2 in [:string, :text], do: true
    
    # Numeric types
    defp types_compatible?(type1, type2) when type1 in [:integer, :decimal, :float] and type2 in [:integer, :decimal, :float], do: true
    
    # Date/time types  
    defp types_compatible?(type1, type2) when type1 in [:date, :utc_datetime, :naive_datetime] and type2 in [:date, :utc_datetime, :naive_datetime], do: true
    
    # Default: incompatible
    defp types_compatible?(_, _), do: false
  end

  @doc """
  Create a UNION set operation between two queries.
  
  ## Options
  
  - `:all` - Use UNION ALL to include duplicates (default: false)  
  - `:column_mapping` - Map columns between incompatible schemas
  
  ## Examples
  
      # Basic UNION (removes duplicates)
      Selecto.union(query1, query2)
      
      # UNION ALL (includes duplicates, faster)
      Selecto.union(query1, query2, all: true)
      
      # UNION with column mapping
      Selecto.union(customers, vendors,
        column_mapping: [
          {"name", "company_name"},
          {"email", "contact_email"}
        ]
      )
  """
  def union(left_query, right_query, opts \\ []) do
    create_set_operation(:union, left_query, right_query, opts)
  end

  @doc """
  Create an INTERSECT set operation between two queries.
  
  Returns only rows that appear in both queries.
  
  ## Options
  
  - `:all` - Use INTERSECT ALL to include duplicate intersections (default: false)
  - `:column_mapping` - Map columns between incompatible schemas
  """
  def intersect(left_query, right_query, opts \\ []) do
    create_set_operation(:intersect, left_query, right_query, opts)
  end

  @doc """
  Create an EXCEPT set operation between two queries.
  
  Returns rows from the first query that don't appear in the second query.
  
  ## Options
  
  - `:all` - Use EXCEPT ALL to include duplicates in difference (default: false)
  - `:column_mapping` - Map columns between incompatible schemas
  """
  def except(left_query, right_query, opts \\ []) do
    create_set_operation(:except, left_query, right_query, opts)
  end

  # Create a set operation specification
  defp create_set_operation(operation, left_query, right_query, opts) do
    options = %{
      all: Keyword.get(opts, :all, false),
      column_mapping: Keyword.get(opts, :column_mapping)
    }
    
    spec = %Spec{
      id: generate_operation_id(operation, left_query, right_query),
      operation: operation,
      left_query: left_query,
      right_query: right_query,
      options: options,
      column_mapping: options.column_mapping,
      validated: false
    }
    
    # Validate schema compatibility
    case Validation.validate_compatibility(spec) do
      {:ok, validated_spec} ->
        # Create a new Selecto struct with the set operation
        create_set_operation_selecto(validated_spec)
        
      {:error, validation_error} ->
        raise validation_error
    end
  end

  # Generate unique ID for set operation
  defp generate_operation_id(operation, left_query, right_query) do
    left_id = inspect(left_query.domain) |> String.slice(0, 8)
    right_id = inspect(right_query.domain) |> String.slice(0, 8) 
    unique = :erlang.unique_integer([:positive])
    
    "#{operation}_#{left_id}_#{right_id}_#{unique}"
  end

  # Create a new Selecto struct representing the set operation
  defp create_set_operation_selecto(spec) do
    # Use the left query as the base for the new struct
    # Set operations inherit the connection and basic configuration from the left side
    base_selecto = spec.left_query
    
    # Add set operation to the query set
    current_set_ops = Map.get(base_selecto.set, :set_operations, [])
    updated_set_ops = current_set_ops ++ [spec]
    
    put_in(base_selecto.set[:set_operations], updated_set_ops)
  end
end