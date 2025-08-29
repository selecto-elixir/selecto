defmodule Selecto.Advanced.LateralJoin do
  @moduledoc """
  LATERAL join support for correlated subqueries and advanced join patterns.
  
  LATERAL joins allow the right side of a join to reference columns from the 
  left side, enabling powerful correlated subquery patterns that are not 
  possible with standard joins.
  
  ## Examples
  
      # Basic LATERAL join with correlated subquery
      selecto
      |> Selecto.lateral_join(
        :left,
        fn base_query ->
          Selecto.configure(rental_domain, connection)
          |> Selecto.select([{:func, "COUNT", ["*"], as: "rental_count"}])
          |> Selecto.filter([{"customer_id", {:ref, "customer.customer_id"}}])
          |> Selecto.filter([{"rental_date", {:>, {:func, "CURRENT_DATE - INTERVAL '30 days'"}}}])
        end,
        as: "recent_rentals"
      )
      
      # LATERAL join with table function
      selecto
      |> Selecto.lateral_join(
        :inner,
        {:unnest, "film.special_features"},
        as: "features"
      )
  """
  
  defmodule Spec do
    @moduledoc """
    Specification for a LATERAL join operation.
    """
    defstruct [
      :id,                    # Unique identifier for the LATERAL join
      :join_type,             # :left, :inner, :right, :full
      :subquery_builder,      # Function that builds the correlated subquery
      :table_function,        # Alternative: table function specification
      :alias,                 # Alias for the LATERAL join results
      :correlation_refs,      # List of parent table references used in subquery
      :validated              # Boolean indicating if correlations have been validated
    ]
    
    @type join_type :: :left | :inner | :right | :full
    @type correlation_ref :: {:ref, String.t()}
    @type table_function :: {:unnest, String.t()} | {:function, atom(), [term()]}
    @type subquery_builder :: (Selecto.t() -> Selecto.t())
    
    @type t :: %__MODULE__{
      id: String.t(),
      join_type: join_type(),
      subquery_builder: subquery_builder() | nil,
      table_function: table_function() | nil,
      alias: String.t(),
      correlation_refs: [correlation_ref()],
      validated: boolean()
    }
  end
  
  defmodule CorrelationError do
    @moduledoc """
    Error raised when LATERAL join correlations are invalid.
    """
    defexception [:type, :message, :available_fields, :referenced_field]
    
    @type t :: %__MODULE__{
      type: :invalid_correlation | :missing_field | :validation_error,
      message: String.t(),
      available_fields: [String.t()],
      referenced_field: String.t()
    }
  end
  
  @doc """
  Create a LATERAL join specification.
  
  ## Parameters
  
  - `join_type` - Type of join (:left, :inner, :right, :full)
  - `subquery_builder` - Function that builds the correlated subquery
  - `alias_name` - Alias for the LATERAL join results
  - `opts` - Additional options
  
  ## Examples
  
      # Correlated subquery LATERAL join
      lateral_spec = LateralJoin.create_lateral_join(
        :left,
        fn base_query ->
          rental_query
          |> Selecto.filter([{"customer_id", {:ref, "customer.customer_id"}}])
          |> Selecto.limit(5)
        end,
        "recent_rentals"
      )
      
      # Table function LATERAL join  
      lateral_spec = LateralJoin.create_lateral_join(
        :inner,
        {:unnest, "film.special_features"},
        "features"
      )
  """
  def create_lateral_join(join_type, subquery_builder_or_function, alias_name, opts \\ [])
  
  def create_lateral_join(join_type, subquery_builder, alias_name, opts) when is_function(subquery_builder) do
    %Spec{
      id: generate_lateral_id(alias_name),
      join_type: join_type,
      subquery_builder: subquery_builder,
      table_function: nil,
      alias: alias_name,
      correlation_refs: [],
      validated: false
    }
  end
  
  def create_lateral_join(join_type, table_function, alias_name, opts) when is_tuple(table_function) do
    %Spec{
      id: generate_lateral_id(alias_name),
      join_type: join_type,
      subquery_builder: nil,
      table_function: table_function,
      alias: alias_name,
      correlation_refs: extract_correlation_refs(table_function),
      validated: false
    }
  end
  
  @doc """
  Validate LATERAL join correlations against the base query.
  
  Ensures that all correlation references in the LATERAL subquery
  refer to valid columns in the parent/left-side tables.
  """
  def validate_correlations(%Spec{} = spec, base_selecto) do
    case spec.subquery_builder do
      nil -> 
        # Table function - validate any embedded correlations
        validate_table_function_correlations(spec, base_selecto)
        
      subquery_builder when is_function(subquery_builder) ->
        # Build subquery to extract correlations
        validate_subquery_correlations(spec, base_selecto)
    end
  end
  
  # Extract correlation references from table functions
  defp extract_correlation_refs({:unnest, column_ref}) do
    case column_ref do
      column when is_binary(column) ->
        if String.contains?(column, ".") do
          [column]
        else
          []
        end
      _ -> []
    end
  end
  
  defp extract_correlation_refs({:function, _func_name, args}) do
    args
    |> Enum.flat_map(&extract_refs_from_arg/1)
  end
  
  defp extract_correlation_refs(_), do: []
  
  # Extract correlation references from function arguments
  defp extract_refs_from_arg({:ref, field}), do: [field]
  defp extract_refs_from_arg(arg) when is_binary(arg) do
    if String.contains?(arg, ".") do
      [arg]
    else
      []
    end
  end
  defp extract_refs_from_arg(_), do: []
  
  # Validate table function correlations
  defp validate_table_function_correlations(%Spec{} = spec, base_selecto) do
    available_fields = get_available_fields(base_selecto)
    
    invalid_refs = 
      spec.correlation_refs
      |> Enum.reject(&field_available?(&1, available_fields))
    
    if Enum.empty?(invalid_refs) do
      {:ok, %{spec | validated: true}}
    else
      [invalid_ref] = Enum.take(invalid_refs, 1)
      
      {:error, %CorrelationError{
        type: :invalid_correlation,
        message: "Cannot reference field '#{invalid_ref}' in LATERAL table function",
        available_fields: available_fields,
        referenced_field: invalid_ref
      }}
    end
  end
  
  # Validate subquery correlations by analyzing the built subquery
  defp validate_subquery_correlations(%Spec{} = spec, base_selecto) do
    try do
      # Create a dummy subquery to extract correlation references
      dummy_subquery = spec.subquery_builder.(base_selecto)
      correlation_refs = extract_subquery_correlations(dummy_subquery)
      
      available_fields = get_available_fields(base_selecto)
      
      invalid_refs = 
        correlation_refs
        |> Enum.reject(&field_available?(&1, available_fields))
      
      if Enum.empty?(invalid_refs) do
        validated_spec = %{spec | 
          correlation_refs: correlation_refs,
          validated: true
        }
        {:ok, validated_spec}
      else
        [invalid_ref] = Enum.take(invalid_refs, 1)
        
        {:error, %CorrelationError{
          type: :invalid_correlation,
          message: "Cannot reference field '#{invalid_ref}' in LATERAL subquery",
          available_fields: available_fields,
          referenced_field: invalid_ref
        }}
      end
    rescue
      error ->
        {:error, %CorrelationError{
          type: :validation_error,
          message: "Error validating LATERAL subquery: #{inspect(error)}",
          available_fields: [],
          referenced_field: ""
        }}
    end
  end
  
  # Extract correlation references from a built subquery
  defp extract_subquery_correlations(selecto) do
    # Check filters for correlation references
    filters = Map.get(selecto.set, :filtered, [])
    
    filters
    |> Enum.flat_map(&extract_refs_from_filter/1)
    |> Enum.uniq()
  end
  
  # Extract correlation references from filter conditions
  defp extract_refs_from_filter({field, {:ref, ref_field}}) when is_binary(ref_field), do: [ref_field]
  defp extract_refs_from_filter({field, {op, {:ref, ref_field}}}) when is_binary(ref_field), do: [ref_field]
  defp extract_refs_from_filter({field, {op, val1, {:ref, ref_field}}}) when is_binary(ref_field), do: [ref_field]
  defp extract_refs_from_filter({field, {op, {:ref, ref_field}, val2}}) when is_binary(ref_field), do: [ref_field]
  defp extract_refs_from_filter(_), do: []
  
  # Get available fields from base selecto query
  defp get_available_fields(selecto) do
    base_table = get_base_table_name(selecto)
    domain_fields = get_domain_fields(selecto)
    join_fields = get_join_fields(selecto)
    
    # Combine all available fields with table prefixes
    base_fields = Enum.map(domain_fields, &"#{base_table}.#{&1}")
    
    # Also include unqualified field names for flexibility
    unqualified_fields = domain_fields
    
    (base_fields ++ unqualified_fields ++ join_fields)
    |> Enum.uniq()
  end
  
  # Extract base table name from domain
  defp get_base_table_name(selecto) do
    case selecto.domain do
      %{source: %{source_table: table}} -> table
      %{source_table: table} -> table
      _ -> "base_table"
    end
  end
  
  # Extract field names from domain
  defp get_domain_fields(selecto) do
    case selecto.domain do
      %{source: %{fields: fields}} when is_list(fields) -> 
        Enum.map(fields, &to_string/1)
      %{source: %{columns: columns}} when is_map(columns) -> 
        Map.keys(columns) |> Enum.map(&to_string/1)
      _ -> []
    end
  end
  
  # Extract field names from joins
  defp get_join_fields(selecto) do
    joins = Map.get(selecto.set, :joins, %{})
    
    joins
    |> Enum.flat_map(fn {_join_name, join_spec} ->
      case join_spec do
        %{target_table: table, fields: fields} when is_list(fields) ->
          Enum.map(fields, &"#{table}.#{&1}")
        _ -> []
      end
    end)
  end
  
  # Check if a field is available in the list
  defp field_available?(field, available_fields) do
    # Extract field name if it's table.field format
    field_name = if String.contains?(field, ".") do
      String.split(field, ".") |> List.last()
    else
      field
    end
    
    Enum.any?(available_fields, fn available ->
      # Exact match
      field == available or 
      # Table.field matches available field
      String.ends_with?(available, ".#{field}") or
      # field matches table.field available 
      String.ends_with?(field, ".#{available}") or
      # Field name matches
      available == field_name or
      # Table.field_name matches
      String.ends_with?(available, ".#{field_name}")
    end)
  end
  
  # Generate unique ID for LATERAL join
  defp generate_lateral_id(alias_name) do
    unique = :erlang.unique_integer([:positive])
    "lateral_#{alias_name}_#{unique}"
  end
end