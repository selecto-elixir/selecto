defmodule Selecto.Builder.SetOperations do
  @moduledoc """
  SQL generation for set operations (UNION, INTERSECT, EXCEPT).
  
  This module handles the generation of SQL for combining multiple queries
  using standard SQL set operations.
  """

  alias Selecto.SetOperations.Spec
  alias Selecto.Builder.SQL

  @doc """
  Build SQL for set operations in the query.
  
  Returns {iodata, [params]} where iodata contains the set operation SQL
  and params contains the bound parameters from all participating queries.
  """
  def build_set_operations(selecto) do
    set_operations = Map.get(selecto.set, :set_operations, [])
    
    case set_operations do
      [] -> 
        {[], []}
        
      [operation] ->
        build_single_set_operation(operation)
        
      multiple_operations ->
        build_chained_set_operations(multiple_operations)
    end
  end

  # Build SQL for a single set operation
  defp build_single_set_operation(spec) do
    {left_sql, left_params} = query_to_sql_with_params(spec.left_query)
    {right_sql, right_params} = query_to_sql_with_params(spec.right_query)
    
    operation_sql = build_operation_sql(spec.operation, spec.options.all)
    
    combined_sql = [
      "(",
      left_sql,
      ")",
      "\n",
      operation_sql,
      "\n",
      "(",
      right_sql,
      ")"
    ]
    
    combined_params = left_params ++ right_params
    
    {combined_sql, combined_params}
  end

  # Build SQL for chained set operations  
  defp build_chained_set_operations([first_op | rest_ops]) do
    # Start with the first operation
    {base_sql, base_params} = build_single_set_operation(first_op)
    
    # Chain additional operations
    {final_sql, final_params} = 
      Enum.reduce(rest_ops, {base_sql, base_params}, fn op, {acc_sql, acc_params} ->
        {right_sql, right_params} = query_to_sql_with_params(op.right_query)
        operation_sql = build_operation_sql(op.operation, op.options.all)
        
        chained_sql = [
          "(",
          acc_sql,
          ")",
          "\n",
          operation_sql,
          "\n", 
          "(",
          right_sql,
          ")"
        ]
        
        chained_params = acc_params ++ right_params
        {chained_sql, chained_params}
      end)
    
    {final_sql, final_params}
  end

  # Convert a Selecto query to SQL with parameters
  defp query_to_sql_with_params(selecto) do
    # Create a copy of the query without set operations to avoid recursion
    clean_selecto = %{selecto | set: Map.delete(selecto.set, :set_operations)}
    
    # Generate SQL for the individual query
    {sql, _aliases, params} = SQL.build(clean_selecto, [])
    {sql, params}
  end

  # Build the operation SQL keyword
  defp build_operation_sql(:union, true), do: "UNION ALL"
  defp build_operation_sql(:union, false), do: "UNION"
  defp build_operation_sql(:intersect, true), do: "INTERSECT ALL"
  defp build_operation_sql(:intersect, false), do: "INTERSECT"
  defp build_operation_sql(:except, true), do: "EXCEPT ALL"
  defp build_operation_sql(:except, false), do: "EXCEPT"

  @doc """
  Check if the query has set operations that need special SQL handling.
  """
  def has_set_operations?(selecto) do
    set_operations = Map.get(selecto.set, :set_operations, [])
    not Enum.empty?(set_operations)
  end

  @doc """
  Wrap a query with set operations in proper parentheses for complex queries.
  
  This is used when set operations need to be combined with ORDER BY, LIMIT, etc.
  """
  def wrap_set_operation_query(sql_iodata, has_outer_clauses) do
    if has_outer_clauses do
      ["(", sql_iodata, ")"]
    else
      sql_iodata
    end
  end

  @doc """
  Extract all parameters from set operation queries.
  
  This ensures all bound parameters from participating queries are included
  in the final parameter list.
  """
  def extract_set_operation_params(selecto) do
    set_operations = Map.get(selecto.set, :set_operations, [])
    
    Enum.flat_map(set_operations, fn spec ->
      {_left_sql, left_params} = query_to_sql_with_params(spec.left_query)
      {_right_sql, right_params} = query_to_sql_with_params(spec.right_query)
      left_params ++ right_params
    end)
  end

  @doc """
  Determine if ORDER BY should be applied to the entire set operation result.
  
  In SQL, ORDER BY on set operations applies to the final combined result.
  """
  def should_apply_outer_order_by?(selecto) do
    has_set_operations?(selecto) and has_order_by?(selecto)
  end

  # Check if query has ORDER BY clauses
  defp has_order_by?(selecto) do
    order_by = Map.get(selecto.set, :order_by, [])
    not Enum.empty?(order_by)
  end

  @doc """
  Validate that set operations are properly structured for SQL generation.
  
  Returns :ok or {:error, reason}.
  """
  def validate_set_operations_for_sql(selecto) do
    set_operations = Map.get(selecto.set, :set_operations, [])
    
    cond do
      Enum.empty?(set_operations) ->
        :ok
        
      not all_operations_validated?(set_operations) ->
        {:error, "Set operations contain unvalidated schemas"}
        
      has_conflicting_clauses?(selecto) ->
        {:error, "Set operations cannot be combined with certain query clauses"}
        
      true ->
        :ok
    end
  end

  # Check if all set operations have been schema-validated
  defp all_operations_validated?(set_operations) do
    Enum.all?(set_operations, & &1.validated)
  end

  # Check for query clauses that conflict with set operations
  defp has_conflicting_clauses?(selecto) do
    # Set operations cannot be combined with certain clauses at the individual query level
    has_group_by = not Enum.empty?(Map.get(selecto.set, :group_by, []))
    has_pivot = Map.has_key?(selecto.set, :pivot_state)
    has_subselects = not Enum.empty?(Map.get(selecto.set, :subselected, []))
    
    has_group_by or has_pivot or has_subselects
  end
end