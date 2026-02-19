defmodule Selecto.Builder.LateralJoin do
  @moduledoc """
  SQL generation for LATERAL joins.
  
  This module handles the conversion of LATERAL join specifications into 
  proper PostgreSQL LATERAL JOIN SQL syntax.
  """
  
  alias Selecto.Advanced.LateralJoin.Spec
  # alias Selecto.Builder.SQL
  
  @doc """
  Build LATERAL JOIN SQL clauses from specifications.
  
  Takes a list of LATERAL join specifications and generates the corresponding
  SQL JOIN clauses with LATERAL keyword and proper correlation handling.
  
  ## Examples
  
      iex> build_lateral_joins([lateral_spec])
      {["LEFT JOIN LATERAL (SELECT ...) AS recent_rentals ON true"], [param1, param2]}
  """
  def build_lateral_joins([]), do: {[], []}
  
  def build_lateral_joins(lateral_specs) when is_list(lateral_specs) do
    lateral_specs
    |> Enum.map(&build_lateral_join/1)
    |> Enum.reduce({[], []}, fn {sql, params}, {acc_sql, acc_params} ->
      {acc_sql ++ [sql], acc_params ++ params}
    end)
  end
  
  @doc """
  Build a single LATERAL JOIN SQL clause.
  """
  def build_lateral_join(%Spec{} = spec) do
    join_type_sql = build_join_type(spec.join_type)
    
    case spec.subquery_builder do
      nil ->
        # Table function LATERAL join
        build_table_function_lateral_join(spec, join_type_sql)
        
      subquery_builder when is_function(subquery_builder) ->
        # Subquery LATERAL join
        build_subquery_lateral_join(spec, join_type_sql)
    end
  end
  
  # Build LATERAL join with table function
  defp build_table_function_lateral_join(%Spec{} = spec, join_type_sql) do
    {function_sql, params} = build_table_function_sql(spec.table_function)
    
    sql = [
      join_type_sql,
      " JOIN LATERAL ",
      function_sql,
      " AS ",
      spec.alias,
      " ON true"
    ]
    
    {sql, params}
  end
  
  # Build LATERAL join with correlated subquery
  defp build_subquery_lateral_join(%Spec{} = spec, join_type_sql) do
    # Build the subquery - we need to pass a dummy base query since
    # the actual correlation will be resolved at SQL generation time
    dummy_base = %Selecto{domain: %{}, postgrex_opts: [], set: %{}}
    subquery = spec.subquery_builder.(dummy_base)
    
    # Generate SQL for the subquery
    {subquery_sql, params} = Selecto.to_sql(subquery)
    subquery_iodata = convert_sql_placeholders_to_iodata(subquery_sql, params)
    
    sql = [
      join_type_sql,
      " JOIN LATERAL (",
      subquery_iodata,
      ") AS ",
      spec.alias,
      " ON true"
    ]
    
    # Params are now embedded as {:param, value} markers in subquery_iodata.
    {sql, []}
  end
  
  # Build table function SQL
  defp build_table_function_sql({:unnest, column_ref}) do
    {["UNNEST(", column_ref, ")"], []}
  end
  
  defp build_table_function_sql({:function, func_name, args}) do
    arg_sql = build_function_args(args)
    
    function_sql = [String.upcase(to_string(func_name)), "(", arg_sql, ")"]
    {function_sql, []}
  end
  
  defp build_table_function_sql(unknown) do
    raise ArgumentError, "Unknown table function specification: #{inspect(unknown)}"
  end
  
  # Build function arguments with parameter binding
  defp build_function_args(args) do
    args
    |> Enum.map(&build_function_arg/1)
    |> Enum.intersperse(", ")
  end
  
  # Build individual function argument
  defp build_function_arg({:ref, field}) do
    field  # Correlation reference - no parameters
  end
  
  defp build_function_arg(value) when is_binary(value) do
    if String.contains?(value, ".") do
      # Column reference
      value
    else
      # Literal string value
      {:param, value}
    end
  end
  
  defp build_function_arg(value) when is_number(value) or is_boolean(value) do
    {:param, value}
  end
  
  defp build_function_arg({:literal, value}) do
    {:param, value}
  end
  
  defp build_function_arg(unknown) do
    # Fallback - treat as parameter
    {:param, unknown}
  end
  
  # Build JOIN type SQL
  defp build_join_type(:left), do: "LEFT"
  defp build_join_type(:inner), do: "INNER" 
  defp build_join_type(:right), do: "RIGHT"
  defp build_join_type(:full), do: "FULL"
  defp build_join_type(unknown) do
    raise ArgumentError, "Unknown LATERAL join type: #{inspect(unknown)}"
  end
  
  @doc """
  Integrate LATERAL joins into the main SQL generation pipeline.
  
  This function is called by the main SQL builder to include LATERAL JOIN
  clauses in the generated SQL.
  """
  def integrate_lateral_joins_sql(base_sql_parts, lateral_specs) when is_list(lateral_specs) do
    case build_lateral_joins(lateral_specs) do
      {[], []} -> 
        {base_sql_parts, []}
        
      {lateral_sql_parts, lateral_params} ->
        # Insert LATERAL JOINs after regular JOINs in the SQL
        updated_sql = insert_lateral_joins(base_sql_parts, lateral_sql_parts)
        {updated_sql, lateral_params}
    end
  end
  
  # Insert LATERAL JOIN clauses into the SQL structure
  defp insert_lateral_joins(base_sql_parts, lateral_sql_parts) do
    # Find the position after regular JOINs and before WHERE clause
    insertion_point = find_lateral_insertion_point(base_sql_parts)
    
    case insertion_point do
      nil ->
        # No specific insertion point found, append after FROM
        base_sql_parts ++ [" "] ++ lateral_sql_parts
        
      index ->
        # Insert at specific position
        {before_parts, after_parts} = Enum.split(base_sql_parts, index)
        before_parts ++ [" "] ++ lateral_sql_parts ++ [" "] ++ after_parts
    end
  end
  
  # Find the appropriate insertion point for LATERAL JOINs
  defp find_lateral_insertion_point(sql_parts) do
    sql_parts
    |> Enum.with_index()
    |> Enum.find_value(fn {part, index} ->
      cond do
        String.contains?(to_string(part), "WHERE") -> index
        String.contains?(to_string(part), "GROUP BY") -> index
        String.contains?(to_string(part), "HAVING") -> index
        String.contains?(to_string(part), "ORDER BY") -> index
        String.contains?(to_string(part), "LIMIT") -> index
        true -> nil
      end
    end)
  end

  # Convert SQL with $1-style placeholders to iodata markers that participate
  # in global parameter numbering.
  defp convert_sql_placeholders_to_iodata(sql, params) do
    values_by_index =
      params
      |> Enum.with_index(1)
      |> Map.new(fn {value, idx} -> {idx, value} end)

    Regex.split(~r/(\$\d+)/, sql, include_captures: true, trim: false)
    |> Enum.map(fn part ->
      case Regex.run(~r/^\$(\d+)$/, part, capture: :all_but_first) do
        [idx] ->
          case Map.fetch(values_by_index, String.to_integer(idx)) do
            {:ok, value} -> {:param, value}
            :error -> part
          end

        _ ->
          part
      end
    end)
  end
end
