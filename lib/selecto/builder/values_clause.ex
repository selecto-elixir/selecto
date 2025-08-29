defmodule Selecto.Builder.ValuesClause do
  @moduledoc """
  SQL generation for VALUES clauses in PostgreSQL queries.
  
  Generates inline table definitions from literal data using the VALUES construct.
  Supports both list-of-lists and list-of-maps data formats with proper
  parameter binding and type handling.
  """
  
  alias Selecto.Advanced.ValuesClause.Spec
  
  @doc """
  Generate SQL for a VALUES clause.
  
  Returns SQL in the form:
  - VALUES ('val1', 'val2', 123), ('val3', 'val4', 456) AS alias (col1, col2, col3)
  """
  def build_values_clause(%Spec{} = spec) do
    case spec.validated do
      false -> 
        raise ArgumentError, "VALUES clause specification must be validated before SQL generation"
      true ->
        generate_values_sql(spec)
    end
  end
  
  # Generate the complete VALUES SQL
  defp generate_values_sql(%Spec{} = spec) do
    values_rows = build_values_rows(spec.data, spec.data_type)
    column_list = build_column_list(spec.columns)
    
    [
      "VALUES ",
      values_rows,
      " AS ",
      spec.alias,
      " ",
      column_list
    ]
  end
  
  # Build the VALUES rows section
  defp build_values_rows(data, :list_of_lists) do
    data
    |> Enum.map(&build_values_row_from_list/1)
    |> Enum.intersperse(", ")
  end
  
  defp build_values_rows(data, :list_of_maps) do
    data
    |> Enum.map(&build_values_row_from_map/1)
    |> Enum.intersperse(", ")
  end
  
  # Build a single VALUES row from a list
  defp build_values_row_from_list(row) when is_list(row) do
    values = 
      row
      |> Enum.map(&format_value/1)
      |> Enum.intersperse(", ")
    
    ["(", values, ")"]
  end
  
  # Build a single VALUES row from a map (ordered by sorted keys)
  defp build_values_row_from_map(row) when is_map(row) do
    values = 
      row
      |> Enum.sort_by(fn {key, _value} -> to_string(key) end)
      |> Enum.map(fn {_key, value} -> format_value(value) end)
      |> Enum.intersperse(", ")
    
    ["(", values, ")"]
  end
  
  # Build the column list section
  defp build_column_list(columns) do
    column_names = 
      columns
      |> Enum.map(&quote_identifier/1)
      |> Enum.intersperse(", ")
    
    ["(", column_names, ")"]
  end
  
  # Format individual values for SQL
  defp format_value(nil), do: "NULL"
  defp format_value(value) when is_binary(value) do
    # Use $n parameter placeholders for proper parameter binding
    # For now, we'll use literal values but this should be enhanced
    # to use parameter binding in the full implementation
    escaped = String.replace(value, "'", "''")
    "'#{escaped}'"
  end
  defp format_value(value) when is_integer(value), do: Integer.to_string(value)
  defp format_value(value) when is_float(value), do: Float.to_string(value)
  defp format_value(true), do: "TRUE"
  defp format_value(false), do: "FALSE"
  defp format_value(%Date{} = date), do: "'#{Date.to_string(date)}'"
  defp format_value(%DateTime{} = datetime), do: "'#{DateTime.to_iso8601(datetime)}'"
  defp format_value(%NaiveDateTime{} = datetime), do: "'#{NaiveDateTime.to_iso8601(datetime)}'"
  defp format_value(value), do: "'#{inspect(value)}'"
  
  # Quote SQL identifiers to handle reserved words and special characters
  defp quote_identifier(identifier) do
    "\"#{identifier}\""
  end
  
  @doc """
  Generate a CTE (Common Table Expression) version of the VALUES clause.
  
  Returns SQL in the form:
  WITH alias (col1, col2, col3) AS (VALUES ('val1', 'val2', 123), ('val3', 'val4', 456))
  """
  def build_values_cte(%Spec{} = spec) do
    case spec.validated do
      false ->
        raise ArgumentError, "VALUES clause specification must be validated before CTE generation"
      true ->
        generate_values_cte_sql(spec)
    end
  end
  
  # Generate CTE-style VALUES SQL
  defp generate_values_cte_sql(%Spec{} = spec) do
    values_rows = build_values_rows(spec.data, spec.data_type)
    column_list = build_column_list(spec.columns)
    
    [
      spec.alias,
      " ",
      column_list,
      " AS (VALUES ",
      values_rows,
      ")"
    ]
  end
  
  @doc """
  Generate parameter-bound VALUES clause for use with prepared statements.
  
  Returns {sql_iodata, parameters} tuple where parameters is a flat list
  of values in the order they appear in the SQL.
  """
  def build_values_clause_with_params(%Spec{} = spec) do
    case spec.validated do
      false ->
        raise ArgumentError, "VALUES clause specification must be validated before parameterized SQL generation"
      true ->
        generate_parameterized_values_sql(spec)
    end
  end
  
  # Generate parameterized VALUES SQL with parameter binding
  defp generate_parameterized_values_sql(%Spec{} = spec) do
    {values_rows, parameters} = build_parameterized_values_rows(spec.data, spec.data_type, 1, [])
    column_list = build_column_list(spec.columns)
    
    sql = [
      "VALUES ",
      values_rows,
      " AS ",
      spec.alias,
      " ",
      column_list
    ]
    
    {sql, parameters}
  end
  
  # Build parameterized VALUES rows with parameter collection
  defp build_parameterized_values_rows([], _data_type, _param_index, parameters) do
    {[], parameters}
  end
  
  defp build_parameterized_values_rows([row | rest], :list_of_lists, param_index, parameters) do
    {row_sql, new_param_index, row_parameters} = build_parameterized_row_from_list(row, param_index)
    {rest_sql, rest_parameters} = build_parameterized_values_rows(rest, :list_of_lists, new_param_index, parameters ++ row_parameters)
    
    combined_sql = case rest_sql do
      [] -> row_sql
      _ -> [row_sql, ", ", rest_sql]
    end
    
    {combined_sql, rest_parameters}
  end
  
  defp build_parameterized_values_rows([row | rest], :list_of_maps, param_index, parameters) do
    {row_sql, new_param_index, row_parameters} = build_parameterized_row_from_map(row, param_index)
    {rest_sql, rest_parameters} = build_parameterized_values_rows(rest, :list_of_maps, new_param_index, parameters ++ row_parameters)
    
    combined_sql = case rest_sql do
      [] -> row_sql
      _ -> [row_sql, ", ", rest_sql]
    end
    
    {combined_sql, rest_parameters}
  end
  
  # Build parameterized row from list
  defp build_parameterized_row_from_list(row, param_index) do
    {values_sql, final_index, row_params} = 
      row
      |> Enum.reduce({[], param_index, []}, fn value, {sql_acc, index, params_acc} ->
        param_placeholder = "$#{index}"
        new_sql = case sql_acc do
          [] -> [param_placeholder]
          _ -> [sql_acc, ", ", param_placeholder]
        end
        {new_sql, index + 1, [value | params_acc]}
      end)
    
    row_sql = ["(", values_sql, ")"]
    {row_sql, final_index, Enum.reverse(row_params)}
  end
  
  # Build parameterized row from map (ordered by sorted keys)
  defp build_parameterized_row_from_map(row, param_index) do
    sorted_values = 
      row
      |> Enum.sort_by(fn {key, _value} -> to_string(key) end)
      |> Enum.map(fn {_key, value} -> value end)
    
    build_parameterized_row_from_list(sorted_values, param_index)
  end
end