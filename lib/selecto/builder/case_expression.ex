defmodule Selecto.Builder.CaseExpression do
  @moduledoc """
  SQL generation for PostgreSQL CASE expressions.
  
  Generates SQL for both simple and searched CASE expressions with proper
  condition handling, value escaping, and PostgreSQL-specific syntax.
  """
  
  alias Selecto.Advanced.CaseExpression.Spec
  alias Selecto.Builder.Sql.Where
  
  @doc """
  Build CASE expression SQL from specification.
  
  Returns {case_expression_iodata, parameters} tuple with proper PostgreSQL
  CASE syntax and parameter bindings.
  """
  def build_case_expression(%Spec{} = spec, selecto \\ nil) do
    case spec.validated do
      false ->
        raise ArgumentError, "CASE expression specification must be validated before SQL generation"
      true ->
        generate_case_sql(spec, selecto)
    end
  end
  
  @doc """
  Build CASE expression SQL for SELECT clause integration.
  
  Returns formatted CASE expression with proper aliasing for column selection.
  """
  def build_case_for_select(%Spec{} = spec, selecto \\ nil) do
    {case_sql, params} = build_case_expression(spec, selecto)
    
    case spec.alias do
      nil ->
        {case_sql, params}
      alias_name ->
        aliased_sql = [case_sql, " AS ", escape_identifier(alias_name)]
        {aliased_sql, params}
    end
  end
  
  # Generate SQL for simple CASE expression  
  defp generate_case_sql(%Spec{type: :simple} = spec, _selecto) do
    # Build WHEN clauses
    {when_clauses_sql, when_params} = build_simple_when_clauses(spec.when_clauses)
    
    # Build ELSE clause
    {else_sql, else_params} = build_else_clause(spec.else_clause)
    
    # Combine into CASE expression
    case_parts = [
      "CASE ", 
      spec.column,
      when_clauses_sql,
      else_sql,
      " END"
    ]
    
    case_sql = Enum.reject(case_parts, &(&1 == ""))
    combined_params = when_params ++ else_params
    
    {case_sql, combined_params}
  end
  
  # Generate SQL for searched CASE expression (needs selecto context)
  defp generate_case_sql(%Spec{type: :searched} = spec, selecto) do
    # Build WHEN clauses with conditions
    {when_clauses_sql, when_params} = build_searched_when_clauses(spec.when_clauses, selecto)
    
    # Build ELSE clause
    {else_sql, else_params} = build_else_clause(spec.else_clause)
    
    # Combine into CASE expression
    case_parts = [
      "CASE",
      when_clauses_sql,
      else_sql,
      " END"
    ]
    
    case_sql = Enum.reject(case_parts, &(&1 == ""))
    combined_params = when_params ++ else_params
    
    {case_sql, combined_params}
  end
  
  # Build WHEN clauses for simple CASE
  defp build_simple_when_clauses(when_clauses) do
    {when_parts, all_params} = 
      when_clauses
      |> Enum.map(fn {value, result} ->
        # For simple values, just use parameter tokens directly
        when_clause = [" WHEN ", {:param, value}, " THEN ", {:param, result}]
        params = [value, result]
        
        {when_clause, params}
      end)
      |> Enum.unzip()
    
    when_clauses_sql = List.flatten(when_parts)
    combined_params = List.flatten(all_params)
    
    {when_clauses_sql, combined_params}
  end
  
  # Build WHEN clauses for searched CASE
  defp build_searched_when_clauses(when_clauses, selecto \\ nil) do
    {when_parts, all_params} = 
      when_clauses
      |> Enum.map(fn {conditions, result} ->
        # Build condition SQL using Where builder
        {conditions_sql, conditions_params} = build_when_conditions(conditions, selecto)
        
        # For simple string literals, just use parameter tokens directly
        when_clause = [" WHEN ", conditions_sql, " THEN ", {:param, result}]
        combined_params = conditions_params ++ [result]
        
        {when_clause, combined_params}
      end)
      |> Enum.unzip()
    
    when_clauses_sql = List.flatten(when_parts)
    combined_params = List.flatten(all_params)
    
    {when_clauses_sql, combined_params}
  end
  
  # Build condition SQL for searched CASE WHEN clauses
  defp build_when_conditions(conditions, selecto) when is_list(conditions) do
    # Use the provided selecto context, or create a minimal one
    working_selecto = selecto || %{set: %{}, domain: %{}, config: %{}}
    
    # Build the WHERE conditions using the existing Where builder
    {_join, conditions_iodata, conditions_params} = 
      Where.build(working_selecto, {:and, conditions})
    
    {conditions_iodata, conditions_params}
  end
  
  # Build ELSE clause
  defp build_else_clause(nil), do: {"", []}
  defp build_else_clause(else_value) do
    # For simple literals, just use parameter tokens directly
    else_clause = [" ELSE ", {:param, else_value}]
    {else_clause, [else_value]}
  end
  
  
  # Escape SQL identifier (table names, column names)
  defp escape_identifier(identifier) when is_binary(identifier) do
    # Simple identifier escaping - quote if contains special characters
    if String.match?(identifier, ~r/^[a-zA-Z_][a-zA-Z0-9_]*$/) and 
       not String.match?(identifier, ~r/^(select|from|where|order|group|having|with|case|when|then|else|end)$/i) do
      identifier
    else
      "\"#{String.replace(identifier, "\"", "\"\"")}\""
    end
  end
end