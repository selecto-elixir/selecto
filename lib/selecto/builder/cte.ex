defmodule Selecto.Builder.Cte do
  @moduledoc """
  Common Table Expression (CTE) builder for advanced join patterns.
  
  Generates parameterized CTEs using iodata for safe SQL construction.
  Supports both simple and recursive CTEs with proper parameter scoping.
  """
  
  @doc """
  Build a simple CTE with parameterized content.
  
  Returns: {cte_iodata, params}
  
  ## Examples
  
      iex> query_iodata = ["SELECT id, name FROM users WHERE active = ", {:param, true}]
      iex> params = [true]
      iex> {cte_iodata, result_params} = Selecto.Builder.Cte.build_cte("active_users", query_iodata, params)
      iex> result_params
      [true]
  """
  def build_cte(name, query_iodata, params) when is_binary(name) do
    cte_iodata = [
      name, " AS (",
      query_iodata,
      ")"
    ]
    {cte_iodata, params}
  end
  
  @doc """
  Build a recursive CTE with base case and recursive case.
  
  Returns: {recursive_cte_iodata, combined_params}
  
  ## Examples
  
      iex> base_iodata = ["SELECT id, name, 0 as level FROM categories WHERE parent_id IS NULL"]
      iex> recursive_iodata = ["SELECT c.id, c.name, p.level + 1 FROM categories c JOIN hierarchy p ON c.parent_id = p.id WHERE p.level < ", {:param, 5}]
      iex> {cte_iodata, params} = Selecto.Builder.Cte.build_recursive_cte("hierarchy", base_iodata, [], recursive_iodata, [5])
      iex> params
      [5]
  """
  def build_recursive_cte(name, base_query_iodata, base_params, recursive_query_iodata, recursive_params) do
    recursive_cte_iodata = [
      "RECURSIVE ", name, " AS (",
      base_query_iodata,
      " UNION ALL ",
      recursive_query_iodata,
      ")"
    ]
    combined_params = base_params ++ recursive_params
    {recursive_cte_iodata, combined_params}
  end
  
  @doc """
  Combine multiple CTEs into a single WITH clause.
  
  Returns: {with_clause_iodata, combined_params}
  
  ## Examples
  
      iex> cte1 = {["users_cte AS (SELECT * FROM users)"], []}
      iex> cte2 = {["posts_cte AS (SELECT * FROM posts WHERE active = ", {:param, true}, ")"], [true]} 
      iex> {with_clause, params} = Selecto.Builder.Cte.build_with_clause([cte1, cte2])
      iex> params
      [true]
  """
  def build_with_clause(ctes) when is_list(ctes) do
    case ctes do
      [] -> {[], []}
      [{first_cte, first_params} | rest] ->
        {cte_parts, all_params} = 
          Enum.reduce(rest, {[first_cte], first_params}, fn {cte_iodata, params}, {acc_ctes, acc_params} ->
            {acc_ctes ++ [", ", cte_iodata], acc_params ++ params}
          end)
        
        with_clause = ["WITH ", cte_parts, " "]
        {with_clause, all_params}
    end
  end
  
  @doc """
  Prepend CTEs to a main query with proper parameter coordination.
  
  Returns: {complete_query_iodata, combined_params}
  
  ## Examples
  
      iex> cte1 = {["users_cte AS (SELECT * FROM users)"], []}
      iex> main_query = ["SELECT * FROM users_cte"]
      iex> {final_query, params} = Selecto.Builder.Cte.integrate_ctes_with_query([cte1], main_query, [])
      iex> params
      []
  """
  def integrate_ctes_with_query([], main_query_iodata, main_params) do
    {main_query_iodata, main_params}
  end
  
  def integrate_ctes_with_query(ctes, main_query_iodata, main_params) when is_list(ctes) do
    {with_clause, cte_params} = build_with_clause(ctes)
    
    complete_query = [
      with_clause,
      main_query_iodata
    ]
    
    combined_params = cte_params ++ main_params
    {complete_query, combined_params}
  end
end