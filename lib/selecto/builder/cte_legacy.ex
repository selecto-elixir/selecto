defmodule Selecto.Builder.Cte do
  @moduledoc false

  alias Selecto.Builder.CteCompat

  defdelegate build_cte(name, query_iodata, params), to: CteCompat

  def build_recursive_cte(name, base_sql, base_params, recursive_sql, recursive_params) do
    CteCompat.build_recursive_cte(name, base_sql, recursive_sql, base_params, recursive_params)
  end

  defdelegate build_cte_from_selecto(name, selecto), to: CteCompat

  defdelegate build_recursive_cte_from_selecto(name, base_selecto, recursive_selecto),
    to: CteCompat

  def build_with_clause_from_selecto([]), do: {[], []}
  defdelegate build_with_clause_from_selecto(cte_queries), to: CteCompat
  defdelegate build_hierarchy_cte_from_selecto(name, domain, connection, opts), to: CteCompat

  def build_hierarchy_cte_from_selecto(name, domain, opts) when is_list(opts) or is_map(opts) do
    CteCompat.build_hierarchy_cte_from_selecto(name, domain, nil, opts)
  end

  def build_hierarchy_cte_from_selecto(name, domain, connection) do
    CteCompat.build_hierarchy_cte_from_selecto(name, domain, connection, %{})
  end

  def build_with_clause([]), do: {[], []}

  def build_with_clause(ctes) do
    {definitions, param_sets} = Enum.unzip(ctes)

    {[
       "WITH ",
       Enum.intersperse(definitions, ", "),
       " "
     ], List.flatten(param_sets)}
  end

  defdelegate integrate_ctes_with_query(ctes, query_iodata, query_params), to: CteCompat
end
