defmodule Selecto.Builder.Cte do
  @moduledoc """
  Backward-compatible CTE helpers retained for legacy tests and integrations.

  New code should prefer `Selecto.Builder.CTE`.
  """

  alias Selecto.Builder.Sql

  @doc """
  Build a legacy non-recursive CTE tuple from a pre-built SQL iodata fragment.
  """
  def build_cte(name, query_iodata, params) when is_binary(name) do
    {[name, " AS (", query_iodata, ")"], params}
  end

  @doc """
  Build a legacy recursive CTE tuple from pre-built anchor and recursive SQL.
  """
  def build_recursive_cte(name, base_sql, recursive_sql, base_params, recursive_params)
      when is_binary(name) do
    {["RECURSIVE ", name, " AS (", base_sql, " UNION ALL ", recursive_sql, ")"], base_params ++ recursive_params}
  end

  @doc """
  Build a single CTE definition directly from a Selecto query struct.
  """
  def build_cte_from_selecto(name, selecto) when is_binary(name) do
    {sql, _aliases, params} = Sql.build(selecto, [])
    {[name, " AS (", [sql], ")"], params}
  end

  @doc """
  Build a recursive CTE definition from base and recursive Selecto query structs.
  """
  def build_recursive_cte_from_selecto(name, base_selecto, recursive_selecto) when is_binary(name) do
    {base_sql, _base_aliases, base_params} = Sql.build(base_selecto, [])
    {recursive_sql, _recursive_aliases, recursive_params} = Sql.build(recursive_selecto, [])

    {["RECURSIVE ", name, " AS (", [base_sql], " UNION ALL ", [recursive_sql], ")"], base_params ++ recursive_params}
  end

  @doc """
  Build a WITH clause iodata fragment from `{name, selecto}` pairs.
  """
  def build_with_clause_from_selecto(cte_queries) when is_list(cte_queries) do
    {definitions, param_sets} =
      cte_queries
      |> Enum.map(fn {name, selecto} -> build_cte_from_selecto(name, selecto) end)
      |> Enum.unzip()

    with_clause = ["WITH ", Enum.intersperse(definitions, ", ")]
    {with_clause, List.flatten(param_sets)}
  end

  @doc """
  Build a lightweight recursive hierarchy CTE fragment with a depth limit parameter.
  """
  def build_hierarchy_cte_from_selecto(name, domain, _connection, opts) when is_binary(name) do
    source_table = domain.source.source_table
    id_field = Map.get(opts, :id_field, "id")
    name_field = Map.get(opts, :name_field, "name")
    parent_field = Map.get(opts, :parent_field, "parent_id")
    depth_limit = Map.get(opts, :depth_limit, 5)

    base_sql = "SELECT #{id_field}, #{name_field}, #{parent_field}, 0 as level FROM #{source_table} WHERE #{parent_field} IS NULL"

    recursive_sql =
      "SELECT c.#{id_field}, c.#{name_field}, c.#{parent_field}, h.level + 1 " <>
        "FROM #{source_table} c JOIN #{name} h ON c.#{parent_field} = h.#{id_field} WHERE h.level < $1"

    {["RECURSIVE ", name, " AS (", [base_sql], " UNION ALL ", [recursive_sql], ")"], [depth_limit]}
  end

  @doc """
  Integrate legacy `{cte_definition_iodata, params}` tuples with a main query.
  """
  def integrate_ctes_with_query(ctes, main_query, main_params) when is_list(ctes) do
    {cte_definitions, cte_param_sets} = Enum.unzip(ctes)

    final_query =
      case cte_definitions do
        [] -> main_query
        _ -> [["WITH ", Enum.intersperse(cte_definitions, ", ")], main_query]
      end

    {final_query, List.flatten(cte_param_sets) ++ main_params}
  end
end
