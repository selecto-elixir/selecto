defmodule Selecto.Phase1IntegrationTest do
  use ExUnit.Case

  test "core compatibility modules are available" do
    assert Code.ensure_loaded?(Selecto.Builder.Cte)
    assert Code.ensure_loaded?(Selecto.Builder.Sql.Hierarchy)
    assert function_exported?(Selecto.Builder.Cte, :build_cte, 3)
    assert function_exported?(Selecto.SQL.Params, :finalize_with_ctes, 1)
  end

  test "hierarchy builders return iodata tuples" do
    {adj_sql, adj_params} =
      Selecto.Builder.Sql.Hierarchy.build_adjacency_list_cte(nil, :test, %{source: "tbl"})

    {mp_sql, mp_params} =
      Selecto.Builder.Sql.Hierarchy.build_materialized_path_query(nil, :test, %{source: "tbl"})

    {cl_sql, cl_params} =
      Selecto.Builder.Sql.Hierarchy.build_closure_table_query(nil, :test, %{source: "tbl"})

    assert is_list(adj_sql) and is_list(adj_params)
    assert is_list(mp_sql) and is_list(mp_params)
    assert is_list(cl_sql) and is_list(cl_params)
  end
end
