defmodule Selecto.CteSqlTest do
  use ExUnit.Case, async: true

  alias Selecto.Advanced.CTE
  alias Selecto.Builder.CteSql
  alias Selecto.SQL.Params

  defp domain do
    %{
      name: "Users",
      source: %{
        source_table: "users",
        primary_key: :id,
        fields: [:id, :name, :active],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          active: %{type: :boolean}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end

  test "build_with_clause/1 supports raw CTE entries" do
    {with_clause, params} =
      CteSql.build_with_clause([
        {:raw_cte, ["active_users AS (SELECT 1 AS id)"], []}
      ])

    with_sql = IO.iodata_to_binary(with_clause)
    assert with_sql =~ ~r/^with\s+/i
    assert with_sql =~ ~r/active_users\s+as\s*\(/i
    assert params == []
  end

  test "build_with_clause/1 uses recursive WITH when recursive entries exist" do
    {with_clause, params} =
      CteSql.build_with_clause([
        {:raw_recursive_cte, ["tree AS (SELECT 1 AS id UNION ALL SELECT 2 AS id)"], []}
      ])

    with_sql = IO.iodata_to_binary(with_clause)
    assert with_sql =~ ~r/^with\s+recursive\s+/i
    assert with_sql =~ ~r/tree\s+as\s*\(/i
    assert params == []
  end

  test "build_with_clause/1 supports validated structured CTE specs" do
    spec =
      CTE.create_cte("active_users", fn ->
        Selecto.configure(domain(), nil, validate: false)
        |> Selecto.select(["id", "name"])
        |> Selecto.filter({"active", true})
      end)

    {with_clause, params} = CteSql.build_with_clause([spec])
    {with_sql, finalized_params} = Params.finalize(with_clause)

    assert with_sql =~ ~r/^with\s+/i
    assert with_sql =~ ~r/active_users\s+as\s*\(/i
    assert with_sql =~ ~r/select/i
    assert params == [true]
    assert finalized_params == [true]
  end

  test "integrate_ctes_with_query/3 keeps CTE params before query params" do
    ctes = [
      {:raw_cte, ["active_users AS (SELECT id FROM users WHERE active = ", {:param, true}, ")"],
       [true]}
    ]

    query_iodata = ["SELECT * FROM active_users WHERE id > ", {:param, 10}]
    query_params = [10]

    {iodata, params} = CteSql.integrate_ctes_with_query(ctes, query_iodata, query_params)
    {sql, finalized_params} = Params.finalize(iodata)

    assert sql =~ ~r/^with\s+/i
    assert sql =~ ~r/select\s+\*/i
    assert params == [true, 10]
    assert finalized_params == [true, 10]
  end

  test "build_with_clause/1 rejects legacy untagged tuple entries" do
    assert_raise ArgumentError, ~r/Unsupported CTE entries/, fn ->
      CteSql.build_with_clause([{["WITH x AS (SELECT 1)"], []}])
    end
  end

  test "create_cte_reference/1 returns queryable reference map" do
    ref = CteSql.create_cte_reference("hierarchy")

    assert ref.__cte_reference__ == true
    assert ref.name == "hierarchy"
    assert ref.source == "hierarchy"
    assert ref.type == :cte
  end
end
