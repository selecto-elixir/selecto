defmodule Selecto.Integration.MySQLTextSearchTest do
  use ExUnit.Case, async: true

  setup do
    domain = %{
      name: "product_domain",
      source: %{
        source_table: "products",
        primary_key: :id,
        fields: [:id, :name, :description, :status],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          description: %{type: :string},
          status: %{type: :string}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }

    {:ok, domain: domain}
  end

  test "mysql to_sql compiles boolean mode text search", %{domain: domain} do
    query =
      Selecto.configure(domain, [], validate: false)
      |> Map.put(:adapter, SelectoDBMySQL.Adapter)
      |> Selecto.select(["name", "status"])
      |> Selecto.filter(
        {["name", "description"], {:text_search, "+wireless -charger", [mode: :boolean]}}
      )

    {sql, params} = Selecto.to_sql(query)

    assert sql =~ "MATCH(selecto_root.name, selecto_root.description) AGAINST (? IN BOOLEAN MODE)"
    assert params == ["+wireless -charger"]
  end

  test "mysql to_sql compiles query expansion text search", %{domain: domain} do
    query =
      Selecto.configure(domain, [], validate: false)
      |> Map.put(:adapter, SelectoDBMySQL.Adapter)
      |> Selecto.select(["name"])
      |> Selecto.filter({"name", {:text_search, "wireless charger", [mode: :query_expansion]}})

    {sql, params} = Selecto.to_sql(query)

    assert sql =~
             "MATCH(selecto_root.name) AGAINST (? IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION)"

    assert params == ["wireless charger"]
  end

  test "mysql to_sql compiles keyword-form text search config with field override", %{
    domain: domain
  } do
    query =
      Selecto.configure(domain, [], validate: false)
      |> Map.put(:adapter, SelectoDBMySQL.Adapter)
      |> Selecto.select(["name", "status"])
      |> Selecto.filter(
        {"name",
         {:text_search,
          [query: "+wireless -charger", fields: ["name", "description"], mode: :boolean]}}
      )

    {sql, params} = Selecto.to_sql(query)

    assert sql =~ "MATCH(selecto_root.name, selecto_root.description) AGAINST (? IN BOOLEAN MODE)"
    assert params == ["+wireless -charger"]
  end
end
