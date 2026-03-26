defmodule Selecto.Integration.SQLiteTextSearchTest do
  use ExUnit.Case, async: true

  setup do
    domain = %{
      name: "sqlite_product_domain",
      source: %{
        source_table: "products_fts",
        primary_key: :rowid,
        fields: [:rowid, :name, :description],
        redact_fields: [],
        columns: %{
          rowid: %{type: :integer},
          name: %{type: :fts5},
          description: %{type: :fts5}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }

    {:ok, domain: domain}
  end

  test "sqlite to_sql compiles FTS5 single-field text search", %{domain: domain} do
    query =
      Selecto.configure(domain, [], validate: false)
      |> Map.put(:adapter, SelectoDBSQLite.Adapter)
      |> Selecto.select(["name"])
      |> Selecto.filter({"name", {:text_search, "wireless charger"}})

    {sql, params} = Selecto.to_sql(query)

    assert sql =~ "selecto_root.name MATCH ?"
    assert params == ["wireless charger"]
  end

  test "sqlite to_sql rejects unsupported multi-field FTS5 predicate", %{domain: domain} do
    query =
      Selecto.configure(domain, [], validate: false)
      |> Map.put(:adapter, SelectoDBSQLite.Adapter)
      |> Selecto.select(["name"])
      |> Selecto.filter({["name", "description"], {:text_search, "wireless charger"}})

    assert_raise RuntimeError, ~r/one configured field per predicate/i, fn ->
      Selecto.to_sql(query)
    end
  end

  test "sqlite to_sql rejects unsupported boolean mode", %{domain: domain} do
    query =
      Selecto.configure(domain, [], validate: false)
      |> Map.put(:adapter, SelectoDBSQLite.Adapter)
      |> Selecto.select(["name"])
      |> Selecto.filter({"name", {:text_search, "wireless charger", [mode: :boolean]}})

    assert_raise RuntimeError, ~r/does not support this text search mode/i, fn ->
      Selecto.to_sql(query)
    end
  end
end
