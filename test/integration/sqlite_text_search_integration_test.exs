defmodule Selecto.Integration.SQLiteTextSearchTest do
  use ExUnit.Case, async: true

  defmodule SQLiteUnavailableAdapter do
    @behaviour Selecto.DB.Adapter

    def name, do: :sqlite
    def connect(_opts), do: {:error, :unsupported}
    def execute(_connection, _query, _params, _opts), do: {:error, :unsupported}
    def placeholder(_index), do: "?"
    def quote_identifier(identifier), do: ~s("#{identifier}")
    def supports?(_feature), do: false
    def fts5_available?(_connection), do: false
  end

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

  test "sqlite to_sql compiles multi-field FTS5 predicate", %{domain: domain} do
    query =
      Selecto.configure(domain, [], validate: false)
      |> Map.put(:adapter, SelectoDBSQLite.Adapter)
      |> Selecto.select(["name"])
      |> Selecto.filter({["name", "description"], {:text_search, "wireless charger"}})

    {sql, params} = Selecto.to_sql(query)

    assert sql =~ "selecto_root.name MATCH ?"
    assert sql =~ "selecto_root.description MATCH ?"
    assert sql =~ ~r/\sOR\s/
    assert params == ["wireless charger", "wireless charger"]
  end

  test "sqlite to_sql accepts boolean mode as fts query syntax passthrough", %{domain: domain} do
    query =
      Selecto.configure(domain, [], validate: false)
      |> Map.put(:adapter, SelectoDBSQLite.Adapter)
      |> Selecto.select(["name"])
      |> Selecto.filter({"name", {:text_search, "wireless charger", [mode: :boolean]}})

    {sql, params} = Selecto.to_sql(query)

    assert sql =~ "selecto_root.name MATCH ?"
    assert params == ["wireless charger"]
  end

  test "sqlite to_sql supports phrase mode by quoting the fts query", %{domain: domain} do
    query =
      Selecto.configure(domain, [], validate: false)
      |> Map.put(:adapter, SelectoDBSQLite.Adapter)
      |> Selecto.select(["name"])
      |> Selecto.filter({"description", {:text_search, "charging pad", [mode: :phrase]}})

    {sql, params} = Selecto.to_sql(query)

    assert sql =~ "selecto_root.description MATCH ?"
    assert params == ["\"charging pad\""]
  end

  test "sqlite_fts_rank adds bm25 selector for configured FTS fields", %{domain: domain} do
    query =
      Selecto.configure(domain, [], validate: false)
      |> Map.put(:adapter, SelectoDBSQLite.Adapter)
      |> Selecto.select(["name"])
      |> Selecto.sqlite_fts_rank(["name", "description"], as: "relevance", weights: [5.0, 1.0])

    {sql, params} = Selecto.to_sql(query)

    assert sql =~ ~r/bm25\(products_fts, 5\.0, 1\.0\) AS "relevance"/i
    assert params == []
  end

  test "sqlite runtime gating raises when FTS5 is unavailable", %{domain: domain} do
    query =
      Selecto.configure(domain, :runtime_probe, validate: false)
      |> Map.put(:adapter, SQLiteUnavailableAdapter)
      |> Selecto.select(["name"])
      |> Selecto.filter({"name", {:text_search, "wireless charger"}})

    assert_raise RuntimeError, ~r/FTS5 is not available on the current connection/i, fn ->
      Selecto.to_sql(query)
    end
  end

  test "sqlite to_sql still rejects unsupported query expansion mode", %{domain: domain} do
    query =
      Selecto.configure(domain, [], validate: false)
      |> Map.put(:adapter, SelectoDBSQLite.Adapter)
      |> Selecto.select(["name"])
      |> Selecto.filter({"name", {:text_search, "wireless charger", [mode: :query_expansion]}})

    assert_raise RuntimeError, ~r/does not support this text search mode/i, fn ->
      Selecto.to_sql(query)
    end
  end
end
