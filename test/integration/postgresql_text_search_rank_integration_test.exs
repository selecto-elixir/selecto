defmodule Selecto.Integration.PostgreSQLTextSearchRankTest do
  use ExUnit.Case, async: true

  setup do
    domain = %{
      name: "postgresql_product_domain",
      source: %{
        source_table: "products",
        primary_key: :id,
        fields: [:id, :name, :search_document],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          search_document: %{type: :tsvector}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }

    {:ok, domain: domain}
  end

  test "postgresql text_search_rank compiles ts_rank with websearch query", %{domain: domain} do
    query =
      Selecto.configure(domain, [], validate: false)
      |> Selecto.select(["name"])
      |> Selecto.text_search_rank(["search_document"],
        as: "relevance",
        query: "wireless charger",
        mode: :web
      )

    {sql, params} = Selecto.to_sql(query)

    assert sql =~
             ~r/ts_rank\(selecto_root\.search_document, websearch_to_tsquery\('wireless charger'\)\)/i

    assert params == []
  end

  test "postgresql text_search_rank compiles plain query mode", %{domain: domain} do
    query =
      Selecto.configure(domain, [], validate: false)
      |> Selecto.select(["name"])
      |> Selecto.text_search_rank(["search_document"],
        as: "relevance",
        query: "wireless charger",
        mode: :natural
      )

    {sql, _params} = Selecto.to_sql(query)

    assert sql =~
             ~r/ts_rank\(selecto_root\.search_document, plainto_tsquery\('wireless charger'\)\)/i
  end

  test "postgresql text_search_rank requires a query option", %{domain: domain} do
    query =
      Selecto.configure(domain, [], validate: false)
      |> Selecto.select(["name"])

    assert_raise ArgumentError, ~r/requires a :query option/i, fn ->
      Selecto.text_search_rank(query, ["search_document"], as: "relevance")
    end
  end
end
