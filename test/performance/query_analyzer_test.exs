defmodule Selecto.Performance.QueryAnalyzerTest do
  use ExUnit.Case, async: true

  alias Selecto.Performance.QueryAnalyzer

  defp selecto do
    domain = %{
      name: "Query analyzer",
      source: %{
        source_table: "users",
        primary_key: :id,
        fields: [:id, :name],
        redact_fields: [],
        columns: %{id: %{type: :integer}, name: %{type: :string}},
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }

    Selecto.configure(domain, [])
    |> Selecto.select(["id", "name"])
  end

  test "analyze_query returns invalid connection error" do
    assert {:error, :invalid_connection} = QueryAnalyzer.analyze_query(selecto())
  end

  test "analyze_query accepts explain options" do
    assert {:error, :invalid_connection} =
             QueryAnalyzer.analyze_query(selecto(),
               format: :text,
               analyze: false,
               buffers: false,
               verbose: true,
               costs: false,
               timing: false,
               summary: false,
               settings: true
             )
  end

  test "analyze_query rejects unsupported explain formats" do
    assert_raise ArgumentError, ~r/invalid EXPLAIN format/, fn ->
      QueryAnalyzer.analyze_query(selecto(), format: "pdf")
    end
  end

  test "repo-like atom connection is wrapped as explain failure" do
    repo_like =
      selecto()
      |> Map.put(:postgrex_opts, :invalid_connection)
      |> Map.put(:connection, :invalid_connection)

    assert {:error, {:explain_failed, _}} = QueryAnalyzer.analyze_query(repo_like)
  end

  test "pool connection failures are wrapped" do
    pooled =
      selecto()
      |> Map.put(:postgrex_opts, {:pool, :bad_pool_ref})
      |> Map.put(:connection, {:pool, :bad_pool_ref})

    assert {:error, {:explain_failed, _}} = QueryAnalyzer.analyze_query(pooled)
  end

  test "table statistics handles mixed query structures and invalid connection" do
    selecto_map = %{
      source: %{source_table: "users"},
      joins: %{posts: %{table: "posts"}},
      postgrex_opts: []
    }

    assert {:ok, stats} = QueryAnalyzer.get_table_statistics(selecto_map)
    assert Map.has_key?(stats, "users")
    assert Map.has_key?(stats, "posts")
    assert stats["users"][:error] == "Could not fetch statistics"
  end

  test "public wrappers propagate analysis errors" do
    assert {:error, :invalid_connection} = QueryAnalyzer.get_query_plan(selecto())
    assert {:error, :invalid_connection} = QueryAnalyzer.analyze_index_usage(selecto())
  end

  test "compare_queries short-circuits on first failure" do
    s1 = selecto()
    s2 = selecto()
    assert {:error, :invalid_connection} = QueryAnalyzer.compare_queries(s1, s2)
  end
end
