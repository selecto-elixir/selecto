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

  test "repo-like atom connection can raise runtime error" do
    repo_like = Map.put(selecto(), :postgrex_opts, :invalid_connection)

    assert_raise RuntimeError, fn ->
      QueryAnalyzer.analyze_query(repo_like)
    end
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
