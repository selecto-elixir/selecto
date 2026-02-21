defmodule Selecto.Performance.QueryCacheTest do
  use ExUnit.Case, async: false

  alias Selecto.Performance.QueryCache

  setup do
    {:ok, _pid} = QueryCache.start_link(max_size: 10, default_ttl: 1000)
    :ok
  end

  test "generate key from SQL is stable" do
    k1 = QueryCache.generate_key_from_sql("select 1", [])
    k2 = QueryCache.generate_key_from_sql("select 1", [])
    assert k1 == k2
  end

  test "basic put/get/invalidate flow" do
    QueryCache.put("k", %{rows: [[1]]})
    Process.sleep(10)

    assert {:ok, %{rows: [[1]]}} = QueryCache.get("k")
    assert {:ok, _} = QueryCache.invalidate("k")
    assert :miss = QueryCache.get("k")
  end

  test "stats and clear are callable" do
    QueryCache.put("k2", "v2")
    Process.sleep(10)

    stats = QueryCache.stats()
    assert is_map(stats)

    assert :ok = QueryCache.clear()
    assert :miss = QueryCache.get("k2")
  end
end
