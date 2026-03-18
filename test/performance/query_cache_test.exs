defmodule Selecto.Performance.QueryCacheTest do
  use ExUnit.Case, async: false

  alias Selecto.Performance.QueryCache

  setup do
    case Process.whereis(QueryCache) do
      nil ->
        :ok

      pid ->
        if Process.alive?(pid) do
          try do
            GenServer.stop(pid)
          catch
            :exit, _reason -> :ok
          end
        else
          :ok
        end
    end

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
    assert stats.item_count == 1

    assert :ok = QueryCache.clear()
    assert :miss = QueryCache.get("k2")

    cleared_stats = QueryCache.stats()
    assert cleared_stats.item_count == 0
    assert cleared_stats.size_bytes == 0
  end

  test "start_link is idempotent" do
    pid = Process.whereis(QueryCache)
    assert is_pid(pid)

    assert {:ok, same_pid} = QueryCache.start_link(max_size: 20)
    assert same_pid == pid
  end

  test "warmup stores entries" do
    key_source = %{query: "warmup"}
    result = %{rows: [[42]]}

    assert {:ok, 1} = QueryCache.warmup([{key_source, result}])

    key = QueryCache.generate_key(key_source)
    assert {:ok, ^result} = QueryCache.get(key)
  end

  test "api is safe when cache is not running" do
    assert is_pid(Process.whereis(QueryCache))
    :ok = GenServer.stop(QueryCache)

    assert :miss = QueryCache.get("missing")
    assert :ok = QueryCache.put("k", "v")
    assert {:ok, 0} = QueryCache.invalidate("k")
    assert {:ok, 0} = QueryCache.invalidate_by_tags(["tag"])
    assert :ok = QueryCache.clear()
    assert %{status: :not_started} = QueryCache.stats()
    assert {:ok, 0} = QueryCache.warmup([])
  end

  test "counter-backed stats track hit and miss totals" do
    QueryCache.put("tracked", %{rows: [[1]]})
    Process.sleep(10)

    assert {:ok, %{rows: [[1]]}} = QueryCache.get("tracked")
    assert :miss = QueryCache.get("missing")

    stats = QueryCache.stats()
    assert stats.hits == 1
    assert stats.misses == 1
    assert stats.hit_rate == 50.0
  end

  test "stats can be disabled" do
    :ok = GenServer.stop(QueryCache)
    {:ok, _pid} = QueryCache.start_link(max_size: 10, default_ttl: 1000, track_stats: false)

    QueryCache.put("x", "y")
    assert {:ok, "y"} = QueryCache.get("x")

    assert %{stats_disabled: true} = QueryCache.stats()
  end
end
