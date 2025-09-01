defmodule Selecto.Performance.QueryCacheTest do
  use ExUnit.Case, async: false
  
  alias Selecto.Performance.QueryCache
  
  setup do
    # Start a fresh cache for each test
    {:ok, pid} = QueryCache.start_link(max_size: 10, default_ttl: 1000)
    
    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)
    
    {:ok, cache: pid}
  end
  
  describe "basic caching" do
    test "stores and retrieves cached results" do
      key = "test_key_1"
      result = %{rows: [[1, "test"]], columns: ["id", "name"]}
      
      QueryCache.put(key, result)
      Process.sleep(10)
      
      assert {:ok, ^result} = QueryCache.get(key)
    end
    
    test "returns miss for non-existent keys" do
      assert :miss = QueryCache.get("nonexistent")
    end
    
    test "generates consistent cache keys" do
      sql = "SELECT * FROM users WHERE active = $1"
      params = [true]
      
      key1 = QueryCache.generate_key_from_sql(sql, params)
      key2 = QueryCache.generate_key_from_sql(sql, params)
      
      assert key1 == key2
      assert is_binary(key1)
    end
    
    test "generates different keys for different queries" do
      key1 = QueryCache.generate_key_from_sql("SELECT * FROM users", [])
      key2 = QueryCache.generate_key_from_sql("SELECT * FROM posts", [])
      
      assert key1 != key2
    end
  end
  
  describe "TTL expiration" do
    test "expires entries after TTL" do
      key = "expiring_key"
      result = %{data: "test"}
      
      # Store with short TTL
      QueryCache.put(key, result, ttl: 50)
      
      # Should exist immediately
      assert {:ok, ^result} = QueryCache.get(key)
      
      # Wait for expiration
      Process.sleep(60)
      
      # Should be expired
      assert :miss = QueryCache.get(key)
    end
    
    test "respects custom TTL per entry" do
      QueryCache.put("short", "data1", ttl: 50)
      QueryCache.put("long", "data2", ttl: 5000)
      
      Process.sleep(60)
      
      assert :miss = QueryCache.get("short")
      assert {:ok, "data2"} = QueryCache.get("long")
    end
  end
  
  describe "eviction policies" do
    test "evicts LRU entries when cache is full" do
      # Fill cache to capacity (max_size: 10)
      for i <- 1..10 do
        QueryCache.put("key_#{i}", "value_#{i}")
      end
      
      Process.sleep(10)
      
      # Access some entries to update LRU
      QueryCache.get("key_5")
      QueryCache.get("key_6")
      
      # Add new entry, should evict least recently used
      QueryCache.put("key_11", "value_11")
      Process.sleep(10)
      
      # One of the unaccessed early entries should be evicted
      # key_5 and key_6 should still exist
      assert {:ok, "value_5"} = QueryCache.get("key_5")
      assert {:ok, "value_6"} = QueryCache.get("key_6")
    end
  end
  
  describe "cache invalidation" do
    test "invalidates specific cache entry" do
      QueryCache.put("key1", "value1")
      QueryCache.put("key2", "value2")
      
      Process.sleep(10)
      
      {:ok, 1} = QueryCache.invalidate("key1")
      
      assert :miss = QueryCache.get("key1")
      assert {:ok, "value2"} = QueryCache.get("key2")
    end
    
    test "invalidates by pattern matching" do
      QueryCache.put("user_1", "data1")
      QueryCache.put("user_2", "data2")
      QueryCache.put("post_1", "data3")
      
      Process.sleep(10)
      
      {:ok, count} = QueryCache.invalidate("user_*")
      assert count >= 2
      
      assert :miss = QueryCache.get("user_1")
      assert :miss = QueryCache.get("user_2")
      assert {:ok, "data3"} = QueryCache.get("post_1")
    end
    
    test "invalidates by tags" do
      QueryCache.put("key1", "value1", tags: ["users", "active"])
      QueryCache.put("key2", "value2", tags: ["posts"])
      QueryCache.put("key3", "value3", tags: ["users", "inactive"])
      
      Process.sleep(10)
      
      {:ok, count} = QueryCache.invalidate_by_tags(["users"])
      assert count == 2
      
      assert :miss = QueryCache.get("key1")
      assert {:ok, "value2"} = QueryCache.get("key2")
      assert :miss = QueryCache.get("key3")
    end
  end
  
  describe "cache statistics" do
    test "tracks hit and miss rates" do
      QueryCache.put("key1", "value1")
      Process.sleep(10)
      
      # Hits
      QueryCache.get("key1")
      QueryCache.get("key1")
      
      # Misses
      QueryCache.get("nonexistent1")
      QueryCache.get("nonexistent2")
      QueryCache.get("nonexistent3")
      
      stats = QueryCache.stats()
      
      assert stats.hits == 2
      assert stats.misses == 3
      assert stats.hit_rate == 40.0  # 2/(2+3) * 100
    end
    
    test "tracks cache size and memory usage" do
      for i <- 1..5 do
        QueryCache.put("key_#{i}", %{data: String.duplicate("x", 100)})
      end
      
      Process.sleep(10)
      
      stats = QueryCache.stats()
      
      assert stats.cache_size == 5
      assert stats.max_size == 10
      assert stats.memory_usage > 0
      assert stats.avg_entry_size > 0
    end
  end
  
  describe "cached execution" do
    test "executes and caches query results" do
      selecto = %{
        select: ["id", "name"],
        filters: %{},
        joins: %{},
        source: %{source_table: "users"}
      }
      
      execute_count = :counters.new(1, [])
      
      execute_fn = fn _selecto ->
        :counters.add(execute_count, 1, 1)
        {:ok, %{rows: [[1, "test"]], columns: ["id", "name"]}}
      end
      
      # First execution - should execute and cache
      {:ok, result1} = QueryCache.cached_execute(selecto, execute_fn)
      assert :counters.get(execute_count, 1) == 1
      
      # Second execution - should return cached
      {:ok, result2} = QueryCache.cached_execute(selecto, execute_fn)
      assert :counters.get(execute_count, 1) == 1  # Not incremented
      
      assert result1 == result2
    end
    
    test "doesn't cache failed executions" do
      selecto = %{
        select: ["id"],
        filters: %{},
        joins: %{},
        source: %{source_table: "users"}
      }
      
      execute_count = :counters.new(1, [])
      
      execute_fn = fn _selecto ->
        count = :counters.add(execute_count, 1, 1)
        if count == 1 do
          {:error, "connection failed"}
        else
          {:ok, %{rows: [[1]], columns: ["id"]}}
        end
      end
      
      # First execution fails
      {:error, _} = QueryCache.cached_execute(selecto, execute_fn)
      
      # Second execution should execute again (not cached)
      {:ok, _result} = QueryCache.cached_execute(selecto, execute_fn)
      assert :counters.get(execute_count, 1) == 2
    end
  end
  
  describe "compression" do
    test "compresses large results when enabled" do
      # Start cache with compression enabled
      {:ok, _pid} = QueryCache.start_link(
        max_size: 10,
        compression: true,
        compression_threshold: 100
      )
      
      # Small data - should not compress
      small_data = "small"
      QueryCache.put("small_key", small_data)
      
      # Large data - should compress
      large_data = String.duplicate("x", 1000)
      QueryCache.put("large_key", large_data)
      
      Process.sleep(10)
      
      # Both should retrieve correctly
      assert {:ok, ^small_data} = QueryCache.get("small_key")
      assert {:ok, ^large_data} = QueryCache.get("large_key")
    end
  end
  
  describe "cache warmup" do
    test "warms cache with pre-computed queries" do
      queries = [
        {%{select: ["id"], source: %{source_table: "users"}}, %{rows: [[1]], columns: ["id"]}},
        {%{select: ["name"], source: %{source_table: "posts"}}, %{rows: [["test"]], columns: ["name"]}}
      ]
      
      {:ok, warmed} = QueryCache.warmup(queries)
      assert warmed == 2
      
      # Verify cached
      key1 = QueryCache.generate_key(%{select: ["id"], source: %{source_table: "users"}})
      assert {:ok, %{rows: [[1]]}} = QueryCache.get(key1)
    end
  end
  
  describe "clearing cache" do
    test "clears entire cache" do
      for i <- 1..5 do
        QueryCache.put("key_#{i}", "value_#{i}")
      end
      
      Process.sleep(10)
      
      stats_before = QueryCache.stats()
      assert stats_before.cache_size == 5
      
      :ok = QueryCache.clear()
      
      stats_after = QueryCache.stats()
      assert stats_after.cache_size == 0
      
      # All entries should be gone
      assert :miss = QueryCache.get("key_1")
    end
  end
end