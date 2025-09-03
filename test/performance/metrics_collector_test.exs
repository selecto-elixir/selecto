defmodule Selecto.Performance.MetricsCollectorTest do
  use ExUnit.Case, async: false
  
  alias Selecto.Performance.MetricsCollector
  
  setup do
    # Start a fresh metrics collector for each test
    {:ok, pid} = MetricsCollector.start_link()
    
    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)
    
    {:ok, collector: pid}
  end
  
  describe "query tracking" do
    test "starts and completes query tracking" do
      query_info = %{
        select: ["id", "name"],
        filters: [{"active", true}],
        joins: []
      }
      
      query_id = MetricsCollector.start_query(query_info)
      assert is_binary(query_id)
      
      # Simulate some work
      Process.sleep(10)
      
      result_info = %{
        row_count: 100,
        column_count: 2
      }
      
      MetricsCollector.complete_query(query_id, result_info)
      
      # Give it time to process
      Process.sleep(10)
      
      {:ok, metrics} = MetricsCollector.get_query_metrics(query_id)
      assert metrics.query_id == query_id
      assert metrics.status == :completed
      assert metrics.row_count == 100
      assert metrics.column_count == 2
      assert metrics.execution_time > 0
    end
    
    test "records query metrics directly" do
      metrics = %{
        query_id: "test_123",
        execution_time: 150,
        row_count: 50,
        column_count: 3,
        status: :completed
      }
      
      MetricsCollector.record_query("test_123", metrics)
      Process.sleep(10)
      
      {:ok, retrieved} = MetricsCollector.get_query_metrics("test_123")
      assert retrieved.execution_time == 150
      assert retrieved.row_count == 50
    end
    
    test "handles missing query gracefully" do
      result = MetricsCollector.get_query_metrics("nonexistent")
      assert {:error, :not_found} = result
    end
  end
  
  describe "statistics" do
    test "calculates aggregate statistics" do
      # Record multiple queries
      for i <- 1..5 do
        MetricsCollector.record_query("query_#{i}", %{
          execution_time: i * 10,
          row_count: i * 100,
          status: :completed
        })
      end
      
      Process.sleep(20)
      
      stats = MetricsCollector.get_stats()
      assert stats.total_queries == 5
      assert stats.avg_execution_time == 30.0  # (10+20+30+40+50)/5
      assert stats.min_execution_time == 10
      assert stats.max_execution_time == 50
    end
    
    test "filters statistics by time range" do
      # Record old query
      MetricsCollector.record_query("old_query", %{
        execution_time: 100,
        started_at: System.monotonic_time(:millisecond) - :timer.hours(2)
      })
      
      # Record recent query
      MetricsCollector.record_query("recent_query", %{
        execution_time: 50,
        started_at: System.monotonic_time(:millisecond)
      })
      
      Process.sleep(10)
      
      stats = MetricsCollector.get_stats(time_range: {:last, 1, :hours})
      assert stats.total_queries == 1
      assert stats.avg_execution_time == 50.0
    end
  end
  
  describe "slow queries" do
    test "tracks slow queries above threshold" do
      # Record fast query
      MetricsCollector.record_query("fast", %{
        execution_time: 50,
        query_info: %{sql: "SELECT * FROM fast"}
      })
      
      # Record slow queries
      MetricsCollector.record_query("slow1", %{
        execution_time: 500,
        query_info: %{sql: "SELECT * FROM slow1"}
      })
      
      MetricsCollector.record_query("slow2", %{
        execution_time: 300,
        query_info: %{sql: "SELECT * FROM slow2"}
      })
      
      Process.sleep(10)
      
      slow_queries = MetricsCollector.get_slow_queries(threshold: 100)
      
      assert length(slow_queries) == 2
      {id1, metrics1} = hd(slow_queries)
      assert metrics1.execution_time >= 100
    end
    
    test "limits number of slow queries returned" do
      for i <- 1..20 do
        MetricsCollector.record_query("slow_#{i}", %{
          execution_time: 100 + i * 10
        })
      end
      
      Process.sleep(10)
      
      slow_queries = MetricsCollector.get_slow_queries(limit: 5)
      assert length(slow_queries) == 5
    end
  end
  
  describe "query patterns" do
    test "identifies common query patterns" do
      # Record similar queries
      for i <- 1..3 do
        MetricsCollector.record_query("pattern1_#{i}", %{
          execution_time: 50 + i,
          query_info: %{
            select: ["id", "name"],
            filters: [{"active", true}],
            joins: [],
            group_by: []
          }
        })
      end
      
      # Record different pattern
      for i <- 1..2 do
        MetricsCollector.record_query("pattern2_#{i}", %{
          execution_time: 100 + i,
          query_info: %{
            select: ["id", "name", "count"],
            filters: [{"status", "complete"}],
            joins: ["orders"],
            group_by: ["status"]
          }
        })
      end
      
      Process.sleep(10)
      
      patterns = MetricsCollector.get_query_patterns()
      
      assert length(patterns) >= 2
      
      # Check first pattern
      pattern1 = hd(patterns)
      assert pattern1.count >= 2
      assert pattern1.avg_execution_time > 0
    end
  end
  
  describe "metrics export" do
    test "exports metrics as JSON" do
      MetricsCollector.record_query("test1", %{
        execution_time: 100,
        row_count: 50
      })
      
      MetricsCollector.record_query("test2", %{
        execution_time: 200,
        row_count: 100
      })
      
      Process.sleep(10)
      
      {:ok, json} = MetricsCollector.export_metrics(:json)
      data = Jason.decode!(json)
      
      assert Map.has_key?(data, "stats")
      assert Map.has_key?(data, "slow_queries")
      assert Map.has_key?(data, "patterns")
    end
    
    test "exports metrics as CSV" do
      MetricsCollector.record_query("test1", %{
        execution_time: 100,
        row_count: 50,
        started_at: System.monotonic_time(:millisecond),
        status: :completed
      })
      
      Process.sleep(10)
      
      {:ok, csv} = MetricsCollector.export_metrics(:csv, limit: 10)
      
      lines = String.split(csv, "\n")
      assert length(lines) >= 2  # Header + at least one row
      
      header = hd(lines)
      assert String.contains?(header, "query_id")
      assert String.contains?(header, "execution_time")
    end
  end
  
  describe "cleanup" do
    test "clears all metrics" do
      # Add some metrics
      for i <- 1..5 do
        MetricsCollector.record_query("query_#{i}", %{
          execution_time: i * 10
        })
      end
      
      Process.sleep(10)
      
      stats_before = MetricsCollector.get_stats()
      assert stats_before.total_queries == 5
      
      # Clear metrics
      :ok = MetricsCollector.clear_metrics()
      
      stats_after = MetricsCollector.get_stats()
      assert stats_after.total_queries == 0
    end
  end
  
  describe "concurrent operations" do
    test "handles concurrent metric recording" do
      tasks = for i <- 1..100 do
        Task.async(fn ->
          MetricsCollector.record_query("concurrent_#{i}", %{
            execution_time: :rand.uniform(500),
            row_count: :rand.uniform(1000)
          })
        end)
      end
      
      Task.await_many(tasks)
      Process.sleep(50)
      
      stats = MetricsCollector.get_stats()
      assert stats.total_queries == 100
    end
  end
end