defmodule Selecto.Performance.MetricsCollector do
  @moduledoc """
  Collects and tracks query performance metrics for Selecto.

  This module provides comprehensive query performance tracking including:
  - Query execution time measurement
  - Query complexity analysis
  - Resource usage tracking
  - Query pattern recognition
  - Performance history management
  """

  use GenServer

  @table_name :selecto_query_metrics
  @history_limit 10_000
  @cleanup_interval :timer.minutes(5)
  @slow_query_threshold_ms 100

  defstruct [
    :ets_table,
    :history,
    :stats,
    :slow_queries,
    :query_patterns,
    :config
  ]

  # Client API

  @doc """
  Start the metrics collector process.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Record metrics for a query execution.
  """
  def record_query(query_id, metrics) do
    GenServer.cast(__MODULE__, {:record_query, query_id, metrics})
  end

  @doc """
  Start tracking a query execution.
  Returns a unique query ID for tracking.
  """
  def start_query(query_info) do
    query_id = generate_query_id()
    GenServer.cast(__MODULE__, {:start_query, query_id, query_info})
    query_id
  end

  @doc """
  Complete tracking a query execution.
  """
  def complete_query(query_id, result_info) do
    GenServer.cast(__MODULE__, {:complete_query, query_id, result_info})
  end

  @doc """
  Get metrics for a specific query.
  """
  def get_query_metrics(query_id) do
    GenServer.call(__MODULE__, {:get_query_metrics, query_id})
  end

  @doc """
  Get aggregated statistics.
  """
  def get_stats(options \\ []) do
    GenServer.call(__MODULE__, {:get_stats, options})
  end

  @doc """
  Get slow query log.
  """
  def get_slow_queries(options \\ []) do
    GenServer.call(__MODULE__, {:get_slow_queries, options})
  end

  @doc """
  Get query pattern analysis.
  """
  def get_query_patterns(options \\ []) do
    GenServer.call(__MODULE__, {:get_query_patterns, options})
  end

  @doc """
  Clear all metrics data.
  """
  def clear_metrics do
    GenServer.call(__MODULE__, :clear_metrics)
  end

  @doc """
  Export metrics to a file or stream.
  """
  def export_metrics(format \\ :json, options \\ []) do
    GenServer.call(__MODULE__, {:export_metrics, format, options})
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    # Create ETS table for fast metric storage
    table =
      :ets.new(@table_name, [
        :set,
        :public,
        :named_table,
        read_concurrency: true,
        write_concurrency: true
      ])

    # Schedule periodic cleanup
    schedule_cleanup()

    config = %{
      slow_query_threshold: opts[:slow_query_threshold] || @slow_query_threshold_ms,
      history_limit: opts[:history_limit] || @history_limit,
      track_patterns: opts[:track_patterns] || true,
      track_memory: opts[:track_memory] || false,
      track_cpu: opts[:track_cpu] || false
    }

    {:ok,
     %__MODULE__{
       ets_table: table,
       history: :queue.new(),
       stats: initialize_stats(),
       slow_queries: [],
       query_patterns: %{},
       config: config
     }}
  end

  @impl true
  def handle_cast({:record_query, query_id, metrics}, state) do
    # Store metrics in ETS
    :ets.insert(state.ets_table, {query_id, metrics})

    # Update statistics
    new_stats = update_stats(state.stats, metrics)

    # Track slow queries
    new_slow_queries =
      if metrics.execution_time > state.config.slow_query_threshold do
        add_slow_query(state.slow_queries, query_id, metrics)
      else
        state.slow_queries
      end

    # Update query patterns
    new_patterns =
      if state.config.track_patterns do
        update_query_patterns(state.query_patterns, metrics)
      else
        state.query_patterns
      end

    # Add to history
    new_history = add_to_history(state.history, query_id, state.config.history_limit)

    {:noreply,
     %{
       state
       | stats: new_stats,
         slow_queries: new_slow_queries,
         query_patterns: new_patterns,
         history: new_history
     }}
  end

  @impl true
  def handle_cast({:start_query, query_id, query_info}, state) do
    start_metrics = %{
      query_id: query_id,
      started_at: System.monotonic_time(:millisecond),
      system_time: System.system_time(:microsecond),
      query_info: query_info,
      status: :running
    }

    # Store initial metrics
    :ets.insert(state.ets_table, {query_id, start_metrics})

    {:noreply, state}
  end

  @impl true
  def handle_cast({:complete_query, query_id, result_info}, state) do
    case :ets.lookup(state.ets_table, query_id) do
      [{^query_id, start_metrics}] ->
        completed_at = System.monotonic_time(:millisecond)
        execution_time = completed_at - start_metrics.started_at

        complete_metrics =
          Map.merge(start_metrics, %{
            completed_at: completed_at,
            execution_time: execution_time,
            status: :completed,
            result_info: result_info,
            row_count: result_info[:row_count] || 0,
            column_count: result_info[:column_count] || 0
          })

        # Add resource tracking if enabled
        complete_metrics =
          if state.config.track_memory do
            Map.put(complete_metrics, :memory_used, calculate_memory_usage())
          else
            complete_metrics
          end

        complete_metrics =
          if state.config.track_cpu do
            Map.put(complete_metrics, :cpu_time, calculate_cpu_time())
          else
            complete_metrics
          end

        # Record the complete metrics
        handle_cast({:record_query, query_id, complete_metrics}, state)

      _ ->
        # Query not found in tracking
        {:noreply, state}
    end
  end

  @impl true
  def handle_call({:get_query_metrics, query_id}, _from, state) do
    metrics =
      case :ets.lookup(state.ets_table, query_id) do
        [{^query_id, metrics}] -> {:ok, metrics}
        [] -> {:error, :not_found}
      end

    {:reply, metrics, state}
  end

  @impl true
  def handle_call({:get_stats, options}, _from, state) do
    time_range = Keyword.get(options, :time_range, :all)
    stats = calculate_stats_for_range(state, time_range)

    {:reply, stats, state}
  end

  @impl true
  def handle_call({:get_slow_queries, options}, _from, state) do
    limit = Keyword.get(options, :limit, 100)
    threshold = Keyword.get(options, :threshold, state.config.slow_query_threshold)

    slow_queries =
      state.slow_queries
      |> Enum.filter(fn {_id, metrics} ->
        metrics.execution_time >= threshold
      end)
      |> Enum.take(limit)

    {:reply, slow_queries, state}
  end

  @impl true
  def handle_call({:get_query_patterns, options}, _from, state) do
    limit = Keyword.get(options, :limit, 50)

    patterns =
      state.query_patterns
      |> Map.to_list()
      |> Enum.sort_by(fn {_pattern, info} -> info.count end, :desc)
      |> Enum.take(limit)
      |> Enum.map(fn {pattern, info} ->
        %{
          pattern: pattern,
          count: info.count,
          avg_execution_time: info.total_time / info.count,
          total_time: info.total_time,
          examples: info.examples
        }
      end)

    {:reply, patterns, state}
  end

  @impl true
  def handle_call(:clear_metrics, _from, state) do
    :ets.delete_all_objects(state.ets_table)

    {:reply, :ok,
     %{
       state
       | history: :queue.new(),
         stats: initialize_stats(),
         slow_queries: [],
         query_patterns: %{}
     }}
  end

  @impl true
  def handle_call({:export_metrics, format, options}, _from, state) do
    result = export_metrics_data(state, format, options)
    {:reply, result, state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    # Remove old metrics from ETS
    cutoff_time = System.monotonic_time(:millisecond) - :timer.hours(24)

    :ets.select_delete(state.ets_table, [
      {
        {:"$1", %{started_at: :"$2"}},
        [{:<, :"$2", cutoff_time}],
        [true]
      }
    ])

    # Schedule next cleanup
    schedule_cleanup()

    {:noreply, state}
  end

  # Private functions

  defp generate_query_id do
    :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
  end

  defp initialize_stats do
    %{
      total_queries: 0,
      total_execution_time: 0,
      avg_execution_time: 0,
      min_execution_time: nil,
      max_execution_time: nil,
      query_count_by_type: %{},
      errors: 0,
      cache_hits: 0,
      cache_misses: 0
    }
  end

  defp update_stats(stats, metrics) do
    %{
      stats
      | total_queries: stats.total_queries + 1,
        total_execution_time: stats.total_execution_time + metrics.execution_time,
        avg_execution_time:
          (stats.total_execution_time + metrics.execution_time) / (stats.total_queries + 1),
        min_execution_time: min_value(stats.min_execution_time, metrics.execution_time),
        max_execution_time: max_value(stats.max_execution_time, metrics.execution_time)
    }
  end

  defp min_value(nil, value), do: value
  defp min_value(current, value), do: min(current, value)

  defp max_value(nil, value), do: value
  defp max_value(current, value), do: max(current, value)

  defp add_slow_query(slow_queries, query_id, metrics) do
    entry = {query_id, metrics}

    [entry | slow_queries]
    |> Enum.sort_by(fn {_id, m} -> m.execution_time end, :desc)
    # Keep top 100 slow queries
    |> Enum.take(100)
  end

  defp update_query_patterns(patterns, metrics) do
    pattern = extract_query_pattern(metrics)
    query_id = Map.get(metrics, :query_id, "unknown")

    Map.update(
      patterns,
      pattern,
      %{count: 1, total_time: metrics.execution_time, examples: [query_id]},
      fn info ->
        %{
          info
          | count: info.count + 1,
            total_time: info.total_time + metrics.execution_time,
            examples: Enum.take([query_id | info.examples], 5)
        }
      end
    )
  end

  defp extract_query_pattern(metrics) do
    # Extract pattern from query structure
    # This is a simplified version - real implementation would analyze the query
    query_info = metrics[:query_info] || %{}

    %{
      select_count: length(query_info[:select] || []),
      filter_count: length(query_info[:filters] || []),
      join_count: length(query_info[:joins] || []),
      group_count: length(query_info[:group_by] || []),
      has_cte: not is_nil(query_info[:cte]),
      has_subquery: not is_nil(query_info[:subquery])
    }
  end

  defp add_to_history(history, query_id, limit) do
    new_history = :queue.in(query_id, history)

    if :queue.len(new_history) > limit do
      {_, trimmed} = :queue.out(new_history)
      trimmed
    else
      new_history
    end
  end

  defp calculate_stats_for_range(state, :all) do
    state.stats
  end

  defp calculate_stats_for_range(state, {:last, amount, unit}) do
    cutoff = System.monotonic_time(:millisecond) - time_to_ms(amount, unit)

    metrics =
      :ets.select(state.ets_table, [
        {
          {:"$1", %{started_at: :"$2"}},
          [{:>=, :"$2", cutoff}],
          [:"$_"]
        }
      ])

    calculate_stats_from_metrics(metrics)
  end

  defp time_to_ms(amount, :seconds), do: amount * 1000
  defp time_to_ms(amount, :minutes), do: amount * 60 * 1000
  defp time_to_ms(amount, :hours), do: amount * 60 * 60 * 1000
  defp time_to_ms(amount, :days), do: amount * 24 * 60 * 60 * 1000

  defp calculate_stats_from_metrics(metrics) do
    Enum.reduce(metrics, initialize_stats(), fn {_id, metric}, acc ->
      update_stats(acc, metric)
    end)
  end

  defp calculate_memory_usage do
    # Get current process memory usage
    {:memory, memory} = Process.info(self(), :memory)
    memory
  end

  defp calculate_cpu_time do
    # Get CPU time for current process
    {:reductions, reductions} = Process.info(self(), :reductions)
    reductions
  end

  defp export_metrics_data(state, :json, options) do
    patterns =
      state.query_patterns
      |> Map.to_list()
      |> Enum.map(fn {pattern, info} -> %{pattern: pattern, info: info} end)
      |> Enum.take(options[:limit] || 50)

    data = %{
      stats: state.stats,
      slow_queries: Enum.take(state.slow_queries, options[:limit] || 100),
      patterns: patterns
    }

    {:ok, Jason.encode!(data, pretty: true)}
  end

  defp export_metrics_data(state, :csv, options) do
    # Export as CSV
    headers = ["query_id", "execution_time", "row_count", "started_at", "status"]

    rows =
      :ets.tab2list(state.ets_table)
      |> Enum.map(fn {id, metrics} ->
        [
          id,
          metrics[:execution_time] || 0,
          metrics[:row_count] || 0,
          metrics[:started_at] || 0,
          metrics[:status] || "unknown"
        ]
      end)
      |> Enum.take(options[:limit] || 1000)

    csv_data =
      [headers | rows]
      |> Enum.map(&Enum.join(&1, ","))
      |> Enum.join("\n")

    {:ok, csv_data}
  end

  defp schedule_cleanup do
    Process.send_after(self(), :cleanup, @cleanup_interval)
  end
end
