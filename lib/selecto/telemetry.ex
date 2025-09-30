defmodule Selecto.Telemetry do
  @moduledoc """
  Telemetry event handlers for query performance monitoring.

  Handles Selecto query events to track performance, timeouts, and
  complexity analysis results. Integrates with QueryTimeoutMonitor
  for circuit breaker decisions.

  ## Usage

  In your application.ex, call `Selecto.Telemetry.setup/1` with your
  app's repo module and query timeout monitor module:

      def start(_type, _args) do
        # ... other children ...

        Selecto.Telemetry.setup(
          repo: MyApp.Repo,
          monitor: MyApp.QueryTimeoutMonitor
        )

        # ... supervision tree ...
      end
  """

  require Logger

  @slow_query_threshold 10_000  # Queries > 10s are "slow"
  @very_slow_query_threshold 30_000  # Queries > 30s are "very slow"

  @doc """
  Attach telemetry handlers for Selecto query monitoring.

  ## Options

  - `:repo` - (required) Your application's Ecto.Repo module for pool monitoring
  - `:monitor` - (required) Your QueryTimeoutMonitor module for recording stats
  - `:slow_threshold` - (optional) Milliseconds for "slow query" threshold (default: 10000)
  - `:very_slow_threshold` - (optional) Milliseconds for "very slow query" threshold (default: 30000)
  - `:app_name` - (optional) Application name for custom telemetry events (default: :selecto)

  Call this during application startup to begin monitoring query performance.
  """
  def setup(opts \\ []) do
    repo = Keyword.fetch!(opts, :repo)
    monitor = Keyword.fetch!(opts, :monitor)
    app_name = Keyword.get(opts, :app_name, :selecto)
    slow_threshold = Keyword.get(opts, :slow_threshold, @slow_query_threshold)
    very_slow_threshold = Keyword.get(opts, :very_slow_threshold, @very_slow_query_threshold)

    config = %{
      repo: repo,
      monitor: monitor,
      app_name: app_name,
      slow_threshold: slow_threshold,
      very_slow_threshold: very_slow_threshold
    }

    :telemetry.attach_many(
      "selecto-query-monitoring",
      [
        [:selecto, :query, :start],
        [:selecto, :query, :complete],
        [:selecto, :query, :error],
        [:selecto, :query, :timeout],
        [:selecto, :query, :complexity_analyzed],
        [:selecto, :query, :complexity_rejected]
      ],
      &handle_event/4,
      config
    )

    Logger.info("[Selecto.Telemetry] Query monitoring enabled")
  end

  @doc """
  Handle Selecto telemetry events.
  """
  def handle_event([:selecto, :query, :start], _measurements, metadata, _config) do
    query_preview = String.slice(metadata[:query] || "unknown", 0, 100)
    Logger.debug("[Selecto] Query started: #{query_preview}...")
  end

  def handle_event([:selecto, :query, :complete], measurements, metadata, config) do
    duration = measurements.duration || measurements.execution_time
    row_count = metadata[:row_count] || 0
    monitor = config.monitor
    app_name = config.app_name

    # Record query completion with monitor
    monitor.record_query(duration)

    # Log slow queries
    cond do
      duration >= config.very_slow_threshold ->
        query_preview = String.slice(metadata[:query] || "unknown", 0, 200)
        Logger.error("""
        [Selecto] VERY SLOW QUERY (#{duration}ms)
        Query: #{query_preview}
        Rows: #{row_count}
        Query ID: #{metadata[:query_id]}
        Consider optimizing or breaking into smaller queries.
        """)

        monitor.record_slow_query(duration)

      duration >= config.slow_threshold ->
        query_preview = String.slice(metadata[:query] || "unknown", 0, 200)
        Logger.warning("""
        [Selecto] Slow query detected (#{duration}ms)
        Query: #{query_preview}
        Rows: #{row_count}
        Query ID: #{metadata[:query_id]}
        """)

        monitor.record_slow_query(duration)

      true ->
        Logger.debug("[Selecto] Query completed: #{duration}ms, #{row_count} rows")
    end

    # Emit custom metrics event for external monitoring
    :telemetry.execute(
      [app_name, :query, :complete],
      %{duration: duration, row_count: row_count},
      %{
        query_id: metadata[:query_id],
        is_slow: duration >= config.slow_threshold,
        is_very_slow: duration >= config.very_slow_threshold
      }
    )
  end

  def handle_event([:selecto, :query, :error], _measurements, metadata, config) do
    error = metadata[:error]
    duration = metadata[:duration]
    app_name = config.app_name

    Logger.error("""
    [Selecto] Query error
    Error: #{inspect(error)}
    Duration: #{duration}ms
    Query ID: #{metadata[:query_id]}
    """)

    # Emit error metric
    :telemetry.execute(
      [app_name, :query, :error],
      %{count: 1, duration: duration || 0},
      %{error_type: classify_error(error)}
    )
  end

  def handle_event([:selecto, :query, :timeout], measurements, metadata, config) do
    duration = measurements[:duration]
    timeout = measurements[:timeout]
    monitor = config.monitor
    app_name = config.app_name

    Logger.error("""
    [Selecto] QUERY TIMEOUT
    Duration: #{duration}ms
    Timeout limit: #{timeout}ms
    Query ID: #{metadata[:query_id]}
    Action: Query was terminated
    """)

    # Record timeout with monitor
    monitor.record_timeout()

    # Emit timeout metric
    :telemetry.execute(
      [app_name, :query, :timeout],
      %{count: 1, duration: duration, timeout: timeout},
      %{query_id: metadata[:query_id]}
    )

    # Check if we should alert
    check_timeout_spike(monitor, app_name)
  end

  def handle_event([:selecto, :query, :complexity_analyzed], measurements, metadata, config) do
    score = measurements[:complexity_score]
    warnings = metadata[:warnings] || []
    app_name = config.app_name

    if length(warnings) > 0 do
      Logger.warning("""
      [Selecto] Query complexity warnings
      Score: #{score}
      Warnings: #{Enum.join(warnings, ", ")}
      Query ID: #{metadata[:query_id]}
      """)
    else
      Logger.debug("[Selecto] Query complexity: #{score}")
    end

    # Emit complexity metric
    :telemetry.execute(
      [app_name, :query, :complexity],
      %{score: score, warning_count: length(warnings)},
      %{query_id: metadata[:query_id]}
    )
  end

  def handle_event([:selecto, :query, :complexity_rejected], measurements, metadata, config) do
    score = measurements[:complexity_score]
    issues = metadata[:blocking_issues] || []
    recommendations = metadata[:recommendations] || []
    app_name = config.app_name

    Logger.error("""
    [Selecto] QUERY REJECTED - Too Complex
    Complexity score: #{score}
    Issues: #{Enum.join(issues, ", ")}
    Recommendations: #{Enum.join(recommendations, ", ")}
    Query ID: #{metadata[:query_id]}
    """)

    # Emit rejection metric
    :telemetry.execute(
      [app_name, :query, :rejected],
      %{count: 1, complexity_score: score},
      %{
        query_id: metadata[:query_id],
        issues: issues,
        recommendations: recommendations
      }
    )
  end

  # Private functions

  defp classify_error(error) when is_binary(error) do
    cond do
      String.contains?(error, "timeout") -> :timeout
      String.contains?(error, "connection") -> :connection
      String.contains?(error, "constraint") -> :constraint
      true -> :unknown
    end
  end

  defp classify_error(%{type: type}) when is_atom(type), do: type
  defp classify_error(_), do: :unknown

  defp check_timeout_spike(monitor, app_name) do
    # Get timeout statistics
    stats = monitor.stats()

    timeout_rate = stats[:timeout_rate] || 0.0
    total_queries = stats[:total_queries] || 0

    # Alert if timeout rate exceeds threshold (> 5% with at least 20 queries)
    if timeout_rate > 5.0 && total_queries >= 20 do
      Logger.error("""
      [ALERT] Timeout spike detected!
      Timeout rate: #{timeout_rate}%
      Total queries: #{total_queries}
      Circuit breaker state: #{stats[:circuit_state]}
      Pool utilization: #{Float.round((stats[:last_pool_utilization] || 0.0) * 100, 1)}%

      Action: Consider investigating slow queries or scaling resources.
      """)

      # Emit alert event for external notification systems
      :telemetry.execute(
        [app_name, :alert, :timeout_spike],
        %{timeout_rate: timeout_rate, total_queries: total_queries},
        %{
          circuit_state: stats[:circuit_state],
          pool_utilization: stats[:last_pool_utilization]
        }
      )
    end
  end

  @doc """
  Get summary statistics for monitoring dashboards.

  ## Options

  - `:repo` - (required) Your application's Ecto.Repo module
  - `:monitor` - (required) Your QueryTimeoutMonitor module
  """
  def get_stats(opts) do
    repo = Keyword.fetch!(opts, :repo)
    monitor = Keyword.fetch!(opts, :monitor)

    monitor_stats = monitor.stats()
    pool_status = get_pool_status(repo)

    %{
      # Monitor stats
      circuit_state: monitor_stats[:circuit_state] || :closed,
      total_queries: monitor_stats[:total_queries] || 0,
      timeout_queries: monitor_stats[:timeout_queries] || 0,
      timeout_rate: monitor_stats[:timeout_rate] || 0.0,
      slow_queries: monitor_stats[:slow_queries] || 0,
      slow_query_rate: monitor_stats[:slow_query_rate] || 0.0,
      very_slow_queries: monitor_stats[:very_slow_queries] || 0,
      pool_saturation_events: monitor_stats[:pool_saturation_events] || 0,

      # Pool stats
      pool_size: pool_status[:size] || 0,
      pool_available: pool_status[:available] || 0,
      pool_utilization: calculate_pool_utilization(pool_status),
      last_pool_utilization: monitor_stats[:last_pool_utilization] || 0.0,

      # Uptime
      uptime_seconds: monitor_stats[:uptime_seconds] || 0
    }
  end

  defp get_pool_status(repo) do
    try do
      case DBConnection.status(repo) do
        %{available: available, size: size} ->
          %{available: available, size: size}

        _ ->
          %{available: 0, size: 0}
      end
    rescue
      _ -> %{available: 0, size: 0}
    catch
      :exit, _ -> %{available: 0, size: 0}
    end
  end

  defp calculate_pool_utilization(%{size: size, available: available}) when size > 0 do
    Float.round((size - available) / size * 100, 1)
  end

  defp calculate_pool_utilization(_), do: 0.0
end
