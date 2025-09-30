defmodule Selecto.QueryTimeoutMonitor do
  @moduledoc """
  Monitors database query performance and implements circuit breaker pattern.

  Protects the database from overload by:
  - Monitoring connection pool utilization
  - Tracking query timeout rates
  - Opening circuit breaker when system is under heavy load
  - Collecting performance statistics

  Circuit breaker states:
  - `:closed` - Normal operation, queries allowed
  - `:open` - System overloaded, queries blocked
  - `:half_open` - Testing if system has recovered

  ## Usage

  In your application, create a module that uses this behavior:

      defmodule MyApp.QueryTimeoutMonitor do
        use Selecto.QueryTimeoutMonitor, repo: MyApp.Repo
      end

  Then add it to your supervision tree:

      children = [
        MyApp.Repo,
        MyApp.QueryTimeoutMonitor,
        ...
      ]

  ## Configuration Options

  - `:repo` - (required) Your application's Ecto.Repo module
  - `:check_interval` - (optional) How often to check pool health in ms (default: 5000)
  - `:circuit_open_threshold` - (optional) Pool utilization % to open circuit (default: 0.9)
  - `:circuit_half_open_timeout` - (optional) Time before trying half-open in ms (default: 30000)
  - `:slow_query_threshold` - (optional) Milliseconds for "slow query" (default: 10000)
  - `:very_slow_query_threshold` - (optional) Milliseconds for "very slow query" (default: 30000)
  """

  defmacro __using__(opts) do
    repo = Keyword.fetch!(opts, :repo)
    check_interval = Keyword.get(opts, :check_interval, 5_000)
    circuit_open_threshold = Keyword.get(opts, :circuit_open_threshold, 0.9)
    circuit_half_open_timeout = Keyword.get(opts, :circuit_half_open_timeout, 30_000)
    slow_query_threshold = Keyword.get(opts, :slow_query_threshold, 10_000)
    very_slow_query_threshold = Keyword.get(opts, :very_slow_query_threshold, 30_000)

    quote do
      use GenServer
      require Logger

      @repo unquote(repo)
      @check_interval unquote(check_interval)
      @circuit_open_threshold unquote(circuit_open_threshold)
      @circuit_half_open_timeout unquote(circuit_half_open_timeout)
      @slow_query_threshold unquote(slow_query_threshold)
      @very_slow_query_threshold unquote(very_slow_query_threshold)

      defstruct [
        circuit_state: :closed,  # :closed, :open, :half_open
        circuit_opened_at: nil,
        consecutive_failures: 0,
        last_check: nil,
        stats: %{
          total_queries: 0,
          timeout_queries: 0,
          slow_queries: 0,
          very_slow_queries: 0,
          pool_saturation_events: 0,
          last_pool_utilization: 0.0
        }
      ]

      ## Public API

      def start_link(opts \\ []) do
        GenServer.start_link(__MODULE__, opts, name: __MODULE__)
      end

      @doc """
      Check if queries should be allowed based on circuit breaker state.

      Returns:
      - `true` - Query allowed
      - `false` - Query blocked (circuit open)
      """
      @spec allow_query? :: boolean()
      def allow_query? do
        try do
          case GenServer.call(__MODULE__, :get_circuit_state, 1000) do
            :closed -> true
            :half_open -> true  # Allow test queries
            :open -> false
          end
        catch
          :exit, _ -> true  # If monitor is down, allow queries
        end
      end

      @doc """
      Get current circuit breaker state.
      """
      @spec circuit_state :: :closed | :open | :half_open
      def circuit_state do
        try do
          GenServer.call(__MODULE__, :get_circuit_state, 1000)
        catch
          :exit, _ -> :closed
        end
      end

      @doc """
      Get performance statistics.
      """
      @spec stats :: map()
      def stats do
        try do
          GenServer.call(__MODULE__, :get_stats, 1000)
        catch
          :exit, _ -> %{error: "Monitor not available"}
        end
      end

      @doc """
      Record a timeout event.
      """
      @spec record_timeout :: :ok
      def record_timeout do
        GenServer.cast(__MODULE__, :timeout_error)
      end

      @doc """
      Record a slow query.
      """
      @spec record_slow_query(non_neg_integer()) :: :ok
      def record_slow_query(duration_ms) do
        GenServer.cast(__MODULE__, {:slow_query, duration_ms})
      end

      @doc """
      Record a completed query.
      """
      @spec record_query(non_neg_integer()) :: :ok
      def record_query(duration_ms) do
        GenServer.cast(__MODULE__, {:query_completed, duration_ms})
      end

      ## GenServer Callbacks

      @impl GenServer
      def init(_opts) do
        schedule_check()

        state = %__MODULE__{
          circuit_state: :closed,
          circuit_opened_at: nil,
          consecutive_failures: 0,
          last_check: System.monotonic_time(:millisecond),
          stats: %{
            total_queries: 0,
            timeout_queries: 0,
            slow_queries: 0,
            very_slow_queries: 0,
            pool_saturation_events: 0,
            last_pool_utilization: 0.0
          }
        }

        Logger.info("[#{__MODULE__}] Started monitoring")
        {:ok, state}
      end

      @impl GenServer
      def handle_call(:get_circuit_state, _from, state) do
        {:reply, state.circuit_state, state}
      end

      @impl GenServer
      def handle_call(:get_stats, _from, state) do
        stats = Map.merge(state.stats, %{
          circuit_state: state.circuit_state,
          uptime_seconds: div(System.monotonic_time(:millisecond) - state.last_check, 1000),
          timeout_rate: calculate_rate(state.stats.timeout_queries, state.stats.total_queries),
          slow_query_rate: calculate_rate(state.stats.slow_queries, state.stats.total_queries)
        })

        {:reply, stats, state}
      end

      @impl GenServer
      def handle_cast(:timeout_error, state) do
        new_stats = Map.update!(state.stats, :timeout_queries, &(&1 + 1))
        {:noreply, %{state | stats: new_stats}}
      end

      @impl GenServer
      def handle_cast({:slow_query, duration_ms}, state) do
        new_stats = state.stats
        |> Map.update!(:slow_queries, &(&1 + 1))

        new_stats = if duration_ms >= @very_slow_query_threshold do
          Map.update!(new_stats, :very_slow_queries, &(&1 + 1))
        else
          new_stats
        end

        {:noreply, %{state | stats: new_stats}}
      end

      @impl GenServer
      def handle_cast({:query_completed, duration_ms}, state) do
        new_stats = Map.update!(state.stats, :total_queries, &(&1 + 1))

        # Also check if it was slow
        new_stats = if duration_ms >= @slow_query_threshold do
          new_stats
          |> Map.update!(:slow_queries, &(&1 + 1))
          |> then(fn stats ->
            if duration_ms >= @very_slow_query_threshold do
              Map.update!(stats, :very_slow_queries, &(&1 + 1))
            else
              stats
            end
          end)
        else
          new_stats
        end

        {:noreply, %{state | stats: new_stats}}
      end

      @impl GenServer
      def handle_info(:check_pool_health, state) do
        new_state = check_pool_health(state)
        schedule_check()
        {:noreply, new_state}
      end

      ## Private Functions

      defp check_pool_health(state) do
        try do
          case DBConnection.status(@repo) do
            %{available: available, size: size} when size > 0 ->
              utilization = (size - available) / size

              new_stats = Map.put(state.stats, :last_pool_utilization, utilization)
              state = %{state | stats: new_stats}

              handle_circuit_state(state, utilization)

            _ ->
              state
          end
        rescue
          error ->
            Logger.warning("[#{__MODULE__}] Error checking pool health: #{inspect(error)}")
            state
        catch
          :exit, reason ->
            Logger.warning("[#{__MODULE__}] Pool health check exited: #{inspect(reason)}")
            state
        end
      end

      defp handle_circuit_state(state, utilization) do
        case state.circuit_state do
          :closed ->
            if utilization >= @circuit_open_threshold do
              open_circuit(state, utilization)
            else
              state
            end

          :open ->
            time_since_opened = System.monotonic_time(:millisecond) - state.circuit_opened_at

            if time_since_opened >= @circuit_half_open_timeout do
              # Try half-open state
              Logger.info("[#{__MODULE__}] Circuit breaker HALF-OPEN - testing recovery")
              %{state | circuit_state: :half_open}
            else
              state
            end

          :half_open ->
            cond do
              utilization < 0.5 ->
                close_circuit(state)
              utilization >= @circuit_open_threshold ->
                # Still overloaded, reopen circuit
                open_circuit(state, utilization)
              true ->
                state
            end
        end
      end

      defp open_circuit(state, utilization) do
        Logger.error("[#{__MODULE__}] Circuit breaker OPEN - pool saturation #{Float.round(utilization * 100, 1)}%")

        %{state |
          circuit_state: :open,
          circuit_opened_at: System.monotonic_time(:millisecond),
          consecutive_failures: state.consecutive_failures + 1,
          stats: Map.update!(state.stats, :pool_saturation_events, &(&1 + 1))
        }
      end

      defp close_circuit(state) do
        Logger.info("[#{__MODULE__}] Circuit breaker CLOSED - pool recovered")

        %{state |
          circuit_state: :closed,
          circuit_opened_at: nil,
          consecutive_failures: 0
        }
      end

      defp schedule_check do
        Process.send_after(self(), :check_pool_health, @check_interval)
      end

      defp calculate_rate(0, _), do: 0.0
      defp calculate_rate(_, 0), do: 0.0
      defp calculate_rate(part, total), do: Float.round(part / total * 100, 2)
    end
  end
end
