defmodule Selecto.QueryTimeoutMonitorTest do
  use ExUnit.Case, async: false

  defmodule FakeRepo do
  end

  defmodule Monitor do
    use Selecto.QueryTimeoutMonitor,
      repo: FakeRepo,
      check_interval: 5,
      circuit_open_threshold: 0.1,
      circuit_half_open_timeout: 1,
      slow_query_threshold: 10,
      very_slow_query_threshold: 20
  end

  setup do
    if pid = Process.whereis(Monitor) do
      GenServer.stop(pid)
    end

    on_exit(fn ->
      if pid = Process.whereis(Monitor) do
        GenServer.stop(pid)
      end
    end)

    :ok
  end

  test "fallback APIs work when monitor is not running" do
    assert Monitor.allow_query?()
    assert :closed == Monitor.circuit_state()
    assert %{error: "Monitor not available"} = Monitor.stats()
  end

  test "records query, timeout, and slow query statistics" do
    {:ok, _pid} = Monitor.start_link()

    :ok = Monitor.record_query(5)
    :ok = Monitor.record_query(25)
    :ok = Monitor.record_timeout()
    :ok = Monitor.record_slow_query(30)

    Process.sleep(20)

    stats = Monitor.stats()
    assert stats.total_queries == 2
    assert stats.timeout_queries == 1
    assert stats.slow_queries >= 2
    assert stats.very_slow_queries >= 1
    assert stats.circuit_state in [:closed, :open, :half_open]
  end

  test "allow_query reflects circuit state" do
    {:ok, _pid} = Monitor.start_link()

    :sys.replace_state(Monitor, fn state -> %{state | circuit_state: :open} end)
    refute Monitor.allow_query?()
    assert :open == Monitor.circuit_state()

    :sys.replace_state(Monitor, fn state -> %{state | circuit_state: :half_open} end)
    assert Monitor.allow_query?()
    assert :half_open == Monitor.circuit_state()
  end

  test "pool health check message is handled safely" do
    {:ok, _pid} = Monitor.start_link()

    send(Monitor, :check_pool_health)
    Process.sleep(10)

    assert is_map(Monitor.stats())
  end
end
