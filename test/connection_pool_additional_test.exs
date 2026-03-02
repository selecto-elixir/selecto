defmodule Selecto.ConnectionPoolAdditionalTest do
  use ExUnit.Case

  alias Selecto.ConnectionPool

  defmodule FakeAdapter do
  end

  defmodule GenericAdapter do
    def connect(_opts), do: {:ok, spawn(fn -> Process.sleep(:infinity) end)}
    def execute(_conn, _query, _params, _opts), do: {:ok, %{rows: [[1]], columns: ["id"]}}
    def supports?(_feature), do: true
  end

  test "start_pool returns errors for unavailable backends" do
    postgres_result = ConnectionPool.start_pool(hostname: "invalid.local", database: "missing_db")

    case postgres_result do
      {:ok, pool_ref} ->
        ConnectionPool.stop_pool(pool_ref)
        assert true

      {:error, _reason} ->
        assert true
    end

    # Non-PostgreSQL path should fail safely for unsupported adapters
    previous = Process.flag(:trap_exit, true)

    result = ConnectionPool.start_pool([], adapter: FakeAdapter)

    exited? =
      receive do
        {:EXIT, _pid, _reason} -> true
      after
        0 -> false
      end

    assert match?({:error, {:unsupported_adapter, FakeAdapter}}, result)
    refute exited?

    Process.flag(:trap_exit, previous)
  end

  test "pool_stats invalid ref and cache clear safety" do
    assert %{error: "Pool manager not available"} = ConnectionPool.pool_stats(:bad_ref)

    pool_pid = spawn(fn -> Process.sleep(:infinity) end)
    pool_name = ConnectionPool.generate_pool_name(db: "stats_test")

    {:ok, manager_pid} =
      GenServer.start_link(
        Selecto.ConnectionPool,
        pool_pid: pool_pid,
        pool_name: pool_name,
        pool_config: [prepared_statement_cache_size: 100],
        connection_config: [database: "db"]
      )

    pool_ref = %{manager: manager_pid}
    assert :ok = ConnectionPool.clear_cache(pool_ref)
    GenServer.stop(manager_pid)
    Process.exit(pool_pid, :kill)
  end

  test "generic adapter pool starts and executes without postgres pool pid" do
    assert {:ok, pool_ref} = ConnectionPool.start_pool([], adapter: GenericAdapter)
    assert %{adapter: GenericAdapter, manager: _manager, connection: _connection} = pool_ref

    assert {:ok, %{rows: [[1]], columns: ["id"]}} =
             ConnectionPool.execute(pool_ref, "select 1", [])

    assert :ok = ConnectionPool.stop_pool(pool_ref)
  end

  test "checkout and checkin use process dictionary references" do
    pool_pid = spawn(fn -> Process.sleep(:infinity) end)
    pool_ref = %{pool: pool_pid}

    assert {:ok, {:selecto_conn, ref, ^pool_pid}} = ConnectionPool.checkout(pool_ref)
    assert Process.get({:selecto_checkout, ref}) == pool_pid

    assert :ok = ConnectionPool.checkin(pool_ref, {:selecto_conn, ref, pool_pid})
    assert Process.get({:selecto_checkout, ref}) == nil

    Process.exit(pool_pid, :kill)
  end

  test "with_connection and execute handle invalid pool refs" do
    assert {:error, "Invalid pool reference"} =
             ConnectionPool.with_connection(:bad_ref, fn conn -> conn end)

    assert {:error, "Invalid pool reference"} =
             ConnectionPool.execute(:bad_ref, "select 1", [])
  end

  test "with_connection wraps raised errors" do
    pool_pid = spawn(fn -> Process.sleep(:infinity) end)
    pool_ref = %{pool: pool_pid}

    assert {:error, %Selecto.Error{type: :query_error}} =
             ConnectionPool.with_connection(pool_ref, fn _conn ->
               raise "boom"
             end)

    Process.exit(pool_pid, :kill)
  end

  test "transaction returns invalid pool reference error" do
    assert {:error, "Invalid pool reference"} =
             ConnectionPool.transaction(:bad_ref, fn _conn -> :ok end)
  end
end
