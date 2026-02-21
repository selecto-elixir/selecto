defmodule Selecto.ConnectionTest do
  use ExUnit.Case, async: true

  alias Selecto.Connection

  defmodule FullAdapter do
    def connect(opts), do: {:ok, {:conn, opts}}
    def disconnect(_conn), do: :ok
    def execute(_conn, query, params, _opts), do: {:ok, %{query: query, params: params}}
    def checkout(pool), do: {:ok, {:checked_out, pool}}
    def checkin(_pool, _connection), do: :ok
    def adapter_name, do: "FullAdapter"
    def dialect, do: "custom"
  end

  defmodule FallbackTxAdapter do
    def connect(_opts), do: {:ok, :conn}
    def execute(_conn, _query, _params, _opts), do: {:ok, %{}}

    def begin(connection, _opts), do: {:ok, connection}
    def commit(_connection), do: :ok

    def rollback(%{test_pid: test_pid}) do
      send(test_pid, :rolled_back)
      :ok
    end
  end

  defmodule ExplicitTxAdapter do
    def connect(_opts), do: {:ok, :conn}
    def execute(_conn, _query, _params, _opts), do: {:ok, %{}}
    def transaction(_connection, fun, _opts), do: {:ok, fun.()}
  end

  defmodule NoConnectAdapter do
    def execute(_conn, _query, _params, _opts), do: {:ok, %{}}
  end

  defmodule NoExecuteAdapter do
    def connect(_opts), do: {:ok, :conn}
  end

  defmodule NameOnlyAdapter do
    def connect(_opts), do: {:ok, :conn}
    def execute(_conn, _query, _params, _opts), do: {:ok, %{}}
    def adapter_name, do: "NameOnly"
    def capabilities, do: %{dialect: "name-only-sql"}
  end

  test "connect and execute delegate to adapter callbacks" do
    assert {:ok, {:conn, [database: "db"]}} = Connection.connect(FullAdapter, database: "db")
    assert :ok = Connection.disconnect(FullAdapter, :conn)

    assert {:ok, %{query: "select 1", params: [1]}} =
             Connection.execute(FullAdapter, :conn, "select 1", [1])
  end

  test "missing adapter callbacks return adapter errors" do
    assert {:error, {:adapter_error, _}} = Connection.connect(NoConnectAdapter, [])

    assert {:error, {:adapter_error, _}} =
             Connection.execute(NoExecuteAdapter, :conn, "select 1", [])

    assert {:error, :not_supported} = Connection.checkout(NoExecuteAdapter, :pool)
    assert :ok = Connection.checkin(NoExecuteAdapter, :pool, :conn)
  end

  test "transaction uses explicit transaction callback when available" do
    assert {:ok, :done} = Connection.transaction(ExplicitTxAdapter, :conn, fn -> :done end)
  end

  test "transaction fallback commits and rolls back correctly" do
    conn = %{test_pid: self()}

    assert {:ok, :value} = Connection.transaction(FallbackTxAdapter, conn, fn -> :value end)

    assert {:error, %RuntimeError{message: "boom"}} =
             Connection.transaction(FallbackTxAdapter, conn, fn -> raise "boom" end)

    assert_received :rolled_back
  end

  test "adapter metadata helpers" do
    assert Selecto.DB.PostgreSQL == Connection.default_adapter()

    assert Connection.adapter_available?(FullAdapter)
    refute Connection.adapter_available?(NoConnectAdapter)

    assert "FullAdapter" == Connection.adapter_name(FullAdapter)
    assert "custom" == Connection.adapter_dialect(FullAdapter)

    assert "NameOnly" == Connection.adapter_name(NameOnlyAdapter)
    assert "name-only-sql" == Connection.adapter_dialect(NameOnlyAdapter)
  end

  test "discover_adapters returns normalized metadata entries" do
    adapters = Connection.discover_adapters()
    assert is_list(adapters)

    Enum.each(adapters, fn adapter ->
      assert Map.has_key?(adapter, :module)
      assert Map.has_key?(adapter, :name)
      assert Map.has_key?(adapter, :dialect)
    end)
  end
end
