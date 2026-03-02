defmodule Selecto.ConfigurationAdapterTest do
  use ExUnit.Case, async: true

  defmodule FakeAdapter do
    @behaviour Selecto.DB.Adapter

    @impl true
    def name, do: :fake

    @impl true
    def connect(_opts), do: {:ok, spawn(fn -> Process.sleep(:infinity) end)}

    @impl true
    def execute(_conn, _query, _params, _opts), do: {:ok, %{rows: [[1]], columns: ["id"]}}

    @impl true
    def placeholder(_index), do: "?"

    @impl true
    def quote_identifier(identifier), do: "`#{identifier}`"

    @impl true
    def supports?(_feature), do: true
  end

  defp domain do
    %{
      name: "Config adapter test",
      source: %{
        source_table: "users",
        primary_key: :id,
        fields: [:id],
        redact_fields: [],
        columns: %{id: %{type: :integer}},
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end

  test "configure uses adapter direct connection when pooling is disabled" do
    selecto =
      domain()
      |> Selecto.configure([], adapter: FakeAdapter, validate: false)
      |> Selecto.select(["id"])

    assert is_pid(selecto.connection)
    assert selecto.adapter == FakeAdapter
    assert {:ok, {[[1]], ["id"], aliases}} = Selecto.execute(selecto, analyze_complexity: false)
    assert length(aliases) == 1
    assert is_binary(hd(aliases))

    Process.exit(selecto.connection, :kill)
  end

  test "configure reuses pooled adapter reference for non-postgresql adapters" do
    selecto =
      domain()
      |> Selecto.configure([], adapter: FakeAdapter, pool: true, validate: false)
      |> Selecto.select(["id"])

    assert {:pool, %{adapter: FakeAdapter} = pool_ref} = selecto.postgrex_opts
    assert %{adapter: FakeAdapter, connection: connection, manager: _manager} = selecto.connection
    assert is_pid(connection)

    assert {:ok, {[[1]], ["id"], aliases}} = Selecto.execute(selecto, analyze_complexity: false)
    assert length(aliases) == 1
    assert is_binary(hd(aliases))
    assert :ok = Selecto.ConnectionPool.stop_pool(pool_ref)
  end
end
