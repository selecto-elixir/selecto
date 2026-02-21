defmodule Selecto.ExecutorTest do
  use ExUnit.Case

  alias Selecto.Executor

  defmodule Adapter do
    def execute(:single, _query, _params, _opts), do: {:ok, %{rows: [[1]], columns: ["id"]}}
    def execute(:empty, _query, _params, _opts), do: {:ok, %{rows: [], columns: ["id"]}}

    def execute(:multiple, _query, _params, _opts),
      do: {:ok, %{rows: [[1], [2]], columns: ["id"]}}

    def execute(:error, _query, _params, _opts), do: {:error, "adapter failed"}

    def execute(:raise, _query, _params, _opts) do
      raise "adapter raised"
    end

    def execute(:exit, _query, _params, _opts) do
      exit(:adapter_exit)
    end

    def execute(:sleep, _query, _params, _opts) do
      Process.sleep(50)
      {:ok, %{rows: [[1]], columns: ["id"]}}
    end
  end

  defp domain do
    %{
      name: "Executor test",
      source: %{
        source_table: "users",
        primary_key: :id,
        fields: [:id, :name],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end

  defp selecto_for(connection) do
    Selecto.configure(domain(), nil)
    |> Selecto.select(["id"])
    |> Map.put(:adapter, Adapter)
    |> Map.put(:connection, connection)
  end

  test "execute_with_adapter normalizes successful results" do
    assert {:ok, {[[1]], ["id"], ["id"]}} =
             Executor.execute_with_adapter(Adapter, :single, "select 1", [], ["id"])
  end

  test "execute_with_adapter wraps adapter errors" do
    assert {:error, %Selecto.Error{type: :query_error}} =
             Executor.execute_with_adapter(Adapter, :error, "select 1", [], ["id"])
  end

  test "execute_with_adapter handles raised exceptions" do
    assert {:error, %Selecto.Error{type: :connection_error}} =
             Executor.execute_with_adapter(Adapter, :raise, "select 1", [], ["id"])
  end

  test "execute_with_adapter handles exits" do
    assert {:error, %Selecto.Error{type: :connection_error}} =
             Executor.execute_with_adapter(Adapter, :exit, "select 1", [], ["id"])
  end

  test "execute_with_postgrex rejects invalid connection types" do
    assert {:error, %Selecto.Error{type: :connection_error}} =
             Executor.execute_with_postgrex(123, "select 1", [], ["id"])
  end

  test "execute returns timeout error for long-running adapter" do
    result = Executor.execute(selecto_for(:sleep), analyze_complexity: false, timeout: 1)
    assert {:error, %Selecto.Error{type: :timeout_error}} = result
  end

  test "execute_one returns row, no_results, and multiple_results variants" do
    assert {:ok, {[1], aliases}} =
             Executor.execute_one(selecto_for(:single), analyze_complexity: false)

    assert is_list(aliases)
    assert length(aliases) == 1
    assert is_binary(hd(aliases))

    assert {:error, %Selecto.Error{type: :no_results}} =
             Executor.execute_one(selecto_for(:empty), analyze_complexity: false)

    assert {:error, %Selecto.Error{type: :multiple_results}} =
             Executor.execute_one(selecto_for(:multiple), analyze_complexity: false)
  end

  test "execute_with_metadata returns sql and execution_time" do
    assert {:ok, _result, metadata} = Executor.execute_with_metadata(selecto_for(:single))
    assert is_binary(metadata.sql)
    assert is_list(metadata.params)
    assert is_integer(metadata.execution_time)
  end

  test "validate_connection checks pid lifecycle" do
    pid = spawn(fn -> Process.sleep(:infinity) end)
    assert :ok == Executor.validate_connection(%Selecto{postgrex_opts: pid})
    Process.exit(pid, :kill)
    Process.sleep(10)

    assert {:error, "Postgrex connection process is not alive"} ==
             Executor.validate_connection(%Selecto{postgrex_opts: pid})
  end

  test "connection_info describes repo, pid, and unknown connections" do
    repo_info = Executor.connection_info(%Selecto{postgrex_opts: SomeRepo})
    assert %{type: :ecto_repo, repo: SomeRepo, status: :connected} = repo_info

    pid = spawn(fn -> Process.sleep(:infinity) end)
    pid_info = Executor.connection_info(%Selecto{postgrex_opts: pid})
    assert %{type: :postgrex, pid: ^pid, status: :connected} = pid_info
    Process.exit(pid, :kill)

    unknown = Executor.connection_info(%Selecto{postgrex_opts: 123})
    assert %{type: :unknown, status: :invalid, value: 123} = unknown
  end
end
