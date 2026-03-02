defmodule Selecto.ExecutorTest do
  use ExUnit.Case

  alias Selecto.Executor
  alias Selecto.Performance.Hooks

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

    def stream(:single_stream, _query, _params, _opts) do
      {:ok, Stream.map([[1], [2]], & &1), ["id"]}
    end

    def stream(:stream_error, _query, _params, _opts), do: {:error, "stream failed"}
  end

  defmodule FakeRepo do
    def __adapter__, do: :fake
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

  defp postgrex_stream_selecto(connection) do
    Selecto.configure(domain(), connection)
    |> Selecto.select(["id"])
    |> Map.put(:adapter, Selecto.DB.PostgreSQL)
    |> Map.put(:postgrex_opts, connection)
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

  test "execute_with_connection_pool returns normalized pooled result" do
    pool_ref = %{adapter: Adapter, connection: :single}

    assert {:ok, {[[1]], ["id"], ["id"]}} =
             Executor.execute_with_connection_pool(pool_ref, "select 1", [], ["id"])
  end

  test "execute_with_connection_pool wraps pooled execution errors" do
    pool_ref = %{adapter: Adapter, connection: :error}

    assert {:error, %Selecto.Error{type: :query_error}} =
             Executor.execute_with_connection_pool(pool_ref, "select 1", [], ["id"])
  end

  test "execute_stream uses adapter stream/4 when available" do
    assert {:ok, stream} =
             Executor.execute_stream(selecto_for(:single_stream), analyze_complexity: false)

    rows = Enum.to_list(stream)

    assert [{[1], ["id"], aliases_1}, {[2], ["id"], aliases_2}] = rows
    assert is_list(aliases_1)
    assert is_binary(hd(aliases_1))
    assert aliases_1 == aliases_2
  end

  test "execute_stream wraps adapter stream errors" do
    assert {:error, %Selecto.Error{type: :query_error}} =
             Executor.execute_stream(selecto_for(:stream_error), analyze_complexity: false)
  end

  test "execute_stream returns validation error when adapter lacks stream support" do
    assert {:error, %Selecto.Error{type: :validation_error}} =
             Executor.execute_stream(selecto_for(:single), analyze_complexity: false)
  end

  test "execute_stream returns explicit contract error for pooled postgres" do
    assert {:error, %Selecto.Error{type: :validation_error, details: details}} =
             Executor.execute_stream(postgrex_stream_selecto({:pool, %{}}),
               analyze_complexity: false
             )

    assert details[:stream_context] == :pool
  end

  test "execute_stream returns explicit contract error for ecto repo context" do
    assert {:error, %Selecto.Error{type: :validation_error, details: details}} =
             Executor.execute_stream(postgrex_stream_selecto(FakeRepo), analyze_complexity: false)

    assert details[:stream_context] == :ecto_repo
  end

  test "execute_stream receive_timeout errors when producer stalls" do
    assert {:ok, stream} =
             Executor.execute_stream(
               postgrex_stream_selecto(:fake_conn),
               analyze_complexity: false,
               receive_timeout: 5,
               queue_timeout: 1,
               stream_producer: fn _send_chunk ->
                 Process.sleep(30)
                 {:ok, :done}
               end
             )

    assert_raise RuntimeError, ~r/Timed out waiting for streamed rows/, fn ->
      Enum.to_list(stream)
    end
  end

  test "execute_stream consumes custom postgrex producer chunks" do
    assert {:ok, stream} =
             Executor.execute_stream(
               postgrex_stream_selecto(:fake_conn),
               analyze_complexity: false,
               stream_producer: fn send_chunk ->
                 send_chunk.([[10], [20]], ["id"])
                 {:ok, :done}
               end
             )

    assert [{[10], ["id"], aliases_1}, {[20], ["id"], aliases_2}] = Enum.to_list(stream)
    assert aliases_1 == aliases_2
    assert is_binary(hd(aliases_1))
  end

  test "execute returns timeout error for long-running adapter" do
    result = Executor.execute(selecto_for(:sleep), analyze_complexity: false, timeout: 1)
    assert {:error, %Selecto.Error{type: :timeout_error}} = result
  end

  test "execute routes through performance hook orchestration" do
    Hooks.unregister(:before_query_build)
    parent = self()

    Hooks.register(:before_query_build, fn context ->
      send(parent, {:before_query_build, context.query_id})
      context
    end)

    assert {:ok, {[[1]], ["id"], _aliases}} =
             Executor.execute(selecto_for(:single), analyze_complexity: false)

    assert_received {:before_query_build, query_id}
    assert is_binary(query_id)

    Hooks.unregister(:before_query_build)
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
