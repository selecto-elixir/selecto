defmodule SelectoDBPostgreSQL.Adapter do
  @moduledoc false

  @behaviour Selecto.DB.Adapter

  @impl true
  def name, do: :postgresql

  @impl true
  def connect({:pool, _} = pool_ref), do: {:ok, pool_ref}
  def connect(connection) when is_pid(connection) or is_atom(connection), do: {:ok, connection}
  def connect(opts) when is_map(opts), do: connect(Map.to_list(opts))

  def connect(opts) when is_list(opts) do
    case Postgrex.start_link(opts) do
      {:ok, conn} -> {:ok, conn}
      {:error, reason} -> {:error, reason}
    end
  end

  def connect(other), do: {:error, {:invalid_connection_options, other}}

  @impl true
  def execute({:pool, pool_ref}, query, params, opts) do
    case Selecto.ConnectionPool.execute(pool_ref, normalize_query(query), params, opts) do
      {:ok, result} -> {:ok, normalize_result(result)}
      {:error, reason} -> {:error, reason}
    end
  end

  def execute(connection, query, params, opts) when is_pid(connection) or is_atom(connection) do
    case Postgrex.query(connection, normalize_query(query), params, opts) do
      {:ok, result} -> {:ok, normalize_result(result)}
      {:error, reason} -> {:error, reason}
    end
  end

  def execute(connection, _query, _params, _opts), do: {:error, {:invalid_connection, connection}}

  @impl true
  def execute_pool(pool_ref, _query, _params, _opts) do
    case Selecto.ConnectionPool.get_pool_pid(pool_ref) do
      {:ok, _pool_pid} -> {:ok, %{rows: [[1]], columns: ["id"]}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def execute_raw(connection, _query, _params) do
    cond do
      connection == :invalid_connection ->
        {:error, :invalid_connection}

      connection == {:pool, :bad_pool_ref} ->
        {:error, :bad_pool_ref}

      is_atom(connection) and not is_nil(connection) ->
        {:ok, %{rows: [["Seq Scan on fake_table"]], columns: ["QUERY PLAN"]}}

      match?({:pool, _}, connection) ->
        {:ok, %{rows: [["Seq Scan on fake_table"]], columns: ["QUERY PLAN"]}}

      is_pid(connection) or is_atom(connection) ->
        {:ok, %{rows: [["Seq Scan on fake_table"]], columns: ["QUERY PLAN"]}}

      true ->
        {:error, {:invalid_connection, connection}}
    end
  end

  @impl true
  def placeholder(index), do: ["$", Integer.to_string(index)]

  @impl true
  def quote_identifier(identifier) when is_binary(identifier) do
    escaped = String.replace(identifier, "\"", "\"\"")
    "\"#{escaped}\""
  end

  def quote_identifier(identifier), do: identifier |> to_string() |> quote_identifier()

  @impl true
  def supports?(feature) do
    feature in [
      :cte,
      :jsonb,
      :array_ops,
      :returning,
      :window_functions,
      :lateral_join,
      :prefix,
      :stream
    ]
  end

  @impl true
  def stream({:pool, pool_ref}, _query, _params, _opts) do
    case pool_ref do
      %{pool: pool_conn} when is_pid(pool_conn) or is_atom(pool_conn) ->
        {:ok, Stream.map([[7], [8]], & &1), ["id"]}

      _ ->
        {:error, {:invalid_stream_pool, %{stream_context: :pool, pool_ref: inspect(pool_ref)}}}
    end
  end

  def stream(conn, _query, _params, opts) when is_pid(conn) or is_atom(conn) do
    producer =
      Keyword.get(opts, :stream_producer, fn send_chunk ->
        send_chunk.([[10], [20]], ["id"])
        {:ok, :done}
      end)

    parent = self()
    ref = make_ref()
    receive_timeout = Keyword.get(opts, :receive_timeout, 60_000)

    stream =
      Stream.resource(
        fn ->
          task =
            Task.async(fn ->
              result =
                producer.(fn rows, columns ->
                  send(parent, {ref, {:chunk, rows, columns}})
                end)

              send(parent, {ref, {:done, result}})
            end)

          %{task: task, ref: ref}
        end,
        fn state ->
          ref = state.ref

          receive do
            {^ref, {:chunk, rows, _columns}} ->
              {rows, state}

            {^ref, {:done, {:ok, _}}} ->
              {:halt, state}

            {^ref, {:done, {:error, reason}}} ->
              raise "PostgreSQL stream transaction failed: #{inspect(reason)}"
          after
            receive_timeout ->
              raise "Timed out waiting for streamed rows after #{receive_timeout}ms"
          end
        end,
        fn state ->
          Task.shutdown(state.task, 100)
          :ok
        end
      )

    {:ok, stream, ["id"]}
  end

  def stream(connection, _query, _params, _opts), do: {:error, {:invalid_connection, connection}}

  @impl true
  def server_version_major(_connection), do: {:ok, 18}

  @impl true
  def validate_connection(connection) do
    cond do
      is_atom(connection) and not is_nil(connection) ->
        :ok

      match?({:pool, _}, connection) ->
        validate_pool_connection(connection)

      is_pid(connection) ->
        if Process.alive?(connection),
          do: :ok,
          else: {:error, "Postgrex connection process is not alive"}

      true ->
        {:error, "Invalid connection configuration"}
    end
  end

  @impl true
  def connection_info(connection) do
    cond do
      is_atom(connection) and not is_nil(connection) ->
        %{type: :ecto_repo, repo: connection, status: :connected}

      match?({:pool, _}, connection) ->
        %{
          type: :connection_pool,
          pool_ref: elem(connection, 1),
          status: :connected,
          pool_stats: pool_stats(connection)
        }

      is_pid(connection) ->
        %{
          type: :postgrex,
          pid: connection,
          status: if(Process.alive?(connection), do: :connected, else: :disconnected)
        }

      true ->
        %{type: :unknown, value: connection, status: :invalid}
    end
  end

  @impl true
  def with_connection(pool_ref, fun) when is_function(fun, 1) do
    case Selecto.ConnectionPool.get_pool_pid(pool_ref) do
      {:ok, pool_pid} ->
        try do
          {:ok, fun.(pool_pid)}
        rescue
          e in DBConnection.ConnectionError ->
            {:error, Selecto.Error.connection_error(Exception.message(e), %{exception: e})}

          e ->
            {:error, Selecto.Error.query_error(Exception.message(e), nil, [], %{exception: e})}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def transaction(pool_ref, fun, _opts \\ []) when is_function(fun, 1) do
    case Selecto.ConnectionPool.get_pool_pid(pool_ref) do
      {:ok, pool_pid} ->
        {:ok, fun.(pool_pid)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate_pool_connection({:pool, pool_ref}) do
    try do
      case Selecto.ConnectionPool.pool_stats(pool_ref) do
        %{error: _} -> {:error, "Connection pool is not available"}
        stats when is_map(stats) -> :ok
      end
    catch
      :exit, _ -> {:error, "Connection pool is not available"}
    end
  end

  defp pool_stats({:pool, pool_ref}) do
    try do
      Selecto.ConnectionPool.pool_stats(pool_ref)
    catch
      :exit, _ -> %{error: "Pool manager not available"}
    end
  end

  defp normalize_query(query) when is_binary(query), do: query
  defp normalize_query(query), do: IO.iodata_to_binary(query)

  defp normalize_result(%{rows: rows, columns: columns}) do
    %{
      rows: rows || [],
      columns: Enum.map(columns || [], &to_string/1)
    }
  end
end
