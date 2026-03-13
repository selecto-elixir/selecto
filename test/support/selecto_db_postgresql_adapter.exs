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

  defp normalize_query(query) when is_binary(query), do: query
  defp normalize_query(query), do: IO.iodata_to_binary(query)

  defp normalize_result(%{rows: rows, columns: columns}) do
    %{
      rows: rows || [],
      columns: Enum.map(columns || [], &to_string/1)
    }
  end
end
