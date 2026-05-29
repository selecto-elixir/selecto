defmodule Selecto.DB.PostgreSQL do
  @moduledoc false

  @behaviour Selecto.DB.Adapter

  @repo_passthrough_keys [
    :username,
    :password,
    :hostname,
    :database,
    :port,
    :socket_dir,
    :socket,
    :parameters,
    :ssl,
    :ssl_opts,
    :types,
    :timeout,
    :connect_timeout,
    :prepare,
    :backoff_type,
    :backoff_min,
    :backoff_max,
    :queue_target,
    :queue_interval,
    :idle_interval,
    :sslmode,
    :cacertfile,
    :certfile,
    :keyfile
  ]

  @impl true
  def name, do: :postgresql

  @impl true
  def connect({:pool, _} = pool_ref), do: {:ok, pool_ref}
  def connect(connection) when is_pid(connection) or is_atom(connection), do: {:ok, connection}
  def connect(opts) when is_map(opts), do: connect(Map.to_list(opts))

  def connect(opts) when is_list(opts) do
    with :ok <- ensure_postgrex_available() do
      Kernel.apply(Postgrex, :start_link, [opts])
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

  def execute(connection, query, params, _opts)
      when is_atom(connection) and not is_nil(connection) do
    cond do
      repo_module?(connection) -> execute_raw(connection, query, params)
      true -> query_postgrex(connection, query, params)
    end
  end

  def execute(connection, query, params, _opts) when is_pid(connection) do
    query_postgrex(connection, query, params)
  end

  def execute(connection, _query, _params, _opts), do: {:error, {:invalid_connection, connection}}

  @impl true
  def execute_raw(connection, query, params) do
    cond do
      is_atom(connection) and not is_nil(connection) and repo_module?(connection) ->
        query_ecto(connection, query, params)

      match?({:pool, _}, connection) ->
        execute(connection, query, params, prepared: false)

      is_pid(connection) or is_atom(connection) ->
        query_postgrex(connection, query, params)

      true ->
        {:error,
         Selecto.Error.connection_error("Invalid connection type", %{
           connection: inspect(connection)
         })}
    end
  rescue
    error -> {:error, Selecto.Error.from_reason(error)}
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
  def rollup_literal_order(index), do: [Integer.to_string(index), " asc nulls first"]

  @impl true
  def rollup_sort_fix(connection) do
    case server_version_major(connection) do
      {:ok, major} when is_integer(major) and major >= 18 -> false
      _ -> true
    end
  end

  @impl true
  def server_version_major(connection) do
    with {:ok, %{rows: [[version_num]]}} <-
           execute_raw(connection, "show server_version_num", []),
         {parsed, _} <- Integer.parse(to_string(version_num)) do
      {:ok, div(parsed, 10_000)}
    else
      _ -> {:error, :invalid_server_version_num}
    end
  end

  @impl true
  def execute_repo_fallback(repo, query, params) do
    with :ok <- ensure_postgrex_available(),
         true <- repo_module?(repo),
         config when is_list(config) <- Kernel.apply(repo, :config, []),
         postgrex_opts <-
           config |> Keyword.take(@repo_passthrough_keys) |> Keyword.put(:supervisor, false),
         {:ok, conn} <- Kernel.apply(Postgrex, :start_link, [postgrex_opts]) do
      try do
        execute(conn, query, params, [])
      after
        GenServer.stop(conn)
      end
    else
      false -> {:error, {:invalid_repo, repo}}
      {:error, _} = error -> error
      other -> {:error, other}
    end
  end

  @impl true
  def supports?(feature) do
    feature in [
      :cte,
      :jsonb,
      :array_ops,
      :array_any_comparison,
      :native_null_ordering,
      :rollup,
      :returning,
      :text_search,
      :window_functions,
      :lateral_join,
      :prefix
    ]
  end

  defp query_postgrex(connection, query, params) do
    with :ok <- ensure_postgrex_available(),
         {:ok, result} <-
           Kernel.apply(Postgrex, :query, [connection, normalize_query(query), params]) do
      {:ok, normalize_result(result)}
    else
      {:error, reason} -> {:error, Selecto.Error.from_reason(reason)}
    end
  end

  defp query_ecto(repo, query, params) do
    with :ok <- ensure_ecto_sql_available(),
         {:ok, result} <-
           Kernel.apply(Ecto.Adapters.SQL, :query, [repo, normalize_query(query), params]) do
      {:ok, normalize_result(result)}
    else
      {:error, reason} -> {:error, Selecto.Error.from_reason(reason)}
    end
  end

  defp repo_module?(connection) when is_atom(connection) do
    Code.ensure_loaded?(connection) and function_exported?(connection, :config, 0) and
      function_exported?(connection, :__adapter__, 0)
  end

  defp repo_module?(_), do: false

  defp ensure_postgrex_available do
    if Code.ensure_loaded?(Postgrex) and function_exported?(Postgrex, :query, 3) do
      :ok
    else
      {:error,
       Selecto.Error.connection_error(
         "Postgrex is not available. Add the `postgrex` dependency or use `selecto_db_postgresql`.",
         %{}
       )}
    end
  end

  defp ensure_ecto_sql_available do
    if Code.ensure_loaded?(Ecto.Adapters.SQL) and function_exported?(Ecto.Adapters.SQL, :query, 3) do
      :ok
    else
      {:error,
       Selecto.Error.connection_error(
         "Ecto SQL is not available. Add the `ecto_sql` dependency or use a Postgrex connection.",
         %{}
       )}
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
