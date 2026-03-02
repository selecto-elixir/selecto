defmodule Selecto.DB.SQLite do
  @moduledoc """
  SQLite adapter for Selecto.

  Uses `Exqlite` when available. If `Exqlite` is not present,
  connect/execute return structured dependency errors.
  """

  @behaviour Selecto.DB.Adapter

  @missing_dependency {:adapter_dependency_missing, :exqlite}

  @impl true
  def name, do: :sqlite

  @impl true
  def connect(connection) when is_reference(connection), do: {:ok, connection}
  def connect(connection) when is_pid(connection) or is_atom(connection), do: {:ok, connection}
  def connect(opts) when is_map(opts), do: connect(Map.to_list(opts))

  def connect(opts) when is_list(opts) do
    if dependency_available?() do
      database = Keyword.get(opts, :database) || Keyword.get(opts, :path) || ":memory:"
      mode = Keyword.get(opts, :mode, :readwrite)

      case Kernel.apply(Exqlite.Sqlite3, :open, [database, [mode: mode]]) do
        {:ok, conn} -> {:ok, conn}
        {:error, reason} -> {:error, reason}
      end
    else
      {:error, @missing_dependency}
    end
  end

  def connect(other), do: {:error, {:invalid_connection_options, other}}

  @impl true
  def execute(connection, query, params, _opts) do
    resolved_connection = resolve_connection(connection)

    cond do
      not dependency_available?() ->
        {:error, @missing_dependency}

      not is_reference(resolved_connection) ->
        {:error, {:invalid_connection, connection}}

      true ->
        execute_statement(resolved_connection, normalize_query(query), params || [])
    end
  end

  @impl true
  def placeholder(_index), do: "?"

  @impl true
  def quote_identifier(identifier) when is_binary(identifier) do
    escaped = String.replace(identifier, "\"", "\"\"")
    "\"#{escaped}\""
  end

  def quote_identifier(identifier), do: identifier |> to_string() |> quote_identifier()

  @impl true
  def supports?(feature) do
    feature in [:cte, :window_functions, :transactions]
  end

  defp execute_statement(connection, query, params) do
    case Kernel.apply(Exqlite.Sqlite3, :prepare, [connection, query]) do
      {:ok, statement} ->
        result =
          with :ok <- bind_params(statement, params),
               {:ok, columns} <- Kernel.apply(Exqlite.Sqlite3, :columns, [connection, statement]),
               {:ok, rows} <- Kernel.apply(Exqlite.Sqlite3, :fetch_all, [connection, statement]) do
            {:ok,
             %{
               rows: rows || [],
               columns: Enum.map(columns || [], &to_string/1)
             }}
          end

        _ = Kernel.apply(Exqlite.Sqlite3, :release, [connection, statement])
        result

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp bind_params(statement, params) do
    try do
      :ok = Kernel.apply(Exqlite.Sqlite3, :bind, [statement, params])
      :ok
    rescue
      error in ArgumentError -> {:error, {:invalid_query_params, Exception.message(error)}}
    end
  end

  defp resolve_connection(connection)

  defp resolve_connection(%{adapter: _adapter, connection: nested_connection}) do
    resolve_connection(nested_connection)
  end

  defp resolve_connection(%{db: db}) when is_reference(db), do: db
  defp resolve_connection(connection), do: connection

  defp normalize_query(query) when is_binary(query), do: query
  defp normalize_query(query), do: IO.iodata_to_binary(query)

  defp dependency_available? do
    Code.ensure_loaded?(Exqlite.Sqlite3) and function_exported?(Exqlite.Sqlite3, :open, 2) and
      function_exported?(Exqlite.Sqlite3, :prepare, 2) and
      function_exported?(Exqlite.Sqlite3, :bind, 2) and
      function_exported?(Exqlite.Sqlite3, :columns, 2) and
      function_exported?(Exqlite.Sqlite3, :fetch_all, 2) and
      function_exported?(Exqlite.Sqlite3, :release, 2)
  end
end
