defmodule Selecto.DB.MariaDB do
  @moduledoc """
  MariaDB adapter for Selecto.

  Uses `MyXQL` when available. If `MyXQL` is not present, connect/execute
  return structured dependency errors.
  """

  @behaviour Selecto.DB.Adapter

  @missing_dependency {:adapter_dependency_missing, :myxql}

  @impl true
  def name, do: :mariadb

  @impl true
  def connect(connection) when is_pid(connection) or is_atom(connection), do: {:ok, connection}
  def connect(opts) when is_map(opts), do: connect(Map.to_list(opts))

  def connect(opts) when is_list(opts) do
    if dependency_available?() do
      case Kernel.apply(MyXQL, :start_link, [opts]) do
        {:ok, conn} -> {:ok, conn}
        {:error, reason} -> {:error, reason}
      end
    else
      {:error, @missing_dependency}
    end
  end

  def connect(other), do: {:error, {:invalid_connection_options, other}}

  @impl true
  def execute(connection, query, params, opts) do
    if is_map(connection) and Map.has_key?(connection, :adapter) and
         Map.has_key?(connection, :connection) do
      execute_direct(Map.get(connection, :connection), query, params, opts)
    else
      execute_direct(connection, query, params, opts)
    end
  end

  defp execute_direct(connection, query, params, opts) do
    if dependency_available?() do
      case Kernel.apply(MyXQL, :query, [connection, normalize_query(query), params, opts]) do
        {:ok, result} -> {:ok, normalize_result(result)}
        {:error, reason} -> {:error, reason}
      end
    else
      {:error, @missing_dependency}
    end
  end

  @impl true
  def placeholder(_index), do: "?"

  @impl true
  def quote_identifier(identifier) when is_binary(identifier) do
    escaped = String.replace(identifier, "`", "``")
    "`#{escaped}`"
  end

  def quote_identifier(identifier), do: identifier |> to_string() |> quote_identifier()

  @impl true
  def supports?(feature), do: feature in [:cte, :window_functions, :transactions]

  defp dependency_available? do
    Code.ensure_loaded?(MyXQL) and function_exported?(MyXQL, :start_link, 1) and
      function_exported?(MyXQL, :query, 4)
  end

  defp normalize_query(query) when is_binary(query), do: query
  defp normalize_query(query), do: IO.iodata_to_binary(query)

  defp normalize_result(%{rows: rows} = result) do
    columns =
      result
      |> Map.get(:columns, [])
      |> Enum.map(&normalize_column_name/1)

    %{
      rows: rows || [],
      columns: columns
    }
  end

  defp normalize_column_name(%{name: name}) when is_binary(name), do: name
  defp normalize_column_name(name) when is_binary(name), do: name
  defp normalize_column_name(name) when is_atom(name), do: Atom.to_string(name)
  defp normalize_column_name(other), do: to_string(other)
end
