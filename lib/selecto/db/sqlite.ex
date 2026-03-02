defmodule Selecto.DB.SQLite do
  @moduledoc """
  SQLite adapter for Selecto.

  This module defines the adapter surface now and returns a structured
  dependency error for connection/execution until a concrete driver bridge is
  configured in the host app.
  """

  @behaviour Selecto.DB.Adapter

  @missing_dependency {:adapter_dependency_missing, :exqlite}

  @impl true
  def name, do: :sqlite

  @impl true
  def connect(connection) when is_pid(connection) or is_atom(connection), do: {:ok, connection}
  def connect(_opts), do: {:error, @missing_dependency}

  @impl true
  def execute(_connection, _query, _params, _opts), do: {:error, @missing_dependency}

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
end
