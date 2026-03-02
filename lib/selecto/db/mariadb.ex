defmodule Selecto.DB.MariaDB do
  @moduledoc """
  MariaDB adapter for Selecto.

  Shares connection and execution strategy with MySQL (`MyXQL`).
  """

  @behaviour Selecto.DB.Adapter

  @impl true
  def name, do: :mariadb

  @impl true
  def connect(opts), do: Selecto.DB.MySQL.connect(opts)

  @impl true
  def execute(connection, query, params, opts) do
    Selecto.DB.MySQL.execute(connection, query, params, opts)
  end

  @impl true
  def placeholder(_index), do: "?"

  @impl true
  def quote_identifier(identifier), do: Selecto.DB.MySQL.quote_identifier(identifier)

  @impl true
  def supports?(feature), do: Selecto.DB.MySQL.supports?(feature)
end
