defmodule Selecto.DB.MariaDB do
  @moduledoc """
  MariaDB adapter for Selecto.

  Shares connection and execution strategy with MySQL (`MyXQL`).
  """

  @behaviour Selecto.DB.Adapter

  @impl true
  def name, do: :mariadb

  @impl true
  def connect(opts), do: SelectoDBMySQL.Adapter.connect(opts)

  @impl true
  def execute(connection, query, params, opts) do
    SelectoDBMySQL.Adapter.execute(connection, query, params, opts)
  end

  @impl true
  def placeholder(_index), do: "?"

  @impl true
  def quote_identifier(identifier), do: SelectoDBMySQL.Adapter.quote_identifier(identifier)

  @impl true
  def supports?(feature), do: SelectoDBMySQL.Adapter.supports?(feature)
end
