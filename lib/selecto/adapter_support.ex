defmodule Selecto.AdapterSupport do
  @moduledoc false

  @default_adapter SelectoDBPostgreSQL.Adapter
  @legacy_postgresql_adapter Selecto.DB.PostgreSQL

  def default_adapter, do: @default_adapter

  def postgresql_adapter?(adapter) do
    adapter in [@default_adapter, @legacy_postgresql_adapter]
  end
end
