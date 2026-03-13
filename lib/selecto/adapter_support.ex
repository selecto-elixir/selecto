defmodule Selecto.AdapterSupport do
  @moduledoc false

  @default_adapter SelectoDBPostgreSQL.Adapter
  @legacy_postgresql_adapter Selecto.DB.PostgreSQL

  def default_adapter, do: @default_adapter

  def postgresql_adapter?(adapter) do
    adapter in [@default_adapter, @legacy_postgresql_adapter]
  end

  def callback_available?(adapter, function, arity)

  def callback_available?(adapter, function, arity)
      when is_atom(adapter) and is_atom(function) and is_integer(arity) do
    Code.ensure_loaded?(adapter) and function_exported?(adapter, function, arity)
  end

  def callback_available?(_adapter, _function, _arity), do: false
end
