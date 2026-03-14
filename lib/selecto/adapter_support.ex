defmodule Selecto.AdapterSupport do
  @moduledoc false

  @default_adapter SelectoDBPostgreSQL.Adapter
  @legacy_postgresql_adapter Selecto.DB.PostgreSQL

  def default_adapter, do: @default_adapter

  def postgresql_adapter?(adapter) do
    adapter in [@default_adapter, @legacy_postgresql_adapter]
  end

  def adapter_name(nil), do: adapter_name(@default_adapter)

  def adapter_name(adapter) when is_atom(adapter) do
    cond do
      callback_available?(adapter, :name, 0) ->
        adapter.name()

      postgresql_adapter?(adapter) ->
        :postgresql

      true ->
        nil
    end
  end

  def adapter_name(_adapter), do: nil

  def supports_feature?(nil, feature), do: supports_feature?(@default_adapter, feature)

  def supports_feature?(adapter, feature) when is_atom(adapter) and is_atom(feature) do
    if callback_available?(adapter, :supports?, 1) do
      adapter.supports?(feature)
    else
      false
    end
  end

  def supports_feature?(_adapter, _feature), do: false

  def callback_available?(adapter, function, arity)

  def callback_available?(adapter, function, arity)
      when is_atom(adapter) and is_atom(function) and is_integer(arity) do
    Code.ensure_loaded?(adapter) and function_exported?(adapter, function, arity)
  end

  def callback_available?(_adapter, _function, _arity), do: false
end
