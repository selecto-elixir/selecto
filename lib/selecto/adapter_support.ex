defmodule Selecto.AdapterSupport do
  @moduledoc false

  @default_adapter SelectoDBPostgreSQL.Adapter
  @legacy_postgresql_adapter Selecto.DB.PostgreSQL
  @postgres_rollup_features [:rollup]

  def default_adapter, do: @default_adapter

  def postgresql_adapter?(adapter) do
    adapter in [@default_adapter, @legacy_postgresql_adapter]
  end

  def supports_feature?(nil, feature), do: supports_feature?(@default_adapter, feature)

  def supports_feature?(adapter, feature) when is_atom(adapter) and is_atom(feature) do
    cond do
      postgresql_adapter?(adapter) and feature in @postgres_rollup_features ->
        true

      callback_available?(adapter, :supports?, 1) ->
        adapter.supports?(feature)

      true ->
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
