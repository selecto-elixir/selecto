defmodule Selecto.AdapterSupport do
  @moduledoc false

  @default_adapter Selecto.DB.PostgreSQL
  @external_postgresql_adapter SelectoDBPostgreSQL.Adapter
  @legacy_postgresql_adapter Selecto.DB.PostgreSQL
  @feature_aliases %{
    text_search_boolean: [:text_search_boolean, :text_search_boolean_mode],
    text_search_query_expansion: [
      :text_search_query_expansion,
      :text_search_query_expansion_mode
    ]
  }

  def default_adapter do
    cond do
      callback_available?(@external_postgresql_adapter, :name, 0) ->
        @external_postgresql_adapter

      callback_available?(@default_adapter, :name, 0) ->
        @default_adapter

      true ->
        @default_adapter
    end
  end

  def postgresql_adapter?(adapter) do
    adapter in [default_adapter(), @external_postgresql_adapter, @legacy_postgresql_adapter]
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
      feature
      |> feature_aliases()
      |> Enum.any?(&adapter.supports?/1)
    else
      false
    end
  end

  def supports_feature?(_adapter, _feature), do: false

  def canonical_feature_name(feature) when is_atom(feature) do
    Enum.find_value(@feature_aliases, feature, fn {canonical, aliases} ->
      if feature in aliases, do: canonical, else: nil
    end)
  end

  def canonical_feature_name(feature), do: feature

  defp feature_aliases(feature) when is_atom(feature) do
    canonical = canonical_feature_name(feature)
    Map.get(@feature_aliases, canonical, [canonical])
  end

  def callback_available?(adapter, function, arity)

  def callback_available?(adapter, function, arity)
      when is_atom(adapter) and is_atom(function) and is_integer(arity) do
    Code.ensure_loaded?(adapter) and function_exported?(adapter, function, arity)
  end

  def callback_available?(_adapter, _function, _arity), do: false
end
