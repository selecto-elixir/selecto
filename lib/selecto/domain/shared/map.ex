defmodule Selecto.Domain.Shared.Map do
  @moduledoc false

  alias Selecto.Domain.Contract.Shared.Core

  defdelegate field_id(field), to: Core
  defdelegate map_value(map, key), to: Core
  defdelegate fetch_key(map, key), to: Core
  defdelegate value_type(value), to: Core
  defdelegate safe_existing_atom(value), to: Core
  defdelegate relation_field_ref(relation_id, field), to: Core

  def relation_field_entries(relation) when is_map(relation) do
    columns =
      case map_value(relation, :columns) do
        columns when is_map(columns) -> columns
        _columns -> %{}
      end

    fields =
      case map_value(relation, :fields) do
        fields when is_list(fields) -> fields
        _fields -> []
      end

    (fields ++ Map.keys(columns))
    |> Enum.uniq_by(&field_id/1)
    |> Enum.sort_by(&field_id/1)
    |> Enum.map(fn field ->
      column =
        case fetch_key(columns, field) do
          {:ok, column} when is_map(column) -> column
          _ -> %{}
        end

      {field, column}
    end)
  end

  def relation_field_entries(_relation), do: []

  def relation_association(relation, association_id) when is_map(relation) do
    case map_value(relation, :associations) do
      associations when is_map(associations) ->
        case fetch_key(associations, association_id) do
          {:ok, association} -> association
          :error -> nil
        end

      _associations ->
        nil
    end
  end

  def relation_association(_relation, _association_id), do: nil

  def field_label(map) when is_map(map) do
    map_value(map, :label) || map_value(map, :name) || map_value(map, :display_name)
  end

  def field_label(_map), do: nil
  def field_ref_or_nil(value) when is_atom(value) and not is_nil(value), do: field_id(value)
  def field_ref_or_nil(value) when is_binary(value), do: value
  def field_ref_or_nil(_value), do: nil

  def first_map_value(map, keys) when is_map(map) do
    Enum.find_value(keys, &map_value(map, &1))
  end

  def first_map_value(_map, _keys), do: nil
  def id_value(value) when is_atom(value) and not is_nil(value), do: value
  def id_value(value) when is_binary(value), do: value
  def id_value(_value), do: nil

  def schema_fields(schemas) when is_map(schemas) do
    schemas
    |> sorted_entries()
    |> Enum.into(%{}, fn {schema_id, schema} -> {schema_id, relation_field_ids(schema)} end)
  end

  def schema_fields(_schemas), do: %{}

  def relation_field_ids(relation) when is_map(relation) do
    fields =
      case map_value(relation, :fields) do
        fields when is_list(fields) -> fields
        _fields -> []
      end

    columns =
      case map_value(relation, :columns) do
        columns when is_map(columns) -> Map.keys(columns)
        _columns -> []
      end

    (fields ++ columns)
    |> Enum.map(&field_id/1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  def relation_field_ids(_relation), do: []

  def query_member_keys(query_members) when is_map(query_members) do
    %{
      ctes: sorted_keys(map_value(query_members, :ctes)),
      values: sorted_keys(map_value(query_members, :values)),
      subqueries: sorted_keys(map_value(query_members, :subqueries)),
      laterals: sorted_keys(map_value(query_members, :laterals)),
      unnests: sorted_keys(map_value(query_members, :unnests))
    }
  end

  def query_member_keys(_query_members) do
    %{
      ctes: [],
      values: [],
      subqueries: [],
      laterals: [],
      unnests: []
    }
  end

  def query_member_count(query_members) when is_map(query_members) do
    query_members
    |> query_member_keys()
    |> Map.values()
    |> Enum.map(&length/1)
    |> Enum.sum()
  end

  def query_member_count(_query_members), do: 0

  def sorted_entries(map) when is_map(map) do
    Enum.sort_by(map, fn {key, _value} -> field_id(key) end)
  end

  def sorted_entries(_map), do: []

  def sorted_keys(map) when is_map(map) do
    map
    |> Map.keys()
    |> Enum.sort_by(&field_id/1)
  end

  def sorted_keys(_map), do: []
  def map_count(map) when is_map(map), do: map_size(map)
  def map_count(_map), do: 0
  def list_count(list) when is_list(list), do: length(list)
  def list_count(_list), do: 0

  def compact_nil(map) when is_map(map) do
    map
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  def function_arity(hook) when is_function(hook) do
    case Function.info(hook, :arity) do
      {:arity, arity} -> arity
      _other -> nil
    end
  end

  def maybe_put(map, _key, nil), do: map
  def maybe_put(map, key, value), do: Map.put(map, key, value)
  def maybe_put_default(map, _key, nil), do: map

  def maybe_put_default(map, key, value) do
    if has_key_variant?(map, key) do
      map
    else
      put_map_value(map, key, value)
    end
  end

  def put_section(domain, key, value), do: put_map_value(domain, key, value)

  def put_map_value(map, key, value) when is_map(map) and is_atom(key) do
    cond do
      Map.has_key?(map, key) -> Map.put(map, key, value)
      Map.has_key?(map, Atom.to_string(key)) -> Map.put(map, Atom.to_string(key), value)
      true -> Map.put(map, key, value)
    end
  end

  def delete_key_variants(map, key) when is_map(map) and is_atom(key) do
    map
    |> Map.delete(key)
    |> Map.delete(Atom.to_string(key))
  end

  def has_key_variant?(map, key) when is_map(map) and is_atom(key) do
    Map.has_key?(map, key) or Map.has_key?(map, Atom.to_string(key))
  end

  def has_key_variant?(_map, _key), do: false

  def section(domain, key, default \\ nil) do
    case fetch_section(domain, key) do
      {:ok, value} -> value
      :error -> default
    end
  end

  def map_section(domain, key, default \\ nil) do
    case fetch_section(domain, key) do
      {:ok, value} -> value
      :error -> default
    end
  end

  def fetch_section(domain, key) when is_atom(key) do
    case Map.fetch(domain, key) do
      {:ok, value} -> {:ok, value}
      :error -> Map.fetch(domain, Atom.to_string(key))
    end
  end
end
