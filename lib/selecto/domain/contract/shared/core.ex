defmodule Selecto.Domain.Contract.Shared.Core do
  @moduledoc false

  use Selecto.Domain.Constants
  def relation_field_ref(:source, field), do: field
  def relation_field_ref(relation_id, field), do: "#{field_id(relation_id)}.#{field_id(field)}"

  def field_index(source, schemas, projection) do
    source_fields =
      source
      |> relation_fields()
      |> MapSet.new()

    schema_fields =
      if is_map(schemas) do
        Enum.flat_map(schemas, fn {schema_id, schema} ->
          schema
          |> relation_fields()
          |> Enum.map(&"#{field_id(schema_id)}.#{&1}")
        end)
      else
        []
      end

    custom_fields =
      projection
      |> map_value(:custom_columns)
      |> case do
        custom_columns when is_map(custom_columns) ->
          Enum.map(custom_columns, fn {field, _} -> field_id(field) end)

        _ ->
          []
      end

    source_fields
    |> MapSet.union(MapSet.new(schema_fields))
    |> MapSet.union(MapSet.new(custom_fields))
  end

  def relation_fields(relation) when is_map(relation) do
    fields =
      case map_value(relation, :fields) do
        fields when is_list(fields) -> fields
        _ -> []
      end

    columns =
      case map_value(relation, :columns) do
        columns when is_map(columns) -> Map.keys(columns)
        _ -> []
      end

    Enum.map(fields ++ columns, &field_id/1)
  end

  def relation_fields(_relation), do: []

  def known_field?(_field_index, field) when not (is_atom(field) or is_binary(field)), do: true
  def known_field?(field_index, field), do: MapSet.member?(field_index, field_id(field))

  def field_in_list?(fields, field) do
    field_id = field_id(field)
    Enum.any?(fields, &(field_id(&1) == field_id))
  end

  def field_ref?(field), do: is_atom(field) or is_binary(field)

  def valid_choice_source_path?(path), do: valid_static_source_path?(path)

  def valid_static_source_path?(path) when is_atom(path), do: not is_nil(path)

  def valid_static_source_path?(path) when is_binary(path) do
    trimmed_path = String.trim(path)

    trimmed_path != "" and not String.starts_with?(trimmed_path, ".") and
      not String.ends_with?(trimmed_path, ".") and not String.contains?(trimmed_path, "..")
  end

  def valid_static_source_path?(_path), do: false

  def enum_value?(value, allowed) when is_atom(value), do: value in allowed

  def enum_value?(value, allowed) when is_binary(value) do
    Enum.any?(allowed, &(Atom.to_string(&1) == value))
  end

  def enum_value?(_value, _allowed), do: false

  def non_empty_atom_or_string?(value) when is_atom(value), do: not is_nil(value)

  def non_empty_atom_or_string?(value) when is_binary(value), do: String.trim(value) != ""

  def non_empty_atom_or_string?(_value), do: false

  def non_empty_string?(value) when is_binary(value), do: String.trim(value) != ""

  def non_empty_string?(_value), do: false

  def valid_arity?(fun, arities) when is_function(fun) do
    Enum.any?(arities, &is_function(fun, &1))
  end

  def valid_arity?(_value, _arities), do: false

  def fetch_map_value(map, key) when is_map(map) and is_atom(key) do
    cond do
      Map.has_key?(map, key) ->
        Map.get(map, key)

      Map.has_key?(map, Atom.to_string(key)) ->
        Map.get(map, Atom.to_string(key))

      true ->
        :__missing__
    end
  end

  def fetch_map_value(map, key) when is_map(map) do
    Map.get(map, key, :__missing__)
  end

  def fetch_map_value(_map, _key), do: :__missing__

  def map_value(map, key) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  def map_value(_map, _key), do: nil

  def has_key?(map, key) when is_map(map) and is_atom(key) do
    Map.has_key?(map, key) or Map.has_key?(map, Atom.to_string(key))
  end

  def has_key?(map, key) when is_map(map), do: Map.has_key?(map, key)
  def has_key?(_map, _key), do: false

  def fetch_key(map, key) when is_map(map) do
    cond do
      Map.has_key?(map, key) ->
        {:ok, Map.fetch!(map, key)}

      is_atom(key) and Map.has_key?(map, Atom.to_string(key)) ->
        {:ok, Map.fetch!(map, Atom.to_string(key))}

      is_binary(key) ->
        atom_key = safe_existing_atom(key)

        if not is_nil(atom_key) and Map.has_key?(map, atom_key) do
          {:ok, Map.fetch!(map, atom_key)}
        else
          :error
        end

      true ->
        :error
    end
  end

  def fetch_key(_map, _key), do: :error

  def safe_existing_atom(value) when is_binary(value) do
    try do
      String.to_existing_atom(value)
    rescue
      ArgumentError -> nil
    end
  end

  def field_id(field) when is_atom(field), do: Atom.to_string(field)
  def field_id(field) when is_binary(field), do: field
  def field_id(field), do: inspect(field)

  def error(code, path, message, attrs \\ []) do
    attrs
    |> Enum.into(%{})
    |> Map.merge(%{
      code: code,
      path: path,
      message: message
    })
  end

  def value_type(value) when is_map(value), do: :map
  def value_type(value) when is_list(value), do: :list
  def value_type(value) when is_binary(value), do: :string
  def value_type(value) when is_atom(value), do: :atom
  def value_type(value) when is_integer(value), do: :integer
  def value_type(value) when is_float(value), do: :float
  def value_type(value) when is_tuple(value), do: :tuple
  def value_type(value) when is_function(value), do: :function
  def value_type(_value), do: :term
end
