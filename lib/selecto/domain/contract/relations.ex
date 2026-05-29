defmodule Selecto.Domain.Contract.Relations do
  @moduledoc false

  use Selecto.Domain.Constants
  alias Selecto.Domain.Contract.Shared.Core

  @relation_required_keys [:source_table, :primary_key, :fields, :columns]

  def validate(errors, source, schemas) do
    errors
    |> validate_relation(:source, source, [:source])
    |> validate_schemas(schemas)
  end

  def validate_relation(errors, relation_id, relation, path) when is_map(relation) do
    errors
    |> validate_required_relation_keys(relation_id, relation, path)
    |> validate_relation_source_table(relation_id, relation, path)
    |> validate_relation_fields(relation_id, relation, path)
    |> validate_relation_columns(relation_id, relation, path)
    |> validate_relation_primary_key(relation_id, relation, path)
    |> validate_relation_field_columns(relation_id, relation, path)
  end

  def validate_relation(errors, relation_id, relation, path) do
    [
      Core.error(
        :invalid_section_shape,
        path,
        "domain relation #{inspect(relation_id)} must be a map",
        expected: :map,
        actual: Core.value_type(relation)
      )
      | errors
    ]
  end

  def validate_required_relation_keys(errors, relation_id, relation, path) do
    missing_keys = Enum.reject(@relation_required_keys, &Core.has_key?(relation, &1))

    case missing_keys do
      [] ->
        errors

      _ ->
        [
          Core.error(
            :missing_required_keys,
            path,
            "domain relation #{inspect(relation_id)} is missing required keys #{inspect(missing_keys)}",
            relation: relation_id,
            keys: missing_keys
          )
          | errors
        ]
    end
  end

  def validate_relation_source_table(errors, relation_id, relation, path) do
    case Core.map_value(relation, :source_table) do
      nil ->
        errors

      source_table when is_binary(source_table) or is_atom(source_table) ->
        errors

      source_table ->
        [
          Core.error(
            :invalid_source_table,
            path ++ [:source_table],
            "domain relation #{inspect(relation_id)} has an invalid source_table",
            relation: relation_id,
            expected: "atom or string",
            actual: Core.value_type(source_table)
          )
          | errors
        ]
    end
  end

  def validate_relation_fields(errors, relation_id, relation, path) do
    case Core.map_value(relation, :fields) do
      nil ->
        errors

      fields when is_list(fields) ->
        errors

      fields ->
        [
          Core.error(
            :invalid_fields,
            path ++ [:fields],
            "domain relation #{inspect(relation_id)} fields must be a list",
            relation: relation_id,
            expected: :list,
            actual: Core.value_type(fields)
          )
          | errors
        ]
    end
  end

  def validate_relation_columns(errors, relation_id, relation, path) do
    case Core.map_value(relation, :columns) do
      nil ->
        errors

      columns when is_map(columns) ->
        errors

      columns ->
        [
          Core.error(
            :invalid_columns,
            path ++ [:columns],
            "domain relation #{inspect(relation_id)} columns must be a map",
            relation: relation_id,
            expected: :map,
            actual: Core.value_type(columns)
          )
          | errors
        ]
    end
  end

  def validate_relation_primary_key(errors, relation_id, relation, path) do
    fields = Core.map_value(relation, :fields)
    primary_key = Core.map_value(relation, :primary_key)

    cond do
      is_nil(primary_key) or not is_list(fields) ->
        errors

      Core.field_ref?(primary_key) and Core.field_in_list?(fields, primary_key) ->
        errors

      Core.field_ref?(primary_key) ->
        [
          Core.error(
            :primary_key_not_found,
            path ++ [:primary_key],
            "domain relation #{inspect(relation_id)} primary_key #{inspect(primary_key)} is not listed in fields",
            relation: relation_id,
            field: primary_key
          )
          | errors
        ]

      true ->
        [
          Core.error(
            :invalid_primary_key,
            path ++ [:primary_key],
            "domain relation #{inspect(relation_id)} primary_key must be an atom or string",
            relation: relation_id,
            expected: "atom or string",
            actual: Core.value_type(primary_key)
          )
          | errors
        ]
    end
  end

  def validate_relation_field_columns(errors, relation_id, relation, path) do
    fields = Core.map_value(relation, :fields)
    columns = Core.map_value(relation, :columns)

    if is_list(fields) and is_map(columns) do
      fields
      |> Enum.reject(&Core.has_key?(columns, &1))
      |> Enum.reduce(errors, fn field, acc ->
        [
          Core.error(
            field_missing_column_code(relation_id),
            path ++ [:columns, field],
            "domain relation #{inspect(relation_id)} field #{inspect(field)} is missing a column definition",
            relation: relation_id,
            field: field
          )
          | acc
        ]
      end)
    else
      errors
    end
  end

  def field_missing_column_code(:source), do: :source_field_missing_column
  def field_missing_column_code(_relation_id), do: :schema_field_missing_column

  def validate_schemas(errors, schemas) when is_map(schemas) do
    Enum.reduce(schemas, errors, fn {schema_id, schema}, acc ->
      validate_relation(acc, schema_id, schema, [:schemas, schema_id])
    end)
  end

  def validate_schemas(errors, schemas) do
    [
      Core.error(
        :invalid_section_shape,
        [:schemas],
        "domain section :schemas must be a map",
        expected: :map,
        actual: Core.value_type(schemas)
      )
      | errors
    ]
  end

end
