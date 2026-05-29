defmodule Selecto.Domain.Contract.Query.Filters do
  @moduledoc false

  use Selecto.Domain.Constants

  alias Selecto.Domain.Contract.Shared.Core
  alias Selecto.Domain.Contract.Shared.FieldReference, as: FieldReference

  @logical_filter_ops [:and, :or]
  @unary_filter_ops [:not]

  def validate_filters(errors, query, field_index) do
    filters = Core.map_value(query, :filters) || %{}
    required_filters = Core.map_value(query, :required_filters) || []

    errors
    |> validate_filter_registry(filters, field_index)
    |> validate_required_filters(required_filters, field_index)
  end

  def validate_filter_registry(errors, filters, field_index) when is_map(filters) do
    Enum.reduce(filters, errors, fn {filter_id, filter_config}, acc ->
      acc
      |> validate_filter_id(filter_id)
      |> validate_filter_config(filter_id, filter_config, field_index)
    end)
  end

  def validate_filter_registry(errors, filters, _field_index) do
    [
      Core.error(
        :invalid_section_shape,
        [:filters],
        "domain section :filters must be a map",
        expected: :map,
        actual: Core.value_type(filters)
      )
      | errors
    ]
  end

  def validate_filter_id(errors, filter_id) do
    if Core.non_empty_atom_or_string?(filter_id) do
      errors
    else
      [
        Core.error(
          :invalid_filter_id,
          [:filters, filter_id],
          "filter id #{inspect(filter_id)} must be a non-empty atom or string",
          expected: "non-empty atom or string",
          actual: Core.value_type(filter_id),
          filter: filter_id
        )
        | errors
      ]
    end
  end

  def validate_filter_config(errors, filter_id, filter_config, field_index)
      when is_map(filter_config) do
    errors
    |> validate_filter_config_field(filter_id, filter_config, field_index)
    |> validate_filter_config_type(filter_id, filter_config)
  end

  def validate_filter_config(errors, filter_id, filter_config, _field_index) do
    [
      Core.error(
        :invalid_filter_config,
        [:filters, filter_id],
        "filter #{inspect(filter_id)} config must be a map",
        expected: :map,
        actual: Core.value_type(filter_config),
        filter: filter_id
      )
      | errors
    ]
  end

  def validate_filter_config_field(errors, filter_id, filter_config, field_index) do
    if Core.has_key?(filter_config, :field) do
      field = Core.map_value(filter_config, :field)

      if Core.valid_static_source_path?(field) do
        FieldReference.validate_field_reference(
          errors,
          field,
          [:filters, filter_id, :field],
          field_index
        )
      else
        [
          Core.error(
            :invalid_filter_field,
            [:filters, filter_id, :field],
            "filter #{inspect(filter_id)} field must be a non-empty atom or dotted string path",
            expected: "non-empty atom or dotted string path",
            actual: Core.value_type(field),
            filter: filter_id,
            field: field
          )
          | errors
        ]
      end
    else
      errors
    end
  end

  def validate_filter_config_type(errors, filter_id, filter_config) do
    if Core.has_key?(filter_config, :type) do
      type = Core.map_value(filter_config, :type)

      if Core.non_empty_atom_or_string?(type) do
        errors
      else
        [
          Core.error(
            :invalid_filter_type,
            [:filters, filter_id, :type],
            "filter #{inspect(filter_id)} type must be a non-empty atom or string",
            expected: "non-empty atom or string",
            actual: Core.value_type(type),
            filter: filter_id,
            type: type
          )
          | errors
        ]
      end
    else
      errors
    end
  end

  def validate_required_filters(errors, required_filters, field_index)
      when is_list(required_filters) do
    required_filters
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {filter, index}, acc ->
      validate_filter_expression(acc, filter, [:required_filters, index], field_index)
    end)
  end

  def validate_required_filters(errors, required_filters, _field_index) do
    [
      Core.error(
        :invalid_section_shape,
        [:required_filters],
        "domain section :required_filters must be a list",
        expected: :list,
        actual: Core.value_type(required_filters)
      )
      | errors
    ]
  end

  def validate_filter_expression(errors, {op, filters}, path, field_index)
      when op in @logical_filter_ops and is_list(filters) do
    filters
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {filter, index}, acc ->
      validate_filter_expression(acc, filter, path ++ [index], field_index)
    end)
  end

  def validate_filter_expression(errors, {op, filter}, path, field_index)
      when op in @unary_filter_ops do
    validate_filter_expression(errors, filter, path ++ [op], field_index)
  end

  def validate_filter_expression(errors, {op, field, _value}, path, field_index)
      when op in @field_filter_ops do
    FieldReference.validate_field_reference(errors, field, path ++ [:field], field_index)
  end

  def validate_filter_expression(errors, {op, field, _left, _right}, path, field_index)
      when op in @field_filter_ops do
    FieldReference.validate_field_reference(errors, field, path ++ [:field], field_index)
  end

  def validate_filter_expression(errors, {field, _value}, path, field_index) do
    FieldReference.validate_field_reference(errors, field, path ++ [:field], field_index)
  end

  def validate_filter_expression(errors, {field, _op, _value}, path, field_index) do
    FieldReference.validate_field_reference(errors, field, path ++ [:field], field_index)
  end

  def validate_filter_expression(errors, filter, path, field_index) when is_map(filter) do
    case Core.map_value(filter, :field) do
      nil ->
        errors

      field ->
        FieldReference.validate_field_reference(errors, field, path ++ [:field], field_index)
    end
  end

  def validate_filter_expression(errors, _filter, _path, _field_index), do: errors
end
