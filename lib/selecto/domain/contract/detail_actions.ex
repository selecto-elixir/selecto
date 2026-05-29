defmodule Selecto.Domain.Contract.DetailActions do
  @moduledoc false

  use Selecto.Domain.Constants
  alias Selecto.Domain.Contract.Shared.Core

  def validate(errors, detail_actions, field_index) do
    validate_detail_actions(errors, detail_actions, field_index)
  end

  def validate_detail_actions(errors, detail_actions, field_index) when is_map(detail_actions) do
    Enum.reduce(detail_actions, errors, fn {action_id, action_spec}, acc ->
      acc
      |> validate_detail_action_id(action_id)
      |> validate_detail_action_spec(action_id, action_spec, field_index)
    end)
  end

  def validate_detail_actions(errors, detail_actions, _field_index) do
    [
      Core.error(
        :invalid_section_shape,
        [:detail_actions],
        "domain section :detail_actions must be a map",
        expected: :map,
        actual: Core.value_type(detail_actions)
      )
      | errors
    ]
  end

  def validate_detail_action_id(errors, action_id) do
    if Core.non_empty_atom_or_string?(action_id) do
      errors
    else
      [
        Core.error(
          :invalid_detail_action_id,
          [:detail_actions, action_id],
          "detail action id #{inspect(action_id)} must be a non-empty atom or string",
          expected: "non-empty atom or string",
          actual: Core.value_type(action_id),
          action: action_id
        )
        | errors
      ]
    end
  end

  def validate_detail_action_spec(errors, action_id, action_spec, field_index)
      when is_map(action_spec) do
    errors
    |> validate_detail_action_name(action_id, action_spec)
    |> validate_detail_action_type(action_id, action_spec)
    |> validate_detail_action_payload(action_id, action_spec)
    |> validate_detail_action_required_fields(action_id, action_spec, field_index)
  end

  def validate_detail_action_spec(errors, action_id, action_spec, _field_index) do
    [
      Core.error(
        :invalid_detail_action_spec,
        [:detail_actions, action_id],
        "detail action #{inspect(action_id)} spec must be a map",
        expected: :map,
        actual: Core.value_type(action_spec),
        action: action_id
      )
      | errors
    ]
  end

  def validate_detail_action_name(errors, action_id, action_spec) do
    name = Core.map_value(action_spec, :name)

    if Core.non_empty_string?(name) do
      errors
    else
      [
        Core.error(
          :invalid_detail_action_name,
          [:detail_actions, action_id, :name],
          "detail action #{inspect(action_id)} name must be a non-empty string",
          expected: "non-empty string",
          actual: Core.value_type(name),
          action: action_id,
          name: name
        )
        | errors
      ]
    end
  end

  def validate_detail_action_type(errors, action_id, action_spec) do
    type = Core.map_value(action_spec, :type)

    if Core.enum_value?(type, @detail_action_types) do
      errors
    else
      [
        Core.error(
          :invalid_detail_action_type,
          [:detail_actions, action_id, :type],
          "detail action #{inspect(action_id)} type must be one of #{inspect(@detail_action_types)}",
          expected: @detail_action_types,
          actual: Core.value_type(type),
          action: action_id,
          type: type
        )
        | errors
      ]
    end
  end

  def validate_detail_action_payload(errors, action_id, action_spec) do
    raw_payload = Core.fetch_map_value(action_spec, :payload)
    payload = detail_action_payload(action_spec)
    type = normalized_detail_action_type(Core.map_value(action_spec, :type))

    errors
    |> validate_detail_action_payload_shape(action_id, raw_payload)
    |> validate_detail_action_url_template(action_id, type, payload)
    |> validate_detail_action_live_component_module(action_id, type, payload)
  end

  def validate_detail_action_payload_shape(errors, _action_id, :__missing__), do: errors

  def validate_detail_action_payload_shape(errors, _action_id, payload) when is_map(payload) do
    errors
  end

  def validate_detail_action_payload_shape(errors, action_id, payload) do
    [
      Core.error(
        :invalid_detail_action_payload,
        [:detail_actions, action_id, :payload],
        "detail action #{inspect(action_id)} payload must be a map when provided",
        expected: :map,
        actual: Core.value_type(payload),
        action: action_id
      )
      | errors
    ]
  end

  def validate_detail_action_url_template(errors, action_id, type, payload)
      when type in [:external_link, :iframe_modal] do
    url_template = Core.map_value(payload, :url_template)

    if Core.non_empty_string?(url_template) do
      errors
    else
      [
        Core.error(
          :missing_detail_action_url_template,
          [:detail_actions, action_id, :payload, :url_template],
          "#{type} detail action #{inspect(action_id)} requires payload.url_template",
          expected: "non-empty string",
          actual: Core.value_type(url_template),
          action: action_id,
          type: type
        )
        | errors
      ]
    end
  end

  def validate_detail_action_url_template(errors, _action_id, _type, _payload), do: errors

  def validate_detail_action_live_component_module(errors, action_id, :live_component, payload) do
    module = Core.map_value(payload, :module)

    if is_atom(module) and not is_nil(module) do
      errors
    else
      [
        Core.error(
          :missing_detail_action_module,
          [:detail_actions, action_id, :payload, :module],
          "live_component detail action #{inspect(action_id)} requires payload.module",
          expected: :atom,
          actual: Core.value_type(module),
          action: action_id,
          type: :live_component
        )
        | errors
      ]
    end
  end

  def validate_detail_action_live_component_module(errors, _action_id, _type, _payload),
    do: errors

  def validate_detail_action_required_fields(errors, action_id, action_spec, field_index) do
    case Core.fetch_map_value(action_spec, :required_fields) do
      :__missing__ ->
        errors

      nil ->
        errors

      required_fields when is_list(required_fields) ->
        required_fields
        |> Enum.with_index()
        |> Enum.reduce(errors, fn {field, index}, acc ->
          validate_detail_action_required_field(acc, action_id, field, index, field_index)
        end)

      required_fields ->
        [
          Core.error(
            :invalid_detail_action_required_fields,
            [:detail_actions, action_id, :required_fields],
            "detail action #{inspect(action_id)} required_fields must be a list when provided",
            expected: :list,
            actual: Core.value_type(required_fields),
            action: action_id
          )
          | errors
        ]
    end
  end

  def validate_detail_action_required_field(errors, action_id, field, index, field_index) do
    cond do
      not Core.non_empty_atom_or_string?(field) ->
        [
          Core.error(
            :invalid_detail_action_required_field,
            [:detail_actions, action_id, :required_fields, index],
            "detail action #{inspect(action_id)} required field must be a non-empty atom or string",
            expected: "non-empty atom or string",
            actual: Core.value_type(field),
            action: action_id,
            field: field
          )
          | errors
        ]

      Core.known_field?(field_index, field) ->
        errors

      true ->
        [
          Core.error(
            :detail_action_field_not_found,
            [:detail_actions, action_id, :required_fields, index],
            "detail action #{inspect(action_id)} required field #{inspect(field)} is not defined in source, schemas, or custom columns",
            action: action_id,
            field: field
          )
          | errors
        ]
    end
  end

  def detail_action_payload(action_spec) do
    case Core.map_value(action_spec, :payload) do
      payload when is_map(payload) -> payload
      _payload -> %{}
    end
  end

  def normalized_detail_action_type(type) do
    if Core.enum_value?(type, @detail_action_types) do
      detail_action_type_id(type)
    end
  end

  def detail_action_type_id(type) when is_atom(type), do: type

  def detail_action_type_id(type) when is_binary(type), do: String.to_existing_atom(type)
end
