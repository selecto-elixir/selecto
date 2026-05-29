defmodule Selecto.Domain.Contract.ChoiceSources do
  @moduledoc false

  use Selecto.Domain.Constants
  alias Selecto.Domain.Contract.Shared.Core
  alias Selecto.Domain.Contract.Shared.IdValue, as: IdValue
  alias Selecto.Domain.Contract.Shared.StaticFilters, as: StaticFilters

  @choice_source_path_keys [:source_path, :value_source, :caption_source, :description_source]
  @choice_source_presentation_controls [:select, :autocomplete, :table_picker]
  @choice_source_presentation_modes [:static, :searchable, :async, :inline]
  @choice_source_presentation_cardinalities [:one, :many]
  @choice_source_constraint_policy_keys [
    :source_relationship,
    :choice_source,
    :domain_of_interest
  ]
  @choice_source_constraint_policy_modes [:best_effort, :fail_closed]
  @order_directions [:asc, :desc]

  def validate(errors, choice_sources, source_relationships, capabilities) do
    validate_choice_sources(errors, choice_sources, source_relationships, capabilities)
  end

  def validate_choice_sources(errors, choice_sources, source_relationships, capabilities)
      when is_map(choice_sources) do
    Enum.reduce(choice_sources, errors, fn {choice_source_id, choice_source}, acc ->
      path = [:choice_sources, choice_source_id]

      acc
      |> validate_choice_source_id(choice_source_id, path)
      |> validate_choice_source(
        choice_source_id,
        choice_source,
        path,
        source_relationships,
        capabilities
      )
    end)
  end

  def validate_choice_sources(errors, choice_sources, _source_relationships, _capabilities) do
    [
      Core.error(
        :invalid_section_shape,
        [:choice_sources],
        "domain section :choice_sources must be a map",
        expected: :map,
        actual: Core.value_type(choice_sources)
      )
      | errors
    ]
  end

  def validate_choice_source_id(errors, choice_source_id, _path)
      when is_atom(choice_source_id) or is_binary(choice_source_id) do
    errors
  end

  def validate_choice_source_id(errors, choice_source_id, path) do
    [
      Core.error(
        :invalid_choice_source_id,
        path,
        "choice source ids must be atoms or strings",
        expected: "atom or string",
        actual: Core.value_type(choice_source_id),
        choice_source: choice_source_id
      )
      | errors
    ]
  end

  def validate_choice_source(
        errors,
        choice_source_id,
        choice_source,
        path,
        source_relationships,
        capabilities
      )
      when is_map(choice_source) do
    errors
    |> validate_choice_source_required_keys(choice_source_id, choice_source, path)
    |> IdValue.validate_id_value(
      Core.map_value(choice_source, :domain),
      path ++ [:domain],
      :invalid_choice_source_domain,
      "choice source #{inspect(choice_source_id)} domain must be an atom or string",
      choice_source: choice_source_id,
      domain: Core.map_value(choice_source, :domain)
    )
    |> IdValue.validate_id_value(
      Core.map_value(choice_source, :value_field),
      path ++ [:value_field],
      :invalid_choice_source_value_field,
      "choice source #{inspect(choice_source_id)} value_field must be an atom or string",
      choice_source: choice_source_id,
      value_field: Core.map_value(choice_source, :value_field)
    )
    |> IdValue.validate_id_value(
      Core.map_value(choice_source, :label_field),
      path ++ [:label_field],
      :invalid_choice_source_label_field,
      "choice source #{inspect(choice_source_id)} label_field must be an atom or string",
      choice_source: choice_source_id,
      label_field: Core.map_value(choice_source, :label_field)
    )
    |> validate_choice_source_paths(choice_source_id, choice_source, path)
    |> validate_choice_source_filters(choice_source_id, choice_source, path)
    |> validate_choice_source_order_by(choice_source_id, choice_source, path)
    |> validate_choice_source_presentation(choice_source_id, choice_source, path)
    |> validate_choice_source_constraint_policy(choice_source_id, choice_source, path)
    |> validate_choice_source_relationship(
      choice_source_id,
      choice_source,
      path,
      source_relationships
    )
    |> validate_choice_source_capability(choice_source_id, choice_source, path, capabilities)
  end

  def validate_choice_source(
        errors,
        choice_source_id,
        choice_source,
        path,
        _source_relationships,
        _capabilities
      ) do
    [
      Core.error(
        :invalid_section_shape,
        path,
        "choice source #{inspect(choice_source_id)} must be a map",
        expected: :map,
        actual: Core.value_type(choice_source),
        choice_source: choice_source_id
      )
      | errors
    ]
  end

  def validate_choice_source_required_keys(errors, choice_source_id, choice_source, path) do
    missing_keys =
      Enum.reject([:domain, :value_field, :label_field], &Core.has_key?(choice_source, &1))

    case missing_keys do
      [] ->
        errors

      _ ->
        [
          Core.error(
            :choice_source_missing_required_keys,
            path,
            "choice source #{inspect(choice_source_id)} is missing required keys #{inspect(missing_keys)}",
            choice_source: choice_source_id,
            keys: missing_keys
          )
          | errors
        ]
    end
  end

  def validate_choice_source_paths(errors, choice_source_id, choice_source, path) do
    Enum.reduce(@choice_source_path_keys, errors, fn key, acc ->
      value = Core.map_value(choice_source, key)

      if is_nil(value) or Core.valid_choice_source_path?(value) do
        acc
      else
        [
          Core.error(
            invalid_choice_source_path_code(key),
            path ++ [key],
            "choice source #{inspect(choice_source_id)} #{key} must be a non-empty atom or dotted string path",
            expected: "non-empty atom or dotted string path",
            actual: Core.value_type(value),
            choice_source: choice_source_id,
            attribute: key,
            value: value
          )
          | acc
        ]
      end
    end)
  end

  def invalid_choice_source_path_code(:source_path), do: :invalid_choice_source_source_path
  def invalid_choice_source_path_code(:value_source), do: :invalid_choice_source_value_source
  def invalid_choice_source_path_code(:caption_source), do: :invalid_choice_source_caption_source

  def invalid_choice_source_path_code(:description_source),
    do: :invalid_choice_source_description_source

  def validate_choice_source_filters(errors, choice_source_id, choice_source, path) do
    case Core.map_value(choice_source, :filters) do
      nil ->
        errors

      filters when is_list(filters) ->
        filters
        |> Enum.with_index()
        |> Enum.reduce(errors, fn {filter, index}, acc ->
          validate_choice_source_filter_expression(
            acc,
            choice_source_id,
            filter,
            path ++ [:filters, index]
          )
        end)

      filters ->
        [
          Core.error(
            :invalid_choice_source_filters,
            path ++ [:filters],
            "choice source #{inspect(choice_source_id)} filters must be a list",
            expected: :list,
            actual: Core.value_type(filters),
            choice_source: choice_source_id
          )
          | errors
        ]
    end
  end

  def validate_choice_source_filter_expression(errors, choice_source_id, filter, path)
      when is_tuple(filter) or is_list(filter) do
    StaticFilters.validate_static_filter_expression(
      errors,
      StaticFilters.static_filter_owner(:choice_source, choice_source_id),
      filter,
      path
    )
  end

  def validate_choice_source_filter_expression(errors, choice_source_id, filter, path) do
    StaticFilters.invalid_static_filter_expression(
      errors,
      StaticFilters.static_filter_owner(:choice_source, choice_source_id),
      filter,
      path
    )
  end

  def validate_choice_source_order_by(errors, choice_source_id, choice_source, path) do
    case Core.map_value(choice_source, :order_by) do
      nil ->
        errors

      order_by when is_list(order_by) ->
        order_by
        |> Enum.with_index()
        |> Enum.reduce(errors, fn {order_entry, index}, acc ->
          validate_choice_source_order_entry(
            acc,
            choice_source_id,
            order_entry,
            path ++ [:order_by, index]
          )
        end)

      order_by ->
        [
          Core.error(
            :invalid_choice_source_order_by,
            path ++ [:order_by],
            "choice source #{inspect(choice_source_id)} order_by must be a list",
            expected: :list,
            actual: Core.value_type(order_by),
            choice_source: choice_source_id
          )
          | errors
        ]
    end
  end

  def validate_choice_source_order_entry(errors, choice_source_id, order_entry, path)
      when is_atom(order_entry) do
    if Core.valid_choice_source_path?(order_entry) do
      errors
    else
      invalid_choice_source_order_entry_error(errors, choice_source_id, order_entry, path)
    end
  end

  def validate_choice_source_order_entry(errors, choice_source_id, order_entry, path)
      when is_binary(order_entry) do
    if Core.valid_choice_source_path?(order_entry) do
      errors
    else
      invalid_choice_source_order_entry_error(errors, choice_source_id, order_entry, path)
    end
  end

  def validate_choice_source_order_entry(errors, choice_source_id, {order_path, direction}, path) do
    errors
    |> validate_choice_source_order_path(choice_source_id, order_path, path)
    |> validate_choice_source_order_direction(choice_source_id, direction, path)
  end

  def validate_choice_source_order_entry(errors, choice_source_id, [order_path, direction], path) do
    errors
    |> validate_choice_source_order_path(choice_source_id, order_path, path)
    |> validate_choice_source_order_direction(choice_source_id, direction, path)
  end

  def validate_choice_source_order_entry(errors, choice_source_id, order_entry, path) do
    invalid_choice_source_order_entry_error(errors, choice_source_id, order_entry, path)
  end

  def validate_choice_source_order_path(errors, choice_source_id, order_path, path) do
    if Core.valid_choice_source_path?(order_path) do
      errors
    else
      invalid_choice_source_order_entry_error(errors, choice_source_id, order_path, path)
    end
  end

  def invalid_choice_source_order_entry_error(errors, choice_source_id, order_entry, path) do
    [
      Core.error(
        :invalid_choice_source_order_by_entry,
        path,
        "choice source #{inspect(choice_source_id)} order_by entries must be paths or {path, direction}",
        expected: "path or {path, direction}",
        actual: Core.value_type(order_entry),
        choice_source: choice_source_id,
        order_by: order_entry
      )
      | errors
    ]
  end

  def validate_choice_source_order_direction(errors, _choice_source_id, direction, _path)
      when direction in @order_directions or direction in ["asc", "desc"] do
    errors
  end

  def validate_choice_source_order_direction(errors, choice_source_id, direction, path) do
    [
      Core.error(
        :invalid_choice_source_order_by_direction,
        path,
        "choice source #{inspect(choice_source_id)} order_by direction must be :asc or :desc",
        expected: @order_directions,
        actual: direction,
        choice_source: choice_source_id,
        direction: direction
      )
      | errors
    ]
  end

  def validate_choice_source_presentation(errors, choice_source_id, choice_source, path) do
    case Core.map_value(choice_source, :presentation) do
      nil ->
        errors

      presentation when is_map(presentation) ->
        errors
        |> validate_choice_source_presentation_enum(
          choice_source_id,
          presentation,
          path,
          :control,
          @choice_source_presentation_controls,
          :invalid_choice_source_presentation_control
        )
        |> validate_choice_source_presentation_enum(
          choice_source_id,
          presentation,
          path,
          :mode,
          @choice_source_presentation_modes,
          :invalid_choice_source_presentation_mode
        )
        |> validate_choice_source_presentation_enum(
          choice_source_id,
          presentation,
          path,
          :cardinality,
          @choice_source_presentation_cardinalities,
          :invalid_choice_source_presentation_cardinality
        )

      presentation ->
        [
          Core.error(
            :invalid_choice_source_presentation,
            path ++ [:presentation],
            "choice source #{inspect(choice_source_id)} presentation must be a map",
            expected: :map,
            actual: Core.value_type(presentation),
            choice_source: choice_source_id
          )
          | errors
        ]
    end
  end

  def validate_choice_source_presentation_enum(
        errors,
        choice_source_id,
        presentation,
        path,
        key,
        allowed,
        code
      ) do
    case Core.map_value(presentation, key) do
      nil ->
        errors

      value when is_atom(value) or is_binary(value) ->
        if Core.enum_value?(value, allowed) do
          errors
        else
          [
            Core.error(
              code,
              path ++ [:presentation, key],
              "choice source #{inspect(choice_source_id)} presentation #{key} has an unknown value",
              expected: allowed,
              actual: value,
              choice_source: choice_source_id,
              attribute: key,
              value: value
            )
            | errors
          ]
        end

      value ->
        [
          Core.error(
            code,
            path ++ [:presentation, key],
            "choice source #{inspect(choice_source_id)} presentation #{key} must be an atom or string",
            expected: allowed,
            actual: Core.value_type(value),
            choice_source: choice_source_id,
            attribute: key,
            value: value
          )
          | errors
        ]
    end
  end

  def validate_choice_source_constraint_policy(errors, choice_source_id, choice_source, path) do
    case Core.map_value(choice_source, :constraint_policy) do
      nil ->
        errors

      policy when is_map(policy) ->
        policy
        |> Enum.reduce(errors, fn {key, value}, acc ->
          validate_choice_source_constraint_policy_entry(
            acc,
            choice_source_id,
            key,
            value,
            path ++ [:constraint_policy, key]
          )
        end)

      policy ->
        [
          Core.error(
            :invalid_choice_source_constraint_policy,
            path ++ [:constraint_policy],
            "choice source #{inspect(choice_source_id)} constraint_policy must be a map",
            expected: :map,
            actual: Core.value_type(policy),
            choice_source: choice_source_id
          )
          | errors
        ]
    end
  end

  def validate_choice_source_constraint_policy_entry(errors, choice_source_id, key, value, path) do
    cond do
      not Core.enum_value?(key, @choice_source_constraint_policy_keys) ->
        [
          Core.error(
            :invalid_choice_source_constraint_policy_key,
            path,
            "choice source #{inspect(choice_source_id)} constraint_policy key is not supported",
            expected: @choice_source_constraint_policy_keys,
            actual: key,
            choice_source: choice_source_id,
            key: key
          )
          | errors
        ]

      Core.enum_value?(value, @choice_source_constraint_policy_modes) ->
        errors

      true ->
        [
          Core.error(
            :invalid_choice_source_constraint_policy_mode,
            path,
            "choice source #{inspect(choice_source_id)} constraint_policy mode is not supported",
            expected: @choice_source_constraint_policy_modes,
            actual: value,
            choice_source: choice_source_id,
            key: key,
            value: value
          )
          | errors
        ]
    end
  end

  def validate_choice_source_relationship(
        errors,
        choice_source_id,
        choice_source,
        path,
        source_relationships
      ) do
    case Core.map_value(choice_source, :source_relationship) do
      nil ->
        errors

      source_relationship when is_atom(source_relationship) or is_binary(source_relationship) ->
        if is_map(source_relationships) and
             Core.fetch_key(source_relationships, source_relationship) != :error do
          errors
        else
          [
            Core.error(
              :choice_source_relationship_not_found,
              path ++ [:source_relationship],
              "choice source #{inspect(choice_source_id)} references missing source relationship #{inspect(source_relationship)}",
              choice_source: choice_source_id,
              source_relationship: source_relationship
            )
            | errors
          ]
        end

      source_relationship ->
        [
          Core.error(
            :invalid_choice_source_relationship,
            path ++ [:source_relationship],
            "choice source #{inspect(choice_source_id)} source_relationship must be an atom or string",
            expected: "atom or string",
            actual: Core.value_type(source_relationship),
            choice_source: choice_source_id,
            source_relationship: source_relationship
          )
          | errors
        ]
    end
  end

  def validate_choice_source_capability(
        errors,
        choice_source_id,
        choice_source,
        path,
        capabilities
      ) do
    case Core.map_value(choice_source, :capability) do
      nil ->
        errors

      capability when is_atom(capability) or is_binary(capability) ->
        if is_map(capabilities) and Core.fetch_key(capabilities, capability) != :error do
          errors
        else
          [
            Core.error(
              :choice_source_capability_not_found,
              path ++ [:capability],
              "choice source #{inspect(choice_source_id)} references missing capability #{inspect(capability)}",
              choice_source: choice_source_id,
              capability: capability
            )
            | errors
          ]
        end

      capability ->
        [
          Core.error(
            :invalid_choice_source_capability,
            path ++ [:capability],
            "choice source #{inspect(choice_source_id)} capability must be an atom or string",
            expected: "atom or string",
            actual: Core.value_type(capability),
            choice_source: choice_source_id,
            capability: capability
          )
          | errors
        ]
    end
  end
end
