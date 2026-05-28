defmodule Selecto.Domain.Contract.Actions do
  @moduledoc false

  alias Selecto.Domain.Contract.Shared.Core

  def validate(errors, actions, capabilities, writes, field_index) do
    validate_actions(errors, actions, capabilities, writes, field_index)
  end

  def validate_actions(errors, actions, capabilities, writes, field_index)
       when is_map(actions) do
    Enum.reduce(actions, errors, fn {action_id, action}, acc ->
      path = [:actions, action_id]

      acc
      |> validate_action_id(action_id, path)
      |> validate_action(action_id, action, path, capabilities, writes, field_index)
    end)
  end

  def validate_actions(errors, actions, _capabilities, _writes, _field_index) do
    [
      Core.error(
        :invalid_section_shape,
        [:actions],
        "domain section :actions must be a map",
        expected: :map,
        actual: Core.value_type(actions)
      )
      | errors
    ]
  end

  def validate_action_id(errors, action_id, _path)
       when is_atom(action_id) or is_binary(action_id) do
    errors
  end

  def validate_action_id(errors, action_id, path) do
    [
      Core.error(
        :invalid_action_id,
        path,
        "action ids must be atoms or strings",
        expected: "atom or string",
        actual: Core.value_type(action_id),
        action: action_id
      )
      | errors
    ]
  end

  def validate_action(errors, action_id, action, path, capabilities, writes, field_index)
       when is_map(action) do
    errors
    |> validate_action_capability(action_id, action, path, capabilities)
    |> validate_action_transition(action_id, action, path, writes, field_index)
    |> validate_action_execution(action_id, action, path)
  end

  def validate_action(errors, action_id, action, path, _capabilities, _writes, _field_index) do
    [
      Core.error(
        :invalid_section_shape,
        path,
        "action #{inspect(action_id)} must be a map",
        expected: :map,
        actual: Core.value_type(action),
        action: action_id
      )
      | errors
    ]
  end

  def validate_action_capability(errors, action_id, action, path, capabilities) do
    case Core.map_value(action, :capability) do
      nil ->
        errors

      capability when is_atom(capability) or is_binary(capability) ->
        if is_map(capabilities) and Core.fetch_key(capabilities, capability) != :error do
          errors
        else
          [
            Core.error(
              :action_capability_not_found,
              path ++ [:capability],
              "action #{inspect(action_id)} references missing capability #{inspect(capability)}",
              action: action_id,
              capability: capability
            )
            | errors
          ]
        end

      capability ->
        [
          Core.error(
            :invalid_action_capability,
            path ++ [:capability],
            "action #{inspect(action_id)} capability must be an atom or string",
            expected: "atom or string",
            actual: Core.value_type(capability),
            action: action_id,
            capability: capability
          )
          | errors
        ]
    end
  end

  def validate_action_transition(errors, action_id, action, path, writes, field_index) do
    transition = Core.map_value(action, :transition)
    action_type = Core.map_value(action, :type)

    cond do
      is_nil(transition) and transition_action_type?(action_type) ->
        [
          Core.error(
            :action_missing_transition,
            path ++ [:transition],
            "transition action #{inspect(action_id)} must declare a direct transition map",
            action: action_id
          )
          | errors
        ]

      is_nil(transition) ->
        errors

      is_map(transition) ->
        errors
        |> validate_action_transition_required_keys(action_id, transition, path ++ [:transition])
        |> validate_action_transition_field(action_id, transition, path, field_index)
        |> validate_action_transition_states(action_id, transition, path)
        |> validate_action_transition_edge(action_id, transition, path, writes, field_index)

      true ->
        [
          Core.error(
            :invalid_action_transition,
            path ++ [:transition],
            "action #{inspect(action_id)} transition must be a map with :field, :from, and :to",
            expected: :map,
            actual: Core.value_type(transition),
            action: action_id
          )
          | errors
        ]
    end
  end

  def validate_action_transition_required_keys(errors, action_id, transition, path) do
    missing_keys = Enum.reject([:field, :from, :to], &Core.has_key?(transition, &1))

    case missing_keys do
      [] ->
        errors

      _ ->
        [
          Core.error(
            :action_transition_missing_required_keys,
            path,
            "action #{inspect(action_id)} transition is missing required keys #{inspect(missing_keys)}",
            action: action_id,
            keys: missing_keys
          )
          | errors
        ]
    end
  end

  def validate_action_transition_field(errors, action_id, transition, path, field_index) do
    case Core.map_value(transition, :field) do
      nil ->
        errors

      field when is_atom(field) or is_binary(field) ->
        if Core.known_field?(field_index, field) do
          errors
        else
          [
            Core.error(
              :action_transition_field_not_found,
              path ++ [:transition, :field],
              "action #{inspect(action_id)} transition field #{inspect(field)} is not defined in source, schemas, or custom columns",
              action: action_id,
              field: field
            )
            | errors
          ]
        end

      field ->
        [
          Core.error(
            :invalid_action_transition_field,
            path ++ [:transition, :field],
            "action #{inspect(action_id)} transition field must be an atom or string",
            expected: "atom or string",
            actual: Core.value_type(field),
            action: action_id,
            field: field
          )
          | errors
        ]
    end
  end

  def validate_action_transition_states(errors, action_id, transition, path) do
    errors
    |> validate_action_transition_state(
      action_id,
      Core.map_value(transition, :from),
      path ++ [:transition, :from],
      :from
    )
    |> validate_action_transition_state(
      action_id,
      Core.map_value(transition, :to),
      path ++ [:transition, :to],
      :to
    )
  end

  def validate_action_transition_state(errors, _action_id, nil, _path, _state_key), do: errors

  def validate_action_transition_state(errors, _action_id, state, _path, _state_key)
       when is_atom(state) or is_binary(state),
       do: errors

  def validate_action_transition_state(errors, action_id, state, path, state_key) do
    [
      Core.error(
        :invalid_action_transition_state,
        path,
        "action #{inspect(action_id)} transition #{inspect(state_key)} state must be an atom or string",
        expected: "atom or string",
        actual: Core.value_type(state),
        action: action_id,
        state: state,
        state_key: state_key
      )
      | errors
    ]
  end

  def validate_action_transition_edge(errors, action_id, transition, path, writes, field_index) do
    field = Core.map_value(transition, :field)
    from_state = Core.map_value(transition, :from)
    to_state = Core.map_value(transition, :to)

    if Core.field_ref?(field) and Core.known_field?(field_index, field) and state_ref?(from_state) and
         state_ref?(to_state) do
      transitions = Core.map_value(writes, :transitions)

      if is_map(transitions) and transition_edge?(transitions, field, from_state, to_state) do
        errors
      else
        [
          Core.error(
            :action_transition_edge_not_found,
            path ++ [:transition],
            "action #{inspect(action_id)} transition edge #{inspect(field)} #{inspect(from_state)} -> #{inspect(to_state)} is not declared in writes.transitions",
            action: action_id,
            field: field,
            from: from_state,
            to: to_state
          )
          | errors
        ]
      end
    else
      errors
    end
  end

  def validate_action_execution(errors, action_id, action, path) do
    transition = Core.map_value(action, :transition)
    action_type = Core.map_value(action, :type)

    if is_nil(transition) and not transition_action_type?(action_type) do
      errors
    else
      validate_direct_action_execution(errors, action_id, action, path)
    end
  end

  def validate_direct_action_execution(errors, action_id, action, path) do
    case Core.map_value(action, :execution) do
      nil ->
        errors

      execution when is_map(execution) ->
        errors
        |> validate_action_execution_kind(action_id, execution, path)
        |> validate_action_execution_operation(action_id, execution, path)
        |> validate_action_execution_set(action_id, action, execution, path)

      execution ->
        [
          Core.error(
            :invalid_action_execution,
            path ++ [:execution],
            "action #{inspect(action_id)} execution must be a map",
            expected: :map,
            actual: Core.value_type(execution),
            action: action_id
          )
          | errors
        ]
    end
  end

  def validate_action_execution_kind(errors, action_id, execution, path) do
    case Core.map_value(execution, :kind) do
      nil ->
        errors

      kind when kind in [:updato, "updato"] ->
        errors

      kind ->
        [
          Core.error(
            :invalid_action_execution_kind,
            path ++ [:execution, :kind],
            "action #{inspect(action_id)} direct transition execution currently supports only :updato",
            expected: :updato,
            actual: kind,
            action: action_id
          )
          | errors
        ]
    end
  end

  def validate_action_execution_operation(errors, action_id, execution, path) do
    case Core.map_value(execution, :operation) do
      nil ->
        errors

      operation when operation in [:update, "update"] ->
        errors

      operation ->
        [
          Core.error(
            :invalid_action_execution_operation,
            path ++ [:execution, :operation],
            "action #{inspect(action_id)} direct transition execution currently supports only :update",
            expected: :update,
            actual: operation,
            action: action_id
          )
          | errors
        ]
    end
  end

  def validate_action_execution_set(errors, action_id, action, execution, path) do
    transition = Core.map_value(action, :transition)

    case Core.map_value(execution, :set) do
      nil ->
        errors

      set when is_map(set) and is_map(transition) ->
        field = Core.map_value(transition, :field)
        to_state = Core.map_value(transition, :to)

        if Core.field_ref?(field) and state_ref?(to_state) and
             execution_sets_transition?(set, field, to_state) do
          errors
        else
          [
            Core.error(
              :action_execution_set_mismatch,
              path ++ [:execution, :set],
              "action #{inspect(action_id)} execution set must set the transition field to the target state",
              action: action_id,
              field: field,
              to: to_state
            )
            | errors
          ]
        end

      set when is_map(set) ->
        errors

      set ->
        [
          Core.error(
            :invalid_action_execution_set,
            path ++ [:execution, :set],
            "action #{inspect(action_id)} execution set must be a map",
            expected: :map,
            actual: Core.value_type(set),
            action: action_id
          )
          | errors
        ]
    end
  end

  def transition_action_type?(type), do: type in [:transition, "transition"]

  def transition_edge?(transitions, field, from_state, to_state) do
    with {:ok, graph} when is_map(graph) <- Core.fetch_key(transitions, field),
         {:ok, target_states} when is_list(target_states) <- Core.fetch_key(graph, from_state) do
      Enum.any?(target_states, &(Core.field_id(&1) == Core.field_id(to_state)))
    else
      _ -> false
    end
  end

  def execution_sets_transition?(set, field, to_state) do
    case Core.fetch_key(set, field) do
      {:ok, value} -> Core.field_id(value) == Core.field_id(to_state)
      :error -> false
    end
  end

  def state_ref?(state), do: is_atom(state) or is_binary(state)

end
