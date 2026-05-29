defmodule Selecto.Domain.Contract.Writes do
  @moduledoc false

  alias Selecto.Domain.Contract.Shared.Core

  def validate(errors, writes, field_index) do
    validate_writes(errors, writes, field_index)
  end

  def validate_writes(errors, writes, field_index) when is_map(writes) do
    case Core.map_value(writes, :transitions) do
      nil ->
        errors

      transitions when is_map(transitions) ->
        validate_transition_graphs(errors, transitions, field_index)

      transitions ->
        [
          Core.error(
            :invalid_section_shape,
            [:writes, :transitions],
            "domain section writes.transitions must be a map",
            expected: :map,
            actual: Core.value_type(transitions)
          )
          | errors
        ]
    end
  end

  def validate_writes(errors, writes, _field_index) do
    [
      Core.error(
        :invalid_section_shape,
        [:writes],
        "domain section :writes must be a map",
        expected: :map,
        actual: Core.value_type(writes)
      )
      | errors
    ]
  end

  def validate_transition_graphs(errors, transitions, field_index) do
    Enum.reduce(transitions, errors, fn {field, graph}, acc ->
      acc
      |> validate_transition_field(field, field_index)
      |> validate_transition_graph(field, graph)
    end)
  end

  def validate_transition_field(errors, field, field_index) do
    cond do
      not Core.field_ref?(field) ->
        [
          Core.error(
            :invalid_transition_field,
            [:writes, :transitions, field],
            "write transition fields must be atoms or strings",
            expected: "atom or string",
            actual: Core.value_type(field),
            field: field
          )
          | errors
        ]

      Core.known_field?(field_index, field) ->
        errors

      true ->
        [
          Core.error(
            :transition_field_not_found,
            [:writes, :transitions, field],
            "write transition field #{inspect(field)} is not defined in source, schemas, or custom columns",
            field: field
          )
          | errors
        ]
    end
  end

  def validate_transition_graph(errors, field, graph) when is_map(graph) do
    Enum.reduce(graph, errors, fn {from_state, target_states}, acc ->
      state_path = [:writes, :transitions, field, from_state]

      acc
      |> validate_transition_state(from_state, state_path)
      |> validate_transition_targets(target_states, state_path)
    end)
  end

  def validate_transition_graph(errors, field, graph) do
    [
      Core.error(
        :invalid_section_shape,
        [:writes, :transitions, field],
        "write transition graph for #{inspect(field)} must be a map",
        expected: :map,
        actual: Core.value_type(graph),
        field: field
      )
      | errors
    ]
  end

  def validate_transition_targets(errors, target_states, path) when is_list(target_states) do
    target_states
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {target_state, index}, acc ->
      validate_transition_state(acc, target_state, path ++ [index])
    end)
  end

  def validate_transition_targets(errors, target_states, path) do
    [
      Core.error(
        :invalid_transition_targets,
        path,
        "write transition targets must be a list of atoms or strings",
        expected: :list,
        actual: Core.value_type(target_states)
      )
      | errors
    ]
  end

  def validate_transition_state(errors, state, _path) when is_atom(state) or is_binary(state) do
    errors
  end

  def validate_transition_state(errors, state, path) do
    [
      Core.error(
        :invalid_transition_state,
        path,
        "write transition states must be atoms or strings",
        expected: "atom or string",
        actual: Core.value_type(state),
        state: state
      )
      | errors
    ]
  end

end
