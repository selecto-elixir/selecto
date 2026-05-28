defmodule Selecto.Domain.Contract.Capabilities do
  @moduledoc false

  use Selecto.Domain.Constants
  alias Selecto.Domain.Contract.Shared.Core

  def validate(errors, capabilities) do
    validate_capabilities(errors, capabilities)
  end

  def validate_query_references(errors, query, detail_actions, capabilities) do
    validate_query_capability_references(errors, query, detail_actions, capabilities)
  end

  def validate_capabilities(errors, capabilities) when is_map(capabilities) do
    Enum.reduce(capabilities, errors, fn {capability_id, capability}, acc ->
      path = [:capabilities, capability_id]

      acc
      |> validate_capability_id(capability_id, path)
      |> validate_capability(capability_id, capability, path)
    end)
  end

  def validate_capabilities(errors, capabilities) do
    [
      Core.error(
        :invalid_section_shape,
        [:capabilities],
        "domain section :capabilities must be a map",
        expected: :map,
        actual: Core.value_type(capabilities)
      )
      | errors
    ]
  end

  def validate_capability_id(errors, capability_id, _path)
       when is_atom(capability_id) or is_binary(capability_id) do
    errors
  end

  def validate_capability_id(errors, capability_id, path) do
    [
      Core.error(
        :invalid_capability_id,
        path,
        "capability ids must be atoms or strings",
        expected: "atom or string",
        actual: Core.value_type(capability_id),
        capability: capability_id
      )
      | errors
    ]
  end

  def validate_capability(errors, capability_id, capability, path) when is_map(capability) do
    case Core.map_value(capability, :operations) do
      nil ->
        [
          Core.error(
            :capability_missing_operations,
            path ++ [:operations],
            "capability #{inspect(capability_id)} must declare a non-empty operations list",
            capability: capability_id
          )
          | errors
        ]

      [] ->
        [
          Core.error(
            :capability_empty_operations,
            path ++ [:operations],
            "capability #{inspect(capability_id)} must declare a non-empty operations list",
            capability: capability_id
          )
          | errors
        ]

      operations when is_list(operations) ->
        validate_capability_operations(errors, capability_id, operations, path ++ [:operations])

      operations ->
        [
          Core.error(
            :invalid_capability_operations,
            path ++ [:operations],
            "capability #{inspect(capability_id)} operations must be a non-empty list",
            expected: :list,
            actual: Core.value_type(operations),
            capability: capability_id
          )
          | errors
        ]
    end
  end

  def validate_capability(errors, capability_id, capability, path) do
    [
      Core.error(
        :invalid_section_shape,
        path,
        "capability #{inspect(capability_id)} must be a map",
        expected: :map,
        actual: Core.value_type(capability),
        capability: capability_id
      )
      | errors
    ]
  end

  def validate_capability_operations(errors, capability_id, operations, path) do
    operations
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {operation, index}, acc ->
      if is_atom(operation) or is_binary(operation) do
        acc
      else
        [
          Core.error(
            :invalid_capability_operation,
            path ++ [index],
            "capability #{inspect(capability_id)} operations must be atoms or strings",
            expected: "atom or string",
            actual: Core.value_type(operation),
            capability: capability_id,
            operation: operation
          )
          | acc
        ]
      end
    end)
  end

  def validate_query_capability_references(errors, query, detail_actions, capabilities) do
    errors
    |> validate_filter_capability_references(Core.map_value(query, :filters), capabilities)
    |> validate_function_capability_references(Core.map_value(query, :functions), capabilities)
    |> validate_query_member_capability_references(Core.map_value(query, :query_members), capabilities)
    |> validate_published_view_capability_references(
      Core.map_value(query, :published_views),
      capabilities
    )
    |> validate_detail_action_capability_references(detail_actions, capabilities)
  end

  def validate_filter_capability_references(errors, filters, capabilities)
       when is_map(filters) do
    Enum.reduce(filters, errors, fn
      {filter_id, filter_config}, acc when is_map(filter_config) ->
        validate_capability_reference(
          acc,
          Core.map_value(filter_config, :capability),
          [:filters, filter_id, :capability],
          capabilities,
          :filter_capability_not_found,
          :invalid_filter_capability,
          "filter #{inspect(filter_id)}",
          filter: filter_id
        )

      _entry, acc ->
        acc
    end)
  end

  def validate_filter_capability_references(errors, _filters, _capabilities), do: errors

  def validate_function_capability_references(errors, functions, capabilities)
       when is_map(functions) do
    Enum.reduce(functions, errors, fn
      {function_id, function_spec}, acc when is_map(function_spec) ->
        validate_capability_reference(
          acc,
          Core.map_value(function_spec, :capability),
          [:functions, function_id, :capability],
          capabilities,
          :function_capability_not_found,
          :invalid_function_capability,
          "function #{inspect(function_id)}",
          function: function_id
        )

      _entry, acc ->
        acc
    end)
  end

  def validate_function_capability_references(errors, _functions, _capabilities), do: errors

  def validate_query_member_capability_references(errors, query_members, capabilities)
       when is_map(query_members) do
    Enum.reduce(@query_member_groups, errors, fn group_key, acc ->
      case Core.fetch_map_value(query_members, group_key) do
        members when is_map(members) ->
          Enum.reduce(members, acc, fn
            {member_id, member_spec}, member_acc when is_map(member_spec) ->
              validate_capability_reference(
                member_acc,
                Core.map_value(member_spec, :capability),
                [:query_members, group_key, member_id, :capability],
                capabilities,
                :query_member_capability_not_found,
                :invalid_query_member_capability,
                "query member #{inspect(member_id)}",
                group: group_key,
                member: member_id
              )

            _entry, member_acc ->
              member_acc
          end)

        _members ->
          acc
      end
    end)
  end

  def validate_query_member_capability_references(errors, _query_members, _capabilities),
    do: errors

  def validate_published_view_capability_references(errors, published_views, capabilities)
       when is_map(published_views) do
    Enum.reduce(published_views, errors, fn
      {view_id, view_spec}, acc when is_map(view_spec) ->
        validate_capability_reference(
          acc,
          Core.map_value(view_spec, :capability),
          [:published_views, view_id, :capability],
          capabilities,
          :published_view_capability_not_found,
          :invalid_published_view_capability,
          "published view #{inspect(view_id)}",
          view: view_id
        )

      _entry, acc ->
        acc
    end)
  end

  def validate_published_view_capability_references(errors, _published_views, _capabilities),
    do: errors

  def validate_detail_action_capability_references(errors, detail_actions, capabilities)
       when is_map(detail_actions) do
    Enum.reduce(detail_actions, errors, fn
      {action_id, action_spec}, acc when is_map(action_spec) ->
        validate_capability_reference(
          acc,
          Core.map_value(action_spec, :capability),
          [:detail_actions, action_id, :capability],
          capabilities,
          :detail_action_capability_not_found,
          :invalid_detail_action_capability,
          "detail action #{inspect(action_id)}",
          action: action_id
        )

      _entry, acc ->
        acc
    end)
  end

  def validate_detail_action_capability_references(errors, _detail_actions, _capabilities),
    do: errors

  def validate_capability_reference(
         errors,
         nil,
         _path,
         _capabilities,
         _missing,
         _invalid,
         _subject,
         _attrs
       ) do
    errors
  end

  def validate_capability_reference(
         errors,
         capability,
         path,
         capabilities,
         missing_code,
         _invalid_code,
         subject,
         attrs
       )
       when is_atom(capability) or is_binary(capability) do
    if is_map(capabilities) and Core.fetch_key(capabilities, capability) != :error do
      errors
    else
      [
        Core.error(
          missing_code,
          path,
          "#{subject} references missing capability #{inspect(capability)}",
          Keyword.put(attrs, :capability, capability)
        )
        | errors
      ]
    end
  end

  def validate_capability_reference(
         errors,
         capability,
         path,
         _capabilities,
         _missing_code,
         invalid_code,
         subject,
         attrs
       ) do
    [
      Core.error(
        invalid_code,
        path,
        "#{subject} capability must be an atom or string",
        Keyword.merge(attrs,
          expected: "atom or string",
          actual: Core.value_type(capability),
          capability: capability
        )
      )
      | errors
    ]
  end

end
