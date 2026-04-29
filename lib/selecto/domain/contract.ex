defmodule Selecto.Domain.Contract do
  @moduledoc """
  First-wave canonical domain contract checks.

  This module validates the normalized shape produced by `Selecto.Domain`.
  It is intentionally small: it covers the required core sections and the first
  strict subschemas for `source`, `schemas`, `joins`, and filter references.
  Existing runtime configuration does not call this module unless a caller opts
  into normalized validation.
  """

  @required_sections [:source, :schemas]
  @relation_required_keys [:source_table, :primary_key, :fields, :columns]
  @logical_filter_ops [:and, :or]
  @unary_filter_ops [:not]
  @field_filter_ops [
    :eq,
    :neq,
    :not_eq,
    :gt,
    :gte,
    :lt,
    :lte,
    :like,
    :ilike,
    :contains,
    :starts_with,
    :ends_with,
    :between,
    :in,
    :not_in,
    :text_search,
    :match_against,
    :array_contains,
    :array_contained,
    :array_overlap,
    :array_eq
  ]

  @type error :: %{
          required(:code) => atom(),
          required(:message) => String.t(),
          required(:path) => [term()]
        }

  @doc """
  Returns `:ok` when a normalized domain satisfies the first-wave contract.
  """
  @spec validate(map()) :: :ok | {:error, [error()]}
  def validate(normalized_domain) when is_map(normalized_domain) do
    case errors(normalized_domain) do
      [] -> :ok
      errors -> {:error, errors}
    end
  end

  @doc """
  Returns structured contract errors for a normalized domain.
  """
  @spec errors(map()) :: [error()]
  def errors(%{authored_domain: authored_domain} = normalized_domain) do
    source = Map.get(normalized_domain, :source)
    schemas = Map.get(normalized_domain, :schemas, %{})
    joins = Map.get(normalized_domain, :joins, %{})
    query = Map.get(normalized_domain, :query, %{})
    projection = Map.get(normalized_domain, :projection, %{})
    writes = Map.get(normalized_domain, :writes, %{})
    capabilities = Map.get(normalized_domain, :capabilities, %{})
    actions = Map.get(normalized_domain, :actions, %{})
    field_index = field_index(source, schemas, projection)

    []
    |> validate_required_sections(authored_domain)
    |> validate_relation(:source, source, [:source])
    |> validate_schemas(schemas)
    |> validate_joins(joins, source, schemas)
    |> validate_filters(query, field_index)
    |> validate_writes(writes, field_index)
    |> validate_capabilities(capabilities)
    |> validate_actions(actions, capabilities, writes, field_index)
    |> Enum.reverse()
  end

  def errors(_normalized_domain) do
    [
      error(
        :invalid_normalized_domain,
        [],
        "expected a normalized Selecto domain from Selecto.Domain.normalize/1"
      )
    ]
  end

  defp validate_required_sections(errors, authored_domain) do
    Enum.reduce(@required_sections, errors, fn section, acc ->
      if has_key?(authored_domain, section) do
        acc
      else
        [
          error(
            :missing_required_section,
            [section],
            "required domain section #{inspect(section)} is missing",
            section: section
          )
          | acc
        ]
      end
    end)
  end

  defp validate_relation(errors, relation_id, relation, path) when is_map(relation) do
    errors
    |> validate_required_relation_keys(relation_id, relation, path)
    |> validate_relation_source_table(relation_id, relation, path)
    |> validate_relation_fields(relation_id, relation, path)
    |> validate_relation_columns(relation_id, relation, path)
    |> validate_relation_primary_key(relation_id, relation, path)
    |> validate_relation_field_columns(relation_id, relation, path)
  end

  defp validate_relation(errors, relation_id, relation, path) do
    [
      error(
        :invalid_section_shape,
        path,
        "domain relation #{inspect(relation_id)} must be a map",
        expected: :map,
        actual: value_type(relation)
      )
      | errors
    ]
  end

  defp validate_required_relation_keys(errors, relation_id, relation, path) do
    missing_keys = Enum.reject(@relation_required_keys, &has_key?(relation, &1))

    case missing_keys do
      [] ->
        errors

      _ ->
        [
          error(
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

  defp validate_relation_source_table(errors, relation_id, relation, path) do
    case map_value(relation, :source_table) do
      nil ->
        errors

      source_table when is_binary(source_table) or is_atom(source_table) ->
        errors

      source_table ->
        [
          error(
            :invalid_source_table,
            path ++ [:source_table],
            "domain relation #{inspect(relation_id)} has an invalid source_table",
            relation: relation_id,
            expected: "atom or string",
            actual: value_type(source_table)
          )
          | errors
        ]
    end
  end

  defp validate_relation_fields(errors, relation_id, relation, path) do
    case map_value(relation, :fields) do
      nil ->
        errors

      fields when is_list(fields) ->
        errors

      fields ->
        [
          error(
            :invalid_fields,
            path ++ [:fields],
            "domain relation #{inspect(relation_id)} fields must be a list",
            relation: relation_id,
            expected: :list,
            actual: value_type(fields)
          )
          | errors
        ]
    end
  end

  defp validate_relation_columns(errors, relation_id, relation, path) do
    case map_value(relation, :columns) do
      nil ->
        errors

      columns when is_map(columns) ->
        errors

      columns ->
        [
          error(
            :invalid_columns,
            path ++ [:columns],
            "domain relation #{inspect(relation_id)} columns must be a map",
            relation: relation_id,
            expected: :map,
            actual: value_type(columns)
          )
          | errors
        ]
    end
  end

  defp validate_relation_primary_key(errors, relation_id, relation, path) do
    fields = map_value(relation, :fields)
    primary_key = map_value(relation, :primary_key)

    cond do
      is_nil(primary_key) or not is_list(fields) ->
        errors

      field_ref?(primary_key) and field_in_list?(fields, primary_key) ->
        errors

      field_ref?(primary_key) ->
        [
          error(
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
          error(
            :invalid_primary_key,
            path ++ [:primary_key],
            "domain relation #{inspect(relation_id)} primary_key must be an atom or string",
            relation: relation_id,
            expected: "atom or string",
            actual: value_type(primary_key)
          )
          | errors
        ]
    end
  end

  defp validate_relation_field_columns(errors, relation_id, relation, path) do
    fields = map_value(relation, :fields)
    columns = map_value(relation, :columns)

    if is_list(fields) and is_map(columns) do
      fields
      |> Enum.reject(&has_key?(columns, &1))
      |> Enum.reduce(errors, fn field, acc ->
        [
          error(
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

  defp field_missing_column_code(:source), do: :source_field_missing_column
  defp field_missing_column_code(_relation_id), do: :schema_field_missing_column

  defp validate_schemas(errors, schemas) when is_map(schemas) do
    Enum.reduce(schemas, errors, fn {schema_id, schema}, acc ->
      validate_relation(acc, schema_id, schema, [:schemas, schema_id])
    end)
  end

  defp validate_schemas(errors, schemas) do
    [
      error(
        :invalid_section_shape,
        [:schemas],
        "domain section :schemas must be a map",
        expected: :map,
        actual: value_type(schemas)
      )
      | errors
    ]
  end

  defp validate_joins(errors, joins, source, schemas) when is_map(joins) do
    validate_join_tree(errors, joins, source, schemas, [:joins], :source)
  end

  defp validate_joins(errors, joins, _source, _schemas) do
    [
      error(
        :invalid_section_shape,
        [:joins],
        "domain section :joins must be a map",
        expected: :map,
        actual: value_type(joins)
      )
      | errors
    ]
  end

  defp validate_join_tree(errors, joins, parent_relation, schemas, path, parent_id)
       when is_map(joins) do
    Enum.reduce(joins, errors, fn {join_id, join_config}, acc ->
      join_path = path ++ [join_id]

      acc
      |> validate_join_config_shape(join_config, join_path)
      |> validate_join_association(
        join_id,
        join_config,
        parent_relation,
        schemas,
        join_path,
        parent_id
      )
    end)
  end

  defp validate_join_config_shape(errors, join_config, path) when is_map(join_config) do
    case map_value(join_config, :joins) do
      nil ->
        errors

      nested_joins when is_map(nested_joins) ->
        errors

      nested_joins ->
        [
          error(
            :invalid_section_shape,
            path ++ [:joins],
            "nested joins must be a map",
            expected: :map,
            actual: value_type(nested_joins)
          )
          | errors
        ]
    end
  end

  defp validate_join_config_shape(errors, join_config, path) do
    [
      error(
        :invalid_section_shape,
        path,
        "join configuration must be a map",
        expected: :map,
        actual: value_type(join_config)
      )
      | errors
    ]
  end

  defp validate_join_association(
         errors,
         _join_id,
         join_config,
         _parent_relation,
         _schemas,
         _path,
         _parent_id
       )
       when not is_map(join_config),
       do: errors

  defp validate_join_association(
         errors,
         join_id,
         join_config,
         parent_relation,
         schemas,
         path,
         parent_id
       ) do
    associations = relation_associations(parent_relation)

    case fetch_key(associations, join_id) do
      {:ok, association} ->
        validate_join_target(errors, join_id, join_config, association, schemas, path)

      :error ->
        [
          error(
            :join_missing_association,
            path,
            "join #{inspect(join_id)} is not declared as an association on #{inspect(parent_id)}",
            parent: parent_id,
            join: join_id
          )
          | errors
        ]
    end
  end

  defp validate_join_target(errors, join_id, join_config, association, schemas, path) do
    queryable = map_value(association, :queryable)

    cond do
      is_nil(queryable) ->
        [
          error(
            :join_association_missing_queryable,
            path,
            "join #{inspect(join_id)} association is missing :queryable",
            join: join_id
          )
          | errors
        ]

      fetch_key(schemas, queryable) == :error and queryable != :source and queryable != "source" ->
        [
          error(
            :join_target_schema_not_found,
            path,
            "join #{inspect(join_id)} targets missing schema #{inspect(queryable)}",
            join: join_id,
            schema: queryable
          )
          | errors
        ]

      true ->
        case map_value(join_config, :joins) do
          nested_joins when is_map(nested_joins) ->
            target_relation =
              if queryable == :source or queryable == "source" do
                nil
              else
                {:ok, relation} = fetch_key(schemas, queryable)
                relation
              end

            validate_join_tree(
              errors,
              nested_joins,
              target_relation,
              schemas,
              path ++ [:joins],
              queryable
            )

          _ ->
            errors
        end
    end
  end

  defp relation_associations(relation) when is_map(relation) do
    case map_value(relation, :associations) do
      associations when is_map(associations) -> associations
      _ -> %{}
    end
  end

  defp relation_associations(_relation), do: %{}

  defp validate_filters(errors, query, field_index) do
    filters = map_value(query, :filters) || %{}
    required_filters = map_value(query, :required_filters) || []

    errors
    |> validate_filter_registry(filters, field_index)
    |> validate_required_filters(required_filters, field_index)
  end

  defp validate_filter_registry(errors, filters, field_index) when is_map(filters) do
    Enum.reduce(filters, errors, fn {filter_id, filter_config}, acc ->
      case map_value(filter_config, :field) do
        nil ->
          acc

        field ->
          validate_field_reference(acc, field, [:filters, filter_id, :field], field_index)
      end
    end)
  end

  defp validate_filter_registry(errors, filters, _field_index) do
    [
      error(
        :invalid_section_shape,
        [:filters],
        "domain section :filters must be a map",
        expected: :map,
        actual: value_type(filters)
      )
      | errors
    ]
  end

  defp validate_required_filters(errors, required_filters, field_index)
       when is_list(required_filters) do
    required_filters
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {filter, index}, acc ->
      validate_filter_expression(acc, filter, [:required_filters, index], field_index)
    end)
  end

  defp validate_required_filters(errors, required_filters, _field_index) do
    [
      error(
        :invalid_section_shape,
        [:required_filters],
        "domain section :required_filters must be a list",
        expected: :list,
        actual: value_type(required_filters)
      )
      | errors
    ]
  end

  defp validate_filter_expression(errors, {op, filters}, path, field_index)
       when op in @logical_filter_ops and is_list(filters) do
    filters
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {filter, index}, acc ->
      validate_filter_expression(acc, filter, path ++ [index], field_index)
    end)
  end

  defp validate_filter_expression(errors, {op, filter}, path, field_index)
       when op in @unary_filter_ops do
    validate_filter_expression(errors, filter, path ++ [op], field_index)
  end

  defp validate_filter_expression(errors, {op, field, _value}, path, field_index)
       when op in @field_filter_ops do
    validate_field_reference(errors, field, path ++ [:field], field_index)
  end

  defp validate_filter_expression(errors, {op, field, _left, _right}, path, field_index)
       when op in @field_filter_ops do
    validate_field_reference(errors, field, path ++ [:field], field_index)
  end

  defp validate_filter_expression(errors, {field, _value}, path, field_index) do
    validate_field_reference(errors, field, path ++ [:field], field_index)
  end

  defp validate_filter_expression(errors, {field, _op, _value}, path, field_index) do
    validate_field_reference(errors, field, path ++ [:field], field_index)
  end

  defp validate_filter_expression(errors, filter, path, field_index) when is_map(filter) do
    case map_value(filter, :field) do
      nil -> errors
      field -> validate_field_reference(errors, field, path ++ [:field], field_index)
    end
  end

  defp validate_filter_expression(errors, _filter, _path, _field_index), do: errors

  defp validate_field_reference(errors, field, path, field_index) do
    if known_field?(field_index, field) do
      errors
    else
      [
        error(
          :filter_field_not_found,
          path,
          "filter field #{inspect(field)} is not defined in source, schemas, or custom columns",
          field: field
        )
        | errors
      ]
    end
  end

  defp validate_writes(errors, writes, field_index) when is_map(writes) do
    case map_value(writes, :transitions) do
      nil ->
        errors

      transitions when is_map(transitions) ->
        validate_transition_graphs(errors, transitions, field_index)

      transitions ->
        [
          error(
            :invalid_section_shape,
            [:writes, :transitions],
            "domain section writes.transitions must be a map",
            expected: :map,
            actual: value_type(transitions)
          )
          | errors
        ]
    end
  end

  defp validate_writes(errors, writes, _field_index) do
    [
      error(
        :invalid_section_shape,
        [:writes],
        "domain section :writes must be a map",
        expected: :map,
        actual: value_type(writes)
      )
      | errors
    ]
  end

  defp validate_transition_graphs(errors, transitions, field_index) do
    Enum.reduce(transitions, errors, fn {field, graph}, acc ->
      acc
      |> validate_transition_field(field, field_index)
      |> validate_transition_graph(field, graph)
    end)
  end

  defp validate_transition_field(errors, field, field_index) do
    cond do
      not field_ref?(field) ->
        [
          error(
            :invalid_transition_field,
            [:writes, :transitions, field],
            "write transition fields must be atoms or strings",
            expected: "atom or string",
            actual: value_type(field),
            field: field
          )
          | errors
        ]

      known_field?(field_index, field) ->
        errors

      true ->
        [
          error(
            :transition_field_not_found,
            [:writes, :transitions, field],
            "write transition field #{inspect(field)} is not defined in source, schemas, or custom columns",
            field: field
          )
          | errors
        ]
    end
  end

  defp validate_transition_graph(errors, field, graph) when is_map(graph) do
    Enum.reduce(graph, errors, fn {from_state, target_states}, acc ->
      state_path = [:writes, :transitions, field, from_state]

      acc
      |> validate_transition_state(from_state, state_path)
      |> validate_transition_targets(target_states, state_path)
    end)
  end

  defp validate_transition_graph(errors, field, graph) do
    [
      error(
        :invalid_section_shape,
        [:writes, :transitions, field],
        "write transition graph for #{inspect(field)} must be a map",
        expected: :map,
        actual: value_type(graph),
        field: field
      )
      | errors
    ]
  end

  defp validate_transition_targets(errors, target_states, path) when is_list(target_states) do
    target_states
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {target_state, index}, acc ->
      validate_transition_state(acc, target_state, path ++ [index])
    end)
  end

  defp validate_transition_targets(errors, target_states, path) do
    [
      error(
        :invalid_transition_targets,
        path,
        "write transition targets must be a list of atoms or strings",
        expected: :list,
        actual: value_type(target_states)
      )
      | errors
    ]
  end

  defp validate_transition_state(errors, state, _path) when is_atom(state) or is_binary(state) do
    errors
  end

  defp validate_transition_state(errors, state, path) do
    [
      error(
        :invalid_transition_state,
        path,
        "write transition states must be atoms or strings",
        expected: "atom or string",
        actual: value_type(state),
        state: state
      )
      | errors
    ]
  end

  defp validate_capabilities(errors, capabilities) when is_map(capabilities) do
    Enum.reduce(capabilities, errors, fn {capability_id, capability}, acc ->
      path = [:capabilities, capability_id]

      acc
      |> validate_capability_id(capability_id, path)
      |> validate_capability(capability_id, capability, path)
    end)
  end

  defp validate_capabilities(errors, capabilities) do
    [
      error(
        :invalid_section_shape,
        [:capabilities],
        "domain section :capabilities must be a map",
        expected: :map,
        actual: value_type(capabilities)
      )
      | errors
    ]
  end

  defp validate_capability_id(errors, capability_id, _path)
       when is_atom(capability_id) or is_binary(capability_id) do
    errors
  end

  defp validate_capability_id(errors, capability_id, path) do
    [
      error(
        :invalid_capability_id,
        path,
        "capability ids must be atoms or strings",
        expected: "atom or string",
        actual: value_type(capability_id),
        capability: capability_id
      )
      | errors
    ]
  end

  defp validate_capability(errors, capability_id, capability, path) when is_map(capability) do
    case map_value(capability, :operations) do
      nil ->
        [
          error(
            :capability_missing_operations,
            path ++ [:operations],
            "capability #{inspect(capability_id)} must declare a non-empty operations list",
            capability: capability_id
          )
          | errors
        ]

      [] ->
        [
          error(
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
          error(
            :invalid_capability_operations,
            path ++ [:operations],
            "capability #{inspect(capability_id)} operations must be a non-empty list",
            expected: :list,
            actual: value_type(operations),
            capability: capability_id
          )
          | errors
        ]
    end
  end

  defp validate_capability(errors, capability_id, capability, path) do
    [
      error(
        :invalid_section_shape,
        path,
        "capability #{inspect(capability_id)} must be a map",
        expected: :map,
        actual: value_type(capability),
        capability: capability_id
      )
      | errors
    ]
  end

  defp validate_capability_operations(errors, capability_id, operations, path) do
    operations
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {operation, index}, acc ->
      if is_atom(operation) or is_binary(operation) do
        acc
      else
        [
          error(
            :invalid_capability_operation,
            path ++ [index],
            "capability #{inspect(capability_id)} operations must be atoms or strings",
            expected: "atom or string",
            actual: value_type(operation),
            capability: capability_id,
            operation: operation
          )
          | acc
        ]
      end
    end)
  end

  defp validate_actions(errors, actions, capabilities, writes, field_index)
       when is_map(actions) do
    Enum.reduce(actions, errors, fn {action_id, action}, acc ->
      path = [:actions, action_id]

      acc
      |> validate_action_id(action_id, path)
      |> validate_action(action_id, action, path, capabilities, writes, field_index)
    end)
  end

  defp validate_actions(errors, actions, _capabilities, _writes, _field_index) do
    [
      error(
        :invalid_section_shape,
        [:actions],
        "domain section :actions must be a map",
        expected: :map,
        actual: value_type(actions)
      )
      | errors
    ]
  end

  defp validate_action_id(errors, action_id, _path)
       when is_atom(action_id) or is_binary(action_id) do
    errors
  end

  defp validate_action_id(errors, action_id, path) do
    [
      error(
        :invalid_action_id,
        path,
        "action ids must be atoms or strings",
        expected: "atom or string",
        actual: value_type(action_id),
        action: action_id
      )
      | errors
    ]
  end

  defp validate_action(errors, action_id, action, path, capabilities, writes, field_index)
       when is_map(action) do
    errors
    |> validate_action_capability(action_id, action, path, capabilities)
    |> validate_action_transition(action_id, action, path, writes, field_index)
    |> validate_action_execution(action_id, action, path)
  end

  defp validate_action(errors, action_id, action, path, _capabilities, _writes, _field_index) do
    [
      error(
        :invalid_section_shape,
        path,
        "action #{inspect(action_id)} must be a map",
        expected: :map,
        actual: value_type(action),
        action: action_id
      )
      | errors
    ]
  end

  defp validate_action_capability(errors, action_id, action, path, capabilities) do
    case map_value(action, :capability) do
      nil ->
        errors

      capability when is_atom(capability) or is_binary(capability) ->
        if is_map(capabilities) and fetch_key(capabilities, capability) != :error do
          errors
        else
          [
            error(
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
          error(
            :invalid_action_capability,
            path ++ [:capability],
            "action #{inspect(action_id)} capability must be an atom or string",
            expected: "atom or string",
            actual: value_type(capability),
            action: action_id,
            capability: capability
          )
          | errors
        ]
    end
  end

  defp validate_action_transition(errors, action_id, action, path, writes, field_index) do
    transition = map_value(action, :transition)
    action_type = map_value(action, :type)

    cond do
      is_nil(transition) and transition_action_type?(action_type) ->
        [
          error(
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
          error(
            :invalid_action_transition,
            path ++ [:transition],
            "action #{inspect(action_id)} transition must be a map with :field, :from, and :to",
            expected: :map,
            actual: value_type(transition),
            action: action_id
          )
          | errors
        ]
    end
  end

  defp validate_action_transition_required_keys(errors, action_id, transition, path) do
    missing_keys = Enum.reject([:field, :from, :to], &has_key?(transition, &1))

    case missing_keys do
      [] ->
        errors

      _ ->
        [
          error(
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

  defp validate_action_transition_field(errors, action_id, transition, path, field_index) do
    case map_value(transition, :field) do
      nil ->
        errors

      field when is_atom(field) or is_binary(field) ->
        if known_field?(field_index, field) do
          errors
        else
          [
            error(
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
          error(
            :invalid_action_transition_field,
            path ++ [:transition, :field],
            "action #{inspect(action_id)} transition field must be an atom or string",
            expected: "atom or string",
            actual: value_type(field),
            action: action_id,
            field: field
          )
          | errors
        ]
    end
  end

  defp validate_action_transition_states(errors, action_id, transition, path) do
    errors
    |> validate_action_transition_state(
      action_id,
      map_value(transition, :from),
      path ++ [:transition, :from],
      :from
    )
    |> validate_action_transition_state(
      action_id,
      map_value(transition, :to),
      path ++ [:transition, :to],
      :to
    )
  end

  defp validate_action_transition_state(errors, _action_id, nil, _path, _state_key), do: errors

  defp validate_action_transition_state(errors, _action_id, state, _path, _state_key)
       when is_atom(state) or is_binary(state),
       do: errors

  defp validate_action_transition_state(errors, action_id, state, path, state_key) do
    [
      error(
        :invalid_action_transition_state,
        path,
        "action #{inspect(action_id)} transition #{inspect(state_key)} state must be an atom or string",
        expected: "atom or string",
        actual: value_type(state),
        action: action_id,
        state: state,
        state_key: state_key
      )
      | errors
    ]
  end

  defp validate_action_transition_edge(errors, action_id, transition, path, writes, field_index) do
    field = map_value(transition, :field)
    from_state = map_value(transition, :from)
    to_state = map_value(transition, :to)

    if field_ref?(field) and known_field?(field_index, field) and state_ref?(from_state) and
         state_ref?(to_state) do
      transitions = map_value(writes, :transitions)

      if is_map(transitions) and transition_edge?(transitions, field, from_state, to_state) do
        errors
      else
        [
          error(
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

  defp validate_action_execution(errors, action_id, action, path) do
    transition = map_value(action, :transition)
    action_type = map_value(action, :type)

    if is_nil(transition) and not transition_action_type?(action_type) do
      errors
    else
      validate_direct_action_execution(errors, action_id, action, path)
    end
  end

  defp validate_direct_action_execution(errors, action_id, action, path) do
    case map_value(action, :execution) do
      nil ->
        errors

      execution when is_map(execution) ->
        errors
        |> validate_action_execution_kind(action_id, execution, path)
        |> validate_action_execution_operation(action_id, execution, path)
        |> validate_action_execution_set(action_id, action, execution, path)

      execution ->
        [
          error(
            :invalid_action_execution,
            path ++ [:execution],
            "action #{inspect(action_id)} execution must be a map",
            expected: :map,
            actual: value_type(execution),
            action: action_id
          )
          | errors
        ]
    end
  end

  defp validate_action_execution_kind(errors, action_id, execution, path) do
    case map_value(execution, :kind) do
      nil ->
        errors

      kind when kind in [:updato, "updato"] ->
        errors

      kind ->
        [
          error(
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

  defp validate_action_execution_operation(errors, action_id, execution, path) do
    case map_value(execution, :operation) do
      nil ->
        errors

      operation when operation in [:update, "update"] ->
        errors

      operation ->
        [
          error(
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

  defp validate_action_execution_set(errors, action_id, action, execution, path) do
    transition = map_value(action, :transition)

    case map_value(execution, :set) do
      nil ->
        errors

      set when is_map(set) and is_map(transition) ->
        field = map_value(transition, :field)
        to_state = map_value(transition, :to)

        if field_ref?(field) and state_ref?(to_state) and
             execution_sets_transition?(set, field, to_state) do
          errors
        else
          [
            error(
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
          error(
            :invalid_action_execution_set,
            path ++ [:execution, :set],
            "action #{inspect(action_id)} execution set must be a map",
            expected: :map,
            actual: value_type(set),
            action: action_id
          )
          | errors
        ]
    end
  end

  defp transition_action_type?(type), do: type in [:transition, "transition"]

  defp transition_edge?(transitions, field, from_state, to_state) do
    with {:ok, graph} when is_map(graph) <- fetch_key(transitions, field),
         {:ok, target_states} when is_list(target_states) <- fetch_key(graph, from_state) do
      Enum.any?(target_states, &(field_id(&1) == field_id(to_state)))
    else
      _ -> false
    end
  end

  defp execution_sets_transition?(set, field, to_state) do
    case fetch_key(set, field) do
      {:ok, value} -> field_id(value) == field_id(to_state)
      :error -> false
    end
  end

  defp state_ref?(state), do: is_atom(state) or is_binary(state)

  defp field_index(source, schemas, projection) do
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

  defp relation_fields(relation) when is_map(relation) do
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

  defp relation_fields(_relation), do: []

  defp known_field?(_field_index, field) when not (is_atom(field) or is_binary(field)), do: true
  defp known_field?(field_index, field), do: MapSet.member?(field_index, field_id(field))

  defp field_in_list?(fields, field) do
    field_id = field_id(field)
    Enum.any?(fields, &(field_id(&1) == field_id))
  end

  defp field_ref?(field), do: is_atom(field) or is_binary(field)

  defp map_value(map, key) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp map_value(_map, _key), do: nil

  defp has_key?(map, key) when is_map(map) and is_atom(key) do
    Map.has_key?(map, key) or Map.has_key?(map, Atom.to_string(key))
  end

  defp has_key?(map, key) when is_map(map), do: Map.has_key?(map, key)
  defp has_key?(_map, _key), do: false

  defp fetch_key(map, key) when is_map(map) do
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

  defp fetch_key(_map, _key), do: :error

  defp safe_existing_atom(value) when is_binary(value) do
    try do
      String.to_existing_atom(value)
    rescue
      ArgumentError -> nil
    end
  end

  defp field_id(field) when is_atom(field), do: Atom.to_string(field)
  defp field_id(field) when is_binary(field), do: field
  defp field_id(field), do: inspect(field)

  defp error(code, path, message, attrs \\ []) do
    attrs
    |> Enum.into(%{})
    |> Map.merge(%{
      code: code,
      path: path,
      message: message
    })
  end

  defp value_type(value) when is_map(value), do: :map
  defp value_type(value) when is_list(value), do: :list
  defp value_type(value) when is_binary(value), do: :string
  defp value_type(value) when is_atom(value), do: :atom
  defp value_type(value) when is_integer(value), do: :integer
  defp value_type(value) when is_float(value), do: :float
  defp value_type(value) when is_tuple(value), do: :tuple
  defp value_type(value) when is_function(value), do: :function
  defp value_type(_value), do: :term
end
