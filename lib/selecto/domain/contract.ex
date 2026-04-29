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
  @choice_source_path_keys [:source_path, :value_source, :caption_source, :description_source]
  @choice_source_presentation_controls [:select, :autocomplete, :table_picker]
  @choice_source_presentation_modes [:static, :searchable, :async, :inline]
  @choice_source_presentation_cardinalities [:one, :many]
  @order_directions [:asc, :desc]

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
    source_relationships = Map.get(normalized_domain, :source_relationships, %{})
    choice_sources = Map.get(normalized_domain, :choice_sources, %{})
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
    |> validate_source_relationships(source_relationships, field_index)
    |> validate_choice_sources(choice_sources, source_relationships, capabilities)
    |> validate_field_choice_source_bindings(
      source,
      schemas,
      projection,
      choice_sources,
      field_index
    )
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

  defp validate_source_relationships(errors, source_relationships, field_index)
       when is_map(source_relationships) do
    Enum.reduce(source_relationships, errors, fn {relationship_id, relationship}, acc ->
      path = [:source_relationships, relationship_id]

      acc
      |> validate_source_relationship_id(relationship_id, path)
      |> validate_source_relationship(relationship_id, relationship, path, field_index)
    end)
  end

  defp validate_source_relationships(errors, source_relationships, _field_index) do
    [
      error(
        :invalid_section_shape,
        [:source_relationships],
        "domain section :source_relationships must be a map",
        expected: :map,
        actual: value_type(source_relationships)
      )
      | errors
    ]
  end

  defp validate_source_relationship_id(errors, relationship_id, _path)
       when is_atom(relationship_id) or is_binary(relationship_id) do
    errors
  end

  defp validate_source_relationship_id(errors, relationship_id, path) do
    [
      error(
        :invalid_source_relationship_id,
        path,
        "source relationship ids must be atoms or strings",
        expected: "atom or string",
        actual: value_type(relationship_id),
        source_relationship: relationship_id
      )
      | errors
    ]
  end

  defp validate_source_relationship(errors, relationship_id, relationship, path, field_index)
       when is_map(relationship) do
    errors
    |> validate_source_relationship_required_keys(relationship_id, relationship, path)
    |> validate_id_value(
      map_value(relationship, :target_domain),
      path ++ [:target_domain],
      :invalid_source_relationship_target_domain,
      "source relationship #{inspect(relationship_id)} target_domain must be an atom or string",
      source_relationship: relationship_id,
      target_domain: map_value(relationship, :target_domain)
    )
    |> validate_source_relationship_source_field(relationship_id, relationship, path, field_index)
    |> validate_id_value(
      map_value(relationship, :target_field),
      path ++ [:target_field],
      :invalid_source_relationship_target_field,
      "source relationship #{inspect(relationship_id)} target_field must be an atom or string",
      source_relationship: relationship_id,
      target_field: map_value(relationship, :target_field)
    )
  end

  defp validate_source_relationship(errors, relationship_id, relationship, path, _field_index) do
    [
      error(
        :invalid_section_shape,
        path,
        "source relationship #{inspect(relationship_id)} must be a map",
        expected: :map,
        actual: value_type(relationship),
        source_relationship: relationship_id
      )
      | errors
    ]
  end

  defp validate_source_relationship_required_keys(errors, relationship_id, relationship, path) do
    missing_keys =
      Enum.reject([:target_domain, :source_field, :target_field], &has_key?(relationship, &1))

    case missing_keys do
      [] ->
        errors

      _ ->
        [
          error(
            :source_relationship_missing_required_keys,
            path,
            "source relationship #{inspect(relationship_id)} is missing required keys #{inspect(missing_keys)}",
            source_relationship: relationship_id,
            keys: missing_keys
          )
          | errors
        ]
    end
  end

  defp validate_source_relationship_source_field(
         errors,
         relationship_id,
         relationship,
         path,
         field_index
       ) do
    case map_value(relationship, :source_field) do
      nil ->
        errors

      source_field when is_atom(source_field) or is_binary(source_field) ->
        if known_field?(field_index, source_field) do
          errors
        else
          [
            error(
              :source_relationship_source_field_not_found,
              path ++ [:source_field],
              "source relationship #{inspect(relationship_id)} source_field #{inspect(source_field)} is not defined in source, schemas, or custom columns",
              source_relationship: relationship_id,
              source_field: source_field
            )
            | errors
          ]
        end

      source_field ->
        [
          error(
            :invalid_source_relationship_source_field,
            path ++ [:source_field],
            "source relationship #{inspect(relationship_id)} source_field must be an atom or string",
            expected: "atom or string",
            actual: value_type(source_field),
            source_relationship: relationship_id,
            source_field: source_field
          )
          | errors
        ]
    end
  end

  defp validate_choice_sources(errors, choice_sources, source_relationships, capabilities)
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

  defp validate_choice_sources(errors, choice_sources, _source_relationships, _capabilities) do
    [
      error(
        :invalid_section_shape,
        [:choice_sources],
        "domain section :choice_sources must be a map",
        expected: :map,
        actual: value_type(choice_sources)
      )
      | errors
    ]
  end

  defp validate_choice_source_id(errors, choice_source_id, _path)
       when is_atom(choice_source_id) or is_binary(choice_source_id) do
    errors
  end

  defp validate_choice_source_id(errors, choice_source_id, path) do
    [
      error(
        :invalid_choice_source_id,
        path,
        "choice source ids must be atoms or strings",
        expected: "atom or string",
        actual: value_type(choice_source_id),
        choice_source: choice_source_id
      )
      | errors
    ]
  end

  defp validate_choice_source(
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
    |> validate_id_value(
      map_value(choice_source, :domain),
      path ++ [:domain],
      :invalid_choice_source_domain,
      "choice source #{inspect(choice_source_id)} domain must be an atom or string",
      choice_source: choice_source_id,
      domain: map_value(choice_source, :domain)
    )
    |> validate_id_value(
      map_value(choice_source, :value_field),
      path ++ [:value_field],
      :invalid_choice_source_value_field,
      "choice source #{inspect(choice_source_id)} value_field must be an atom or string",
      choice_source: choice_source_id,
      value_field: map_value(choice_source, :value_field)
    )
    |> validate_id_value(
      map_value(choice_source, :label_field),
      path ++ [:label_field],
      :invalid_choice_source_label_field,
      "choice source #{inspect(choice_source_id)} label_field must be an atom or string",
      choice_source: choice_source_id,
      label_field: map_value(choice_source, :label_field)
    )
    |> validate_choice_source_paths(choice_source_id, choice_source, path)
    |> validate_choice_source_filters(choice_source_id, choice_source, path)
    |> validate_choice_source_order_by(choice_source_id, choice_source, path)
    |> validate_choice_source_presentation(choice_source_id, choice_source, path)
    |> validate_choice_source_relationship(
      choice_source_id,
      choice_source,
      path,
      source_relationships
    )
    |> validate_choice_source_capability(choice_source_id, choice_source, path, capabilities)
  end

  defp validate_choice_source(
         errors,
         choice_source_id,
         choice_source,
         path,
         _source_relationships,
         _capabilities
       ) do
    [
      error(
        :invalid_section_shape,
        path,
        "choice source #{inspect(choice_source_id)} must be a map",
        expected: :map,
        actual: value_type(choice_source),
        choice_source: choice_source_id
      )
      | errors
    ]
  end

  defp validate_choice_source_required_keys(errors, choice_source_id, choice_source, path) do
    missing_keys =
      Enum.reject([:domain, :value_field, :label_field], &has_key?(choice_source, &1))

    case missing_keys do
      [] ->
        errors

      _ ->
        [
          error(
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

  defp validate_choice_source_paths(errors, choice_source_id, choice_source, path) do
    Enum.reduce(@choice_source_path_keys, errors, fn key, acc ->
      value = map_value(choice_source, key)

      if is_nil(value) or valid_choice_source_path?(value) do
        acc
      else
        [
          error(
            invalid_choice_source_path_code(key),
            path ++ [key],
            "choice source #{inspect(choice_source_id)} #{key} must be a non-empty atom or dotted string path",
            expected: "non-empty atom or dotted string path",
            actual: value_type(value),
            choice_source: choice_source_id,
            attribute: key,
            value: value
          )
          | acc
        ]
      end
    end)
  end

  defp invalid_choice_source_path_code(:source_path), do: :invalid_choice_source_source_path
  defp invalid_choice_source_path_code(:value_source), do: :invalid_choice_source_value_source
  defp invalid_choice_source_path_code(:caption_source), do: :invalid_choice_source_caption_source

  defp invalid_choice_source_path_code(:description_source),
    do: :invalid_choice_source_description_source

  defp validate_choice_source_filters(errors, choice_source_id, choice_source, path) do
    case map_value(choice_source, :filters) do
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
          error(
            :invalid_choice_source_filters,
            path ++ [:filters],
            "choice source #{inspect(choice_source_id)} filters must be a list",
            expected: :list,
            actual: value_type(filters),
            choice_source: choice_source_id
          )
          | errors
        ]
    end
  end

  defp validate_choice_source_filter_expression(errors, choice_source_id, filter, path)
       when is_tuple(filter) do
    validate_choice_source_filter_parts(
      errors,
      choice_source_id,
      Tuple.to_list(filter),
      filter,
      path
    )
  end

  defp validate_choice_source_filter_expression(errors, choice_source_id, filter, path)
       when is_list(filter) do
    validate_choice_source_filter_parts(errors, choice_source_id, filter, filter, path)
  end

  defp validate_choice_source_filter_expression(errors, choice_source_id, filter, path) do
    [
      error(
        :invalid_choice_source_filter_expression,
        path,
        "choice source #{inspect(choice_source_id)} filter must be an operator tuple or list",
        expected: "operator tuple or list",
        actual: value_type(filter),
        choice_source: choice_source_id,
        filter: filter
      )
      | errors
    ]
  end

  defp validate_choice_source_filter_parts(
         errors,
         choice_source_id,
         [op, operands],
         _filter,
         path
       ) do
    cond do
      choice_source_logical_filter_op?(op) ->
        validate_choice_source_logical_filter(errors, choice_source_id, op, operands, path)

      choice_source_unary_filter_op?(op) ->
        validate_choice_source_filter_expression(
          errors,
          choice_source_id,
          operands,
          path ++ [:operand]
        )

      choice_source_known_filter_op?(op) ->
        invalid_choice_source_filter_operands(errors, choice_source_id, op, path, operands)

      choice_source_filter_operator_value?(op) ->
        invalid_choice_source_filter_operator(errors, choice_source_id, op, path)

      true ->
        invalid_choice_source_filter_expression(errors, choice_source_id, [op, operands], path)
    end
  end

  defp validate_choice_source_filter_parts(
         errors,
         choice_source_id,
         [op, field, _value],
         _filter,
         path
       ) do
    validate_choice_source_field_filter(errors, choice_source_id, op, field, path)
  end

  defp validate_choice_source_filter_parts(
         errors,
         choice_source_id,
         [op, field, _left, _right],
         _filter,
         path
       ) do
    validate_choice_source_field_filter(errors, choice_source_id, op, field, path)
  end

  defp validate_choice_source_filter_parts(
         errors,
         choice_source_id,
         [op | _] = filter,
         _raw,
         path
       ) do
    cond do
      choice_source_known_filter_op?(op) ->
        invalid_choice_source_filter_operands(errors, choice_source_id, op, path, filter)

      choice_source_filter_operator_value?(op) ->
        invalid_choice_source_filter_operator(errors, choice_source_id, op, path)

      true ->
        invalid_choice_source_filter_expression(errors, choice_source_id, filter, path)
    end
  end

  defp validate_choice_source_filter_parts(errors, choice_source_id, filter, _raw, path) do
    invalid_choice_source_filter_expression(errors, choice_source_id, filter, path)
  end

  defp validate_choice_source_logical_filter(errors, choice_source_id, _op, filters, path)
       when is_list(filters) do
    filters
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {filter, index}, acc ->
      validate_choice_source_filter_expression(acc, choice_source_id, filter, path ++ [index])
    end)
  end

  defp validate_choice_source_logical_filter(errors, choice_source_id, op, filters, path) do
    invalid_choice_source_filter_operands(errors, choice_source_id, op, path, filters)
  end

  defp validate_choice_source_field_filter(errors, choice_source_id, op, field, path) do
    cond do
      choice_source_field_filter_op?(op) and valid_choice_source_path?(field) ->
        errors

      choice_source_field_filter_op?(op) ->
        [
          error(
            :invalid_choice_source_filter_path,
            path ++ [:field],
            "choice source #{inspect(choice_source_id)} filter field must be a non-empty atom or dotted string path",
            expected: "non-empty atom or dotted string path",
            actual: value_type(field),
            choice_source: choice_source_id,
            field: field
          )
          | errors
        ]

      choice_source_known_filter_op?(op) ->
        invalid_choice_source_filter_operands(errors, choice_source_id, op, path, field)

      choice_source_filter_operator_value?(op) ->
        invalid_choice_source_filter_operator(errors, choice_source_id, op, path)

      true ->
        invalid_choice_source_filter_expression(errors, choice_source_id, [op, field], path)
    end
  end

  defp invalid_choice_source_filter_operator(errors, choice_source_id, op, path) do
    [
      error(
        :invalid_choice_source_filter_operator,
        path,
        "choice source #{inspect(choice_source_id)} filter operator #{inspect(op)} is not supported",
        expected: "known filter operator",
        actual: value_type(op),
        choice_source: choice_source_id,
        operator: op
      )
      | errors
    ]
  end

  defp invalid_choice_source_filter_operands(errors, choice_source_id, op, path, operands) do
    [
      error(
        :invalid_choice_source_filter_operands,
        path,
        "choice source #{inspect(choice_source_id)} filter operator #{inspect(op)} has invalid operands",
        expected: "operator operands",
        actual: value_type(operands),
        choice_source: choice_source_id,
        operator: op
      )
      | errors
    ]
  end

  defp invalid_choice_source_filter_expression(errors, choice_source_id, filter, path) do
    [
      error(
        :invalid_choice_source_filter_expression,
        path,
        "choice source #{inspect(choice_source_id)} filter must be an operator tuple or list",
        expected: "operator tuple or list",
        actual: value_type(filter),
        choice_source: choice_source_id,
        filter: filter
      )
      | errors
    ]
  end

  defp choice_source_known_filter_op?(op) do
    choice_source_logical_filter_op?(op) or choice_source_unary_filter_op?(op) or
      choice_source_field_filter_op?(op)
  end

  defp choice_source_logical_filter_op?(op), do: enum_value?(op, @logical_filter_ops)

  defp choice_source_unary_filter_op?(op), do: enum_value?(op, @unary_filter_ops)

  defp choice_source_field_filter_op?(op), do: enum_value?(op, @field_filter_ops)

  defp choice_source_filter_operator_value?(op) when is_atom(op), do: not is_nil(op)

  defp choice_source_filter_operator_value?(op) when is_binary(op), do: String.trim(op) != ""

  defp choice_source_filter_operator_value?(_op), do: false

  defp validate_choice_source_order_by(errors, choice_source_id, choice_source, path) do
    case map_value(choice_source, :order_by) do
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
          error(
            :invalid_choice_source_order_by,
            path ++ [:order_by],
            "choice source #{inspect(choice_source_id)} order_by must be a list",
            expected: :list,
            actual: value_type(order_by),
            choice_source: choice_source_id
          )
          | errors
        ]
    end
  end

  defp validate_choice_source_order_entry(errors, choice_source_id, order_entry, path)
       when is_atom(order_entry) do
    if valid_choice_source_path?(order_entry) do
      errors
    else
      invalid_choice_source_order_entry_error(errors, choice_source_id, order_entry, path)
    end
  end

  defp validate_choice_source_order_entry(errors, choice_source_id, order_entry, path)
       when is_binary(order_entry) do
    if valid_choice_source_path?(order_entry) do
      errors
    else
      invalid_choice_source_order_entry_error(errors, choice_source_id, order_entry, path)
    end
  end

  defp validate_choice_source_order_entry(errors, choice_source_id, {order_path, direction}, path) do
    errors
    |> validate_choice_source_order_path(choice_source_id, order_path, path)
    |> validate_choice_source_order_direction(choice_source_id, direction, path)
  end

  defp validate_choice_source_order_entry(errors, choice_source_id, [order_path, direction], path) do
    errors
    |> validate_choice_source_order_path(choice_source_id, order_path, path)
    |> validate_choice_source_order_direction(choice_source_id, direction, path)
  end

  defp validate_choice_source_order_entry(errors, choice_source_id, order_entry, path) do
    invalid_choice_source_order_entry_error(errors, choice_source_id, order_entry, path)
  end

  defp validate_choice_source_order_path(errors, choice_source_id, order_path, path) do
    if valid_choice_source_path?(order_path) do
      errors
    else
      invalid_choice_source_order_entry_error(errors, choice_source_id, order_path, path)
    end
  end

  defp invalid_choice_source_order_entry_error(errors, choice_source_id, order_entry, path) do
    [
      error(
        :invalid_choice_source_order_by_entry,
        path,
        "choice source #{inspect(choice_source_id)} order_by entries must be paths or {path, direction}",
        expected: "path or {path, direction}",
        actual: value_type(order_entry),
        choice_source: choice_source_id,
        order_by: order_entry
      )
      | errors
    ]
  end

  defp validate_choice_source_order_direction(errors, _choice_source_id, direction, _path)
       when direction in @order_directions or direction in ["asc", "desc"] do
    errors
  end

  defp validate_choice_source_order_direction(errors, choice_source_id, direction, path) do
    [
      error(
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

  defp validate_choice_source_presentation(errors, choice_source_id, choice_source, path) do
    case map_value(choice_source, :presentation) do
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
          error(
            :invalid_choice_source_presentation,
            path ++ [:presentation],
            "choice source #{inspect(choice_source_id)} presentation must be a map",
            expected: :map,
            actual: value_type(presentation),
            choice_source: choice_source_id
          )
          | errors
        ]
    end
  end

  defp validate_choice_source_presentation_enum(
         errors,
         choice_source_id,
         presentation,
         path,
         key,
         allowed,
         code
       ) do
    case map_value(presentation, key) do
      nil ->
        errors

      value when is_atom(value) or is_binary(value) ->
        if enum_value?(value, allowed) do
          errors
        else
          [
            error(
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
          error(
            code,
            path ++ [:presentation, key],
            "choice source #{inspect(choice_source_id)} presentation #{key} must be an atom or string",
            expected: allowed,
            actual: value_type(value),
            choice_source: choice_source_id,
            attribute: key,
            value: value
          )
          | errors
        ]
    end
  end

  defp validate_choice_source_relationship(
         errors,
         choice_source_id,
         choice_source,
         path,
         source_relationships
       ) do
    case map_value(choice_source, :source_relationship) do
      nil ->
        errors

      source_relationship when is_atom(source_relationship) or is_binary(source_relationship) ->
        if is_map(source_relationships) and
             fetch_key(source_relationships, source_relationship) != :error do
          errors
        else
          [
            error(
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
          error(
            :invalid_choice_source_relationship,
            path ++ [:source_relationship],
            "choice source #{inspect(choice_source_id)} source_relationship must be an atom or string",
            expected: "atom or string",
            actual: value_type(source_relationship),
            choice_source: choice_source_id,
            source_relationship: source_relationship
          )
          | errors
        ]
    end
  end

  defp validate_choice_source_capability(
         errors,
         choice_source_id,
         choice_source,
         path,
         capabilities
       ) do
    case map_value(choice_source, :capability) do
      nil ->
        errors

      capability when is_atom(capability) or is_binary(capability) ->
        if is_map(capabilities) and fetch_key(capabilities, capability) != :error do
          errors
        else
          [
            error(
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
          error(
            :invalid_choice_source_capability,
            path ++ [:capability],
            "choice source #{inspect(choice_source_id)} capability must be an atom or string",
            expected: "atom or string",
            actual: value_type(capability),
            choice_source: choice_source_id,
            capability: capability
          )
          | errors
        ]
    end
  end

  defp validate_id_value(errors, nil, _path, _code, _message, _attrs), do: errors

  defp validate_id_value(errors, value, _path, _code, _message, _attrs)
       when is_atom(value) or is_binary(value),
       do: errors

  defp validate_id_value(errors, value, path, code, message, attrs) do
    [
      error(
        code,
        path,
        message,
        Keyword.put(attrs, :actual, value_type(value))
      )
      | errors
    ]
  end

  defp validate_field_choice_source_bindings(
         errors,
         _source,
         _schemas,
         _projection,
         choice_sources,
         _field_index
       )
       when not is_map(choice_sources),
       do: errors

  defp validate_field_choice_source_bindings(
         errors,
         source,
         schemas,
         projection,
         choice_sources,
         field_index
       ) do
    errors
    |> validate_relation_choice_source_bindings(
      :source,
      source,
      [:source, :columns],
      choice_sources,
      field_index
    )
    |> validate_schema_choice_source_bindings(schemas, choice_sources, field_index)
    |> validate_projection_choice_source_bindings(projection, choice_sources, field_index)
  end

  defp validate_schema_choice_source_bindings(errors, schemas, choice_sources, field_index)
       when is_map(schemas) do
    Enum.reduce(schemas, errors, fn {schema_id, schema}, acc ->
      validate_relation_choice_source_bindings(
        acc,
        schema_id,
        schema,
        [:schemas, schema_id, :columns],
        choice_sources,
        field_index
      )
    end)
  end

  defp validate_schema_choice_source_bindings(errors, _schemas, _choice_sources, _field_index) do
    errors
  end

  defp validate_projection_choice_source_bindings(errors, projection, choice_sources, field_index) do
    case map_value(projection, :columns) do
      columns when is_map(columns) ->
        validate_field_choice_source_bindings_in_columns(
          errors,
          columns,
          [:columns],
          choice_sources,
          field_index,
          & &1
        )

      _ ->
        errors
    end
  end

  defp validate_relation_choice_source_bindings(
         errors,
         relation_id,
         relation,
         path,
         choice_sources,
         field_index
       )
       when is_map(relation) do
    case map_value(relation, :columns) do
      columns when is_map(columns) ->
        validate_field_choice_source_bindings_in_columns(
          errors,
          columns,
          path,
          choice_sources,
          field_index,
          &relation_field_ref(relation_id, &1)
        )

      _ ->
        errors
    end
  end

  defp validate_relation_choice_source_bindings(
         errors,
         _relation_id,
         _relation,
         _path,
         _choice_sources,
         _field_index
       ) do
    errors
  end

  defp validate_field_choice_source_bindings_in_columns(
         errors,
         columns,
         path,
         choice_sources,
         field_index,
         field_ref_fun
       ) do
    Enum.reduce(columns, errors, fn {field, column}, acc ->
      field_ref = field_ref_fun.(field)
      column_path = path ++ [field]

      acc
      |> validate_column_choice_source(field_ref, column, column_path, choice_sources)
      |> validate_column_reference_binding(
        field_ref,
        column,
        column_path,
        choice_sources,
        field_index
      )
    end)
  end

  defp validate_column_choice_source(errors, _field, column, _path, _choice_sources)
       when not is_map(column),
       do: errors

  defp validate_column_choice_source(errors, field, column, path, choice_sources) do
    case map_value(column, :choice_source) do
      nil ->
        errors

      choice_source when is_atom(choice_source) or is_binary(choice_source) ->
        if fetch_key(choice_sources, choice_source) != :error do
          errors
        else
          [
            error(
              :field_choice_source_not_found,
              path ++ [:choice_source],
              "field #{inspect(field)} references missing choice source #{inspect(choice_source)}",
              field: field,
              choice_source: choice_source
            )
            | errors
          ]
        end

      choice_source ->
        [
          error(
            :invalid_field_choice_source,
            path ++ [:choice_source],
            "field #{inspect(field)} choice_source must be an atom or string",
            expected: "atom or string",
            actual: value_type(choice_source),
            field: field,
            choice_source: choice_source
          )
          | errors
        ]
    end
  end

  defp validate_column_reference_binding(
         errors,
         _field,
         column,
         _path,
         _choice_sources,
         _field_index
       )
       when not is_map(column),
       do: errors

  defp validate_column_reference_binding(errors, field, column, path, choice_sources, field_index) do
    case map_value(column, :reference) do
      nil ->
        errors

      reference when is_map(reference) ->
        reference_path = path ++ [:reference]

        errors
        |> validate_field_reference_choice_source(
          field,
          reference,
          reference_path,
          choice_sources
        )
        |> validate_field_reference_source_hint(
          field,
          reference,
          reference_path,
          :value_source,
          :invalid_field_reference_value_source
        )
        |> validate_field_reference_source_hint(
          field,
          reference,
          reference_path,
          :caption_source,
          :invalid_field_reference_caption_source
        )
        |> validate_field_reference_caption_field(field, reference, reference_path, field_index)

      reference ->
        [
          error(
            :invalid_field_reference,
            path ++ [:reference],
            "field #{inspect(field)} reference must be a map",
            expected: :map,
            actual: value_type(reference),
            field: field
          )
          | errors
        ]
    end
  end

  defp validate_field_reference_choice_source(
         field_errors,
         field,
         reference,
         path,
         choice_sources
       ) do
    case map_value(reference, :choice_source) do
      nil ->
        field_errors

      choice_source when is_atom(choice_source) or is_binary(choice_source) ->
        if fetch_key(choice_sources, choice_source) != :error do
          field_errors
        else
          [
            error(
              :field_reference_choice_source_not_found,
              path ++ [:choice_source],
              "field #{inspect(field)} reference points at missing choice source #{inspect(choice_source)}",
              field: field,
              choice_source: choice_source
            )
            | field_errors
          ]
        end

      choice_source ->
        [
          error(
            :invalid_field_reference_choice_source,
            path ++ [:choice_source],
            "field #{inspect(field)} reference choice_source must be an atom or string",
            expected: "atom or string",
            actual: value_type(choice_source),
            field: field,
            choice_source: choice_source
          )
          | field_errors
        ]
    end
  end

  defp validate_field_reference_source_hint(errors, field, reference, path, key, code) do
    value = map_value(reference, key)

    validate_id_value(
      errors,
      value,
      path ++ [key],
      code,
      "field #{inspect(field)} reference #{key} must be an atom or string",
      [{:field, field}, {key, value}]
    )
  end

  defp validate_field_reference_caption_field(errors, field, reference, path, field_index) do
    case map_value(reference, :caption_field) do
      nil ->
        errors

      caption_field when is_atom(caption_field) or is_binary(caption_field) ->
        if known_field?(field_index, caption_field) do
          errors
        else
          [
            error(
              :field_reference_caption_field_not_found,
              path ++ [:caption_field],
              "field #{inspect(field)} reference caption_field #{inspect(caption_field)} is not defined in source, schemas, or custom columns",
              field: field,
              caption_field: caption_field
            )
            | errors
          ]
        end

      caption_field ->
        [
          error(
            :invalid_field_reference_caption_field,
            path ++ [:caption_field],
            "field #{inspect(field)} reference caption_field must be an atom or string",
            expected: "atom or string",
            actual: value_type(caption_field),
            field: field,
            caption_field: caption_field
          )
          | errors
        ]
    end
  end

  defp relation_field_ref(:source, field), do: field
  defp relation_field_ref(relation_id, field), do: "#{field_id(relation_id)}.#{field_id(field)}"

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

  defp valid_choice_source_path?(path) when is_atom(path), do: not is_nil(path)

  defp valid_choice_source_path?(path) when is_binary(path) do
    trimmed_path = String.trim(path)

    trimmed_path != "" and not String.starts_with?(trimmed_path, ".") and
      not String.ends_with?(trimmed_path, ".") and not String.contains?(trimmed_path, "..")
  end

  defp valid_choice_source_path?(_path), do: false

  defp enum_value?(value, allowed) when is_atom(value), do: value in allowed

  defp enum_value?(value, allowed) when is_binary(value) do
    Enum.any?(allowed, &(Atom.to_string(&1) == value))
  end

  defp enum_value?(_value, _allowed), do: false

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
