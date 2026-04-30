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
  @query_order_directions [
    :asc,
    :desc,
    :asc_nulls_first,
    :asc_nulls_last,
    :desc_nulls_first,
    :desc_nulls_last
  ]
  @query_group_wrappers [:rollup, :grouping_set]
  @query_member_groups [:ctes, :values, :subqueries, :laterals, :unnests]
  @query_member_join_types [:left, :inner, :right, :full]
  @detail_action_types [:modal, :iframe_modal, :external_link, :live_component]

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
    detail_actions = Map.get(normalized_domain, :detail_actions, %{})
    field_index = field_index(source, schemas, projection)

    []
    |> validate_required_sections(authored_domain)
    |> validate_relation(:source, source, [:source])
    |> validate_schemas(schemas)
    |> validate_joins(joins, source, schemas)
    |> validate_query_field_lists(query, field_index)
    |> validate_filters(query, field_index)
    |> validate_functions(query)
    |> validate_query_members(query)
    |> validate_published_views(query)
    |> validate_detail_actions(detail_actions, field_index)
    |> validate_writes(writes, field_index)
    |> validate_capabilities(capabilities)
    |> validate_query_capability_references(query, detail_actions, capabilities)
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

  defp validate_query_field_lists(errors, query, field_index) do
    functions = map_value(query, :functions)

    errors
    |> validate_query_selection_list(query, :default_selected, field_index, functions)
    |> validate_query_selection_list(query, :required_selected, field_index, functions)
    |> validate_query_order_list(query, :required_order_by, field_index, functions)
    |> validate_query_group_list(query, :required_group_by, field_index, functions)
  end

  defp validate_query_selection_list(errors, query, section, field_index, functions) do
    case map_value(query, section) do
      nil ->
        errors

      selections when is_list(selections) ->
        selections
        |> Enum.with_index()
        |> Enum.reduce(errors, fn {selection, index}, acc ->
          validate_query_selection_entry(
            acc,
            section,
            selection,
            [section, index],
            field_index,
            functions
          )
        end)

      selections ->
        invalid_query_list(errors, section, selections)
    end
  end

  defp validate_query_order_list(errors, query, section, field_index, functions) do
    case map_value(query, section) do
      nil ->
        errors

      order_by when is_list(order_by) ->
        order_by
        |> Enum.with_index()
        |> Enum.reduce(errors, fn {order_entry, index}, acc ->
          validate_query_order_entry(
            acc,
            section,
            order_entry,
            [section, index],
            field_index,
            functions
          )
        end)

      order_by ->
        invalid_query_list(errors, section, order_by)
    end
  end

  defp validate_query_group_list(errors, query, section, field_index, functions) do
    case map_value(query, section) do
      nil ->
        errors

      group_by when is_list(group_by) ->
        group_by
        |> Enum.with_index()
        |> Enum.reduce(errors, fn {group_entry, index}, acc ->
          validate_query_group_entry(
            acc,
            section,
            group_entry,
            [section, index],
            field_index,
            functions
          )
        end)

      group_by ->
        invalid_query_list(errors, section, group_by)
    end
  end

  defp invalid_query_list(errors, section, value) do
    [
      error(
        :invalid_section_shape,
        [section],
        "domain section #{inspect(section)} must be a list",
        expected: :list,
        actual: value_type(value)
      )
      | errors
    ]
  end

  defp validate_query_selection_entry(errors, section, field, path, field_index, _functions)
       when is_atom(field) or is_binary(field) do
    validate_query_field_reference(errors, section, field, path, field_index)
  end

  defp validate_query_selection_entry(
         errors,
         section,
         {:field, field},
         path,
         field_index,
         functions
       ) do
    validate_query_selector_reference(
      errors,
      section,
      :select,
      field,
      path ++ [:field],
      field_index,
      functions
    )
  end

  defp validate_query_selection_entry(
         errors,
         section,
         {:field, field, _alias},
         path,
         field_index,
         functions
       ) do
    validate_query_selector_reference(
      errors,
      section,
      :select,
      field,
      path ++ [:field],
      field_index,
      functions
    )
  end

  defp validate_query_selection_entry(
         errors,
         section,
         {:udf, function_id, args},
         path,
         field_index,
         functions
       )
       when is_list(args) do
    validate_query_function_reference(
      errors,
      section,
      :select,
      function_id,
      args,
      path,
      field_index,
      functions
    )
  end

  defp validate_query_selection_entry(errors, _section, entry, _path, _field_index, _functions)
       when is_tuple(entry) or is_map(entry) do
    errors
  end

  defp validate_query_selection_entry(errors, section, entry, path, _field_index, _functions) do
    invalid_query_field_reference(errors, section, entry, path)
  end

  defp validate_query_order_entry(
         errors,
         _section,
         {:raw_sql, _sql},
         _path,
         _field_index,
         _functions
       ) do
    errors
  end

  defp validate_query_order_entry(
         errors,
         section,
         {:udf, function_id, args},
         path,
         field_index,
         functions
       )
       when is_list(args) do
    validate_query_function_reference(
      errors,
      section,
      :order_by,
      function_id,
      args,
      path,
      field_index,
      functions
    )
  end

  defp validate_query_order_entry(
         errors,
         section,
         {direction, field},
         path,
         field_index,
         functions
       )
       when direction in @query_order_directions do
    validate_query_selector_reference(
      errors,
      section,
      :order_by,
      field,
      path ++ [:field],
      field_index,
      functions
    )
  end

  defp validate_query_order_entry(
         errors,
         section,
         {field, direction},
         path,
         field_index,
         functions
       )
       when direction in @query_order_directions do
    validate_query_selector_reference(
      errors,
      section,
      :order_by,
      field,
      path ++ [:field],
      field_index,
      functions
    )
  end

  defp validate_query_order_entry(
         errors,
         section,
         {field, direction},
         path,
         field_index,
         _functions
       )
       when (is_atom(field) or is_binary(field)) and (is_atom(direction) or is_binary(direction)) do
    if query_order_direction?(direction) do
      validate_query_field_reference(errors, section, field, path ++ [:field], field_index)
    else
      [
        error(
          :invalid_query_order_direction,
          path ++ [:direction],
          "query order direction #{inspect(direction)} is not supported",
          expected: @query_order_directions,
          actual: value_type(direction),
          section: section,
          direction: direction
        )
        | errors
      ]
    end
  end

  defp validate_query_order_entry(errors, section, field, path, field_index, _functions)
       when is_atom(field) or is_binary(field) do
    validate_query_field_reference(errors, section, field, path, field_index)
  end

  defp validate_query_order_entry(errors, _section, entry, _path, _field_index, _functions)
       when is_tuple(entry) or is_map(entry) do
    errors
  end

  defp validate_query_order_entry(errors, section, entry, path, _field_index, _functions) do
    invalid_query_field_reference(errors, section, entry, path)
  end

  defp validate_query_group_entry(
         errors,
         _section,
         {:raw_sql, _sql},
         _path,
         _field_index,
         _functions
       ) do
    errors
  end

  defp validate_query_group_entry(
         errors,
         section,
         {:udf, function_id, args},
         path,
         field_index,
         functions
       )
       when is_list(args) do
    validate_query_function_reference(
      errors,
      section,
      :group_by,
      function_id,
      args,
      path,
      field_index,
      functions
    )
  end

  defp validate_query_group_entry(
         errors,
         section,
         {wrapper, groups},
         path,
         field_index,
         functions
       )
       when wrapper in @query_group_wrappers do
    if is_list(groups) do
      groups
      |> Enum.with_index()
      |> Enum.reduce(errors, fn {group, index}, acc ->
        validate_query_group_entry(
          acc,
          section,
          group,
          path ++ [wrapper, index],
          field_index,
          functions
        )
      end)
    else
      [
        error(
          :invalid_query_group_wrapper,
          path,
          "query group wrapper #{inspect(wrapper)} must contain a list of fields",
          expected: :list,
          actual: value_type(groups),
          section: section,
          wrapper: wrapper
        )
        | errors
      ]
    end
  end

  defp validate_query_group_entry(errors, section, field, path, field_index, _functions)
       when is_atom(field) or is_binary(field) do
    validate_query_field_reference(errors, section, field, path, field_index)
  end

  defp validate_query_group_entry(errors, _section, entry, _path, _field_index, _functions)
       when is_tuple(entry) or is_map(entry) do
    errors
  end

  defp validate_query_group_entry(errors, section, entry, path, _field_index, _functions) do
    invalid_query_field_reference(errors, section, entry, path)
  end

  defp validate_query_selector_reference(
         errors,
         section,
         call_site,
         {:udf, function_id, args},
         path,
         field_index,
         functions
       )
       when is_list(args) do
    validate_query_function_reference(
      errors,
      section,
      call_site,
      function_id,
      args,
      path,
      field_index,
      functions
    )
  end

  defp validate_query_selector_reference(
         errors,
         section,
         _call_site,
         field,
         path,
         field_index,
         _functions
       )
       when is_atom(field) or is_binary(field) do
    validate_query_field_reference(errors, section, field, path, field_index)
  end

  defp validate_query_selector_reference(
         errors,
         _section,
         _call_site,
         expression,
         _path,
         _field_index,
         _functions
       )
       when is_tuple(expression) or is_map(expression) do
    errors
  end

  defp validate_query_selector_reference(
         errors,
         section,
         _call_site,
         field,
         path,
         _field_index,
         _functions
       ) do
    invalid_query_field_reference(errors, section, field, path)
  end

  defp validate_query_function_reference(
         errors,
         section,
         call_site,
         function_id,
         args,
         path,
         field_index,
         functions
       ) do
    function_path = path ++ [:function]

    cond do
      not non_empty_atom_or_string?(function_id) ->
        [
          error(
            :invalid_query_function_id,
            function_path,
            "query function references must use a non-empty atom or string id",
            expected: "non-empty atom or string",
            actual: value_type(function_id),
            section: section,
            function: function_id,
            call_site: call_site
          )
          | errors
        ]

      not is_map(functions) ->
        query_function_not_found_error(errors, section, call_site, function_id, function_path)

      true ->
        case fetch_key(functions, function_id) do
          {:ok, function_spec} ->
            errors
            |> validate_query_function_call_site(
              section,
              call_site,
              function_id,
              function_spec,
              function_path
            )
            |> validate_query_function_args(
              section,
              call_site,
              function_id,
              args,
              function_spec,
              path,
              field_index,
              functions
            )

          :error ->
            query_function_not_found_error(errors, section, call_site, function_id, function_path)
        end
    end
  end

  defp query_function_not_found_error(errors, section, call_site, function_id, path) do
    [
      error(
        :query_function_not_found,
        path,
        "query references missing function #{inspect(function_id)}",
        section: section,
        function: function_id,
        call_site: call_site
      )
      | errors
    ]
  end

  defp validate_query_function_call_site(
         errors,
         _section,
         _call_site,
         _function_id,
         function_spec,
         _path
       )
       when not is_map(function_spec) do
    errors
  end

  defp validate_query_function_call_site(
         errors,
         section,
         call_site,
         function_id,
         function_spec,
         path
       ) do
    case map_value(function_spec, :allowed_in) do
      nil ->
        errors

      allowed_in when is_list(allowed_in) ->
        if call_site in allowed_in do
          errors
        else
          [
            error(
              :query_function_call_site_not_allowed,
              path,
              "function #{inspect(function_id)} is not allowed in query #{call_site}",
              section: section,
              function: function_id,
              call_site: call_site,
              allowed_in: allowed_in
            )
            | errors
          ]
        end

      _allowed_in ->
        errors
    end
  end

  defp validate_query_function_args(
         errors,
         _section,
         _call_site,
         _function_id,
         _args,
         function_spec,
         _path,
         _field_index,
         _functions
       )
       when not is_map(function_spec) do
    errors
  end

  defp validate_query_function_args(
         errors,
         section,
         call_site,
         function_id,
         args,
         function_spec,
         path,
         field_index,
         functions
       ) do
    case query_function_spec_args(function_spec) do
      :invalid ->
        errors

      spec_args ->
        errors
        |> validate_query_function_arg_count(
          section,
          call_site,
          function_id,
          args,
          spec_args,
          path
        )
        |> validate_query_function_selector_args(
          section,
          function_id,
          args,
          spec_args,
          path,
          field_index,
          functions
        )
    end
  end

  defp query_function_spec_args(function_spec) do
    case map_value(function_spec, :args) do
      nil -> []
      args when is_list(args) -> args
      _args -> :invalid
    end
  end

  defp validate_query_function_arg_count(
         errors,
         section,
         call_site,
         function_id,
         args,
         spec_args,
         path
       ) do
    expected = length(spec_args)
    actual = length(args)

    if expected == actual do
      errors
    else
      [
        error(
          :query_function_arg_count_mismatch,
          path ++ [:args],
          "function #{inspect(function_id)} expects #{expected} query argument(s), got #{actual}",
          expected: expected,
          actual: actual,
          section: section,
          function: function_id,
          call_site: call_site
        )
        | errors
      ]
    end
  end

  defp validate_query_function_selector_args(
         errors,
         section,
         function_id,
         args,
         spec_args,
         path,
         field_index,
         functions
       ) do
    args
    |> Enum.zip(spec_args)
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {{arg, arg_spec}, index}, acc ->
      validate_query_function_arg(
        acc,
        section,
        function_id,
        arg,
        arg_spec,
        path ++ [:args, index],
        field_index,
        functions
      )
    end)
  end

  defp validate_query_function_arg(
         errors,
         section,
         _function_id,
         arg,
         arg_spec,
         path,
         field_index,
         functions
       )
       when is_map(arg_spec) do
    case map_value(arg_spec, :source) do
      :selector ->
        validate_query_function_selector_arg(errors, section, arg, path, field_index, functions)

      _source ->
        errors
    end
  end

  defp validate_query_function_arg(
         errors,
         _section,
         _function_id,
         _arg,
         _arg_spec,
         _path,
         _field_index,
         _functions
       ) do
    errors
  end

  defp validate_query_function_selector_arg(
         errors,
         section,
         {:field, field},
         path,
         field_index,
         functions
       ) do
    validate_query_selector_reference(
      errors,
      section,
      :select,
      field,
      path ++ [:field],
      field_index,
      functions
    )
  end

  defp validate_query_function_selector_arg(
         errors,
         section,
         {:field, field, _alias},
         path,
         field_index,
         functions
       ) do
    validate_query_selector_reference(
      errors,
      section,
      :select,
      field,
      path ++ [:field],
      field_index,
      functions
    )
  end

  defp validate_query_function_selector_arg(
         errors,
         section,
         {:udf, function_id, args},
         path,
         field_index,
         functions
       )
       when is_list(args) do
    validate_query_function_reference(
      errors,
      section,
      :select,
      function_id,
      args,
      path,
      field_index,
      functions
    )
  end

  defp validate_query_function_selector_arg(
         errors,
         section,
         field,
         path,
         field_index,
         _functions
       )
       when is_atom(field) or is_binary(field) do
    validate_query_field_reference(errors, section, field, path, field_index)
  end

  defp validate_query_function_selector_arg(
         errors,
         _section,
         _expression,
         _path,
         _field_index,
         _functions
       ) do
    errors
  end

  defp validate_query_field_reference(errors, section, field, path, field_index) do
    cond do
      not valid_static_source_path?(field) ->
        invalid_query_field_reference(errors, section, field, path)

      known_field?(field_index, field) ->
        errors

      true ->
        [
          error(
            :query_field_not_found,
            path,
            "query field #{inspect(field)} is not defined in source, schemas, or custom columns",
            section: section,
            field: field
          )
          | errors
        ]
    end
  end

  defp invalid_query_field_reference(errors, section, field, path) do
    [
      error(
        :invalid_query_field_reference,
        path,
        "query field references must be non-empty atoms or dotted string paths",
        expected: "non-empty atom or dotted string path",
        actual: value_type(field),
        section: section,
        field: field
      )
      | errors
    ]
  end

  defp query_order_direction?(direction), do: enum_value?(direction, @query_order_directions)

  defp validate_filters(errors, query, field_index) do
    filters = map_value(query, :filters) || %{}
    required_filters = map_value(query, :required_filters) || []

    errors
    |> validate_filter_registry(filters, field_index)
    |> validate_required_filters(required_filters, field_index)
  end

  defp validate_filter_registry(errors, filters, field_index) when is_map(filters) do
    Enum.reduce(filters, errors, fn {filter_id, filter_config}, acc ->
      acc
      |> validate_filter_id(filter_id)
      |> validate_filter_config(filter_id, filter_config, field_index)
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

  defp validate_filter_id(errors, filter_id) do
    if non_empty_atom_or_string?(filter_id) do
      errors
    else
      [
        error(
          :invalid_filter_id,
          [:filters, filter_id],
          "filter id #{inspect(filter_id)} must be a non-empty atom or string",
          expected: "non-empty atom or string",
          actual: value_type(filter_id),
          filter: filter_id
        )
        | errors
      ]
    end
  end

  defp validate_filter_config(errors, filter_id, filter_config, field_index)
       when is_map(filter_config) do
    errors
    |> validate_filter_config_field(filter_id, filter_config, field_index)
    |> validate_filter_config_type(filter_id, filter_config)
  end

  defp validate_filter_config(errors, filter_id, filter_config, _field_index) do
    [
      error(
        :invalid_filter_config,
        [:filters, filter_id],
        "filter #{inspect(filter_id)} config must be a map",
        expected: :map,
        actual: value_type(filter_config),
        filter: filter_id
      )
      | errors
    ]
  end

  defp validate_filter_config_field(errors, filter_id, filter_config, field_index) do
    if has_key?(filter_config, :field) do
      field = map_value(filter_config, :field)

      if valid_static_source_path?(field) do
        validate_field_reference(errors, field, [:filters, filter_id, :field], field_index)
      else
        [
          error(
            :invalid_filter_field,
            [:filters, filter_id, :field],
            "filter #{inspect(filter_id)} field must be a non-empty atom or dotted string path",
            expected: "non-empty atom or dotted string path",
            actual: value_type(field),
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

  defp validate_filter_config_type(errors, filter_id, filter_config) do
    if has_key?(filter_config, :type) do
      type = map_value(filter_config, :type)

      if non_empty_atom_or_string?(type) do
        errors
      else
        [
          error(
            :invalid_filter_type,
            [:filters, filter_id, :type],
            "filter #{inspect(filter_id)} type must be a non-empty atom or string",
            expected: "non-empty atom or string",
            actual: value_type(type),
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

  defp validate_functions(errors, query) do
    case map_value(query, :functions) do
      nil ->
        errors

      functions when is_map(functions) ->
        Enum.reduce(functions, errors, fn {function_id, function_spec}, acc ->
          acc
          |> validate_function_id(function_id)
          |> validate_function_spec(function_id, function_spec)
        end)

      functions ->
        [
          error(
            :invalid_section_shape,
            [:functions],
            "domain section :functions must be a map",
            expected: :map,
            actual: value_type(functions)
          )
          | errors
        ]
    end
  end

  defp validate_function_id(errors, function_id) do
    if non_empty_atom_or_string?(function_id) do
      errors
    else
      [
        error(
          :invalid_function_id,
          [:functions, function_id],
          "function id #{inspect(function_id)} must be a non-empty atom or string",
          expected: "non-empty atom or string",
          actual: value_type(function_id),
          function: function_id
        )
        | errors
      ]
    end
  end

  defp validate_function_spec(errors, function_id, function_spec) when is_map(function_spec) do
    errors
    |> validate_function_kind(function_id, function_spec)
    |> validate_function_sql_name(function_id, function_spec)
    |> validate_function_allowed_in(function_id, function_spec)
    |> validate_function_args(function_id, function_spec)
    |> validate_function_returns(function_id, function_spec)
  end

  defp validate_function_spec(errors, function_id, function_spec) do
    [
      error(
        :invalid_function_spec,
        [:functions, function_id],
        "function #{inspect(function_id)} spec must be a map",
        expected: :map,
        actual: value_type(function_spec),
        function: function_id
      )
      | errors
    ]
  end

  defp validate_function_kind(errors, function_id, function_spec) do
    case map_value(function_spec, :kind) do
      kind when kind in [:scalar, :predicate, :table] ->
        errors

      kind ->
        [
          error(
            :invalid_function_kind,
            [:functions, function_id, :kind],
            "function #{inspect(function_id)} kind must be :scalar, :predicate, or :table",
            expected: [:scalar, :predicate, :table],
            actual: value_type(kind),
            function: function_id,
            kind: kind
          )
          | errors
        ]
    end
  end

  defp validate_function_sql_name(errors, function_id, function_spec) do
    sql_name = map_value(function_spec, :sql_name)

    if Selecto.UDF.valid_sql_name?(sql_name) do
      errors
    else
      [
        error(
          :invalid_function_sql_name,
          [:functions, function_id, :sql_name],
          "function #{inspect(function_id)} sql_name must be a safe qualified SQL function name",
          expected: "safe qualified SQL function name",
          actual: value_type(sql_name),
          function: function_id,
          sql_name: sql_name
        )
        | errors
      ]
    end
  end

  defp validate_function_allowed_in(errors, function_id, function_spec) do
    case map_value(function_spec, :allowed_in) do
      nil ->
        errors

      allowed_in when is_list(allowed_in) ->
        allowed_in
        |> Enum.with_index()
        |> Enum.reduce(errors, fn {call_site, index}, acc ->
          validate_function_call_site(acc, function_id, call_site, [
            :functions,
            function_id,
            :allowed_in,
            index
          ])
        end)

      allowed_in ->
        [
          error(
            :invalid_function_allowed_in,
            [:functions, function_id, :allowed_in],
            "function #{inspect(function_id)} allowed_in must be a list",
            expected: :list,
            actual: value_type(allowed_in),
            function: function_id
          )
          | errors
        ]
    end
  end

  defp validate_function_call_site(errors, function_id, call_site, path) do
    if Selecto.UDF.valid_call_site?(call_site) do
      errors
    else
      [
        error(
          :invalid_function_call_site,
          path,
          "function #{inspect(function_id)} call site #{inspect(call_site)} is not supported",
          expected: Selecto.UDF.allowed_call_sites(),
          actual: value_type(call_site),
          function: function_id,
          call_site: call_site
        )
        | errors
      ]
    end
  end

  defp validate_function_args(errors, function_id, function_spec) do
    case map_value(function_spec, :args) do
      nil ->
        errors

      args when is_list(args) ->
        args
        |> Enum.with_index()
        |> Enum.reduce(errors, fn {arg_spec, index}, acc ->
          validate_function_arg_spec(acc, function_id, arg_spec, [
            :functions,
            function_id,
            :args,
            index
          ])
        end)

      args ->
        [
          error(
            :invalid_function_args,
            [:functions, function_id, :args],
            "function #{inspect(function_id)} args must be a list",
            expected: :list,
            actual: value_type(args),
            function: function_id
          )
          | errors
        ]
    end
  end

  defp validate_function_arg_spec(errors, function_id, arg_spec, path) when is_map(arg_spec) do
    errors
    |> validate_function_arg_name(function_id, arg_spec, path)
    |> validate_function_arg_type(function_id, arg_spec, path)
    |> validate_function_arg_source(function_id, arg_spec, path)
  end

  defp validate_function_arg_spec(errors, function_id, arg_spec, path) do
    [
      error(
        :invalid_function_arg_spec,
        path,
        "function #{inspect(function_id)} arg spec must be a map",
        expected: :map,
        actual: value_type(arg_spec),
        function: function_id
      )
      | errors
    ]
  end

  defp validate_function_arg_name(errors, function_id, arg_spec, path) do
    name = map_value(arg_spec, :name)

    if non_empty_atom_or_string?(name) do
      errors
    else
      [
        error(
          :invalid_function_arg_name,
          path ++ [:name],
          "function #{inspect(function_id)} arg name must be a non-empty atom or string",
          expected: "non-empty atom or string",
          actual: value_type(name),
          function: function_id,
          name: name
        )
        | errors
      ]
    end
  end

  defp validate_function_arg_type(errors, function_id, arg_spec, path) do
    if has_key?(arg_spec, :type) do
      errors
    else
      [
        error(
          :missing_function_arg_type,
          path ++ [:type],
          "function #{inspect(function_id)} arg must declare a type",
          function: function_id
        )
        | errors
      ]
    end
  end

  defp validate_function_arg_source(errors, function_id, arg_spec, path) do
    source = map_value(arg_spec, :source)

    if Selecto.UDF.valid_arg_source?(source) do
      errors
    else
      [
        error(
          :invalid_function_arg_source,
          path ++ [:source],
          "function #{inspect(function_id)} arg source must be :selector, :value, or :literal",
          expected: Selecto.UDF.allowed_arg_sources(),
          actual: value_type(source),
          function: function_id,
          source: source
        )
        | errors
      ]
    end
  end

  defp validate_function_returns(errors, function_id, function_spec) do
    case map_value(function_spec, :kind) do
      :predicate ->
        validate_predicate_function_returns(errors, function_id, function_spec)

      :table ->
        validate_table_function_returns(errors, function_id, function_spec)

      :scalar ->
        validate_scalar_function_returns(errors, function_id, function_spec)

      _kind ->
        errors
    end
  end

  defp validate_predicate_function_returns(errors, function_id, function_spec) do
    case map_value(function_spec, :returns) do
      :boolean ->
        errors

      returns ->
        [
          error(
            :invalid_function_returns,
            [:functions, function_id, :returns],
            "predicate function #{inspect(function_id)} must declare returns: :boolean",
            expected: :boolean,
            actual: value_type(returns),
            function: function_id,
            returns: returns
          )
          | errors
        ]
    end
  end

  defp validate_table_function_returns(errors, function_id, function_spec) do
    columns =
      case map_value(function_spec, :returns) do
        %{} = returns -> map_value(returns, :columns)
        _returns -> nil
      end

    if is_map(columns) and map_size(columns) > 0 do
      errors
    else
      [
        error(
          :invalid_function_returns,
          [:functions, function_id, :returns],
          "table function #{inspect(function_id)} must declare returns: %{columns: %{...}}",
          expected: "%{columns: %{...}}",
          actual: value_type(map_value(function_spec, :returns)),
          function: function_id,
          returns: map_value(function_spec, :returns)
        )
        | errors
      ]
    end
  end

  defp validate_scalar_function_returns(errors, function_id, function_spec) do
    case map_value(function_spec, :returns) do
      nil ->
        errors

      returns when is_atom(returns) ->
        errors

      {:array, _type} ->
        errors

      returns ->
        [
          error(
            :invalid_function_returns,
            [:functions, function_id, :returns],
            "scalar function #{inspect(function_id)} returns must be an atom or array tuple when provided",
            expected: "atom or {:array, type}",
            actual: value_type(returns),
            function: function_id,
            returns: returns
          )
          | errors
        ]
    end
  end

  defp validate_query_members(errors, query) do
    case map_value(query, :query_members) do
      nil ->
        errors

      query_members when is_map(query_members) ->
        Enum.reduce(@query_member_groups, errors, fn group_key, acc ->
          validate_query_member_group(acc, query_members, group_key)
        end)

      query_members ->
        [
          error(
            :invalid_section_shape,
            [:query_members],
            "domain section :query_members must be a map",
            expected: :map,
            actual: value_type(query_members)
          )
          | errors
        ]
    end
  end

  defp validate_query_member_group(errors, query_members, group_key) do
    case fetch_map_value(query_members, group_key) do
      :__missing__ ->
        errors

      nil ->
        errors

      members when is_map(members) ->
        Enum.reduce(members, errors, fn {member_id, member_spec}, acc ->
          acc
          |> validate_query_member_id(group_key, member_id)
          |> validate_query_member_spec(group_key, member_id, member_spec)
        end)

      members ->
        [
          error(
            :invalid_query_member_group,
            [:query_members, group_key],
            "query_members.#{group_key} must be a map of named members",
            expected: :map,
            actual: value_type(members),
            group: group_key
          )
          | errors
        ]
    end
  end

  defp validate_query_member_id(errors, group_key, member_id) do
    if non_empty_atom_or_string?(member_id) do
      errors
    else
      [
        error(
          :invalid_query_member_id,
          [:query_members, group_key, member_id],
          "query member id #{inspect(member_id)} in #{group_key} must be a non-empty atom or string",
          expected: "non-empty atom or string",
          actual: value_type(member_id),
          group: group_key,
          member: member_id
        )
        | errors
      ]
    end
  end

  defp validate_query_member_spec(errors, group_key, member_id, member_spec)
       when is_map(member_spec) do
    case group_key do
      :ctes -> validate_cte_member(errors, member_id, member_spec)
      :values -> validate_values_member(errors, member_id, member_spec)
      :subqueries -> validate_subquery_member(errors, member_id, member_spec)
      :laterals -> validate_lateral_member(errors, member_id, member_spec)
      :unnests -> validate_unnest_member(errors, member_id, member_spec)
    end
  end

  defp validate_query_member_spec(errors, group_key, member_id, member_spec) do
    [
      error(
        :invalid_query_member_spec,
        [:query_members, group_key, member_id],
        "query member #{inspect(member_id)} in #{group_key} must be a map",
        expected: :map,
        actual: value_type(member_spec),
        group: group_key,
        member: member_id
      )
      | errors
    ]
  end

  defp validate_cte_member(errors, member_id, member_spec) do
    errors
    |> validate_cte_member_query(member_id, member_spec)
    |> validate_query_member_join(:ctes, member_id, member_spec)
  end

  defp validate_cte_member_query(errors, member_id, member_spec) do
    recursive? =
      map_value(member_spec, :type) == :recursive or
        not is_nil(map_value(member_spec, :base_query)) or
        not is_nil(map_value(member_spec, :recursive_query))

    if recursive? do
      errors
      |> validate_query_member_function(
        :ctes,
        member_id,
        map_value(member_spec, :base_query),
        [:query_members, :ctes, member_id, :base_query],
        [0, 1],
        "recursive CTE #{inspect(member_id)} requires base_query function with arity 0 or 1"
      )
      |> validate_query_member_function(
        :ctes,
        member_id,
        map_value(member_spec, :recursive_query),
        [:query_members, :ctes, member_id, :recursive_query],
        [1, 2],
        "recursive CTE #{inspect(member_id)} requires recursive_query function with arity 1 or 2"
      )
    else
      {query_builder, query_key} = query_member_query_builder(member_spec)

      validate_query_member_function(
        errors,
        :ctes,
        member_id,
        query_builder,
        [:query_members, :ctes, member_id, query_key],
        [0, 1],
        "CTE #{inspect(member_id)} requires query or query_builder function with arity 0 or 1"
      )
    end
  end

  defp validate_values_member(errors, member_id, member_spec) do
    errors
    |> validate_values_member_rows(member_id, member_spec)
    |> validate_values_member_columns(member_id, member_spec)
    |> validate_query_member_join(:values, member_id, member_spec)
    |> validate_query_member_alias(:values, member_id, member_spec)
  end

  defp validate_values_member_rows(errors, member_id, member_spec) do
    {rows, rows_key} =
      case fetch_map_value(member_spec, :rows) do
        :__missing__ ->
          case fetch_map_value(member_spec, :data) do
            :__missing__ -> {:__missing__, :rows}
            data -> {data, :data}
          end

        rows ->
          {rows, :rows}
      end

    if is_list(rows) do
      errors
    else
      [
        error(
          :invalid_query_member_rows,
          [:query_members, :values, member_id, rows_key],
          "VALUES member #{inspect(member_id)} requires rows or data as a list",
          expected: :list,
          actual: value_type(rows),
          group: :values,
          member: member_id
        )
        | errors
      ]
    end
  end

  defp validate_values_member_columns(errors, member_id, member_spec) do
    case fetch_map_value(member_spec, :columns) do
      :__missing__ ->
        errors

      columns when is_list(columns) ->
        errors

      columns ->
        [
          error(
            :invalid_query_member_columns,
            [:query_members, :values, member_id, :columns],
            "VALUES member #{inspect(member_id)} columns must be a list when provided",
            expected: :list,
            actual: value_type(columns),
            group: :values,
            member: member_id
          )
          | errors
        ]
    end
  end

  defp validate_subquery_member(errors, member_id, member_spec) do
    errors
    |> validate_subquery_member_kind(member_id, member_spec)
    |> validate_subquery_member_query(member_id, member_spec)
    |> validate_subquery_member_on(member_id, member_spec)
    |> validate_query_member_join_type(:subqueries, member_id, member_spec, :type)
    |> validate_subquery_member_join_id(member_id, member_spec)
  end

  defp validate_subquery_member_kind(errors, member_id, member_spec) do
    case map_value(member_spec, :kind) do
      nil ->
        errors

      :join ->
        errors

      kind ->
        [
          error(
            :invalid_query_member_kind,
            [:query_members, :subqueries, member_id, :kind],
            "subquery member #{inspect(member_id)} kind must be :join when provided",
            expected: :join,
            actual: value_type(kind),
            group: :subqueries,
            member: member_id,
            kind: kind
          )
          | errors
        ]
    end
  end

  defp validate_subquery_member_query(errors, member_id, member_spec) do
    {query_builder, query_key} = query_member_query_builder(member_spec)

    validate_query_member_function(
      errors,
      :subqueries,
      member_id,
      query_builder,
      [:query_members, :subqueries, member_id, query_key],
      [0, 1],
      "subquery member #{inspect(member_id)} requires query or query_builder function with arity 0 or 1"
    )
  end

  defp validate_subquery_member_on(errors, member_id, member_spec) do
    case fetch_map_value(member_spec, :on) do
      :__missing__ ->
        errors

      on when is_list(on) ->
        errors

      on ->
        [
          error(
            :invalid_query_member_on,
            [:query_members, :subqueries, member_id, :on],
            "subquery member #{inspect(member_id)} on must be a list when provided",
            expected: :list,
            actual: value_type(on),
            group: :subqueries,
            member: member_id
          )
          | errors
        ]
    end
  end

  defp validate_subquery_member_join_id(errors, member_id, member_spec) do
    case fetch_map_value(member_spec, :join_id) do
      :__missing__ ->
        errors

      join_id when is_atom(join_id) and not is_nil(join_id) ->
        errors

      join_id when is_binary(join_id) ->
        if String.trim(join_id) == "" do
          invalid_query_member_join_id(errors, member_id, join_id)
        else
          errors
        end

      join_id ->
        invalid_query_member_join_id(errors, member_id, join_id)
    end
  end

  defp invalid_query_member_join_id(errors, member_id, join_id) do
    [
      error(
        :invalid_query_member_join_id,
        [:query_members, :subqueries, member_id, :join_id],
        "subquery member #{inspect(member_id)} join_id must be a non-empty atom or string when provided",
        expected: "non-empty atom or string",
        actual: value_type(join_id),
        group: :subqueries,
        member: member_id,
        join_id: join_id
      )
      | errors
    ]
  end

  defp validate_lateral_member(errors, member_id, member_spec) do
    errors
    |> validate_lateral_member_source(member_id, member_spec)
    |> validate_query_member_join_type(:laterals, member_id, member_spec, :join_type)
    |> validate_query_member_alias(:laterals, member_id, member_spec)
    |> validate_query_member_options(:laterals, member_id, member_spec)
  end

  defp validate_lateral_member_source(errors, member_id, member_spec) do
    {source, source_key} = lateral_member_source(member_spec)

    if valid_lateral_source?(source) do
      errors
    else
      [
        error(
          :invalid_query_member_source,
          [:query_members, :laterals, member_id, source_key],
          "lateral member #{inspect(member_id)} requires query, source, or lateral_source as a tuple or function",
          expected: "tuple or function with arity 0, 1, or 2",
          actual: value_type(source),
          group: :laterals,
          member: member_id
        )
        | errors
      ]
    end
  end

  defp validate_unnest_member(errors, member_id, member_spec) do
    errors
    |> validate_unnest_member_field(member_id, member_spec)
    |> validate_unnest_member_ordinality(member_id, member_spec)
    |> validate_query_member_alias(:unnests, member_id, member_spec)
    |> validate_query_member_options(:unnests, member_id, member_spec)
  end

  defp validate_unnest_member_field(errors, member_id, member_spec) do
    {field, field_key} = unnest_member_field(member_spec)

    if valid_unnest_field?(field) do
      errors
    else
      [
        error(
          :invalid_query_member_field,
          [:query_members, :unnests, member_id, field_key],
          "UNNEST member #{inspect(member_id)} requires array_field or field as a non-empty atom, string, or tuple expression",
          expected: "non-empty atom, non-empty string, or tuple expression",
          actual: value_type(field),
          group: :unnests,
          member: member_id,
          field: field
        )
        | errors
      ]
    end
  end

  defp validate_unnest_member_ordinality(errors, member_id, member_spec) do
    case map_value(member_spec, :ordinality) do
      nil ->
        errors

      ordinality when is_atom(ordinality) and not is_nil(ordinality) ->
        errors

      ordinality when is_binary(ordinality) ->
        if String.trim(ordinality) == "" do
          invalid_query_member_ordinality(errors, member_id, ordinality)
        else
          errors
        end

      ordinality ->
        invalid_query_member_ordinality(errors, member_id, ordinality)
    end
  end

  defp invalid_query_member_ordinality(errors, member_id, ordinality) do
    [
      error(
        :invalid_query_member_ordinality,
        [:query_members, :unnests, member_id, :ordinality],
        "UNNEST member #{inspect(member_id)} ordinality must be a non-empty atom or string when provided",
        expected: "non-empty atom or string",
        actual: value_type(ordinality),
        group: :unnests,
        member: member_id,
        ordinality: ordinality
      )
      | errors
    ]
  end

  defp validate_query_member_function(errors, group_key, member_id, fun, path, arities, message) do
    if valid_arity?(fun, arities) do
      errors
    else
      [
        error(
          :invalid_query_member_query,
          path,
          message,
          expected: "function with arity #{Enum.join(arities, " or ")}",
          actual: value_type(fun),
          group: group_key,
          member: member_id
        )
        | errors
      ]
    end
  end

  defp validate_query_member_join(errors, group_key, member_id, member_spec) do
    case fetch_map_value(member_spec, :join) do
      :__missing__ ->
        errors

      join when join in [nil, false, true] ->
        errors

      join when is_list(join) or is_map(join) ->
        errors

      join ->
        [
          error(
            :invalid_query_member_join,
            [:query_members, group_key, member_id, :join],
            "query member #{inspect(member_id)} join must be true, false, nil, a list, or a map",
            expected: "true, false, nil, list, or map",
            actual: value_type(join),
            group: group_key,
            member: member_id
          )
          | errors
        ]
    end
  end

  defp validate_query_member_join_type(errors, group_key, member_id, member_spec, preferred_key) do
    {join_type, join_type_key} = query_member_join_type(member_spec, preferred_key)

    cond do
      join_type == :__missing__ ->
        errors

      join_type in @query_member_join_types ->
        errors

      true ->
        [
          error(
            :invalid_query_member_join_type,
            [:query_members, group_key, member_id, join_type_key],
            "query member #{inspect(member_id)} join type must be one of #{inspect(@query_member_join_types)}",
            expected: @query_member_join_types,
            actual: value_type(join_type),
            group: group_key,
            member: member_id,
            join_type: join_type
          )
          | errors
        ]
    end
  end

  defp validate_query_member_alias(errors, group_key, member_id, member_spec) do
    case query_member_alias(member_spec) do
      :__missing__ ->
        errors

      nil ->
        errors

      alias_name when is_atom(alias_name) and not is_nil(alias_name) ->
        errors

      alias_name when is_binary(alias_name) ->
        if String.trim(alias_name) == "" do
          invalid_query_member_alias(errors, group_key, member_id, alias_name)
        else
          errors
        end

      alias_name ->
        invalid_query_member_alias(errors, group_key, member_id, alias_name)
    end
  end

  defp invalid_query_member_alias(errors, group_key, member_id, alias_name) do
    [
      error(
        :invalid_query_member_alias,
        [:query_members, group_key, member_id, :as],
        "query member #{inspect(member_id)} alias must be a non-empty atom or string when provided",
        expected: "non-empty atom or string",
        actual: value_type(alias_name),
        group: group_key,
        member: member_id,
        alias_name: alias_name
      )
      | errors
    ]
  end

  defp validate_query_member_options(errors, group_key, member_id, member_spec) do
    case fetch_map_value(member_spec, :options) do
      :__missing__ ->
        errors

      options when is_list(options) or is_map(options) ->
        errors

      options ->
        [
          error(
            :invalid_query_member_options,
            [:query_members, group_key, member_id, :options],
            "query member #{inspect(member_id)} options must be a list or map when provided",
            expected: "list or map",
            actual: value_type(options),
            group: group_key,
            member: member_id
          )
          | errors
        ]
    end
  end

  defp query_member_query_builder(member_spec) do
    case fetch_map_value(member_spec, :query_builder) do
      :__missing__ ->
        {map_value(member_spec, :query), :query}

      query_builder ->
        {query_builder, :query_builder}
    end
  end

  defp query_member_join_type(member_spec, preferred_key) do
    case fetch_map_value(member_spec, preferred_key) do
      :__missing__ ->
        fallback_key = if preferred_key == :join_type, do: :type, else: :join_type

        case fetch_map_value(member_spec, fallback_key) do
          :__missing__ -> {:__missing__, preferred_key}
          value -> {value, fallback_key}
        end

      value ->
        {value, preferred_key}
    end
  end

  defp query_member_alias(member_spec) do
    case fetch_map_value(member_spec, :as) do
      :__missing__ ->
        case fetch_map_value(member_spec, :alias) do
          :__missing__ -> fetch_map_value(member_spec, :alias_name)
          value -> value
        end

      value ->
        value
    end
  end

  defp lateral_member_source(member_spec) do
    case fetch_map_value(member_spec, :query) do
      :__missing__ ->
        case fetch_map_value(member_spec, :source) do
          :__missing__ ->
            {map_value(member_spec, :lateral_source), :lateral_source}

          source ->
            {source, :source}
        end

      query ->
        {query, :query}
    end
  end

  defp unnest_member_field(member_spec) do
    case fetch_map_value(member_spec, :array_field) do
      :__missing__ -> {map_value(member_spec, :field), :field}
      array_field -> {array_field, :array_field}
    end
  end

  defp valid_lateral_source?(source) when is_tuple(source), do: true
  defp valid_lateral_source?(source) when is_function(source, 0), do: true
  defp valid_lateral_source?(source) when is_function(source, 1), do: true
  defp valid_lateral_source?(source) when is_function(source, 2), do: true
  defp valid_lateral_source?(_source), do: false

  defp valid_unnest_field?(field) when is_tuple(field), do: true

  defp valid_unnest_field?(field), do: non_empty_atom_or_string?(field)

  defp validate_published_views(errors, query) do
    case map_value(query, :published_views) do
      nil ->
        errors

      published_views when is_map(published_views) ->
        Enum.reduce(published_views, errors, fn {view_id, view_spec}, acc ->
          acc
          |> validate_published_view_id(view_id)
          |> validate_published_view_spec(view_id, view_spec)
        end)

      published_views ->
        [
          error(
            :invalid_section_shape,
            [:published_views],
            "domain section :published_views must be a map",
            expected: :map,
            actual: value_type(published_views)
          )
          | errors
        ]
    end
  end

  defp validate_published_view_id(errors, view_id) do
    if non_empty_atom_or_string?(view_id) do
      errors
    else
      [
        error(
          :invalid_published_view_id,
          [:published_views, view_id],
          "published view id #{inspect(view_id)} must be a non-empty atom or string",
          expected: "non-empty atom or string",
          actual: value_type(view_id),
          view: view_id
        )
        | errors
      ]
    end
  end

  defp validate_published_view_spec(errors, view_id, view_spec) when is_map(view_spec) do
    errors
    |> validate_published_view_database_name(view_id, view_spec)
    |> validate_published_view_kind(view_id, view_spec)
    |> validate_published_view_query(view_id, view_spec)
    |> validate_published_view_columns(view_id, view_spec)
    |> validate_published_view_indexes(view_id, view_spec)
    |> validate_published_view_refresh(view_id, view_spec)
  end

  defp validate_published_view_spec(errors, view_id, view_spec) do
    [
      error(
        :invalid_published_view_spec,
        [:published_views, view_id],
        "published view #{inspect(view_id)} spec must be a map",
        expected: :map,
        actual: value_type(view_spec),
        view: view_id
      )
      | errors
    ]
  end

  defp validate_published_view_database_name(errors, view_id, view_spec) do
    database_name = map_value(view_spec, :database_name)

    if is_binary(database_name) and String.trim(database_name) != "" do
      errors
    else
      [
        error(
          :invalid_published_view_database_name,
          [:published_views, view_id, :database_name],
          "published view #{inspect(view_id)} database_name must be a non-empty string",
          expected: "non-empty string",
          actual: value_type(database_name),
          view: view_id,
          database_name: database_name
        )
        | errors
      ]
    end
  end

  defp validate_published_view_kind(errors, view_id, view_spec) do
    case map_value(view_spec, :kind) do
      kind when kind in [:view, :materialized_view] ->
        errors

      kind ->
        [
          error(
            :invalid_published_view_kind,
            [:published_views, view_id, :kind],
            "published view #{inspect(view_id)} kind must be :view or :materialized_view",
            expected: [:view, :materialized_view],
            actual: value_type(kind),
            view: view_id,
            kind: kind
          )
          | errors
        ]
    end
  end

  defp validate_published_view_query(errors, view_id, view_spec) do
    query = map_value(view_spec, :query)

    if valid_arity?(query, [1]) do
      errors
    else
      [
        error(
          :invalid_published_view_query,
          [:published_views, view_id, :query],
          "published view #{inspect(view_id)} query must be a function with arity 1",
          expected: "function with arity 1",
          actual: value_type(query),
          view: view_id
        )
        | errors
      ]
    end
  end

  defp validate_published_view_columns(errors, view_id, view_spec) do
    case map_value(view_spec, :columns) do
      columns when is_map(columns) and map_size(columns) > 0 ->
        Enum.reduce(columns, errors, fn {column_id, column_spec}, acc ->
          validate_published_view_column(acc, view_id, column_id, column_spec)
        end)

      columns ->
        [
          error(
            :invalid_published_view_columns,
            [:published_views, view_id, :columns],
            "published view #{inspect(view_id)} columns must be a non-empty map",
            expected: "non-empty map",
            actual: value_type(columns),
            view: view_id
          )
          | errors
        ]
    end
  end

  defp validate_published_view_column(errors, view_id, column_id, column_spec) do
    errors
    |> validate_published_view_column_id(view_id, column_id)
    |> validate_published_view_column_spec(view_id, column_id, column_spec)
  end

  defp validate_published_view_column_id(errors, view_id, column_id) do
    if non_empty_atom_or_string?(column_id) do
      errors
    else
      [
        error(
          :invalid_published_view_column,
          [:published_views, view_id, :columns, column_id],
          "published view #{inspect(view_id)} column ids must be non-empty atoms or strings",
          expected: "non-empty atom or string",
          actual: value_type(column_id),
          view: view_id,
          column: column_id
        )
        | errors
      ]
    end
  end

  defp validate_published_view_column_spec(errors, view_id, column_id, column_spec) do
    if is_map(column_spec) do
      errors
    else
      [
        error(
          :invalid_published_view_column,
          [:published_views, view_id, :columns, column_id],
          "published view #{inspect(view_id)} column #{inspect(column_id)} spec must be a map",
          expected: :map,
          actual: value_type(column_spec),
          view: view_id,
          column: column_id
        )
        | errors
      ]
    end
  end

  defp validate_published_view_indexes(errors, view_id, view_spec) do
    case fetch_map_value(view_spec, :indexes) do
      :__missing__ ->
        errors

      nil ->
        errors

      indexes when is_list(indexes) ->
        indexes
        |> Enum.with_index()
        |> Enum.reduce(errors, fn {index_spec, index}, acc ->
          validate_published_view_index(acc, view_id, index_spec, index)
        end)

      indexes ->
        [
          error(
            :invalid_published_view_indexes,
            [:published_views, view_id, :indexes],
            "published view #{inspect(view_id)} indexes must be a list when provided",
            expected: :list,
            actual: value_type(indexes),
            view: view_id
          )
          | errors
        ]
    end
  end

  defp validate_published_view_index(errors, view_id, index_spec, index)
       when is_map(index_spec) do
    errors
    |> validate_published_view_index_columns(view_id, index_spec, index)
    |> validate_published_view_index_boolean(view_id, index_spec, index, :unique)
    |> validate_published_view_index_boolean(view_id, index_spec, index, :concurrently)
  end

  defp validate_published_view_index(errors, view_id, index_spec, index) do
    [
      error(
        :invalid_published_view_index,
        [:published_views, view_id, :indexes, index],
        "published view #{inspect(view_id)} index specs must be maps",
        expected: :map,
        actual: value_type(index_spec),
        view: view_id
      )
      | errors
    ]
  end

  defp validate_published_view_index_columns(errors, view_id, index_spec, index) do
    columns = map_value(index_spec, :columns)

    if is_list(columns) and columns != [] and Enum.all?(columns, &non_empty_atom_or_string?/1) do
      errors
    else
      [
        error(
          :invalid_published_view_index_columns,
          [:published_views, view_id, :indexes, index, :columns],
          "published view #{inspect(view_id)} index columns must be a non-empty list of atoms or strings",
          expected: "non-empty list of atoms or strings",
          actual: value_type(columns),
          view: view_id,
          columns: columns
        )
        | errors
      ]
    end
  end

  defp validate_published_view_index_boolean(errors, view_id, index_spec, index, key) do
    case fetch_map_value(index_spec, key) do
      :__missing__ ->
        errors

      nil ->
        errors

      value when is_boolean(value) ->
        errors

      value ->
        [
          error(
            :invalid_published_view_index_option,
            [:published_views, view_id, :indexes, index, key],
            "published view #{inspect(view_id)} index #{key} must be boolean when provided",
            expected: :boolean,
            actual: value_type(value),
            view: view_id,
            option: key
          )
          | errors
        ]
    end
  end

  defp validate_published_view_refresh(errors, view_id, view_spec) do
    case fetch_map_value(view_spec, :refresh) do
      :__missing__ ->
        errors

      nil ->
        errors

      refresh when is_map(refresh) ->
        errors

      refresh ->
        [
          error(
            :invalid_published_view_refresh,
            [:published_views, view_id, :refresh],
            "published view #{inspect(view_id)} refresh must be a map when provided",
            expected: :map,
            actual: value_type(refresh),
            view: view_id
          )
          | errors
        ]
    end
  end

  defp validate_detail_actions(errors, detail_actions, field_index) when is_map(detail_actions) do
    Enum.reduce(detail_actions, errors, fn {action_id, action_spec}, acc ->
      acc
      |> validate_detail_action_id(action_id)
      |> validate_detail_action_spec(action_id, action_spec, field_index)
    end)
  end

  defp validate_detail_actions(errors, detail_actions, _field_index) do
    [
      error(
        :invalid_section_shape,
        [:detail_actions],
        "domain section :detail_actions must be a map",
        expected: :map,
        actual: value_type(detail_actions)
      )
      | errors
    ]
  end

  defp validate_detail_action_id(errors, action_id) do
    if non_empty_atom_or_string?(action_id) do
      errors
    else
      [
        error(
          :invalid_detail_action_id,
          [:detail_actions, action_id],
          "detail action id #{inspect(action_id)} must be a non-empty atom or string",
          expected: "non-empty atom or string",
          actual: value_type(action_id),
          action: action_id
        )
        | errors
      ]
    end
  end

  defp validate_detail_action_spec(errors, action_id, action_spec, field_index)
       when is_map(action_spec) do
    errors
    |> validate_detail_action_name(action_id, action_spec)
    |> validate_detail_action_type(action_id, action_spec)
    |> validate_detail_action_payload(action_id, action_spec)
    |> validate_detail_action_required_fields(action_id, action_spec, field_index)
  end

  defp validate_detail_action_spec(errors, action_id, action_spec, _field_index) do
    [
      error(
        :invalid_detail_action_spec,
        [:detail_actions, action_id],
        "detail action #{inspect(action_id)} spec must be a map",
        expected: :map,
        actual: value_type(action_spec),
        action: action_id
      )
      | errors
    ]
  end

  defp validate_detail_action_name(errors, action_id, action_spec) do
    name = map_value(action_spec, :name)

    if non_empty_string?(name) do
      errors
    else
      [
        error(
          :invalid_detail_action_name,
          [:detail_actions, action_id, :name],
          "detail action #{inspect(action_id)} name must be a non-empty string",
          expected: "non-empty string",
          actual: value_type(name),
          action: action_id,
          name: name
        )
        | errors
      ]
    end
  end

  defp validate_detail_action_type(errors, action_id, action_spec) do
    type = map_value(action_spec, :type)

    if enum_value?(type, @detail_action_types) do
      errors
    else
      [
        error(
          :invalid_detail_action_type,
          [:detail_actions, action_id, :type],
          "detail action #{inspect(action_id)} type must be one of #{inspect(@detail_action_types)}",
          expected: @detail_action_types,
          actual: value_type(type),
          action: action_id,
          type: type
        )
        | errors
      ]
    end
  end

  defp validate_detail_action_payload(errors, action_id, action_spec) do
    raw_payload = fetch_map_value(action_spec, :payload)
    payload = detail_action_payload(action_spec)
    type = normalized_detail_action_type(map_value(action_spec, :type))

    errors
    |> validate_detail_action_payload_shape(action_id, raw_payload)
    |> validate_detail_action_url_template(action_id, type, payload)
    |> validate_detail_action_live_component_module(action_id, type, payload)
  end

  defp validate_detail_action_payload_shape(errors, _action_id, :__missing__), do: errors

  defp validate_detail_action_payload_shape(errors, _action_id, payload) when is_map(payload) do
    errors
  end

  defp validate_detail_action_payload_shape(errors, action_id, payload) do
    [
      error(
        :invalid_detail_action_payload,
        [:detail_actions, action_id, :payload],
        "detail action #{inspect(action_id)} payload must be a map when provided",
        expected: :map,
        actual: value_type(payload),
        action: action_id
      )
      | errors
    ]
  end

  defp validate_detail_action_url_template(errors, action_id, type, payload)
       when type in [:external_link, :iframe_modal] do
    url_template = map_value(payload, :url_template)

    if non_empty_string?(url_template) do
      errors
    else
      [
        error(
          :missing_detail_action_url_template,
          [:detail_actions, action_id, :payload, :url_template],
          "#{type} detail action #{inspect(action_id)} requires payload.url_template",
          expected: "non-empty string",
          actual: value_type(url_template),
          action: action_id,
          type: type
        )
        | errors
      ]
    end
  end

  defp validate_detail_action_url_template(errors, _action_id, _type, _payload), do: errors

  defp validate_detail_action_live_component_module(errors, action_id, :live_component, payload) do
    module = map_value(payload, :module)

    if is_atom(module) and not is_nil(module) do
      errors
    else
      [
        error(
          :missing_detail_action_module,
          [:detail_actions, action_id, :payload, :module],
          "live_component detail action #{inspect(action_id)} requires payload.module",
          expected: :atom,
          actual: value_type(module),
          action: action_id,
          type: :live_component
        )
        | errors
      ]
    end
  end

  defp validate_detail_action_live_component_module(errors, _action_id, _type, _payload),
    do: errors

  defp validate_detail_action_required_fields(errors, action_id, action_spec, field_index) do
    case fetch_map_value(action_spec, :required_fields) do
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
          error(
            :invalid_detail_action_required_fields,
            [:detail_actions, action_id, :required_fields],
            "detail action #{inspect(action_id)} required_fields must be a list when provided",
            expected: :list,
            actual: value_type(required_fields),
            action: action_id
          )
          | errors
        ]
    end
  end

  defp validate_detail_action_required_field(errors, action_id, field, index, field_index) do
    cond do
      not non_empty_atom_or_string?(field) ->
        [
          error(
            :invalid_detail_action_required_field,
            [:detail_actions, action_id, :required_fields, index],
            "detail action #{inspect(action_id)} required field must be a non-empty atom or string",
            expected: "non-empty atom or string",
            actual: value_type(field),
            action: action_id,
            field: field
          )
          | errors
        ]

      known_field?(field_index, field) ->
        errors

      true ->
        [
          error(
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

  defp detail_action_payload(action_spec) do
    case map_value(action_spec, :payload) do
      payload when is_map(payload) -> payload
      _payload -> %{}
    end
  end

  defp normalized_detail_action_type(type) do
    if enum_value?(type, @detail_action_types) do
      detail_action_type_id(type)
    end
  end

  defp detail_action_type_id(type) when is_atom(type), do: type

  defp detail_action_type_id(type) when is_binary(type), do: String.to_existing_atom(type)

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

  defp validate_query_capability_references(errors, query, detail_actions, capabilities) do
    errors
    |> validate_filter_capability_references(map_value(query, :filters), capabilities)
    |> validate_function_capability_references(map_value(query, :functions), capabilities)
    |> validate_query_member_capability_references(map_value(query, :query_members), capabilities)
    |> validate_published_view_capability_references(
      map_value(query, :published_views),
      capabilities
    )
    |> validate_detail_action_capability_references(detail_actions, capabilities)
  end

  defp validate_filter_capability_references(errors, filters, capabilities)
       when is_map(filters) do
    Enum.reduce(filters, errors, fn
      {filter_id, filter_config}, acc when is_map(filter_config) ->
        validate_capability_reference(
          acc,
          map_value(filter_config, :capability),
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

  defp validate_filter_capability_references(errors, _filters, _capabilities), do: errors

  defp validate_function_capability_references(errors, functions, capabilities)
       when is_map(functions) do
    Enum.reduce(functions, errors, fn
      {function_id, function_spec}, acc when is_map(function_spec) ->
        validate_capability_reference(
          acc,
          map_value(function_spec, :capability),
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

  defp validate_function_capability_references(errors, _functions, _capabilities), do: errors

  defp validate_query_member_capability_references(errors, query_members, capabilities)
       when is_map(query_members) do
    Enum.reduce(@query_member_groups, errors, fn group_key, acc ->
      case fetch_map_value(query_members, group_key) do
        members when is_map(members) ->
          Enum.reduce(members, acc, fn
            {member_id, member_spec}, member_acc when is_map(member_spec) ->
              validate_capability_reference(
                member_acc,
                map_value(member_spec, :capability),
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

  defp validate_query_member_capability_references(errors, _query_members, _capabilities),
    do: errors

  defp validate_published_view_capability_references(errors, published_views, capabilities)
       when is_map(published_views) do
    Enum.reduce(published_views, errors, fn
      {view_id, view_spec}, acc when is_map(view_spec) ->
        validate_capability_reference(
          acc,
          map_value(view_spec, :capability),
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

  defp validate_published_view_capability_references(errors, _published_views, _capabilities),
    do: errors

  defp validate_detail_action_capability_references(errors, detail_actions, capabilities)
       when is_map(detail_actions) do
    Enum.reduce(detail_actions, errors, fn
      {action_id, action_spec}, acc when is_map(action_spec) ->
        validate_capability_reference(
          acc,
          map_value(action_spec, :capability),
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

  defp validate_detail_action_capability_references(errors, _detail_actions, _capabilities),
    do: errors

  defp validate_capability_reference(
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

  defp validate_capability_reference(
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
    if is_map(capabilities) and fetch_key(capabilities, capability) != :error do
      errors
    else
      [
        error(
          missing_code,
          path,
          "#{subject} references missing capability #{inspect(capability)}",
          Keyword.put(attrs, :capability, capability)
        )
        | errors
      ]
    end
  end

  defp validate_capability_reference(
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
      error(
        invalid_code,
        path,
        "#{subject} capability must be an atom or string",
        Keyword.merge(attrs,
          expected: "atom or string",
          actual: value_type(capability),
          capability: capability
        )
      )
      | errors
    ]
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
    |> validate_source_relationship_source_path(relationship_id, relationship, path)
    |> validate_source_relationship_virtual_join(relationship_id, relationship, path, field_index)
    |> validate_source_relationship_filters(relationship_id, relationship, path)
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

  defp validate_source_relationship_source_path(errors, relationship_id, relationship, path) do
    case map_value(relationship, :source_path) do
      nil ->
        errors

      source_path ->
        if valid_static_source_path?(source_path) do
          errors
        else
          [
            error(
              :invalid_source_relationship_source_path,
              path ++ [:source_path],
              "source relationship #{inspect(relationship_id)} source_path must be a non-empty atom or dotted string path",
              expected: "non-empty atom or dotted string path",
              actual: value_type(source_path),
              source_relationship: relationship_id,
              source_path: source_path
            )
            | errors
          ]
        end
    end
  end

  defp validate_source_relationship_virtual_join(
         errors,
         relationship_id,
         relationship,
         path,
         field_index
       ) do
    case map_value(relationship, :virtual_join) do
      nil ->
        errors

      virtual_join when is_list(virtual_join) ->
        virtual_join
        |> Enum.with_index()
        |> Enum.reduce(errors, fn {entry, index}, acc ->
          validate_source_relationship_virtual_join_entry(
            acc,
            relationship_id,
            entry,
            path ++ [:virtual_join, index],
            field_index
          )
        end)

      virtual_join ->
        [
          error(
            :invalid_source_relationship_virtual_join,
            path ++ [:virtual_join],
            "source relationship #{inspect(relationship_id)} virtual_join must be a list",
            expected: :list,
            actual: value_type(virtual_join),
            source_relationship: relationship_id
          )
          | errors
        ]
    end
  end

  defp validate_source_relationship_virtual_join_entry(
         errors,
         relationship_id,
         entry,
         path,
         field_index
       )
       when is_map(entry) do
    errors
    |> validate_source_relationship_virtual_join_required_keys(relationship_id, entry, path)
    |> validate_source_relationship_virtual_join_working_field(
      relationship_id,
      entry,
      path,
      field_index
    )
    |> validate_source_relationship_virtual_join_source_field(relationship_id, entry, path)
    |> validate_source_relationship_virtual_join_required(relationship_id, entry, path)
  end

  defp validate_source_relationship_virtual_join_entry(
         errors,
         relationship_id,
         entry,
         path,
         _field_index
       ) do
    [
      error(
        :invalid_source_relationship_virtual_join_entry,
        path,
        "source relationship #{inspect(relationship_id)} virtual_join entries must be maps",
        expected: :map,
        actual: value_type(entry),
        source_relationship: relationship_id
      )
      | errors
    ]
  end

  defp validate_source_relationship_virtual_join_required_keys(
         errors,
         relationship_id,
         entry,
         path
       ) do
    missing_keys = Enum.reject([:working_field, :source_field], &has_key?(entry, &1))

    case missing_keys do
      [] ->
        errors

      _ ->
        [
          error(
            :source_relationship_virtual_join_missing_required_keys,
            path,
            "source relationship #{inspect(relationship_id)} virtual_join entry is missing required keys #{inspect(missing_keys)}",
            source_relationship: relationship_id,
            keys: missing_keys
          )
          | errors
        ]
    end
  end

  defp validate_source_relationship_virtual_join_working_field(
         errors,
         relationship_id,
         entry,
         path,
         field_index
       ) do
    case map_value(entry, :working_field) do
      nil ->
        errors

      working_field when is_atom(working_field) or is_binary(working_field) ->
        if known_field?(field_index, working_field) do
          errors
        else
          [
            error(
              :source_relationship_virtual_join_working_field_not_found,
              path ++ [:working_field],
              "source relationship #{inspect(relationship_id)} virtual_join working_field #{inspect(working_field)} is not defined in source, schemas, or custom columns",
              source_relationship: relationship_id,
              working_field: working_field
            )
            | errors
          ]
        end

      working_field ->
        [
          error(
            :invalid_source_relationship_virtual_join_working_field,
            path ++ [:working_field],
            "source relationship #{inspect(relationship_id)} virtual_join working_field must be an atom or string",
            expected: "atom or string",
            actual: value_type(working_field),
            source_relationship: relationship_id,
            working_field: working_field
          )
          | errors
        ]
    end
  end

  defp validate_source_relationship_virtual_join_source_field(
         errors,
         relationship_id,
         entry,
         path
       ) do
    case map_value(entry, :source_field) do
      nil ->
        errors

      source_field ->
        if valid_static_source_path?(source_field) do
          errors
        else
          [
            error(
              :invalid_source_relationship_virtual_join_source_field,
              path ++ [:source_field],
              "source relationship #{inspect(relationship_id)} virtual_join source_field must be a non-empty atom or dotted string path",
              expected: "non-empty atom or dotted string path",
              actual: value_type(source_field),
              source_relationship: relationship_id,
              source_field: source_field
            )
            | errors
          ]
        end
    end
  end

  defp validate_source_relationship_virtual_join_required(
         errors,
         relationship_id,
         entry,
         path
       ) do
    case map_value(entry, :required) do
      nil ->
        errors

      required when is_boolean(required) ->
        errors

      required ->
        [
          error(
            :invalid_source_relationship_virtual_join_required,
            path ++ [:required],
            "source relationship #{inspect(relationship_id)} virtual_join required must be a boolean",
            expected: :boolean,
            actual: value_type(required),
            source_relationship: relationship_id,
            required: required
          )
          | errors
        ]
    end
  end

  defp validate_source_relationship_filters(errors, relationship_id, relationship, path) do
    case map_value(relationship, :filters) do
      nil ->
        errors

      filters when is_list(filters) ->
        filters
        |> Enum.with_index()
        |> Enum.reduce(errors, fn {filter, index}, acc ->
          validate_static_filter_expression(
            acc,
            static_filter_owner(:source_relationship, relationship_id),
            filter,
            path ++ [:filters, index]
          )
        end)

      filters ->
        [
          error(
            :invalid_source_relationship_filters,
            path ++ [:filters],
            "source relationship #{inspect(relationship_id)} filters must be a list",
            expected: :list,
            actual: value_type(filters),
            source_relationship: relationship_id
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
       when is_tuple(filter) or is_list(filter) do
    validate_static_filter_expression(
      errors,
      static_filter_owner(:choice_source, choice_source_id),
      filter,
      path
    )
  end

  defp validate_choice_source_filter_expression(errors, choice_source_id, filter, path) do
    invalid_static_filter_expression(
      errors,
      static_filter_owner(:choice_source, choice_source_id),
      filter,
      path
    )
  end

  defp validate_static_filter_expression(errors, owner, filter, path) when is_tuple(filter) do
    validate_static_filter_parts(errors, owner, Tuple.to_list(filter), filter, path)
  end

  defp validate_static_filter_expression(errors, owner, filter, path) when is_list(filter) do
    validate_static_filter_parts(errors, owner, filter, filter, path)
  end

  defp validate_static_filter_expression(errors, owner, filter, path) do
    invalid_static_filter_expression(errors, owner, filter, path)
  end

  defp validate_static_filter_parts(
         errors,
         owner,
         [op, operands],
         _filter,
         path
       ) do
    cond do
      static_logical_filter_op?(op) ->
        validate_static_logical_filter(errors, owner, op, operands, path)

      static_unary_filter_op?(op) ->
        validate_static_filter_expression(
          errors,
          owner,
          operands,
          path ++ [:operand]
        )

      static_known_filter_op?(op) ->
        invalid_static_filter_operands(errors, owner, op, path, operands)

      static_filter_operator_value?(op) ->
        invalid_static_filter_operator(errors, owner, op, path)

      true ->
        invalid_static_filter_expression(errors, owner, [op, operands], path)
    end
  end

  defp validate_static_filter_parts(
         errors,
         owner,
         [op, field, _value],
         _filter,
         path
       ) do
    validate_static_field_filter(errors, owner, op, field, path)
  end

  defp validate_static_filter_parts(
         errors,
         owner,
         [op, field, _left, _right],
         _filter,
         path
       ) do
    validate_static_field_filter(errors, owner, op, field, path)
  end

  defp validate_static_filter_parts(
         errors,
         owner,
         [op | _] = filter,
         _raw,
         path
       ) do
    cond do
      static_known_filter_op?(op) ->
        invalid_static_filter_operands(errors, owner, op, path, filter)

      static_filter_operator_value?(op) ->
        invalid_static_filter_operator(errors, owner, op, path)

      true ->
        invalid_static_filter_expression(errors, owner, filter, path)
    end
  end

  defp validate_static_filter_parts(errors, owner, filter, _raw, path) do
    invalid_static_filter_expression(errors, owner, filter, path)
  end

  defp validate_static_logical_filter(errors, owner, _op, filters, path)
       when is_list(filters) do
    filters
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {filter, index}, acc ->
      validate_static_filter_expression(acc, owner, filter, path ++ [index])
    end)
  end

  defp validate_static_logical_filter(errors, owner, op, filters, path) do
    invalid_static_filter_operands(errors, owner, op, path, filters)
  end

  defp validate_static_field_filter(errors, owner, op, field, path) do
    cond do
      static_field_filter_op?(op) and valid_static_source_path?(field) ->
        errors

      static_field_filter_op?(op) ->
        [
          error(
            static_filter_error_code(owner, :path),
            path ++ [:field],
            "#{static_filter_subject(owner)} filter field must be a non-empty atom or dotted string path",
            static_filter_attrs(owner,
              expected: "non-empty atom or dotted string path",
              actual: value_type(field),
              field: field
            )
          )
          | errors
        ]

      static_known_filter_op?(op) ->
        invalid_static_filter_operands(errors, owner, op, path, field)

      static_filter_operator_value?(op) ->
        invalid_static_filter_operator(errors, owner, op, path)

      true ->
        invalid_static_filter_expression(errors, owner, [op, field], path)
    end
  end

  defp invalid_static_filter_operator(errors, owner, op, path) do
    [
      error(
        static_filter_error_code(owner, :operator),
        path,
        "#{static_filter_subject(owner)} filter operator #{inspect(op)} is not supported",
        static_filter_attrs(owner,
          expected: "known filter operator",
          actual: value_type(op),
          operator: op
        )
      )
      | errors
    ]
  end

  defp invalid_static_filter_operands(errors, owner, op, path, operands) do
    [
      error(
        static_filter_error_code(owner, :operands),
        path,
        "#{static_filter_subject(owner)} filter operator #{inspect(op)} has invalid operands",
        static_filter_attrs(owner,
          expected: "operator operands",
          actual: value_type(operands),
          operator: op
        )
      )
      | errors
    ]
  end

  defp invalid_static_filter_expression(errors, owner, filter, path) do
    [
      error(
        static_filter_error_code(owner, :expression),
        path,
        "#{static_filter_subject(owner)} filter must be an operator tuple or list",
        static_filter_attrs(owner,
          expected: "operator tuple or list",
          actual: value_type(filter),
          filter: filter
        )
      )
      | errors
    ]
  end

  defp static_known_filter_op?(op) do
    static_logical_filter_op?(op) or static_unary_filter_op?(op) or static_field_filter_op?(op)
  end

  defp static_logical_filter_op?(op), do: enum_value?(op, @logical_filter_ops)

  defp static_unary_filter_op?(op), do: enum_value?(op, @unary_filter_ops)

  defp static_field_filter_op?(op), do: enum_value?(op, @field_filter_ops)

  defp static_filter_operator_value?(op) when is_atom(op), do: not is_nil(op)

  defp static_filter_operator_value?(op) when is_binary(op), do: String.trim(op) != ""

  defp static_filter_operator_value?(_op), do: false

  defp static_filter_owner(:choice_source, id) do
    %{kind: :choice_source, id: id, attr: :choice_source, label: "choice source"}
  end

  defp static_filter_owner(:source_relationship, id) do
    %{
      kind: :source_relationship,
      id: id,
      attr: :source_relationship,
      label: "source relationship"
    }
  end

  defp static_filter_subject(owner), do: "#{owner.label} #{inspect(owner.id)}"

  defp static_filter_attrs(owner, attrs), do: Keyword.put(attrs, owner.attr, owner.id)

  defp static_filter_error_code(%{kind: :choice_source}, :operator),
    do: :invalid_choice_source_filter_operator

  defp static_filter_error_code(%{kind: :choice_source}, :path),
    do: :invalid_choice_source_filter_path

  defp static_filter_error_code(%{kind: :choice_source}, :expression),
    do: :invalid_choice_source_filter_expression

  defp static_filter_error_code(%{kind: :choice_source}, :operands),
    do: :invalid_choice_source_filter_operands

  defp static_filter_error_code(%{kind: :source_relationship}, :operator),
    do: :invalid_source_relationship_filter_operator

  defp static_filter_error_code(%{kind: :source_relationship}, :path),
    do: :invalid_source_relationship_filter_path

  defp static_filter_error_code(%{kind: :source_relationship}, :expression),
    do: :invalid_source_relationship_filter_expression

  defp static_filter_error_code(%{kind: :source_relationship}, :operands),
    do: :invalid_source_relationship_filter_operands

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

  defp valid_choice_source_path?(path), do: valid_static_source_path?(path)

  defp valid_static_source_path?(path) when is_atom(path), do: not is_nil(path)

  defp valid_static_source_path?(path) when is_binary(path) do
    trimmed_path = String.trim(path)

    trimmed_path != "" and not String.starts_with?(trimmed_path, ".") and
      not String.ends_with?(trimmed_path, ".") and not String.contains?(trimmed_path, "..")
  end

  defp valid_static_source_path?(_path), do: false

  defp enum_value?(value, allowed) when is_atom(value), do: value in allowed

  defp enum_value?(value, allowed) when is_binary(value) do
    Enum.any?(allowed, &(Atom.to_string(&1) == value))
  end

  defp enum_value?(_value, _allowed), do: false

  defp non_empty_atom_or_string?(value) when is_atom(value), do: not is_nil(value)

  defp non_empty_atom_or_string?(value) when is_binary(value), do: String.trim(value) != ""

  defp non_empty_atom_or_string?(_value), do: false

  defp non_empty_string?(value) when is_binary(value), do: String.trim(value) != ""

  defp non_empty_string?(_value), do: false

  defp valid_arity?(fun, arities) when is_function(fun) do
    Enum.any?(arities, &is_function(fun, &1))
  end

  defp valid_arity?(_value, _arities), do: false

  defp fetch_map_value(map, key) when is_map(map) and is_atom(key) do
    cond do
      Map.has_key?(map, key) ->
        Map.get(map, key)

      Map.has_key?(map, Atom.to_string(key)) ->
        Map.get(map, Atom.to_string(key))

      true ->
        :__missing__
    end
  end

  defp fetch_map_value(map, key) when is_map(map) do
    Map.get(map, key, :__missing__)
  end

  defp fetch_map_value(_map, _key), do: :__missing__

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
