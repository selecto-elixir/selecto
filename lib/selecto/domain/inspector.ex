defmodule Selecto.Domain.Inspector do
  @moduledoc false

  use Selecto.Domain.Constants

  alias Selecto.Domain.Shared.Map, as: MapHelpers
  alias Selecto.Domain.FieldBindings

  @projections [:query, :write, :ui, :api, :query_contract]
  @security_review_sections [
    actions: "business command definitions and execution surfaces",
    capabilities: "authorization capability catalog",
    choice_sources: "cross-domain choices and constraint policy",
    detail_actions: "user-visible detail actions",
    source_relationships: "cross-domain source bindings",
    writes:
      "write operations, fields, relationships, scope, hooks, validations, constraints, and transitions"
  ]

  def inspection_output(normalized, diagnostics) do
    field_choice_bindings = FieldBindings.field_choice_bindings(normalized)
    capability_usage = inspect_capability_usage(normalized)
    capabilities = inspect_capabilities(Map.get(normalized, :capabilities, %{}))

    %{
      schema_version: Map.fetch!(normalized, :schema_version),
      name: MapHelpers.map_value(Map.fetch!(normalized, :domain), :name),
      sections: inspection_sections(diagnostics),
      diagnostics: inspection_diagnostics(diagnostics),
      projections: @projections,
      counts: inspection_counts(normalized, field_choice_bindings, capability_usage, diagnostics),
      registries: inspection_registries(normalized),
      writes: inspect_writes(Map.get(normalized, :writes, %{})),
      actions: inspect_actions(Map.get(normalized, :actions, %{})),
      capabilities: capabilities,
      capability_usage: capability_usage,
      capability_visibility: inspect_capability_visibility(capabilities, capability_usage),
      security_review: inspect_security_review(normalized),
      source_relationships:
        inspect_source_relationships(Map.get(normalized, :source_relationships, %{})),
      choice_sources: inspect_choice_sources(Map.get(normalized, :choice_sources, %{})),
      field_choice_bindings: field_choice_bindings
    }
    |> MapHelpers.maybe_put(:domain_version, Map.get(normalized, :domain_version))
    |> MapHelpers.maybe_put(:domain_fingerprint, Map.get(normalized, :domain_fingerprint))
  end
  def inspection_sections(diagnostics) do
    %{
      canonical: diagnostics.canonical_sections,
      projection: diagnostics.projection_sections,
      proposed: diagnostics.proposed_sections,
      unknown: diagnostics.unknown_sections
    }
  end
  def inspection_diagnostics(diagnostics) do
    %{
      error_count: length(diagnostics.errors),
      warning_count: length(diagnostics.warnings),
      error_codes: diagnostics.errors |> diagnostic_codes() |> Enum.uniq(),
      warning_codes: diagnostics.warnings |> diagnostic_codes() |> Enum.uniq(),
      schema_version_inferred: diagnostics.schema_version_inferred
    }
  end
  def diagnostic_codes(diagnostics) do
    Enum.map(diagnostics, &Map.get(&1, :code))
  end
  def inspection_counts(normalized, field_choice_bindings, capability_usage, diagnostics) do
    query = Map.get(normalized, :query, %{})
    projection = Map.get(normalized, :projection, %{})
    writes = Map.get(normalized, :writes, %{})

    %{
      source_fields: length(MapHelpers.relation_field_ids(Map.get(normalized, :source))),
      schemas: MapHelpers.map_count(Map.get(normalized, :schemas)),
      joins: MapHelpers.map_count(Map.get(normalized, :joins)),
      filters: MapHelpers.map_count(MapHelpers.map_value(query, :filters)),
      functions: MapHelpers.map_count(MapHelpers.map_value(query, :functions)),
      query_members: MapHelpers.query_member_count(MapHelpers.map_value(query, :query_members)),
      custom_columns: MapHelpers.map_count(MapHelpers.map_value(projection, :custom_columns)),
      writes: %{
        operations: MapHelpers.map_count(MapHelpers.map_value(writes, :operations)),
        fields: MapHelpers.map_count(MapHelpers.map_value(writes, :fields)),
        relationships: MapHelpers.map_count(MapHelpers.map_value(writes, :relationships)),
        transitions: MapHelpers.map_count(MapHelpers.map_value(writes, :transitions)),
        validations: MapHelpers.list_count(MapHelpers.map_value(writes, :validations)),
        constraints: MapHelpers.list_count(MapHelpers.map_value(writes, :constraints)),
        scope: MapHelpers.map_count(MapHelpers.map_value(writes, :scope)),
        hooks: MapHelpers.map_count(MapHelpers.map_value(writes, :hooks))
      },
      actions: MapHelpers.map_count(Map.get(normalized, :actions)),
      capabilities: MapHelpers.map_count(Map.get(normalized, :capabilities)),
      capability_usages: length(capability_usage),
      source_relationships: MapHelpers.map_count(Map.get(normalized, :source_relationships)),
      choice_sources: MapHelpers.map_count(Map.get(normalized, :choice_sources)),
      field_choice_bindings: length(field_choice_bindings),
      warnings: length(diagnostics.warnings),
      errors: length(diagnostics.errors)
    }
  end
  def inspection_registries(normalized) do
    query = Map.get(normalized, :query, %{})
    projection = Map.get(normalized, :projection, %{})

    %{
      source_fields: MapHelpers.relation_field_ids(Map.get(normalized, :source)),
      schemas: MapHelpers.sorted_keys(Map.get(normalized, :schemas)),
      schema_fields: MapHelpers.schema_fields(Map.get(normalized, :schemas)),
      joins: MapHelpers.sorted_keys(Map.get(normalized, :joins)),
      filters: MapHelpers.sorted_keys(MapHelpers.map_value(query, :filters)),
      functions: MapHelpers.sorted_keys(MapHelpers.map_value(query, :functions)),
      query_members: MapHelpers.query_member_keys(MapHelpers.map_value(query, :query_members)),
      custom_columns: MapHelpers.sorted_keys(MapHelpers.map_value(projection, :custom_columns)),
      actions: MapHelpers.sorted_keys(Map.get(normalized, :actions)),
      capabilities: MapHelpers.sorted_keys(Map.get(normalized, :capabilities)),
      source_relationships: MapHelpers.sorted_keys(Map.get(normalized, :source_relationships)),
      choice_sources: MapHelpers.sorted_keys(Map.get(normalized, :choice_sources))
    }
  end
  def inspect_writes(writes) when is_map(writes) do
    %{
      operations: MapHelpers.sorted_keys(MapHelpers.map_value(writes, :operations)),
      fields: MapHelpers.sorted_keys(MapHelpers.map_value(writes, :fields)),
      relationships: MapHelpers.sorted_keys(MapHelpers.map_value(writes, :relationships)),
      transitions: MapHelpers.sorted_keys(MapHelpers.map_value(writes, :transitions)),
      validations_count: MapHelpers.list_count(MapHelpers.map_value(writes, :validations)),
      constraints_count: MapHelpers.list_count(MapHelpers.map_value(writes, :constraints)),
      scope: inspect_write_scope(MapHelpers.map_value(writes, :scope)),
      hooks: inspect_write_hooks(MapHelpers.map_value(writes, :hooks))
    }
  end

  def inspect_writes(_writes) do
    %{
      operations: [],
      fields: [],
      relationships: [],
      transitions: [],
      validations_count: 0,
      constraints_count: 0,
      scope: %{},
      hooks: []
    }
  end
  def inspect_write_scope(scope) when is_map(scope) do
    scope
    |> MapHelpers.sorted_entries()
    |> Enum.map(fn {scope_id, spec} ->
      {scope_id, inspect_write_scope_entry(scope_id, spec)}
    end)
    |> Map.new()
  end

  def inspect_write_scope(_scope), do: %{}
  def inspect_write_scope_entry(:tenant, spec) when is_map(spec) do
    sources = List.wrap(MapHelpers.map_value(spec, :satisfied_by) || MapHelpers.map_value(spec, :sources))

    scope =
      %{
        required: MapHelpers.map_value(spec, :required),
        field: MapHelpers.map_value(spec, :field) || MapHelpers.map_value(spec, :tenant_field)
      }
      |> MapHelpers.compact_nil()

    if sources == [], do: scope, else: Map.put(scope, :satisfied_by, sources)
  end

  def inspect_write_scope_entry("tenant", spec) when is_map(spec) do
    inspect_write_scope_entry(:tenant, spec)
  end

  def inspect_write_scope_entry(_scope_id, spec) when is_map(spec) do
    spec
    |> Enum.map(fn {key, value} -> {key, value} end)
    |> Map.new()
  end

  def inspect_write_scope_entry(_scope_id, spec), do: spec
  def inspect_write_hooks(hooks) when is_map(hooks) do
    hooks
    |> MapHelpers.sorted_entries()
    |> Enum.map(fn {hook_type, refs} ->
      hook_refs = List.wrap(refs)

      %{
        id: MapHelpers.field_id(hook_type),
        runtime: :host_code,
        portable: false,
        count: length(hook_refs),
        refs: Enum.map(hook_refs, &inspect_write_hook_ref/1)
      }
    end)
  end

  def inspect_write_hooks(_hooks), do: []
  def inspect_write_hook_ref(hook) when is_function(hook) do
    %{
      kind: :function,
      arity: MapHelpers.function_arity(hook)
    }
    |> MapHelpers.compact_nil()
  end

  def inspect_write_hook_ref({module, function}) when is_atom(module) and is_atom(function) do
    %{
      kind: :module_function,
      module: inspect(module),
      function: MapHelpers.field_id(function),
      extra_args: 0
    }
  end

  def inspect_write_hook_ref({module, function, args})
       when is_atom(module) and is_atom(function) and is_list(args) do
    %{
      kind: :module_function,
      module: inspect(module),
      function: MapHelpers.field_id(function),
      extra_args: length(args)
    }
  end

  def inspect_write_hook_ref(hook) do
    %{
      kind: :term,
      inspect: inspect(hook)
    }
  end
  def inspect_actions(actions) when is_map(actions) do
    actions
    |> MapHelpers.sorted_entries()
    |> Enum.map(fn {id, action} ->
      %{
        id: id,
        type: MapHelpers.map_value(action, :type),
        capability: MapHelpers.map_value(action, :capability),
        transition: MapHelpers.map_value(action, :transition)
      }
    end)
  end

  def inspect_actions(_actions), do: []
  def inspect_capabilities(capabilities) when is_map(capabilities) do
    capabilities
    |> MapHelpers.sorted_entries()
    |> Enum.map(fn {id, capability} ->
      %{
        id: id,
        operations: List.wrap(MapHelpers.map_value(capability, :operations)),
        action: MapHelpers.map_value(capability, :action)
      }
    end)
  end

  def inspect_capabilities(_capabilities), do: []
  def inspect_capability_visibility(capabilities, capability_usage) do
    catalog_ids =
      capabilities
      |> Enum.map(&normalize_capability_id(MapHelpers.map_value(&1, :id)))
      |> Enum.reject(&is_nil/1)
      |> Enum.sort()

    referenced_ids =
      capability_usage
      |> Enum.map(&normalize_capability_id(MapHelpers.map_value(&1, :capability)))
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()
      |> Enum.sort()

    %{
      format_version: 1,
      summary: %{
        catalog_count: length(catalog_ids),
        referenced_count: length(referenced_ids),
        unreferenced_capabilities: catalog_ids -- referenced_ids,
        undeclared_references: referenced_ids -- catalog_ids,
        runtime_policy: :not_sampled
      },
      defaults: %{
        undeclared_capability: :allow,
        declared_without_resolver: :inspect_only,
        runtime_decision_source: :none
      },
      catalog:
        capabilities
        |> Enum.map(fn capability ->
          capability_visibility_entry(
            capability,
            capability_visibility_references(capability_usage, MapHelpers.map_value(capability, :id))
          )
        end)
        |> Enum.sort_by(&to_string(MapHelpers.map_value(&1, :id) || "")),
      references:
        capability_usage
        |> Enum.map(&capability_visibility_reference/1)
        |> Enum.sort_by(
          &{to_string(MapHelpers.map_value(&1, :capability) || ""), to_string(MapHelpers.map_value(&1, :path) || "")}
        )
    }
  end
  def capability_visibility_entry(capability, references) do
    %{
      id: normalize_capability_id(MapHelpers.map_value(capability, :id)),
      operations:
        capability
        |> MapHelpers.map_value(:operations)
        |> List.wrap()
        |> Enum.map(&normalize_capability_id/1),
      referenced_by: references,
      runtime_samples: []
    }
    |> maybe_put_nonempty(:actions, capability_actions(capability))
  end
  def capability_actions(capability) do
    []
    |> Kernel.++(List.wrap(MapHelpers.map_value(capability, :action)))
    |> Kernel.++(List.wrap(MapHelpers.map_value(capability, :actions)))
    |> Enum.map(&normalize_capability_id/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> Enum.sort()
  end
  def capability_visibility_references(capability_usage, capability_id) do
    capability_id = normalize_capability_id(capability_id)

    capability_usage
    |> Enum.filter(&(normalize_capability_id(MapHelpers.map_value(&1, :capability)) == capability_id))
    |> Enum.map(&capability_visibility_reference/1)
  end
  def capability_visibility_reference(usage) do
    %{
      capability: normalize_capability_id(MapHelpers.map_value(usage, :capability)),
      section: normalize_capability_id(MapHelpers.map_value(usage, :section)),
      role: normalize_capability_id(MapHelpers.map_value(usage, :role)),
      id: normalize_capability_id(MapHelpers.map_value(usage, :id) || MapHelpers.map_value(usage, :field)),
      group: normalize_capability_id(MapHelpers.map_value(usage, :group)),
      path: usage |> MapHelpers.map_value(:path) |> format_capability_path()
    }
    |> MapHelpers.compact_nil()
  end
  def format_capability_path(path) when is_list(path), do: Enum.map_join(path, ".", &MapHelpers.field_id/1)
  def format_capability_path(path) when is_nil(path), do: nil
  def format_capability_path(path), do: to_string(path)
  def normalize_capability_id(nil), do: nil
  def normalize_capability_id(value) when is_binary(value), do: value
  def normalize_capability_id(value) when is_atom(value), do: Atom.to_string(value)
  def normalize_capability_id(value), do: to_string(value)
  def maybe_put_nonempty(map, _key, []), do: map
  def maybe_put_nonempty(map, key, value), do: Map.put(map, key, value)
  def inspect_security_review(normalized) do
    @security_review_sections
    |> Enum.flat_map(fn
      {:writes, reason} ->
        inspect_security_writes(Map.get(normalized, :writes), reason)

      {section, reason} ->
        inspect_security_registry(section, Map.get(normalized, section), reason)
    end)
  end
  def inspect_security_registry(section, registry, reason) when is_map(registry) do
    items = MapHelpers.sorted_keys(registry)

    case items do
      [] ->
        []

      items ->
        [
          %{
            section: section,
            count: length(items),
            items: items,
            reason: reason
          }
        ]
    end
  end

  def inspect_security_registry(_section, _registry, _reason), do: []
  def inspect_security_writes(writes, reason) when is_map(writes) do
    items = inspect_writes(writes)

    count =
      length(Map.fetch!(items, :operations)) +
        length(Map.fetch!(items, :fields)) +
        length(Map.fetch!(items, :relationships)) +
        length(Map.fetch!(items, :transitions)) +
        Map.fetch!(items, :validations_count) +
        Map.fetch!(items, :constraints_count) +
        MapHelpers.map_count(Map.fetch!(items, :scope)) +
        length(Map.fetch!(items, :hooks))

    if count > 0 do
      [%{section: :writes, count: count, items: items, reason: reason}]
    else
      []
    end
  end

  def inspect_security_writes(_writes, _reason), do: []
  def inspect_capability_usage(normalized) do
    query = Map.get(normalized, :query, %{})
    projection = Map.get(normalized, :projection, %{})

    []
    |> Kernel.++(
      inspect_relation_capability_usage(:source, Map.get(normalized, :source), [:source])
    )
    |> Kernel.++(inspect_schema_capability_usage(Map.get(normalized, :schemas)))
    |> Kernel.++(
      inspect_capability_section_usage(
        :custom_columns,
        MapHelpers.map_value(projection, :custom_columns),
        :custom_column,
        [:custom_columns]
      )
    )
    |> Kernel.++(
      inspect_capability_section_usage(:filters, MapHelpers.map_value(query, :filters), :query_filter, [
        :filters
      ])
    )
    |> Kernel.++(
      inspect_capability_section_usage(
        :functions,
        MapHelpers.map_value(query, :functions),
        :query_function,
        [:functions]
      )
    )
    |> Kernel.++(inspect_query_member_capability_usage(MapHelpers.map_value(query, :query_members)))
    |> Kernel.++(
      inspect_capability_section_usage(
        :published_views,
        MapHelpers.map_value(query, :published_views),
        :published_view,
        [:published_views]
      )
    )
    |> Kernel.++(
      inspect_capability_section_usage(
        :detail_actions,
        Map.get(normalized, :detail_actions),
        :detail_action,
        [:detail_actions]
      )
    )
    |> Kernel.++(
      inspect_capability_section_usage(:actions, Map.get(normalized, :actions), :action, [
        :actions
      ])
    )
    |> Kernel.++(
      inspect_capability_section_usage(
        :choice_sources,
        Map.get(normalized, :choice_sources),
        :choice_source,
        [:choice_sources]
      )
    )
    |> Enum.sort_by(&capability_usage_sort_key/1)
  end
  def inspect_schema_capability_usage(schemas) when is_map(schemas) do
    schemas
    |> MapHelpers.sorted_entries()
    |> Enum.flat_map(fn {schema_id, schema} ->
      inspect_relation_capability_usage(:schemas, schema, [:schemas, schema_id])
    end)
  end

  def inspect_schema_capability_usage(_schemas), do: []
  def inspect_relation_capability_usage(section, relation, path_prefix) when is_map(relation) do
    relation
    |> MapHelpers.relation_field_entries()
    |> Enum.flat_map(fn {field, column} ->
      capability_usage_entries(MapHelpers.map_value(column, :capability), %{
        section: section,
        role: :field,
        field: field,
        path: path_prefix ++ [:columns, field, :capability]
      })
    end)
  end

  def inspect_relation_capability_usage(_section, _relation, _path_prefix), do: []
  def inspect_capability_section_usage(section, registry, role, path_prefix)
       when is_map(registry) do
    registry
    |> MapHelpers.sorted_entries()
    |> Enum.flat_map(fn {id, spec} ->
      capability_usage_entries(MapHelpers.map_value(spec, :capability), %{
        section: section,
        role: role,
        id: id,
        path: path_prefix ++ [id, :capability]
      })
    end)
  end

  def inspect_capability_section_usage(_section, _registry, _role, _path_prefix), do: []
  def inspect_query_member_capability_usage(query_members) when is_map(query_members) do
    Enum.flat_map(@query_member_groups, fn group ->
      case MapHelpers.map_value(query_members, group) do
        members when is_map(members) ->
          members
          |> MapHelpers.sorted_entries()
          |> Enum.flat_map(fn {id, member} ->
            capability_usage_entries(MapHelpers.map_value(member, :capability), %{
              section: :query_members,
              role: :query_member,
              group: group,
              id: id,
              path: [:query_members, group, id, :capability]
            })
          end)

        _members ->
          []
      end
    end)
  end

  def inspect_query_member_capability_usage(_query_members), do: []
  def capability_usage_entries(capability, attrs)
       when not is_nil(capability) and (is_atom(capability) or is_binary(capability)) do
    [Map.put(attrs, :capability, capability)]
  end

  def capability_usage_entries(_capability, _attrs), do: []
  def capability_usage_sort_key(usage) do
    path =
      usage
      |> Map.fetch!(:path)
      |> Enum.map(&MapHelpers.field_id/1)
      |> Enum.join(".")

    {MapHelpers.field_id(Map.fetch!(usage, :capability)), path}
  end
  def inspect_source_relationships(source_relationships) when is_map(source_relationships) do
    source_relationships
    |> MapHelpers.sorted_entries()
    |> Enum.map(fn {id, relationship} ->
      %{
        id: id,
        target_domain: MapHelpers.map_value(relationship, :target_domain),
        source_field: MapHelpers.map_value(relationship, :source_field),
        target_field: MapHelpers.map_value(relationship, :target_field),
        source_path: MapHelpers.map_value(relationship, :source_path),
        virtual_join_count: MapHelpers.list_count(MapHelpers.map_value(relationship, :virtual_join)),
        filters_count: MapHelpers.list_count(MapHelpers.map_value(relationship, :filters))
      }
    end)
  end

  def inspect_source_relationships(_source_relationships), do: []
  def inspect_choice_sources(choice_sources) when is_map(choice_sources) do
    choice_sources
    |> MapHelpers.sorted_entries()
    |> Enum.map(fn {id, choice_source} ->
      %{
        id: id,
        domain: MapHelpers.map_value(choice_source, :domain),
        source_relationship: MapHelpers.map_value(choice_source, :source_relationship),
        value_field: MapHelpers.map_value(choice_source, :value_field),
        label_field: MapHelpers.map_value(choice_source, :label_field),
        source_path: MapHelpers.map_value(choice_source, :source_path),
        filters_count: MapHelpers.list_count(MapHelpers.map_value(choice_source, :filters)),
        order_by_count: MapHelpers.list_count(MapHelpers.map_value(choice_source, :order_by)),
        presentation: MapHelpers.map_value(choice_source, :presentation),
        constraint_policy: MapHelpers.map_value(choice_source, :constraint_policy)
      }
    end)
  end

  def inspect_choice_sources(_choice_sources), do: []
end
