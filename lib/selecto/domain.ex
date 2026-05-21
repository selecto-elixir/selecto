defmodule Selecto.Domain do
  @moduledoc """
  Compatibility-safe normalization entry point for Selecto domains.

  This module does not participate in `Selecto.configure/3` yet. It provides a
  read-only normalization boundary for callers that want a stable, diagnostic
  view of authored domain maps while existing runtime behavior remains
  unchanged.
  """

  alias Selecto.Domain.Diagnostics
  alias Selecto.Domain.Sections

  @current_schema_version 1
  @projections [:query, :write, :ui, :api, :query_contract]
  @query_member_groups [:ctes, :values, :subqueries, :laterals, :unnests]
  @query_contract_numeric_types ~w(integer float decimal)
  @query_contract_temporal_types ~w(date time datetime naive_datetime utc_datetime)
  @query_contract_text_types ~w(string text)
  @query_contract_exact_types ~w(boolean uuid enum)
  @query_contract_sortable_types @query_contract_numeric_types ++
                                   @query_contract_temporal_types ++
                                   @query_contract_text_types ++
                                   @query_contract_exact_types
  @query_contract_groupable_types @query_contract_numeric_types ++
                                    @query_contract_temporal_types ++
                                    ["string"] ++
                                    @query_contract_exact_types
  @query_projection_sections [
    :custom_columns,
    :jsonb_schemas,
    :subfilters,
    :window_functions,
    :pagination,
    :retarget
  ]
  @ui_query_sections [
    :default_selected,
    :required_selected,
    :required_filters,
    :required_order_by,
    :required_group_by,
    :filters
  ]
  @ui_projection_sections [
    :columns,
    :custom_columns,
    :jsonb_schemas,
    :pagination,
    :redact_fields
  ]
  @api_projection_sections [
    :columns,
    :custom_columns,
    :jsonb_schemas,
    :subfilters,
    :window_functions,
    :pagination,
    :retarget,
    :redact_fields
  ]
  @map_sections [
    :source,
    :schemas,
    :joins,
    :filters,
    :functions,
    :query_members,
    :published_views,
    :detail_actions,
    :columns,
    :custom_columns,
    :jsonb_schemas,
    :subfilters,
    :window_functions,
    :pagination,
    :retarget,
    :writes,
    :actions,
    :capabilities,
    :source_relationships,
    :choice_sources
  ]
  @list_sections [
    :default_selected,
    :required_selected,
    :required_filters,
    :required_order_by,
    :required_group_by,
    :redact_fields,
    :extensions
  ]
  @known_sections [
    :schema_version,
    :domain_version,
    :domain_fingerprint,
    :name | @map_sections ++ @list_sections
  ]
  @collision_warning_sections [
    :actions,
    :capabilities,
    :source_relationships,
    :choice_sources
  ]
  @security_review_sections [
    actions: "business command definitions and execution surfaces",
    capabilities: "authorization capability catalog",
    choice_sources: "cross-domain choices and constraint policy",
    detail_actions: "user-visible detail actions",
    source_relationships: "cross-domain source bindings",
    writes:
      "write operations, fields, relationships, scope, hooks, validations, constraints, and transitions"
  ]

  @doc """
  Normalizes an authored domain map into a compatibility-safe contract.

  The normalizer currently:

  - infers `schema_version` as `1` when it is missing
  - preserves optional `domain_version` metadata as an opaque authored-domain
    version label
  - preserves optional `domain_fingerprint` metadata as an opaque authored-domain
    content identity label
  - expands supported field-level choice-source shorthand into canonical
    `source_relationships`, `choice_sources`, and field reference bindings
  - classifies authored top-level sections as canonical, projection, proposed,
    or unknown
  - exposes current query, write, action, capability, relationship, and choice
    registries without rewriting existing runtime behavior

  Returns `{:ok, normalized, diagnostics}` for maps and `{:error, diagnostics}`
  for non-map inputs.
  """
  @spec normalize(term()) :: {:ok, map(), Diagnostics.t()} | {:error, Diagnostics.t()}
  def normalize(domain) when is_map(domain) do
    {schema_version, schema_version_inferred, schema_version_warnings} = schema_version(domain)
    domain_version = domain_version(domain)
    domain_fingerprint = domain_fingerprint(domain)
    sections = Sections.classify_top_level_keys(domain)

    diagnostics =
      Diagnostics.new(
        warnings: schema_version_warnings ++ section_shape_warnings(domain),
        sections: sections,
        schema_version: schema_version,
        schema_version_inferred: schema_version_inferred
      )

    canonical_domain =
      domain
      |> Map.put(:schema_version, schema_version)
      |> maybe_put_domain_version(domain_version)
      |> maybe_put_domain_fingerprint(domain_fingerprint)
      |> normalize_authoring_shorthand()

    {:ok,
     normalized_domain(
       domain,
       canonical_domain,
       schema_version,
       domain_version,
       domain_fingerprint,
       sections
     ), diagnostics}
  end

  def normalize(_domain) do
    diagnostics =
      Diagnostics.new(
        errors: [
          %{
            code: :invalid_domain,
            message: "Selecto domains must be maps"
          }
        ]
      )

    {:error, diagnostics}
  end

  @doc """
  Normalizes an authored domain and validates it against the first-wave
  canonical contract.

  This is still a compatibility-safe entry point: it does not participate in
  `Selecto.configure/3` unless a caller opts in elsewhere.
  """
  @spec validate(term()) :: {:ok, map(), Diagnostics.t()} | {:error, Diagnostics.t()}
  def validate(domain) do
    with {:ok, normalized, diagnostics} <- normalize(domain) do
      case Selecto.Domain.Contract.errors(normalized) do
        [] ->
          {:ok, normalized, diagnostics}

        errors ->
          {:error, %{diagnostics | errors: diagnostics.errors ++ errors}}
      end
    end
  end

  @doc """
  Composes an authored domain with one or more domain overlays.

  This is the Stage 2 composition boundary. It is opt-in and does not participate
  in `Selecto.configure/3` yet. Composition uses explicit, deterministic merge
  semantics:

  - maps deep-merge by section
  - `redact_fields`, including `source.redact_fields`, are unioned
  - `extensions` are appended uniquely
  - other lists and scalar values are replaced by later overlays

  After overlays are merged, declared extension `merge_domain/2` callbacks are
  applied in declaration order and the result is normalized again.
  """
  @spec compose(term(), term()) :: {:ok, map(), Diagnostics.t()} | {:error, Diagnostics.t()}
  def compose(domain, overlays \\ []) do
    with {:ok, normalized, _diagnostics} <- normalize(domain),
         {:ok, overlays} <- domain_overlays(overlays) do
      {composed_domain, composition_warnings} =
        overlays
        |> Enum.with_index()
        |> Enum.reduce({normalized.domain, []}, fn {overlay, index}, {acc, warnings} ->
          overlay = normalize_authoring_shorthand(overlay)

          {
            merge_domain_maps(acc, overlay),
            warnings ++ composition_collision_warnings(acc, overlay, index)
          }
        end)

      composed_domain = apply_composed_extensions(composed_domain)

      with {:ok, normalized, diagnostics} <- normalize(composed_domain) do
        {:ok, normalized, %{diagnostics | warnings: composition_warnings ++ diagnostics.warnings}}
      end
    end
  end

  @doc """
  Returns structured inspection output for an authored or normalized domain.

  The inspection map is intentionally compact and deterministic so generators,
  Studio, docs, and tests can reason about the normalized contract without
  walking the whole domain map directly.
  """
  @spec describe(term()) :: {:ok, map(), Diagnostics.t()} | {:error, Diagnostics.t()}
  def describe(
        %{
          schema_version: schema_version,
          domain: %{} = _domain,
          query: %{} = _query,
          projection: %{} = _projection,
          sections: sections
        } = normalized
      ) do
    diagnostics =
      Diagnostics.new(
        sections: sections,
        schema_version: schema_version,
        schema_version_inferred: false
      )

    {:ok, inspection_output(normalized, diagnostics), diagnostics}
  end

  def describe(domain) do
    with {:ok, normalized, diagnostics} <- normalize(domain) do
      {:ok, inspection_output(normalized, diagnostics), diagnostics}
    end
  end

  @doc """
  Returns the constrained query contract projection for an authored or normalized
  domain.

  This is a convenience wrapper around `normalize/1` and
  `project(normalized, :query_contract)` for Components, AI tooling, and other
  consumers that should not need to walk the normalized domain map directly.
  """
  @spec query_contract(term()) :: {:ok, map(), Diagnostics.t()} | {:error, Diagnostics.t()}
  def query_contract(
        %{
          schema_version: schema_version,
          domain: %{} = _domain,
          query: %{} = _query,
          projection: %{} = _projection,
          sections: sections
        } = normalized
      ) do
    diagnostics =
      Diagnostics.new(
        sections: sections,
        schema_version: schema_version,
        schema_version_inferred: false
      )

    {:ok, project(normalized, :query_contract), diagnostics}
  end

  def query_contract(domain) do
    with {:ok, normalized, diagnostics} <- normalize(domain) do
      {:ok, project(normalized, :query_contract), diagnostics}
    end
  end

  @doc """
  Projects a normalized domain into a read-only consumer view.

  Projection helpers are intentionally conservative in this slice. They reshape
  the normalized map for future consumers, but no existing runtime path calls
  them yet.

  Supported projections:

  - `:query` - query/runtime-facing sections
  - `:write` - write/action/reference sections
  - `:ui` - display defaults, choices, actions, and detail actions
  - `:api` - read/write/action contract for API-style consumers
  - `:query_contract` - constrained query metadata for tools, Components, and AI
  """
  @spec project(map(), :query | :write | :ui | :api | :query_contract) :: map()
  def project(%{schema_version: _schema_version, domain: _domain} = normalized, :query) do
    normalized
    |> base_projection()
    |> Map.merge(Map.fetch!(normalized, :query))
    |> Map.merge(take_projection_sections(normalized, @query_projection_sections))
  end

  def project(
        %{schema_version: _schema_version, domain: _domain, query: %{} = query} = normalized,
        :query_contract
      ) do
    field_choice_bindings = field_choice_bindings(normalized)

    %{
      schema_version: Map.fetch!(normalized, :schema_version),
      name: map_section(Map.fetch!(normalized, :domain), :name),
      projection: :query_contract,
      source: query_contract_source(normalized),
      fields: query_contract_fields(normalized, field_choice_bindings),
      joins: query_contract_joins(normalized),
      defaults: query_contract_defaults(query),
      filters: query_contract_filters(map_value(query, :filters)),
      functions: query_contract_functions(map_value(query, :functions)),
      query_members: query_contract_query_members(map_value(query, :query_members)),
      published_views: query_contract_published_views(map_value(query, :published_views)),
      source_relationships:
        query_contract_source_relationships(Map.get(normalized, :source_relationships, %{})),
      choice_sources: query_contract_choice_sources(Map.get(normalized, :choice_sources, %{})),
      field_choice_bindings: query_contract_choice_bindings(field_choice_bindings),
      capability_ids: sorted_keys(Map.get(normalized, :capabilities, %{}))
    }
    |> maybe_put(:domain_version, Map.get(normalized, :domain_version))
    |> maybe_put(:domain_fingerprint, Map.get(normalized, :domain_fingerprint))
  end

  def project(%{schema_version: _schema_version, domain: _domain} = normalized, :write) do
    normalized
    |> base_projection()
    |> Map.merge(%{
      columns: projection_section(normalized, :columns, %{}),
      writes: Map.get(normalized, :writes, %{}),
      actions: Map.get(normalized, :actions, %{}),
      capabilities: Map.get(normalized, :capabilities, %{}),
      source_relationships: Map.get(normalized, :source_relationships, %{}),
      choice_sources: Map.get(normalized, :choice_sources, %{})
    })
  end

  def project(%{schema_version: _schema_version, domain: _domain} = normalized, :ui) do
    normalized
    |> base_projection()
    |> Map.merge(take_query_sections(normalized, @ui_query_sections))
    |> Map.merge(take_projection_sections(normalized, @ui_projection_sections))
    |> Map.merge(%{
      detail_actions: Map.get(normalized, :detail_actions, %{}),
      actions: Map.get(normalized, :actions, %{}),
      capabilities: Map.get(normalized, :capabilities, %{}),
      choice_sources: Map.get(normalized, :choice_sources, %{})
    })
  end

  def project(%{schema_version: _schema_version, domain: _domain} = normalized, :api) do
    normalized
    |> base_projection()
    |> Map.merge(Map.fetch!(normalized, :query))
    |> Map.merge(take_projection_sections(normalized, @api_projection_sections))
    |> Map.merge(%{
      writes: Map.get(normalized, :writes, %{}),
      actions: Map.get(normalized, :actions, %{}),
      capabilities: Map.get(normalized, :capabilities, %{}),
      source_relationships: Map.get(normalized, :source_relationships, %{}),
      choice_sources: Map.get(normalized, :choice_sources, %{}),
      detail_actions: Map.get(normalized, :detail_actions, %{})
    })
  end

  def project(%{schema_version: _schema_version, domain: _domain}, projection)
      when projection not in @projections do
    raise ArgumentError,
          "unknown Selecto domain projection #{inspect(projection)}; expected one of #{inspect(@projections)}"
  end

  def project(_normalized, projection) do
    raise ArgumentError,
          "expected a normalized Selecto domain from Selecto.Domain.normalize/1 before projecting #{inspect(projection)}"
  end

  defp normalized_domain(
         authored_domain,
         canonical_domain,
         schema_version,
         domain_version,
         domain_fingerprint,
         sections
       ) do
    %{
      schema_version: schema_version,
      domain_version: domain_version,
      domain_fingerprint: domain_fingerprint,
      authored_domain: authored_domain,
      domain: canonical_domain,
      sections: sections,
      source: section(canonical_domain, :source),
      schemas: section(canonical_domain, :schemas, %{}),
      joins: section(canonical_domain, :joins, %{}),
      query: query_sections(canonical_domain),
      projection: projection_sections(canonical_domain),
      writes: section(canonical_domain, :writes, %{}),
      actions: section(canonical_domain, :actions, %{}),
      capabilities: section(canonical_domain, :capabilities, %{}),
      source_relationships: section(canonical_domain, :source_relationships, %{}),
      choice_sources: section(canonical_domain, :choice_sources, %{}),
      detail_actions: section(canonical_domain, :detail_actions, %{}),
      domain_data: section(canonical_domain, :domain_data, %{}),
      extensions: section(canonical_domain, :extensions, [])
    }
  end

  defp inspection_output(normalized, diagnostics) do
    field_choice_bindings = field_choice_bindings(normalized)
    capability_usage = inspect_capability_usage(normalized)
    capabilities = inspect_capabilities(Map.get(normalized, :capabilities, %{}))

    %{
      schema_version: Map.fetch!(normalized, :schema_version),
      name: map_value(Map.fetch!(normalized, :domain), :name),
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
    |> maybe_put(:domain_version, Map.get(normalized, :domain_version))
    |> maybe_put(:domain_fingerprint, Map.get(normalized, :domain_fingerprint))
  end

  defp inspection_sections(diagnostics) do
    %{
      canonical: diagnostics.canonical_sections,
      projection: diagnostics.projection_sections,
      proposed: diagnostics.proposed_sections,
      unknown: diagnostics.unknown_sections
    }
  end

  defp inspection_diagnostics(diagnostics) do
    %{
      error_count: length(diagnostics.errors),
      warning_count: length(diagnostics.warnings),
      error_codes: diagnostics.errors |> diagnostic_codes() |> Enum.uniq(),
      warning_codes: diagnostics.warnings |> diagnostic_codes() |> Enum.uniq(),
      schema_version_inferred: diagnostics.schema_version_inferred
    }
  end

  defp diagnostic_codes(diagnostics) do
    Enum.map(diagnostics, &Map.get(&1, :code))
  end

  defp inspection_counts(normalized, field_choice_bindings, capability_usage, diagnostics) do
    query = Map.get(normalized, :query, %{})
    projection = Map.get(normalized, :projection, %{})
    writes = Map.get(normalized, :writes, %{})

    %{
      source_fields: length(relation_field_ids(Map.get(normalized, :source))),
      schemas: map_count(Map.get(normalized, :schemas)),
      joins: map_count(Map.get(normalized, :joins)),
      filters: map_count(map_value(query, :filters)),
      functions: map_count(map_value(query, :functions)),
      query_members: query_member_count(map_value(query, :query_members)),
      custom_columns: map_count(map_value(projection, :custom_columns)),
      writes: %{
        operations: map_count(map_value(writes, :operations)),
        fields: map_count(map_value(writes, :fields)),
        relationships: map_count(map_value(writes, :relationships)),
        transitions: map_count(map_value(writes, :transitions)),
        validations: list_count(map_value(writes, :validations)),
        constraints: list_count(map_value(writes, :constraints)),
        scope: map_count(map_value(writes, :scope)),
        hooks: map_count(map_value(writes, :hooks))
      },
      actions: map_count(Map.get(normalized, :actions)),
      capabilities: map_count(Map.get(normalized, :capabilities)),
      capability_usages: length(capability_usage),
      source_relationships: map_count(Map.get(normalized, :source_relationships)),
      choice_sources: map_count(Map.get(normalized, :choice_sources)),
      field_choice_bindings: length(field_choice_bindings),
      warnings: length(diagnostics.warnings),
      errors: length(diagnostics.errors)
    }
  end

  defp inspection_registries(normalized) do
    query = Map.get(normalized, :query, %{})
    projection = Map.get(normalized, :projection, %{})

    %{
      source_fields: relation_field_ids(Map.get(normalized, :source)),
      schemas: sorted_keys(Map.get(normalized, :schemas)),
      schema_fields: schema_fields(Map.get(normalized, :schemas)),
      joins: sorted_keys(Map.get(normalized, :joins)),
      filters: sorted_keys(map_value(query, :filters)),
      functions: sorted_keys(map_value(query, :functions)),
      query_members: query_member_keys(map_value(query, :query_members)),
      custom_columns: sorted_keys(map_value(projection, :custom_columns)),
      actions: sorted_keys(Map.get(normalized, :actions)),
      capabilities: sorted_keys(Map.get(normalized, :capabilities)),
      source_relationships: sorted_keys(Map.get(normalized, :source_relationships)),
      choice_sources: sorted_keys(Map.get(normalized, :choice_sources))
    }
  end

  defp inspect_writes(writes) when is_map(writes) do
    %{
      operations: sorted_keys(map_value(writes, :operations)),
      fields: sorted_keys(map_value(writes, :fields)),
      relationships: sorted_keys(map_value(writes, :relationships)),
      transitions: sorted_keys(map_value(writes, :transitions)),
      validations_count: list_count(map_value(writes, :validations)),
      constraints_count: list_count(map_value(writes, :constraints)),
      scope: inspect_write_scope(map_value(writes, :scope)),
      hooks: inspect_write_hooks(map_value(writes, :hooks))
    }
  end

  defp inspect_writes(_writes) do
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

  defp inspect_write_scope(scope) when is_map(scope) do
    scope
    |> sorted_entries()
    |> Enum.map(fn {scope_id, spec} ->
      {scope_id, inspect_write_scope_entry(scope_id, spec)}
    end)
    |> Map.new()
  end

  defp inspect_write_scope(_scope), do: %{}

  defp inspect_write_scope_entry(:tenant, spec) when is_map(spec) do
    sources = List.wrap(map_value(spec, :satisfied_by) || map_value(spec, :sources))

    scope =
      %{
        required: map_value(spec, :required),
        field: map_value(spec, :field) || map_value(spec, :tenant_field)
      }
      |> compact_nil()

    if sources == [], do: scope, else: Map.put(scope, :satisfied_by, sources)
  end

  defp inspect_write_scope_entry("tenant", spec) when is_map(spec) do
    inspect_write_scope_entry(:tenant, spec)
  end

  defp inspect_write_scope_entry(_scope_id, spec) when is_map(spec) do
    spec
    |> Enum.map(fn {key, value} -> {key, value} end)
    |> Map.new()
  end

  defp inspect_write_scope_entry(_scope_id, spec), do: spec

  defp inspect_write_hooks(hooks) when is_map(hooks) do
    hooks
    |> sorted_entries()
    |> Enum.map(fn {hook_type, refs} ->
      hook_refs = List.wrap(refs)

      %{
        id: field_id(hook_type),
        runtime: :host_code,
        portable: false,
        count: length(hook_refs),
        refs: Enum.map(hook_refs, &inspect_write_hook_ref/1)
      }
    end)
  end

  defp inspect_write_hooks(_hooks), do: []

  defp inspect_write_hook_ref(hook) when is_function(hook) do
    %{
      kind: :function,
      arity: function_arity(hook)
    }
    |> compact_nil()
  end

  defp inspect_write_hook_ref({module, function}) when is_atom(module) and is_atom(function) do
    %{
      kind: :module_function,
      module: inspect(module),
      function: field_id(function),
      extra_args: 0
    }
  end

  defp inspect_write_hook_ref({module, function, args})
       when is_atom(module) and is_atom(function) and is_list(args) do
    %{
      kind: :module_function,
      module: inspect(module),
      function: field_id(function),
      extra_args: length(args)
    }
  end

  defp inspect_write_hook_ref(hook) do
    %{
      kind: :term,
      inspect: inspect(hook)
    }
  end

  defp inspect_actions(actions) when is_map(actions) do
    actions
    |> sorted_entries()
    |> Enum.map(fn {id, action} ->
      %{
        id: id,
        type: map_value(action, :type),
        capability: map_value(action, :capability),
        transition: map_value(action, :transition)
      }
    end)
  end

  defp inspect_actions(_actions), do: []

  defp inspect_capabilities(capabilities) when is_map(capabilities) do
    capabilities
    |> sorted_entries()
    |> Enum.map(fn {id, capability} ->
      %{
        id: id,
        operations: List.wrap(map_value(capability, :operations)),
        action: map_value(capability, :action)
      }
    end)
  end

  defp inspect_capabilities(_capabilities), do: []

  defp inspect_capability_visibility(capabilities, capability_usage) do
    catalog_ids =
      capabilities
      |> Enum.map(&normalize_capability_id(map_value(&1, :id)))
      |> Enum.reject(&is_nil/1)
      |> Enum.sort()

    referenced_ids =
      capability_usage
      |> Enum.map(&normalize_capability_id(map_value(&1, :capability)))
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
            capability_visibility_references(capability_usage, map_value(capability, :id))
          )
        end)
        |> Enum.sort_by(&to_string(map_value(&1, :id) || "")),
      references:
        capability_usage
        |> Enum.map(&capability_visibility_reference/1)
        |> Enum.sort_by(
          &{to_string(map_value(&1, :capability) || ""), to_string(map_value(&1, :path) || "")}
        )
    }
  end

  defp capability_visibility_entry(capability, references) do
    %{
      id: normalize_capability_id(map_value(capability, :id)),
      operations:
        capability
        |> map_value(:operations)
        |> List.wrap()
        |> Enum.map(&normalize_capability_id/1),
      referenced_by: references,
      runtime_samples: []
    }
    |> maybe_put_nonempty(:actions, capability_actions(capability))
  end

  defp capability_actions(capability) do
    []
    |> Kernel.++(List.wrap(map_value(capability, :action)))
    |> Kernel.++(List.wrap(map_value(capability, :actions)))
    |> Enum.map(&normalize_capability_id/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp capability_visibility_references(capability_usage, capability_id) do
    capability_id = normalize_capability_id(capability_id)

    capability_usage
    |> Enum.filter(&(normalize_capability_id(map_value(&1, :capability)) == capability_id))
    |> Enum.map(&capability_visibility_reference/1)
  end

  defp capability_visibility_reference(usage) do
    %{
      capability: normalize_capability_id(map_value(usage, :capability)),
      section: normalize_capability_id(map_value(usage, :section)),
      role: normalize_capability_id(map_value(usage, :role)),
      id: normalize_capability_id(map_value(usage, :id) || map_value(usage, :field)),
      group: normalize_capability_id(map_value(usage, :group)),
      path: usage |> map_value(:path) |> format_capability_path()
    }
    |> compact_nil()
  end

  defp format_capability_path(path) when is_list(path), do: Enum.map_join(path, ".", &field_id/1)
  defp format_capability_path(path) when is_nil(path), do: nil
  defp format_capability_path(path), do: to_string(path)

  defp normalize_capability_id(nil), do: nil
  defp normalize_capability_id(value) when is_binary(value), do: value
  defp normalize_capability_id(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_capability_id(value), do: to_string(value)

  defp maybe_put_nonempty(map, _key, []), do: map
  defp maybe_put_nonempty(map, key, value), do: Map.put(map, key, value)

  defp inspect_security_review(normalized) do
    @security_review_sections
    |> Enum.flat_map(fn
      {:writes, reason} ->
        inspect_security_writes(Map.get(normalized, :writes), reason)

      {section, reason} ->
        inspect_security_registry(section, Map.get(normalized, section), reason)
    end)
  end

  defp inspect_security_registry(section, registry, reason) when is_map(registry) do
    items = sorted_keys(registry)

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

  defp inspect_security_registry(_section, _registry, _reason), do: []

  defp inspect_security_writes(writes, reason) when is_map(writes) do
    items = inspect_writes(writes)

    count =
      length(Map.fetch!(items, :operations)) +
        length(Map.fetch!(items, :fields)) +
        length(Map.fetch!(items, :relationships)) +
        length(Map.fetch!(items, :transitions)) +
        Map.fetch!(items, :validations_count) +
        Map.fetch!(items, :constraints_count) +
        map_count(Map.fetch!(items, :scope)) +
        length(Map.fetch!(items, :hooks))

    if count > 0 do
      [%{section: :writes, count: count, items: items, reason: reason}]
    else
      []
    end
  end

  defp inspect_security_writes(_writes, _reason), do: []

  defp inspect_capability_usage(normalized) do
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
        map_value(projection, :custom_columns),
        :custom_column,
        [:custom_columns]
      )
    )
    |> Kernel.++(
      inspect_capability_section_usage(:filters, map_value(query, :filters), :query_filter, [
        :filters
      ])
    )
    |> Kernel.++(
      inspect_capability_section_usage(
        :functions,
        map_value(query, :functions),
        :query_function,
        [:functions]
      )
    )
    |> Kernel.++(inspect_query_member_capability_usage(map_value(query, :query_members)))
    |> Kernel.++(
      inspect_capability_section_usage(
        :published_views,
        map_value(query, :published_views),
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

  defp inspect_schema_capability_usage(schemas) when is_map(schemas) do
    schemas
    |> sorted_entries()
    |> Enum.flat_map(fn {schema_id, schema} ->
      inspect_relation_capability_usage(:schemas, schema, [:schemas, schema_id])
    end)
  end

  defp inspect_schema_capability_usage(_schemas), do: []

  defp inspect_relation_capability_usage(section, relation, path_prefix) when is_map(relation) do
    relation
    |> relation_field_entries()
    |> Enum.flat_map(fn {field, column} ->
      capability_usage_entries(map_value(column, :capability), %{
        section: section,
        role: :field,
        field: field,
        path: path_prefix ++ [:columns, field, :capability]
      })
    end)
  end

  defp inspect_relation_capability_usage(_section, _relation, _path_prefix), do: []

  defp inspect_capability_section_usage(section, registry, role, path_prefix)
       when is_map(registry) do
    registry
    |> sorted_entries()
    |> Enum.flat_map(fn {id, spec} ->
      capability_usage_entries(map_value(spec, :capability), %{
        section: section,
        role: role,
        id: id,
        path: path_prefix ++ [id, :capability]
      })
    end)
  end

  defp inspect_capability_section_usage(_section, _registry, _role, _path_prefix), do: []

  defp inspect_query_member_capability_usage(query_members) when is_map(query_members) do
    Enum.flat_map(@query_member_groups, fn group ->
      case map_value(query_members, group) do
        members when is_map(members) ->
          members
          |> sorted_entries()
          |> Enum.flat_map(fn {id, member} ->
            capability_usage_entries(map_value(member, :capability), %{
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

  defp inspect_query_member_capability_usage(_query_members), do: []

  defp capability_usage_entries(capability, attrs)
       when not is_nil(capability) and (is_atom(capability) or is_binary(capability)) do
    [Map.put(attrs, :capability, capability)]
  end

  defp capability_usage_entries(_capability, _attrs), do: []

  defp capability_usage_sort_key(usage) do
    path =
      usage
      |> Map.fetch!(:path)
      |> Enum.map(&field_id/1)
      |> Enum.join(".")

    {field_id(Map.fetch!(usage, :capability)), path}
  end

  defp inspect_source_relationships(source_relationships) when is_map(source_relationships) do
    source_relationships
    |> sorted_entries()
    |> Enum.map(fn {id, relationship} ->
      %{
        id: id,
        target_domain: map_value(relationship, :target_domain),
        source_field: map_value(relationship, :source_field),
        target_field: map_value(relationship, :target_field),
        source_path: map_value(relationship, :source_path),
        virtual_join_count: list_count(map_value(relationship, :virtual_join)),
        filters_count: list_count(map_value(relationship, :filters))
      }
    end)
  end

  defp inspect_source_relationships(_source_relationships), do: []

  defp inspect_choice_sources(choice_sources) when is_map(choice_sources) do
    choice_sources
    |> sorted_entries()
    |> Enum.map(fn {id, choice_source} ->
      %{
        id: id,
        domain: map_value(choice_source, :domain),
        source_relationship: map_value(choice_source, :source_relationship),
        value_field: map_value(choice_source, :value_field),
        label_field: map_value(choice_source, :label_field),
        source_path: map_value(choice_source, :source_path),
        filters_count: list_count(map_value(choice_source, :filters)),
        order_by_count: list_count(map_value(choice_source, :order_by)),
        presentation: map_value(choice_source, :presentation),
        constraint_policy: map_value(choice_source, :constraint_policy)
      }
    end)
  end

  defp inspect_choice_sources(_choice_sources), do: []

  defp query_contract_source(normalized) do
    source = Map.get(normalized, :source, %{})

    %{
      source_table: map_value(source, :source_table),
      primary_key: map_value(source, :primary_key)
    }
  end

  defp query_contract_defaults(query) do
    %{
      default_selected: map_value(query, :default_selected) || [],
      required_selected: map_value(query, :required_selected) || [],
      required_filters: map_value(query, :required_filters) || [],
      required_order_by: map_value(query, :required_order_by) || [],
      required_group_by: map_value(query, :required_group_by) || []
    }
  end

  defp query_contract_fields(normalized, field_choice_bindings) do
    choice_index = query_contract_choice_index(field_choice_bindings)

    filterable_fields =
      normalized
      |> Map.get(:query, %{})
      |> map_value(:filters)
      |> query_contract_filterable_fields()

    []
    |> Kernel.++(
      query_contract_relation_fields(
        :source,
        Map.get(normalized, :source),
        :source,
        choice_index,
        filterable_fields
      )
    )
    |> Kernel.++(
      query_contract_schema_fields(Map.get(normalized, :schemas), choice_index, filterable_fields)
    )
    |> Kernel.++(
      query_contract_custom_fields(
        map_value(Map.get(normalized, :projection, %{}), :custom_columns),
        choice_index,
        filterable_fields
      )
    )
    |> Enum.sort_by(& &1.id)
  end

  defp query_contract_schema_fields(schemas, choice_index, filterable_fields)
       when is_map(schemas) do
    schemas
    |> sorted_entries()
    |> Enum.flat_map(fn {schema_id, schema} ->
      query_contract_relation_fields(schema_id, schema, :schema, choice_index, filterable_fields)
    end)
  end

  defp query_contract_schema_fields(_schemas, _choice_index, _filterable_fields), do: []

  defp query_contract_relation_fields(
         relation_id,
         relation,
         source_kind,
         choice_index,
         filterable_fields
       )
       when is_map(relation) do
    relation
    |> relation_field_entries()
    |> Enum.map(fn {field, column} ->
      field_ref = relation_field_ref(relation_id, field)
      id = field_id(field_ref)

      type = map_value(column, :type)

      %{
        id: id,
        source: source_kind,
        relation: relation_id,
        field: field_id(field),
        type: type,
        label: field_label(column),
        capability: map_value(column, :capability),
        capability_target: map_value(column, :capability_target),
        choice_source: query_contract_choice_source(choice_index, id)
      }
      |> Map.merge(query_contract_field_surface(column, id, source_kind, type, filterable_fields))
    end)
  end

  defp query_contract_relation_fields(
         _relation_id,
         _relation,
         _source_kind,
         _choice_index,
         _filterable_fields
       ),
       do: []

  defp query_contract_custom_fields(custom_columns, choice_index, filterable_fields)
       when is_map(custom_columns) do
    custom_columns
    |> sorted_entries()
    |> Enum.map(fn {field, column} ->
      id = field_id(field)
      type = map_value(column, :type)

      %{
        id: id,
        source: :custom_column,
        relation: nil,
        field: id,
        type: type,
        label: field_label(column),
        capability: map_value(column, :capability),
        capability_target: map_value(column, :capability_target),
        choice_source: query_contract_choice_source(choice_index, id)
      }
      |> Map.merge(
        query_contract_field_surface(column, id, :custom_column, type, filterable_fields)
      )
    end)
  end

  defp query_contract_custom_fields(_custom_columns, _choice_index, _filterable_fields), do: []

  defp query_contract_joins(normalized) do
    query_contract_join_tree(
      Map.get(normalized, :joins, %{}),
      Map.get(normalized, :source),
      Map.get(normalized, :schemas, %{}),
      [],
      :source
    )
  end

  defp query_contract_join_tree(joins, parent_relation, schemas, path, parent_id)
       when is_map(joins) do
    joins
    |> sorted_entries()
    |> Enum.flat_map(fn {join_id, join_config} ->
      join_path = path ++ [join_id]
      association = relation_association(parent_relation, join_id)
      target_schema = if is_map(association), do: map_value(association, :queryable)

      target_relation =
        query_contract_join_target_relation(target_schema, parent_relation, schemas)

      nested_joins = map_value(join_config, :joins)

      entry = %{
        id: field_id(join_id),
        path: Enum.map(join_path, &field_id/1),
        parent: parent_id,
        target_schema: target_schema,
        type: map_value(join_config, :type),
        fields: relation_field_ids(target_relation),
        nested_count: map_count(nested_joins)
      }

      [
        entry
        | query_contract_join_tree(
            nested_joins,
            target_relation,
            schemas,
            join_path,
            target_schema
          )
      ]
    end)
  end

  defp query_contract_join_tree(_joins, _parent_relation, _schemas, _path, _parent_id), do: []

  defp query_contract_join_target_relation(target_schema, source, _schemas)
       when target_schema in [:source, "source"] do
    source
  end

  defp query_contract_join_target_relation(target_schema, _source, schemas) do
    case fetch_key(schemas, target_schema) do
      {:ok, schema} -> schema
      :error -> nil
    end
  end

  defp query_contract_filters(filters) when is_map(filters) do
    filters
    |> sorted_entries()
    |> Enum.map(fn {id, filter} ->
      type = map_value(filter, :type)
      virtual? = is_nil(map_value(filter, :field))

      %{
        id: id,
        field: field_ref_or_nil(map_value(filter, :field)),
        type: type,
        label: field_label(filter),
        capability: map_value(filter, :capability),
        virtual?: virtual?,
        comparators: query_contract_filter_comparators(filter, type, virtual?)
      }
    end)
  end

  defp query_contract_filters(_filters), do: []

  defp query_contract_filterable_fields(filters) when is_map(filters) do
    filters
    |> Map.values()
    |> Enum.reduce(MapSet.new(), fn
      filter, acc when is_map(filter) ->
        case field_ref_or_nil(map_value(filter, :field)) do
          nil -> acc
          field -> MapSet.put(acc, field)
        end

      _filter, acc ->
        acc
    end)
  end

  defp query_contract_filterable_fields(_filters), do: MapSet.new()

  defp query_contract_field_surface(column, id, source_kind, type, filterable_fields) do
    type_id = query_contract_type_id(type)
    detail_selectable? = query_contract_detail_selectable?(column)
    filterable? = query_contract_filterable?(column, id, source_kind, filterable_fields)
    aggregatable? = query_contract_aggregatable?(column, type_id, detail_selectable?)

    %{
      detail_selectable: detail_selectable?,
      filterable: filterable?,
      sortable: query_contract_sortable?(column, type_id, detail_selectable?),
      groupable: query_contract_groupable?(column, type_id, detail_selectable?),
      aggregatable: aggregatable?,
      comparators: query_contract_comparators(column, type_id, filterable?),
      aggregate_functions: query_contract_aggregate_functions(column, type_id, aggregatable?)
    }
  end

  defp query_contract_detail_selectable?(column) do
    query_contract_bool(
      column,
      [:detail_selectable, :detail_selectable?, :selectable, :selectable?],
      true
    )
  end

  defp query_contract_filterable?(column, id, source_kind, filterable_fields) do
    default = source_kind in [:source, :schema] or MapSet.member?(filterable_fields, id)

    query_contract_bool(
      column,
      [:filterable, :filterable?, :query_filterable, :query_filterable?],
      default
    )
  end

  defp query_contract_sortable?(column, type_id, detail_selectable?) do
    query_contract_bool(
      column,
      [:sortable, :sortable?],
      detail_selectable? and type_id in @query_contract_sortable_types
    )
  end

  defp query_contract_groupable?(column, type_id, detail_selectable?) do
    query_contract_bool(
      column,
      [:groupable, :groupable?],
      detail_selectable? and type_id in @query_contract_groupable_types
    )
  end

  defp query_contract_aggregatable?(column, type_id, detail_selectable?) do
    query_contract_bool(
      column,
      [:aggregatable, :aggregatable?],
      detail_selectable? and type_id in @query_contract_numeric_types
    )
  end

  defp query_contract_comparators(column, type_id, filterable?) do
    case query_contract_list_override(column, [:comparators, :operators]) do
      {:ok, comparators} -> comparators
      :error -> if filterable?, do: query_contract_type_comparators(type_id), else: []
    end
  end

  defp query_contract_filter_comparators(filter, type, virtual?) do
    type_id = query_contract_type_id(type)

    case query_contract_list_override(filter, [:comparators, :operators]) do
      {:ok, comparators} -> comparators
      :error -> if virtual?, do: [], else: query_contract_type_comparators(type_id)
    end
  end

  defp query_contract_aggregate_functions(column, _type_id, aggregatable?) do
    case query_contract_list_override(column, [:aggregate_functions, :aggregates]) do
      {:ok, aggregate_functions} -> aggregate_functions
      :error -> if aggregatable?, do: [:count, :count_distinct, :sum, :avg, :min, :max], else: []
    end
  end

  defp query_contract_type_comparators(type_id) when type_id in @query_contract_numeric_types,
    do: [:eq, :neq, :gt, :gte, :lt, :lte, :between, :in, :not_in, :is_null, :not_null]

  defp query_contract_type_comparators(type_id) when type_id in @query_contract_temporal_types,
    do: [:eq, :neq, :gt, :gte, :lt, :lte, :between, :in, :not_in, :is_null, :not_null]

  defp query_contract_type_comparators(type_id) when type_id in @query_contract_text_types,
    do: [:eq, :neq, :contains, :starts_with, :ends_with, :in, :not_in, :is_null, :not_null]

  defp query_contract_type_comparators("boolean"), do: [:eq, :neq, :is_null, :not_null]

  defp query_contract_type_comparators(type_id) when type_id in @query_contract_exact_types,
    do: [:eq, :neq, :in, :not_in, :is_null, :not_null]

  defp query_contract_type_comparators(_type_id),
    do: [:eq, :neq, :in, :not_in, :is_null, :not_null]

  defp query_contract_bool(map, keys, default) do
    case query_contract_bool_override(map, keys) do
      {:ok, value} -> value
      :error -> default
    end
  end

  defp query_contract_bool_override(map, keys) when is_map(map) do
    Enum.reduce_while(keys, :error, fn key, _acc ->
      case map_value(map, key) do
        value when is_boolean(value) -> {:halt, {:ok, value}}
        _value -> {:cont, :error}
      end
    end)
  end

  defp query_contract_bool_override(_map, _keys), do: :error

  defp query_contract_list_override(map, keys) when is_map(map) do
    Enum.reduce_while(keys, :error, fn key, _acc ->
      case map_value(map, key) do
        values when is_list(values) -> {:halt, {:ok, values}}
        _value -> {:cont, :error}
      end
    end)
  end

  defp query_contract_list_override(_map, _keys), do: :error

  defp query_contract_type_id(type) when is_atom(type), do: Atom.to_string(type)

  defp query_contract_type_id(type) when is_binary(type) do
    type
    |> String.downcase()
    |> String.replace("-", "_")
  end

  defp query_contract_type_id(_type), do: ""

  defp query_contract_functions(functions) when is_map(functions) do
    functions
    |> sorted_entries()
    |> Enum.map(fn {id, function} ->
      %{
        id: id,
        kind: map_value(function, :kind),
        sql_name: map_value(function, :sql_name),
        allowed_in: List.wrap(map_value(function, :allowed_in)),
        args: query_contract_function_args(map_value(function, :args)),
        returns: map_value(function, :returns),
        capability: map_value(function, :capability)
      }
    end)
  end

  defp query_contract_functions(_functions), do: []

  defp query_contract_function_args(args) when is_list(args) do
    Enum.map(args, fn
      arg when is_map(arg) ->
        %{
          name: map_value(arg, :name),
          type: map_value(arg, :type),
          source: map_value(arg, :source)
        }

      _arg ->
        %{}
    end)
  end

  defp query_contract_function_args(_args), do: []

  defp query_contract_query_members(query_members) when is_map(query_members) do
    Enum.into(@query_member_groups, %{}, fn group ->
      {group, query_contract_query_member_group(group, map_value(query_members, group))}
    end)
  end

  defp query_contract_query_members(_query_members) do
    Enum.into(@query_member_groups, %{}, &{&1, []})
  end

  defp query_contract_query_member_group(group, members) when is_map(members) do
    members
    |> sorted_entries()
    |> Enum.map(fn {id, member} -> query_contract_query_member(group, id, member) end)
  end

  defp query_contract_query_member_group(_group, _members), do: []

  defp query_contract_query_member(group, id, member) when is_map(member) do
    base = %{
      id: id,
      capability: map_value(member, :capability)
    }

    case group do
      :ctes ->
        Map.merge(base, %{
          columns: List.wrap(map_value(member, :columns)),
          recursive?:
            has_key_variant?(member, :base_query) or has_key_variant?(member, :recursive_query),
          join?: not is_nil(map_value(member, :join))
        })

      :values ->
        Map.merge(base, %{
          columns: List.wrap(map_value(member, :columns)),
          alias: query_contract_alias(member),
          rows_count: list_count(map_value(member, :rows) || map_value(member, :data))
        })

      :subqueries ->
        Map.merge(base, %{
          kind: map_value(member, :kind),
          join_type: map_value(member, :type),
          join_id: map_value(member, :join_id),
          on_count: list_count(map_value(member, :on))
        })

      :laterals ->
        Map.merge(base, %{
          join_type: map_value(member, :join_type) || map_value(member, :type),
          alias: query_contract_alias(member),
          source: query_contract_lateral_source(member)
        })

      :unnests ->
        Map.merge(base, %{
          field: field_ref_or_nil(map_value(member, :array_field) || map_value(member, :field)),
          alias: query_contract_alias(member),
          ordinality: map_value(member, :ordinality)
        })
    end
  end

  defp query_contract_query_member(_group, id, _member), do: %{id: id}

  defp query_contract_published_views(published_views) when is_map(published_views) do
    published_views
    |> sorted_entries()
    |> Enum.map(fn {id, view} ->
      %{
        id: id,
        database_name: map_value(view, :database_name),
        kind: map_value(view, :kind),
        columns: query_contract_columns(map_value(view, :columns)),
        indexes_count: list_count(map_value(view, :indexes)),
        refresh: map_value(view, :refresh),
        capability: map_value(view, :capability)
      }
    end)
  end

  defp query_contract_published_views(_published_views), do: []

  defp query_contract_source_relationships(source_relationships)
       when is_map(source_relationships) do
    source_relationships
    |> sorted_entries()
    |> Enum.map(fn {id, relationship} ->
      %{
        id: id,
        target_domain: map_value(relationship, :target_domain),
        source_field: field_ref_or_nil(map_value(relationship, :source_field)),
        target_field: field_ref_or_nil(map_value(relationship, :target_field)),
        source_path: map_value(relationship, :source_path),
        virtual_join_count: list_count(map_value(relationship, :virtual_join)),
        filters_count: list_count(map_value(relationship, :filters))
      }
    end)
  end

  defp query_contract_source_relationships(_source_relationships), do: []

  defp query_contract_choice_sources(choice_sources) when is_map(choice_sources) do
    choice_sources
    |> sorted_entries()
    |> Enum.map(fn {id, choice_source} ->
      %{
        id: id,
        domain: map_value(choice_source, :domain),
        source_relationship: map_value(choice_source, :source_relationship),
        value_field: field_ref_or_nil(map_value(choice_source, :value_field)),
        label_field: field_ref_or_nil(map_value(choice_source, :label_field)),
        source_path: map_value(choice_source, :source_path),
        value_source: field_ref_or_nil(map_value(choice_source, :value_source)),
        caption_source: field_ref_or_nil(map_value(choice_source, :caption_source)),
        filters_count: list_count(map_value(choice_source, :filters)),
        order_by_count: list_count(map_value(choice_source, :order_by)),
        presentation: map_value(choice_source, :presentation),
        constraint_policy: map_value(choice_source, :constraint_policy),
        capability: map_value(choice_source, :capability)
      }
    end)
  end

  defp query_contract_choice_sources(_choice_sources), do: []

  defp query_contract_choice_bindings(field_choice_bindings) do
    Enum.map(field_choice_bindings, fn binding ->
      %{
        field: field_id(binding.field),
        choice_source: binding.choice_source,
        compact?: binding.compact?,
        reference?: binding.reference?,
        path: binding.path
      }
    end)
  end

  defp query_contract_columns(columns) when is_map(columns) do
    columns
    |> sorted_entries()
    |> Enum.map(fn {id, column} ->
      %{
        id: field_id(id),
        type: map_value(column, :type),
        label: field_label(column),
        capability: map_value(column, :capability)
      }
    end)
  end

  defp query_contract_columns(_columns), do: []

  defp query_contract_alias(member) do
    first_map_value(member, [:as, :alias, :alias_name])
  end

  defp query_contract_lateral_source(member) do
    source = first_map_value(member, [:query, :source, :lateral_source])

    cond do
      is_function(source) -> :function
      is_tuple(source) -> :tuple
      is_nil(source) -> nil
      true -> value_type(source)
    end
  end

  defp query_contract_choice_index(field_choice_bindings) do
    field_choice_bindings
    |> Enum.group_by(&field_id(&1.field), & &1.choice_source)
    |> Map.new(fn {field, choice_sources} ->
      {field, choice_sources |> Enum.reject(&is_nil/1) |> Enum.uniq_by(&field_id/1)}
    end)
  end

  defp query_contract_choice_source(choice_index, field) do
    case Map.get(choice_index, field, []) do
      [] -> nil
      [choice_source] -> choice_source
      choice_sources -> choice_sources
    end
  end

  defp relation_field_entries(relation) when is_map(relation) do
    columns =
      case map_value(relation, :columns) do
        columns when is_map(columns) -> columns
        _columns -> %{}
      end

    fields =
      case map_value(relation, :fields) do
        fields when is_list(fields) -> fields
        _fields -> []
      end

    (fields ++ Map.keys(columns))
    |> Enum.uniq_by(&field_id/1)
    |> Enum.sort_by(&field_id/1)
    |> Enum.map(fn field ->
      column =
        case fetch_key(columns, field) do
          {:ok, column} when is_map(column) -> column
          _ -> %{}
        end

      {field, column}
    end)
  end

  defp relation_field_entries(_relation), do: []

  defp relation_association(relation, association_id) when is_map(relation) do
    case map_value(relation, :associations) do
      associations when is_map(associations) ->
        case fetch_key(associations, association_id) do
          {:ok, association} -> association
          :error -> nil
        end

      _associations ->
        nil
    end
  end

  defp relation_association(_relation, _association_id), do: nil

  defp field_label(map) when is_map(map) do
    map_value(map, :label) || map_value(map, :name) || map_value(map, :display_name)
  end

  defp field_label(_map), do: nil

  defp field_ref_or_nil(value) when is_atom(value) and not is_nil(value), do: field_id(value)
  defp field_ref_or_nil(value) when is_binary(value), do: value
  defp field_ref_or_nil(_value), do: nil

  defp first_map_value(map, keys) when is_map(map) do
    Enum.find_value(keys, &map_value(map, &1))
  end

  defp first_map_value(_map, _keys), do: nil

  defp field_choice_bindings(normalized) do
    []
    |> Kernel.++(
      relation_field_choice_bindings(:source, Map.get(normalized, :source), [:source, :columns])
    )
    |> Kernel.++(schema_field_choice_bindings(Map.get(normalized, :schemas)))
    |> Kernel.++(
      column_field_choice_bindings(
        map_value(Map.get(normalized, :projection, %{}), :columns),
        [:columns],
        & &1
      )
    )
    |> Enum.sort_by(&{field_id(&1.field), field_id(&1.choice_source), inspect(&1.path)})
  end

  defp schema_field_choice_bindings(schemas) when is_map(schemas) do
    schemas
    |> sorted_entries()
    |> Enum.flat_map(fn {schema_id, schema} ->
      relation_field_choice_bindings(schema_id, schema, [:schemas, schema_id, :columns])
    end)
  end

  defp schema_field_choice_bindings(_schemas), do: []

  defp relation_field_choice_bindings(relation_id, relation, path) when is_map(relation) do
    relation
    |> map_value(:columns)
    |> column_field_choice_bindings(path, &relation_field_ref(relation_id, &1))
  end

  defp relation_field_choice_bindings(_relation_id, _relation, _path), do: []

  defp column_field_choice_bindings(columns, path, field_ref_fun) when is_map(columns) do
    columns
    |> sorted_entries()
    |> Enum.flat_map(fn {field, column} ->
      column_field_choice_binding(field_ref_fun.(field), column, path ++ [field])
    end)
  end

  defp column_field_choice_bindings(_columns, _path, _field_ref_fun), do: []

  defp column_field_choice_binding(field, column, path) when is_map(column) do
    compact_choice_source = id_value(map_value(column, :choice_source))

    reference_choice_source =
      case map_value(column, :reference) do
        reference when is_map(reference) -> id_value(map_value(reference, :choice_source))
        _reference -> nil
      end

    cond do
      is_nil(compact_choice_source) and is_nil(reference_choice_source) ->
        []

      is_nil(reference_choice_source) or compact_choice_source == reference_choice_source ->
        [
          %{
            field: field,
            choice_source: compact_choice_source,
            compact?: not is_nil(compact_choice_source),
            reference?: not is_nil(reference_choice_source),
            path: path
          }
        ]

      is_nil(compact_choice_source) ->
        [
          %{
            field: field,
            choice_source: reference_choice_source,
            compact?: false,
            reference?: true,
            path: path ++ [:reference]
          }
        ]

      true ->
        [
          %{
            field: field,
            choice_source: compact_choice_source,
            compact?: true,
            reference?: false,
            path: path
          },
          %{
            field: field,
            choice_source: reference_choice_source,
            compact?: false,
            reference?: true,
            path: path ++ [:reference]
          }
        ]
    end
  end

  defp column_field_choice_binding(_field, _column, _path), do: []

  defp id_value(value) when is_atom(value) and not is_nil(value), do: value
  defp id_value(value) when is_binary(value), do: value
  defp id_value(_value), do: nil

  defp schema_fields(schemas) when is_map(schemas) do
    schemas
    |> sorted_entries()
    |> Enum.into(%{}, fn {schema_id, schema} -> {schema_id, relation_field_ids(schema)} end)
  end

  defp schema_fields(_schemas), do: %{}

  defp relation_field_ids(relation) when is_map(relation) do
    fields =
      case map_value(relation, :fields) do
        fields when is_list(fields) -> fields
        _fields -> []
      end

    columns =
      case map_value(relation, :columns) do
        columns when is_map(columns) -> Map.keys(columns)
        _columns -> []
      end

    (fields ++ columns)
    |> Enum.map(&field_id/1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp relation_field_ids(_relation), do: []

  defp query_member_keys(query_members) when is_map(query_members) do
    %{
      ctes: sorted_keys(map_value(query_members, :ctes)),
      values: sorted_keys(map_value(query_members, :values)),
      subqueries: sorted_keys(map_value(query_members, :subqueries)),
      laterals: sorted_keys(map_value(query_members, :laterals)),
      unnests: sorted_keys(map_value(query_members, :unnests))
    }
  end

  defp query_member_keys(_query_members) do
    %{
      ctes: [],
      values: [],
      subqueries: [],
      laterals: [],
      unnests: []
    }
  end

  defp query_member_count(query_members) when is_map(query_members) do
    query_members
    |> query_member_keys()
    |> Map.values()
    |> Enum.map(&length/1)
    |> Enum.sum()
  end

  defp query_member_count(_query_members), do: 0

  defp sorted_entries(map) when is_map(map) do
    Enum.sort_by(map, fn {key, _value} -> field_id(key) end)
  end

  defp sorted_entries(_map), do: []

  defp sorted_keys(map) when is_map(map) do
    map
    |> Map.keys()
    |> Enum.sort_by(&field_id/1)
  end

  defp sorted_keys(_map), do: []

  defp map_count(map) when is_map(map), do: map_size(map)
  defp map_count(_map), do: 0

  defp list_count(list) when is_list(list), do: length(list)
  defp list_count(_list), do: 0

  defp compact_nil(map) when is_map(map) do
    map
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  defp function_arity(hook) when is_function(hook) do
    case Function.info(hook, :arity) do
      {:arity, arity} -> arity
      _other -> nil
    end
  end

  defp domain_overlays(nil), do: {:ok, []}
  defp domain_overlays([]), do: {:ok, []}
  defp domain_overlays(%{} = overlay), do: {:ok, [overlay]}

  defp domain_overlays(overlays) when is_list(overlays) do
    overlays
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {overlay, index}, {:ok, acc} ->
      if is_map(overlay) do
        {:cont, {:ok, [overlay | acc]}}
      else
        {:halt, {:error, invalid_domain_overlay_diagnostics(overlay, index)}}
      end
    end)
    |> case do
      {:ok, overlays} -> {:ok, Enum.reverse(overlays)}
      error -> error
    end
  end

  defp domain_overlays(overlay), do: {:error, invalid_domain_overlay_diagnostics(overlay, nil)}

  defp invalid_domain_overlay_diagnostics(overlay, index) do
    Diagnostics.new(
      errors: [
        %{
          code: :invalid_domain_overlay,
          message: "Selecto domain overlays must be maps",
          overlay_index: index,
          actual: value_type(overlay)
        }
      ]
    )
  end

  defp apply_composed_extensions(domain) do
    extension_specs =
      domain
      |> section(:extensions, [])
      |> Selecto.Extensions.normalize_specs()

    domain
    |> Selecto.Extensions.merge_domain_extensions(extension_specs)
    |> normalize_authoring_shorthand()
  end

  defp merge_domain_maps(base, overlay) when is_map(base) and is_map(overlay) do
    Enum.reduce(overlay, base, fn {key, overlay_value}, acc ->
      section_key = canonical_section_key(key)
      base_value = fetch_merge_value(acc, section_key)

      merged_value =
        merge_section_value(section_key, base_value, overlay_value, [section_key])

      put_merge_value(acc, section_key, merged_value)
    end)
  end

  defp merge_section_value(:extensions, base, overlay, _path)
       when is_list(base) and is_list(overlay),
       do: unique_list(base ++ overlay)

  defp merge_section_value(:redact_fields, base, overlay, _path)
       when is_list(base) and is_list(overlay),
       do: unique_list(base ++ overlay)

  defp merge_section_value(:source, base, overlay, path) when is_map(base) and is_map(overlay) do
    deep_merge_domain_maps(base, overlay, path)
  end

  defp merge_section_value(section, base, overlay, path)
       when section in @collision_warning_sections and is_map(base) and is_map(overlay) do
    merge_registry_maps(base, overlay, path)
  end

  defp merge_section_value(section, base, overlay, path)
       when section in @map_sections and is_map(base) and is_map(overlay) do
    deep_merge_domain_maps(base, overlay, path)
  end

  defp merge_section_value(_section, _base, overlay, _path), do: overlay

  defp deep_merge_domain_maps(base, overlay, path) do
    Map.merge(base, overlay, fn key, base_value, overlay_value ->
      merge_nested_value(path ++ [canonical_section_key(key)], base_value, overlay_value)
    end)
  end

  defp merge_nested_value([:source, :redact_fields], base, overlay)
       when is_list(base) and is_list(overlay),
       do: unique_list(base ++ overlay)

  defp merge_nested_value(_path, base, overlay) when is_map(base) and is_map(overlay) do
    deep_merge_domain_maps(base, overlay, [])
  end

  defp merge_nested_value(_path, _base, overlay), do: overlay

  defp merge_registry_maps(base, overlay, path) do
    Enum.reduce(overlay, base, fn {overlay_key, overlay_value}, acc ->
      case equivalent_map_key(acc, overlay_key) do
        nil ->
          Map.put(acc, overlay_key, overlay_value)

        base_key ->
          merged_value =
            merge_nested_value(path ++ [base_key], Map.fetch!(acc, base_key), overlay_value)

          Map.put(acc, base_key, merged_value)
      end
    end)
  end

  defp equivalent_map_key(map, key) do
    Enum.find(Map.keys(map), &(merge_key_id(&1) == merge_key_id(key)))
  end

  defp composition_collision_warnings(base, overlay, overlay_index) do
    Enum.flat_map(@collision_warning_sections, fn section ->
      base_section = section(base, section, %{})
      overlay_section = section(overlay, section, %{})

      if is_map(base_section) and is_map(overlay_section) do
        base_keys = Map.keys(base_section)

        overlay_section
        |> Map.keys()
        |> Enum.filter(&merge_key_member?(base_keys, &1))
        |> Enum.map(&composition_collision_warning(section, &1, overlay_index))
      else
        []
      end
    end)
  end

  defp composition_collision_warning(section, key, overlay_index) do
    %{
      code: :domain_composition_collision,
      message:
        "domain overlay #{overlay_index} updates existing #{inspect(section)} entry #{inspect(key)}",
      section: section,
      key: key,
      overlay_index: overlay_index
    }
  end

  defp merge_key_member?(keys, key) do
    Enum.any?(keys, &(merge_key_id(&1) == merge_key_id(key)))
  end

  defp merge_key_id(key) when is_atom(key), do: Atom.to_string(key)
  defp merge_key_id(key) when is_binary(key), do: key
  defp merge_key_id(key), do: inspect(key)

  defp fetch_merge_value(map, key) when is_atom(key), do: section(map, key)
  defp fetch_merge_value(map, key), do: Map.get(map, key)

  defp put_merge_value(map, key, value) when is_atom(key), do: put_section(map, key, value)
  defp put_merge_value(map, key, value), do: Map.put(map, key, value)

  defp canonical_section_key(key) when is_atom(key), do: key

  defp canonical_section_key(key) when is_binary(key) do
    Enum.find(@known_sections, &(Atom.to_string(&1) == key)) || key
  end

  defp canonical_section_key(key), do: key

  defp unique_list(values), do: Enum.uniq(values)

  defp normalize_authoring_shorthand(domain) do
    choice_sources = section(domain, :choice_sources, %{})
    source_relationships = section(domain, :source_relationships, %{})

    if is_map(choice_sources) and is_map(source_relationships) do
      acc = %{
        choice_sources: choice_sources,
        source_relationships: source_relationships,
        changed?: false
      }

      {domain, acc} =
        domain
        |> normalize_source_choice_shorthand(acc)
        |> normalize_schema_choice_shorthand()
        |> normalize_projection_choice_shorthand()

      if acc.changed? do
        domain
        |> put_section(:choice_sources, acc.choice_sources)
        |> put_section(:source_relationships, acc.source_relationships)
      else
        domain
      end
    else
      domain
    end
  end

  defp normalize_source_choice_shorthand(domain, acc) do
    case section(domain, :source) do
      source when is_map(source) ->
        {source, acc} = normalize_relation_choice_shorthand(source, :source, acc)
        {put_section(domain, :source, source), acc}

      _source ->
        {domain, acc}
    end
  end

  defp normalize_schema_choice_shorthand({domain, acc}) do
    case section(domain, :schemas, %{}) do
      schemas when is_map(schemas) ->
        {schemas, acc} =
          Enum.reduce(schemas, {%{}, acc}, fn {schema_id, schema}, {schemas_acc, acc} ->
            {schema, acc} = normalize_relation_choice_shorthand(schema, {:schema, schema_id}, acc)
            {Map.put(schemas_acc, schema_id, schema), acc}
          end)

        {put_section(domain, :schemas, schemas), acc}

      _schemas ->
        {domain, acc}
    end
  end

  defp normalize_projection_choice_shorthand({domain, acc}) do
    case fetch_section(domain, :columns) do
      {:ok, columns} when is_map(columns) ->
        {columns, acc} = normalize_columns_choice_shorthand(columns, :projection, acc)
        {put_section(domain, :columns, columns), acc}

      {:ok, _columns} ->
        {domain, acc}

      :error ->
        {domain, acc}
    end
  end

  defp normalize_relation_choice_shorthand(relation, scope, acc) when is_map(relation) do
    case map_value(relation, :columns) do
      columns when is_map(columns) ->
        {columns, acc} = normalize_columns_choice_shorthand(columns, scope, acc)
        {put_map_value(relation, :columns, columns), acc}

      _columns ->
        {relation, acc}
    end
  end

  defp normalize_relation_choice_shorthand(relation, _scope, acc), do: {relation, acc}

  defp normalize_columns_choice_shorthand(columns, scope, acc) do
    Enum.reduce(columns, {%{}, acc}, fn {field, column}, {columns_acc, acc} ->
      {column, acc} = normalize_column_choice_shorthand(column, scope, field, acc)
      {Map.put(columns_acc, field, column), acc}
    end)
  end

  defp normalize_column_choice_shorthand(column, scope, field, acc) when is_map(column) do
    case map_value(column, :choice_source) do
      shorthand when is_map(shorthand) ->
        choice_source_id = shorthand_choice_source_id(shorthand, scope, field)

        {choice_source, acc} =
          shorthand_choice_source(shorthand, choice_source_id, scope, field, acc)

        acc =
          acc
          |> put_registry_entry(:choice_sources, choice_source_id, choice_source)
          |> Map.put(:changed?, true)

        column =
          column
          |> delete_key_variants(:choice_source)
          |> put_map_value(:choice_source, choice_source_id)
          |> put_map_value(
            :reference,
            shorthand_field_reference(column, shorthand, choice_source_id)
          )

        {column, acc}

      _choice_source ->
        {column, acc}
    end
  end

  defp normalize_column_choice_shorthand(column, _scope, _field, acc), do: {column, acc}

  defp shorthand_choice_source(shorthand, _choice_source_id, scope, field, acc) do
    source_relationship = map_value(shorthand, :source_relationship)

    source_relationship_id =
      shorthand_source_relationship_id(shorthand, source_relationship, scope, field)

    {relationship_ref, acc} =
      case source_relationship do
        relationship when is_map(relationship) ->
          relationship =
            shorthand_source_relationship(
              relationship,
              shorthand,
              source_relationship_id,
              scope,
              field
            )

          acc =
            acc
            |> put_registry_entry(:source_relationships, source_relationship_id, relationship)
            |> Map.put(:changed?, true)

          {source_relationship_id, acc}

        relationship
        when (is_atom(relationship) and not is_nil(relationship)) or is_binary(relationship) ->
          {relationship, acc}

        _relationship ->
          {nil, acc}
      end

    choice_source =
      shorthand
      |> delete_key_variants(:id)
      |> delete_key_variants(:source_relationship_id)
      |> maybe_put_default(:domain, map_value(shorthand, :domain))
      |> maybe_put_default(:value_field, path_leaf(map_value(shorthand, :value_source)))
      |> maybe_put_default(:label_field, path_leaf(map_value(shorthand, :caption_source)))
      |> maybe_put_default(:source_path, path_parent(map_value(shorthand, :value_source)))
      |> normalize_choice_source_presentation()

    choice_source =
      if is_nil(relationship_ref) do
        delete_key_variants(choice_source, :source_relationship)
      else
        put_map_value(choice_source, :source_relationship, relationship_ref)
      end

    {choice_source, acc}
  end

  defp shorthand_source_relationship(relationship, choice_source, _relationship_id, scope, field) do
    virtual_join = map_value(relationship, :virtual_join)
    first_virtual_join = first_virtual_join_entry(virtual_join)
    virtual_join_source_field = map_value(first_virtual_join, :source_field)

    relationship
    |> delete_key_variants(:id)
    |> delete_key_variants(:domain)
    |> maybe_put_default(
      :target_domain,
      map_value(relationship, :target_domain) || map_value(relationship, :domain) ||
        map_value(choice_source, :domain)
    )
    |> maybe_put_default(
      :source_field,
      map_value(relationship, :source_field) || map_value(first_virtual_join, :working_field) ||
        scoped_field_ref(scope, field)
    )
    |> maybe_put_default(
      :target_field,
      map_value(relationship, :target_field) || path_leaf(virtual_join_source_field) ||
        map_value(choice_source, :value_field) ||
        path_leaf(map_value(choice_source, :value_source))
    )
    |> maybe_put_default(
      :source_path,
      map_value(relationship, :source_path) || path_parent(virtual_join_source_field) ||
        map_value(choice_source, :source_path)
    )
  end

  defp shorthand_field_reference(column, shorthand, choice_source_id) do
    base_reference =
      case map_value(column, :reference) do
        reference when is_map(reference) -> reference
        _reference -> %{}
      end

    base_reference
    |> put_map_value(:choice_source, choice_source_id)
    |> maybe_put_default(:value_source, map_value(shorthand, :value_source))
    |> maybe_put_default(:caption_source, map_value(shorthand, :caption_source))
  end

  defp shorthand_choice_source_id(shorthand, scope, field) do
    case map_value(shorthand, :id) do
      id when (is_atom(id) and not is_nil(id)) or is_binary(id) -> id
      _id -> "#{shorthand_field_prefix(scope, field)}_choice_source"
    end
  end

  defp shorthand_source_relationship_id(shorthand, source_relationship, scope, field) do
    id =
      cond do
        is_map(source_relationship) -> map_value(source_relationship, :id)
        true -> nil
      end

    cond do
      (is_atom(id) and not is_nil(id)) or is_binary(id) ->
        id

      source_relationship_id = map_value(shorthand, :source_relationship_id) ->
        if (is_atom(source_relationship_id) and not is_nil(source_relationship_id)) or
             is_binary(source_relationship_id) do
          source_relationship_id
        else
          "#{shorthand_field_prefix(scope, field)}_source_relationship"
        end

      true ->
        "#{shorthand_field_prefix(scope, field)}_source_relationship"
    end
  end

  defp shorthand_field_prefix(:source, field), do: field_id(field)
  defp shorthand_field_prefix(:projection, field), do: "projection_#{field_id(field)}"

  defp shorthand_field_prefix({:schema, schema_id}, field),
    do: "#{field_id(schema_id)}_#{field_id(field)}"

  defp scoped_field_ref(:source, field), do: field
  defp scoped_field_ref(:projection, field), do: field

  defp scoped_field_ref({:schema, schema_id}, field),
    do: "#{field_id(schema_id)}.#{field_id(field)}"

  defp relation_field_ref(:source, field), do: field
  defp relation_field_ref(relation_id, field), do: "#{field_id(relation_id)}.#{field_id(field)}"

  defp normalize_choice_source_presentation(choice_source) do
    case map_value(choice_source, :presentation) do
      presentation
      when (is_atom(presentation) and not is_nil(presentation)) or is_binary(presentation) ->
        put_map_value(choice_source, :presentation, %{control: presentation})

      _presentation ->
        choice_source
    end
  end

  defp first_virtual_join_entry([entry | _entries]) when is_map(entry), do: entry
  defp first_virtual_join_entry(_virtual_join), do: %{}

  defp path_leaf(path) when is_atom(path), do: path

  defp path_leaf(path) when is_binary(path) do
    path
    |> String.split(".", trim: true)
    |> List.last()
  end

  defp path_leaf(_path), do: nil

  defp path_parent(path) when is_binary(path) do
    parts = String.split(path, ".", trim: true)

    case parts do
      [_only] -> nil
      [_ | _] -> parts |> Enum.drop(-1) |> Enum.join(".")
      [] -> nil
    end
  end

  defp path_parent(_path), do: nil

  defp put_registry_entry(acc, registry_key, id, entry) do
    registry = Map.fetch!(acc, registry_key)

    if registry_has_key?(registry, id) do
      acc
    else
      Map.put(acc, registry_key, Map.put(registry, id, entry))
    end
  end

  defp registry_has_key?(registry, id) when is_map(registry) do
    Map.has_key?(registry, id) or
      (is_atom(id) and Map.has_key?(registry, Atom.to_string(id))) or
      (is_binary(id) and registry_has_atom_key?(registry, id))
  end

  defp registry_has_key?(_registry, _id), do: false

  defp registry_has_atom_key?(registry, id) do
    Enum.any?(Map.keys(registry), &(is_atom(&1) and Atom.to_string(&1) == id))
  end

  defp maybe_put_default(map, _key, nil), do: map

  defp maybe_put_default(map, key, value) do
    if has_key_variant?(map, key) do
      map
    else
      put_map_value(map, key, value)
    end
  end

  defp put_section(domain, key, value), do: put_map_value(domain, key, value)

  defp put_map_value(map, key, value) when is_map(map) and is_atom(key) do
    cond do
      Map.has_key?(map, key) -> Map.put(map, key, value)
      Map.has_key?(map, Atom.to_string(key)) -> Map.put(map, Atom.to_string(key), value)
      true -> Map.put(map, key, value)
    end
  end

  defp delete_key_variants(map, key) when is_map(map) and is_atom(key) do
    map
    |> Map.delete(key)
    |> Map.delete(Atom.to_string(key))
  end

  defp has_key_variant?(map, key) when is_map(map) and is_atom(key) do
    Map.has_key?(map, key) or Map.has_key?(map, Atom.to_string(key))
  end

  defp has_key_variant?(_map, _key), do: false

  defp base_projection(normalized) do
    authored_domain = Map.fetch!(normalized, :domain)

    %{
      schema_version: Map.fetch!(normalized, :schema_version),
      name: map_section(authored_domain, :name),
      source: Map.get(normalized, :source),
      schemas: Map.get(normalized, :schemas, %{}),
      joins: Map.get(normalized, :joins, %{}),
      domain_data: Map.get(normalized, :domain_data, %{}),
      extensions: Map.get(normalized, :extensions, [])
    }
    |> maybe_put(:domain_version, Map.get(normalized, :domain_version))
    |> maybe_put(:domain_fingerprint, Map.get(normalized, :domain_fingerprint))
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp take_query_sections(normalized, keys) do
    normalized
    |> Map.fetch!(:query)
    |> Map.take(keys)
  end

  defp take_projection_sections(normalized, keys) do
    normalized
    |> Map.fetch!(:projection)
    |> Map.take(keys)
  end

  defp projection_section(normalized, key, default) do
    normalized
    |> Map.fetch!(:projection)
    |> Map.get(key, default)
  end

  defp query_sections(domain) do
    %{
      default_selected: section(domain, :default_selected, []),
      required_selected: section(domain, :required_selected, []),
      required_filters: section(domain, :required_filters, []),
      required_order_by: section(domain, :required_order_by, []),
      required_group_by: section(domain, :required_group_by, []),
      filters: section(domain, :filters, %{}),
      functions: section(domain, :functions, %{}),
      query_members: section(domain, :query_members, %{}),
      published_views: section(domain, :published_views, %{})
    }
  end

  defp projection_sections(domain) do
    %{
      columns: section(domain, :columns, %{}),
      custom_columns: section(domain, :custom_columns, %{}),
      jsonb_schemas: section(domain, :jsonb_schemas, %{}),
      subfilters: section(domain, :subfilters, %{}),
      window_functions: section(domain, :window_functions, %{}),
      pagination: section(domain, :pagination, %{}),
      retarget: section(domain, :retarget, %{}),
      redact_fields: section(domain, :redact_fields, [])
    }
  end

  defp domain_version(domain) do
    case fetch_section(domain, :domain_version) do
      {:ok, version} when is_binary(version) ->
        case String.trim(version) do
          "" -> nil
          trimmed -> trimmed
        end

      {:ok, version} when is_atom(version) or is_integer(version) ->
        version

      {:ok, _version} ->
        nil

      :error ->
        nil
    end
  end

  defp maybe_put_domain_version(domain, nil), do: domain

  defp maybe_put_domain_version(domain, domain_version),
    do: put_section(domain, :domain_version, domain_version)

  defp domain_fingerprint(domain) do
    case fetch_section(domain, :domain_fingerprint) do
      {:ok, fingerprint} when is_binary(fingerprint) ->
        case String.trim(fingerprint) do
          "" -> nil
          trimmed -> trimmed
        end

      {:ok, _fingerprint} ->
        nil

      :error ->
        nil
    end
  end

  defp maybe_put_domain_fingerprint(domain, nil), do: domain

  defp maybe_put_domain_fingerprint(domain, domain_fingerprint),
    do: put_section(domain, :domain_fingerprint, domain_fingerprint)

  defp schema_version(domain) do
    case fetch_section(domain, :schema_version) do
      {:ok, version} -> normalize_schema_version(version)
      :error -> {@current_schema_version, true, []}
    end
  end

  defp normalize_schema_version(version) do
    case parse_schema_version(version) do
      version when is_integer(version) and version > @current_schema_version ->
        {version, false, [unsupported_schema_version_warning(version)]}

      version when is_integer(version) and version > 0 ->
        {version, false, []}

      _invalid ->
        {@current_schema_version, false, [invalid_schema_version_warning(version)]}
    end
  end

  defp parse_schema_version(version) when is_integer(version), do: version

  defp parse_schema_version(version) when is_binary(version) do
    case Integer.parse(version) do
      {integer, ""} -> integer
      _ -> version
    end
  end

  defp parse_schema_version(version), do: version

  defp unsupported_schema_version_warning(version) do
    %{
      code: :unsupported_schema_version,
      message: "schema_version is newer than this Selecto release understands",
      schema_version: version,
      supported_schema_version: @current_schema_version
    }
  end

  defp invalid_schema_version_warning(version) do
    %{
      code: :invalid_schema_version,
      message: "schema_version must be a positive integer; using the current schema version",
      value: version,
      schema_version: @current_schema_version
    }
  end

  defp section_shape_warnings(domain) do
    []
    |> Kernel.++(shape_warnings(domain, [:name], "atom or string", &name?/1))
    |> Kernel.++(
      shape_warnings(
        domain,
        [:domain_version],
        "non-empty atom, string, or integer",
        &domain_version?/1
      )
    )
    |> Kernel.++(
      shape_warnings(domain, [:domain_fingerprint], "non-empty string", &domain_fingerprint?/1)
    )
    |> Kernel.++(shape_warnings(domain, @map_sections, "map", &is_map/1))
    |> Kernel.++(shape_warnings(domain, @list_sections, "list", &is_list/1))
  end

  defp shape_warnings(domain, sections, expected, valid?) do
    Enum.flat_map(sections, fn section ->
      case fetch_section(domain, section) do
        {:ok, value} ->
          if valid?.(value) do
            []
          else
            [invalid_section_shape_warning(section, expected, value)]
          end

        :error ->
          []
      end
    end)
  end

  defp invalid_section_shape_warning(section, expected, value) do
    %{
      code: :invalid_section_shape,
      message: "domain section #{inspect(section)} should be a #{expected}",
      section: section,
      expected: expected,
      actual: value_type(value)
    }
  end

  defp name?(value), do: is_atom(value) or is_binary(value)

  defp domain_version?(value) when is_binary(value), do: String.trim(value) != ""
  defp domain_version?(value), do: is_atom(value) or is_integer(value)

  defp domain_fingerprint?(value) when is_binary(value), do: String.trim(value) != ""
  defp domain_fingerprint?(_value), do: false

  defp value_type(value) when is_map(value), do: :map
  defp value_type(value) when is_list(value), do: :list
  defp value_type(value) when is_binary(value), do: :string
  defp value_type(value) when is_atom(value), do: :atom
  defp value_type(value) when is_integer(value), do: :integer
  defp value_type(value) when is_float(value), do: :float
  defp value_type(value) when is_tuple(value), do: :tuple
  defp value_type(value) when is_function(value), do: :function
  defp value_type(_value), do: :term

  defp field_id(field) when is_atom(field), do: Atom.to_string(field)
  defp field_id(field) when is_binary(field), do: field
  defp field_id(field), do: inspect(field)

  defp map_value(map, key) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp map_value(_map, _key), do: nil

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

  defp safe_existing_atom(_value), do: nil

  defp section(domain, key, default \\ nil) do
    case fetch_section(domain, key) do
      {:ok, value} -> value
      :error -> default
    end
  end

  defp map_section(domain, key, default \\ nil) do
    case fetch_section(domain, key) do
      {:ok, value} -> value
      :error -> default
    end
  end

  defp fetch_section(domain, key) when is_atom(key) do
    case Map.fetch(domain, key) do
      {:ok, value} -> {:ok, value}
      :error -> Map.fetch(domain, Atom.to_string(key))
    end
  end
end
