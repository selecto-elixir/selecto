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
  @projections [:query, :write, :ui, :api]
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
  @known_sections [:schema_version, :name | @map_sections ++ @list_sections]
  @collision_warning_sections [
    :actions,
    :capabilities,
    :source_relationships,
    :choice_sources
  ]

  @doc """
  Normalizes an authored domain map into a compatibility-safe contract.

  The normalizer currently:

  - infers `schema_version` as `1` when it is missing
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
    sections = Sections.classify_top_level_keys(domain)

    diagnostics =
      Diagnostics.new(
        warnings: schema_version_warnings ++ section_shape_warnings(domain),
        sections: sections,
        schema_version: schema_version,
        schema_version_inferred: schema_version_inferred
      )

    canonical_domain =
      normalize_authoring_shorthand(Map.put(domain, :schema_version, schema_version))

    {:ok, normalized_domain(domain, canonical_domain, schema_version, sections), diagnostics}
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
  Projects a normalized domain into a read-only consumer view.

  Projection helpers are intentionally conservative in this slice. They reshape
  the normalized map for future consumers, but no existing runtime path calls
  them yet.

  Supported projections:

  - `:query` - query/runtime-facing sections
  - `:write` - write/action/reference sections
  - `:ui` - display defaults, choices, actions, and detail actions
  - `:api` - read/write/action contract for API-style consumers
  """
  @spec project(map(), :query | :write | :ui | :api) :: map()
  def project(%{schema_version: _schema_version, domain: _domain} = normalized, :query) do
    normalized
    |> base_projection()
    |> Map.merge(Map.fetch!(normalized, :query))
    |> Map.merge(take_projection_sections(normalized, @query_projection_sections))
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

  defp normalized_domain(authored_domain, canonical_domain, schema_version, sections) do
    %{
      schema_version: schema_version,
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
    case section(domain, :columns, %{}) do
      columns when is_map(columns) ->
        {columns, acc} = normalize_columns_choice_shorthand(columns, :projection, acc)
        {put_section(domain, :columns, columns), acc}

      _columns ->
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
  end

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
