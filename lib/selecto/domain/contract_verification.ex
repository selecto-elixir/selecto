defmodule Selecto.Domain.ContractVerification do
  @moduledoc """
  Read-only verification for published Selecto domain query surfaces.

  This module is the first consumer-aware contract verification slice. It
  projects provider domains into named published query surfaces, reads consumer
  `domain_dependencies`, and verifies that each consumer still depends only on
  fields, filters, query members, versions, and fingerprints the provider
  currently publishes.
  """

  alias Selecto.Domain
  alias Selecto.Domain.Shared.Map, as: MapHelpers

  @snapshot_format "selecto.domain_contract_snapshot"
  @snapshot_format_version 1
  @query_member_groups [:ctes, :values, :subqueries, :laterals, :unnests]

  @type verification_result :: {:ok, map()} | {:error, map()}

  @doc """
  Verifies one consumer domain against one provider domain.
  """
  @spec verify(term(), term(), keyword()) :: verification_result()
  def verify(provider, consumer, opts \\ []) do
    with {:ok, provider_projection} <- published_surfaces(provider, opts),
         {:ok, consumer_normalized} <- normalize_input(consumer) do
      dependencies = consumer_dependencies(consumer_normalized)
      provider_id = provider_identity(provider_projection)

      dependency_results =
        dependencies
        |> Enum.filter(&dependency_for_provider?(&1, provider_id))
        |> Enum.map(&verify_dependency(&1, provider_projection))

      errors = Enum.flat_map(dependency_results, &Map.get(&1, :errors, []))
      warnings = Enum.flat_map(dependency_results, &Map.get(&1, :warnings, []))

      report = %{
        provider: provider_projection.provider,
        consumer: consumer_identity(consumer_normalized),
        dependencies: dependency_results,
        errors: errors,
        warnings: warnings
      }

      if errors == [], do: {:ok, report}, else: {:error, report}
    end
  end

  @doc """
  Projects published query surfaces from a provider domain and validates that
  explicit surface references resolve against the provider query contract.
  """
  @spec published_surfaces(term(), keyword()) :: {:ok, map()} | {:error, term()}
  def published_surfaces(provider, opts \\ []) do
    with {:ok, normalized} <- normalize_input(provider),
         {:ok, query_contract, diagnostics} <- Domain.query_contract(normalized) do
      surfaces = build_surfaces(normalized, query_contract)
      errors = Enum.flat_map(surfaces, &surface_errors(&1, query_contract))

      projection = %{
        provider: provider_summary(normalized, query_contract),
        surfaces: surfaces,
        diagnostics: diagnostics,
        errors: errors
      }

      projection =
        case Keyword.get(opts, :strict, true) do
          false -> projection
          true -> projection
        end

      if errors == [], do: {:ok, projection}, else: {:error, projection}
    end
  end

  @doc """
  Builds a stable snapshot artifact for a provider's published query surfaces.
  """
  @spec snapshot(term(), keyword()) :: {:ok, map()} | {:error, term()}
  def snapshot(provider, opts \\ []) do
    with {:ok, projection} <- published_surfaces(provider, opts) do
      {:ok,
       %{
         format: @snapshot_format,
         format_version: @snapshot_format_version,
         generated_at: Keyword.get(opts, :generated_at, "stable"),
         provider: projection.provider,
         surfaces: projection.surfaces
       }}
    end
  end

  @doc """
  Diffs two contract snapshots and classifies compatible, review-required, and
  breaking changes.
  """
  @spec diff_snapshots(map(), map()) :: map()
  def diff_snapshots(left, right) when is_map(left) and is_map(right) do
    left_surfaces = snapshot_surface_index(left)
    right_surfaces = snapshot_surface_index(right)
    left_ids = left_surfaces |> Map.keys() |> Enum.sort()
    right_ids = right_surfaces |> Map.keys() |> Enum.sort()

    added = right_ids -- left_ids
    removed = left_ids -- right_ids

    changed =
      left_ids
      |> Enum.filter(&Map.has_key?(right_surfaces, &1))
      |> Enum.map(fn id ->
        surface_diff(id, Map.fetch!(left_surfaces, id), Map.fetch!(right_surfaces, id))
      end)
      |> Enum.reject(&(Map.get(&1, :changes) == []))

    changes =
      Enum.map(added, &surface_added(Map.fetch!(right_surfaces, &1))) ++
        Enum.map(removed, &surface_removed(Map.fetch!(left_surfaces, &1))) ++ changed

    %{
      left: snapshot_identity(left),
      right: snapshot_identity(right),
      surfaces: %{added: added, removed: removed, changed: changed},
      changes: changes,
      breaking?: Enum.any?(changes, &(Map.get(&1, :classification) == :breaking)),
      changed?: changes != []
    }
  end

  def snapshot_format, do: @snapshot_format
  def snapshot_format_version, do: @snapshot_format_version

  defp verify_dependency(dependency, provider_projection) do
    surface_id = dependency_contract_id(dependency)
    surface = Enum.find(provider_projection.surfaces, &(Map.get(&1, :id) == surface_id))

    cond do
      is_nil(surface_id) ->
        dependency_result(dependency, nil, [
          error(:missing_contract, "consumer dependency must name a provider contract")
        ])

      is_nil(surface) ->
        dependency_result(dependency, surface_id, [
          error(:missing_provider_contract, "provider does not publish contract #{surface_id}",
            contract: surface_id
          )
        ])

      true ->
        errors =
          []
          |> verify_version(dependency, surface)
          |> verify_fingerprint(dependency, provider_projection.provider, surface)
          |> verify_used_values(dependency, surface, :fields)
          |> verify_used_values(dependency, surface, :filters)
          |> verify_used_values(dependency, surface, :query_members)
          |> verify_required_filters(dependency, surface)

        dependency_result(dependency, surface_id, errors)
    end
  end

  defp dependency_result(dependency, surface_id, errors) do
    %{
      provider: dependency_provider(dependency),
      contract: surface_id,
      uses: dependency_uses(dependency),
      errors: errors,
      warnings: []
    }
  end

  defp verify_version(errors, dependency, surface) do
    accepts = map_get(dependency, :accepts)
    version = Map.get(surface, :version)

    cond do
      blank?(accepts) or blank?(version) ->
        errors

      version_matches?(version, accepts) ->
        errors

      true ->
        [
          error(
            :incompatible_contract_version,
            "provider contract version #{version} does not match #{accepts}",
            contract: Map.get(surface, :id),
            version: version,
            accepts: accepts
          )
          | errors
        ]
    end
  end

  defp verify_fingerprint(errors, dependency, provider, surface) do
    expected = map_get(dependency, :expected_fingerprint)
    actual = Map.get(surface, :fingerprint) || Map.get(provider, :domain_fingerprint)

    cond do
      blank?(expected) or blank?(actual) ->
        errors

      expected == actual ->
        errors

      true ->
        [
          error(:provider_fingerprint_changed, "provider fingerprint changed",
            contract: Map.get(surface, :id),
            expected_fingerprint: expected,
            actual_fingerprint: actual
          )
          | errors
        ]
    end
  end

  defp verify_used_values(errors, dependency, surface, key) do
    used = dependency_uses(dependency) |> Map.get(key, []) |> normalize_id_list()
    available = Map.get(surface, key, []) |> normalize_id_list()
    missing = used -- available

    Enum.reduce(missing, errors, fn id, acc ->
      [
        error(
          :"missing_#{key |> Atom.to_string() |> String.trim_trailing("s")}",
          "provider contract #{Map.get(surface, :id)} does not publish #{key} #{id}",
          contract: Map.get(surface, :id),
          value: id,
          section: key
        )
        | acc
      ]
    end)
  end

  defp verify_required_filters(errors, dependency, surface) do
    satisfiable =
      dependency
      |> map_get(:satisfies, [])
      |> normalize_id_list()

    required =
      surface
      |> Map.get(:required_filters, [])
      |> normalize_id_list()

    missing = required -- satisfiable

    Enum.reduce(missing, errors, fn filter, acc ->
      [
        error(:unsatisfied_required_filter, "consumer does not satisfy required filter #{filter}",
          contract: Map.get(surface, :id),
          filter: filter
        )
        | acc
      ]
    end)
  end

  defp surface_errors(surface, query_contract) do
    field_ids = query_contract |> Map.get(:fields, []) |> entry_ids()
    filter_ids = query_contract |> Map.get(:filters, []) |> entry_ids()
    query_member_ids = query_contract |> Map.get(:query_members, %{}) |> query_member_ids()

    []
    |> missing_surface_values(surface, :fields, field_ids)
    |> missing_surface_values(surface, :filters, filter_ids)
    |> missing_surface_values(surface, :query_members, query_member_ids)
    |> missing_surface_values(surface, :required_filters, Enum.uniq(field_ids ++ filter_ids))
    |> invalid_result_types(surface)
  end

  defp missing_surface_values(errors, surface, key, available) do
    explicit? = Map.get(surface, :"#{key}_explicit?", false)
    values = surface |> Map.get(key, []) |> normalize_id_list()

    if explicit? do
      missing = values -- available

      Enum.reduce(missing, errors, fn value, acc ->
        [
          error(
            :invalid_published_surface_reference,
            "published surface references missing #{key} #{value}",
            contract: Map.get(surface, :id),
            section: key,
            value: value
          )
          | acc
        ]
      end)
    else
      errors
    end
  end

  defp invalid_result_types(errors, surface) do
    surface
    |> Map.get(:result_shape, %{})
    |> Enum.reduce(errors, fn
      {_field, type}, acc when is_binary(type) and type != "" ->
        acc

      {field, type}, acc ->
        [
          error(
            :invalid_published_surface_result_type,
            "published surface result field #{field} is missing a result type",
            contract: Map.get(surface, :id),
            field: field,
            type: type
          )
          | acc
        ]
    end)
  end

  defp build_surfaces(normalized, query_contract) do
    published_views = Map.get(query_contract, :published_views, [])
    domain = Map.fetch!(normalized, :domain)

    Enum.map(published_views, fn view ->
      view_id = view |> map_get(:id) |> id_string()
      spec = published_view_spec(domain, view_id)
      columns = map_get(view, :columns, [])
      result_shape = result_shape(columns)
      explicit_fields = explicit_list(spec, :fields)
      explicit_filters = explicit_list(spec, :filters)
      explicit_query_members = explicit_list(spec, :query_members)
      explicit_required_filters = explicit_list(spec, :required_filters)

      %{
        id: view_id,
        kind: :query_surface,
        source: :published_view,
        version:
          map_get(spec, :version) || map_get(spec, :domain_version) ||
            Map.get(normalized, :domain_version),
        compatibility: map_get(spec, :compatibility),
        stable: map_get(spec, :stable) != false,
        fields: explicit_fields || Map.keys(result_shape),
        filters: explicit_filters || [],
        query_members: explicit_query_members || [],
        required_filters: explicit_required_filters || [],
        result_shape: result_shape,
        capability: map_get(view, :capability),
        fingerprint: map_get(spec, :fingerprint)
      }
      |> Map.put(:fields_explicit?, not is_nil(explicit_fields))
      |> Map.put(:filters_explicit?, not is_nil(explicit_filters))
      |> Map.put(:query_members_explicit?, not is_nil(explicit_query_members))
      |> Map.put(:required_filters_explicit?, not is_nil(explicit_required_filters))
    end)
  end

  defp provider_summary(normalized, query_contract) do
    %{
      name: query_contract |> map_get(:name) |> id_string(),
      schema_version: Map.get(normalized, :schema_version),
      domain_version: Map.get(normalized, :domain_version),
      domain_fingerprint: Map.get(normalized, :domain_fingerprint)
    }
  end

  defp consumer_identity(normalized) do
    domain = Map.fetch!(normalized, :domain)

    %{
      name: domain |> map_get(:name) |> id_string(),
      schema_version: Map.get(normalized, :schema_version),
      domain_version: Map.get(normalized, :domain_version),
      domain_fingerprint: Map.get(normalized, :domain_fingerprint)
    }
  end

  defp consumer_dependencies(normalized) do
    normalized
    |> Map.fetch!(:domain)
    |> map_get(:domain_dependencies, [])
    |> case do
      dependencies when is_list(dependencies) -> dependencies
      dependencies when is_map(dependencies) -> Map.values(dependencies)
      _dependencies -> []
    end
    |> Enum.filter(&is_map/1)
  end

  defp dependency_for_provider?(dependency, provider_id) do
    provider = dependency_provider(dependency)
    blank?(provider) or provider == provider_id.name
  end

  defp provider_identity(%{provider: provider}), do: provider

  defp dependency_provider(dependency), do: dependency |> map_get(:provider) |> id_string()

  defp dependency_contract_id(dependency) do
    (map_get(dependency, :contract) || map_get(dependency, :surface) || map_get(dependency, :name))
    |> id_string()
  end

  defp dependency_uses(dependency) do
    uses = map_get(dependency, :uses, %{})

    %{
      fields: map_get(uses, :fields, map_get(dependency, :fields, [])),
      filters: map_get(uses, :filters, map_get(dependency, :filters, [])),
      query_members: map_get(uses, :query_members, map_get(dependency, :query_members, []))
    }
    |> Map.new(fn {key, values} -> {key, normalize_id_list(values)} end)
  end

  defp normalize_input(%{schema_version: _schema_version, domain: %{}, query: %{}} = normalized),
    do: {:ok, normalized}

  defp normalize_input(domain) do
    case Domain.normalize(domain) do
      {:ok, normalized, _diagnostics} -> {:ok, normalized}
      {:error, diagnostics} -> {:error, diagnostics}
    end
  end

  defp published_view_spec(domain, view_id) do
    domain
    |> map_get(:published_views, %{})
    |> fetch_id(view_id, %{})
  end

  defp result_shape(columns) when is_list(columns) do
    columns
    |> Enum.map(fn column ->
      id = column |> map_get(:id) |> id_string()
      {id, column |> map_get(:type) |> id_string()}
    end)
    |> Enum.reject(fn {id, _type} -> blank?(id) end)
    |> Map.new()
  end

  defp result_shape(_columns), do: %{}

  defp explicit_list(map, key) do
    case map_get(map, key) do
      values when is_list(values) -> normalize_id_list(values)
      _value -> nil
    end
  end

  defp entry_ids(entries) when is_list(entries) do
    entries
    |> Enum.map(&(map_get(&1, :id) |> id_string()))
    |> Enum.reject(&blank?/1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp entry_ids(_entries), do: []

  defp query_member_ids(query_members) when is_map(query_members) do
    Enum.flat_map(@query_member_groups, fn group ->
      query_members
      |> map_get(group, [])
      |> Enum.map(&(id_string(group) <> "." <> id_string(map_get(&1, :id))))
    end)
    |> Enum.reject(&String.ends_with?(&1, "."))
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp query_member_ids(_query_members), do: []

  defp fetch_id(map, id, default) when is_map(map) do
    case MapHelpers.fetch_key(map, id) do
      {:ok, value} -> value
      :error -> default
    end
  end

  defp fetch_id(_map, _id, default), do: default

  defp normalize_id_list(values) when is_list(values) do
    values
    |> Enum.map(&id_string/1)
    |> Enum.reject(&blank?/1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp normalize_id_list(value), do: normalize_id_list(List.wrap(value))

  defp map_get(map, key, default \\ nil)

  defp map_get(map, key, default) when is_map(map) do
    case MapHelpers.fetch_key(map, key) do
      {:ok, value} -> value
      :error -> default
    end
  end

  defp map_get(_map, _key, default), do: default

  defp id_string(nil), do: nil
  defp id_string(value) when is_atom(value), do: Atom.to_string(value)
  defp id_string(value) when is_binary(value), do: value
  defp id_string(value), do: to_string(value)

  defp blank?(nil), do: true
  defp blank?(value) when is_binary(value), do: String.trim(value) == ""
  defp blank?(_value), do: false

  defp version_matches?(version, accepts) do
    Version.match?(to_string(version), to_string(accepts))
  rescue
    Version.InvalidRequirementError -> to_string(version) == to_string(accepts)
    Version.InvalidVersionError -> to_string(version) == to_string(accepts)
  end

  defp snapshot_surface_index(snapshot) do
    snapshot
    |> map_get(:surfaces, [])
    |> Enum.filter(&is_map/1)
    |> Map.new(fn surface -> {surface |> map_get(:id) |> id_string(), surface} end)
  end

  defp snapshot_identity(snapshot) do
    %{
      format: map_get(snapshot, :format),
      format_version: map_get(snapshot, :format_version),
      provider: map_get(snapshot, :provider, %{})
    }
  end

  defp surface_added(surface) do
    %{
      contract: surface |> map_get(:id) |> id_string(),
      classification: :compatible,
      changes: [%{kind: :surface_added}]
    }
  end

  defp surface_removed(surface) do
    %{
      contract: surface |> map_get(:id) |> id_string(),
      classification: :breaking,
      changes: [%{kind: :surface_removed}]
    }
  end

  defp surface_diff(id, left, right) do
    changes =
      []
      |> diff_list_surface(:fields, left, right)
      |> diff_list_surface(:filters, left, right)
      |> diff_list_surface(:query_members, left, right)
      |> diff_result_shape(left, right)
      |> diff_value_surface(:version, left, right, :review_required)
      |> diff_value_surface(:fingerprint, left, right, :review_required)

    %{
      contract: id,
      classification: classify_changes(changes),
      changes: Enum.reverse(changes)
    }
  end

  defp diff_list_surface(changes, key, left, right) do
    left_values = left |> map_get(key, []) |> normalize_id_list()
    right_values = right |> map_get(key, []) |> normalize_id_list()

    removed =
      Enum.map(left_values -- right_values, fn value ->
        %{
          kind: :"#{key |> Atom.to_string() |> String.trim_trailing("s")}_removed",
          value: value,
          classification: :breaking
        }
      end)

    added =
      Enum.map(right_values -- left_values, fn value ->
        %{
          kind: :"#{key |> Atom.to_string() |> String.trim_trailing("s")}_added",
          value: value,
          classification: :compatible
        }
      end)

    added ++ removed ++ changes
  end

  defp diff_result_shape(changes, left, right) do
    left_shape = map_get(left, :result_shape, %{})
    right_shape = map_get(right, :result_shape, %{})
    left_fields = left_shape |> Map.keys() |> Enum.sort()
    right_fields = right_shape |> Map.keys() |> Enum.sort()

    removed =
      Enum.map(left_fields -- right_fields, fn field ->
        %{kind: :result_field_removed, field: field, classification: :breaking}
      end)

    added =
      Enum.map(right_fields -- left_fields, fn field ->
        %{kind: :result_field_added, field: field, classification: :compatible}
      end)

    changed =
      left_fields
      |> Enum.filter(&Map.has_key?(right_shape, &1))
      |> Enum.flat_map(fn field ->
        if Map.get(left_shape, field) == Map.get(right_shape, field) do
          []
        else
          [
            %{
              kind: :result_type_changed,
              field: field,
              left: Map.get(left_shape, field),
              right: Map.get(right_shape, field),
              classification: :breaking
            }
          ]
        end
      end)

    changed ++ added ++ removed ++ changes
  end

  defp diff_value_surface(changes, key, left, right, classification) do
    left_value = map_get(left, key)
    right_value = map_get(right, key)

    if left_value == right_value do
      changes
    else
      [
        %{
          kind: :"#{key}_changed",
          left: left_value,
          right: right_value,
          classification: classification
        }
        | changes
      ]
    end
  end

  defp classify_changes(changes) do
    cond do
      Enum.any?(changes, &(Map.get(&1, :classification) == :breaking)) -> :breaking
      Enum.any?(changes, &(Map.get(&1, :classification) == :review_required)) -> :review_required
      changes == [] -> :unchanged
      true -> :compatible
    end
  end

  defp error(code, message, extra \\ []) do
    extra
    |> Map.new()
    |> Map.merge(%{code: code, message: message})
  end
end
