defmodule Selecto.Domain do
  @moduledoc """
  Compatibility-safe normalization entry point for Selecto domains.

  This module does not participate in `Selecto.configure/3` yet. It provides a
  read-only normalization boundary for callers that want a stable, diagnostic
  view of authored domain maps while existing runtime behavior remains
  unchanged.
  """

  use Selecto.Domain.Constants

  alias Selecto.Domain.Diagnostics
  alias Selecto.Domain.Sections
  alias Selecto.Domain.Shared.Map, as: MapHelpers
  alias Selecto.Domain.Shorthand
  alias Selecto.Domain.Inspector
  alias Selecto.Domain.Projector
  alias Selecto.Domain.Compose

  @current_schema_version 1
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
      |> Shorthand.normalize_authoring_shorthand()

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
    Compose.compose(domain, overlays, &normalize/1)
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

    {:ok, Inspector.inspection_output(normalized, diagnostics), diagnostics}
  end

  def describe(domain) do
    with {:ok, normalized, diagnostics} <- normalize(domain) do
      {:ok, Inspector.inspection_output(normalized, diagnostics), diagnostics}
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

    {:ok, Projector.project(normalized, :query_contract), diagnostics}
  end

  def query_contract(domain) do
    with {:ok, normalized, diagnostics} <- normalize(domain) do
      {:ok, Projector.project(normalized, :query_contract), diagnostics}
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
  defdelegate project(normalized, projection), to: Projector

  def normalized_domain(
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
      source: MapHelpers.section(canonical_domain, :source),
      schemas: MapHelpers.section(canonical_domain, :schemas, %{}),
      joins: MapHelpers.section(canonical_domain, :joins, %{}),
      query: Projector.query_sections(canonical_domain),
      projection: Projector.projection_sections(canonical_domain),
      writes: MapHelpers.section(canonical_domain, :writes, %{}),
      actions: MapHelpers.section(canonical_domain, :actions, %{}),
      capabilities: MapHelpers.section(canonical_domain, :capabilities, %{}),
      source_relationships: MapHelpers.section(canonical_domain, :source_relationships, %{}),
      choice_sources: MapHelpers.section(canonical_domain, :choice_sources, %{}),
      detail_actions: MapHelpers.section(canonical_domain, :detail_actions, %{}),
      domain_data: MapHelpers.section(canonical_domain, :domain_data, %{}),
      extensions: MapHelpers.section(canonical_domain, :extensions, [])
    }
  end
  def domain_version(domain) do
    case MapHelpers.fetch_section(domain, :domain_version) do
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
  def maybe_put_domain_version(domain, nil), do: domain

  def maybe_put_domain_version(domain, domain_version),
    do: MapHelpers.put_section(domain, :domain_version, domain_version)
  def domain_fingerprint(domain) do
    case MapHelpers.fetch_section(domain, :domain_fingerprint) do
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
  def maybe_put_domain_fingerprint(domain, nil), do: domain

  def maybe_put_domain_fingerprint(domain, domain_fingerprint),
    do: MapHelpers.put_section(domain, :domain_fingerprint, domain_fingerprint)
  def schema_version(domain) do
    case MapHelpers.fetch_section(domain, :schema_version) do
      {:ok, version} -> normalize_schema_version(version)
      :error -> {@current_schema_version, true, []}
    end
  end
  def normalize_schema_version(version) do
    case parse_schema_version(version) do
      version when is_integer(version) and version > @current_schema_version ->
        {version, false, [unsupported_schema_version_warning(version)]}

      version when is_integer(version) and version > 0 ->
        {version, false, []}

      _invalid ->
        {@current_schema_version, false, [invalid_schema_version_warning(version)]}
    end
  end
  def parse_schema_version(version) when is_integer(version), do: version

  def parse_schema_version(version) when is_binary(version) do
    case Integer.parse(version) do
      {integer, ""} -> integer
      _ -> version
    end
  end

  def parse_schema_version(version), do: version
  def unsupported_schema_version_warning(version) do
    %{
      code: :unsupported_schema_version,
      message: "schema_version is newer than this Selecto release understands",
      schema_version: version,
      supported_schema_version: @current_schema_version
    }
  end
  def invalid_schema_version_warning(version) do
    %{
      code: :invalid_schema_version,
      message: "schema_version must be a positive integer; using the current schema version",
      value: version,
      schema_version: @current_schema_version
    }
  end
  def section_shape_warnings(domain) do
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
  def shape_warnings(domain, sections, expected, valid?) do
    Enum.flat_map(sections, fn section ->
      case MapHelpers.fetch_section(domain, section) do
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
  def invalid_section_shape_warning(section, expected, value) do
    %{
      code: :invalid_section_shape,
      message: "domain section #{inspect(section)} should be a #{expected}",
      section: section,
      expected: expected,
      actual: MapHelpers.value_type(value)
    }
  end
  def name?(value), do: is_atom(value) or is_binary(value)
  def domain_version?(value) when is_binary(value), do: String.trim(value) != ""
  def domain_version?(value), do: is_atom(value) or is_integer(value)
  def domain_fingerprint?(value) when is_binary(value), do: String.trim(value) != ""
  def domain_fingerprint?(_value), do: false
end
