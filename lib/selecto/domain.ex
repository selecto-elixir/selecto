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

  @doc """
  Normalizes an authored domain map into a compatibility-safe contract.

  The normalizer currently:

  - infers `schema_version` as `1` when it is missing
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

    {:ok, normalized_domain(domain, schema_version, sections), diagnostics}
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

  defp normalized_domain(domain, schema_version, sections) do
    %{
      schema_version: schema_version,
      authored_domain: domain,
      domain: Map.put(domain, :schema_version, schema_version),
      sections: sections,
      source: section(domain, :source),
      schemas: section(domain, :schemas, %{}),
      joins: section(domain, :joins, %{}),
      query: query_sections(domain),
      projection: projection_sections(domain),
      writes: section(domain, :writes, %{}),
      actions: section(domain, :actions, %{}),
      capabilities: section(domain, :capabilities, %{}),
      source_relationships: section(domain, :source_relationships, %{}),
      choice_sources: section(domain, :choice_sources, %{}),
      detail_actions: section(domain, :detail_actions, %{}),
      domain_data: section(domain, :domain_data, %{}),
      extensions: section(domain, :extensions, [])
    }
  end

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
