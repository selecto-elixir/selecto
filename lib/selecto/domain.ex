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
    {schema_version, schema_version_inferred} = schema_version(domain)
    sections = Sections.classify_top_level_keys(domain)

    diagnostics =
      Diagnostics.new(
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
      {:ok, version} -> {normalize_schema_version(version), false}
      :error -> {@current_schema_version, true}
    end
  end

  defp normalize_schema_version(version) when is_integer(version), do: version

  defp normalize_schema_version(version) when is_binary(version) do
    case Integer.parse(version) do
      {integer, ""} -> integer
      _ -> version
    end
  end

  defp normalize_schema_version(version), do: version

  defp section(domain, key, default \\ nil) do
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
