#!/usr/bin/env python3
"""Mechanically split Selecto.Domain into focused submodules."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOMAIN = ROOT / "lib/selecto/domain.ex"
OUT_DIR = ROOT / "lib/selecto/domain"

SHARED_MAP_FUNCS = [
    "relation_field_entries",
    "relation_association",
    "field_label",
    "field_ref_or_nil",
    "first_map_value",
    "id_value",
    "schema_fields",
    "relation_field_ids",
    "query_member_keys",
    "query_member_count",
    "sorted_entries",
    "sorted_keys",
    "map_count",
    "list_count",
    "compact_nil",
    "function_arity",
    "maybe_put",
    "maybe_put_default",
    "put_section",
    "put_map_value",
    "delete_key_variants",
    "has_key_variant?",
    "section",
    "map_section",
    "fetch_section",
]

FIELD_BINDINGS_FUNCS = [
    "field_choice_bindings",
    "schema_field_choice_bindings",
    "relation_field_choice_bindings",
    "column_field_choice_bindings",
    "column_field_choice_binding",
]

INSPECTOR_FUNCS = [
    "inspection_output",
    "inspection_sections",
    "inspection_diagnostics",
    "diagnostic_codes",
    "inspection_counts",
    "inspection_registries",
    "inspect_writes",
    "inspect_write_scope",
    "inspect_write_scope_entry",
    "inspect_write_hooks",
    "inspect_write_hook_ref",
    "inspect_actions",
    "inspect_capabilities",
    "inspect_capability_visibility",
    "capability_visibility_entry",
    "capability_actions",
    "capability_visibility_references",
    "capability_visibility_reference",
    "format_capability_path",
    "normalize_capability_id",
    "maybe_put_nonempty",
    "inspect_security_review",
    "inspect_security_registry",
    "inspect_security_writes",
    "inspect_capability_usage",
    "inspect_schema_capability_usage",
    "inspect_relation_capability_usage",
    "inspect_capability_section_usage",
    "inspect_query_member_capability_usage",
    "capability_usage_entries",
    "capability_usage_sort_key",
    "inspect_source_relationships",
    "inspect_choice_sources",
]

PROJECTOR_FUNCS = [
    "query_contract_source",
    "query_contract_defaults",
    "query_contract_fields",
    "query_contract_schema_fields",
    "query_contract_relation_fields",
    "query_contract_custom_fields",
    "query_contract_joins",
    "query_contract_join_tree",
    "query_contract_join_target_relation",
    "query_contract_filters",
    "query_contract_filterable_fields",
    "query_contract_field_surface",
    "query_contract_detail_selectable?",
    "query_contract_filterable?",
    "query_contract_sortable?",
    "query_contract_groupable?",
    "query_contract_aggregatable?",
    "query_contract_comparators",
    "query_contract_filter_comparators",
    "query_contract_aggregate_functions",
    "query_contract_type_comparators",
    "query_contract_bool",
    "query_contract_bool_override",
    "query_contract_list_override",
    "query_contract_type_id",
    "query_contract_functions",
    "query_contract_function_args",
    "query_contract_query_members",
    "query_contract_query_member_group",
    "query_contract_query_member",
    "query_contract_published_views",
    "query_contract_source_relationships",
    "query_contract_choice_sources",
    "query_contract_choice_bindings",
    "query_contract_columns",
    "query_contract_alias",
    "query_contract_lateral_source",
    "query_contract_choice_index",
    "query_contract_choice_source",
    "base_projection",
    "take_query_sections",
    "take_projection_sections",
    "projection_section",
    "query_sections",
    "projection_sections",
]

COMPOSE_FUNCS = [
    "domain_overlays",
    "invalid_domain_overlay_diagnostics",
    "apply_composed_extensions",
    "merge_domain_maps",
    "merge_section_value",
    "deep_merge_domain_maps",
    "merge_nested_value",
    "merge_registry_maps",
    "equivalent_map_key",
    "composition_collision_warnings",
    "composition_collision_warning",
    "merge_key_member?",
    "merge_key_id",
    "fetch_merge_value",
    "put_merge_value",
    "canonical_section_key",
    "unique_list",
]

SHORTHAND_FUNCS = [
    "normalize_authoring_shorthand",
    "normalize_source_choice_shorthand",
    "normalize_schema_choice_shorthand",
    "normalize_projection_choice_shorthand",
    "normalize_relation_choice_shorthand",
    "normalize_columns_choice_shorthand",
    "normalize_column_choice_shorthand",
    "shorthand_choice_source",
    "shorthand_source_relationship",
    "shorthand_field_reference",
    "shorthand_choice_source_id",
    "shorthand_source_relationship_id",
    "shorthand_field_prefix",
    "scoped_field_ref",
    "normalize_choice_source_presentation",
    "first_virtual_join_entry",
    "path_leaf",
    "path_parent",
    "put_registry_entry",
    "registry_has_key?",
    "registry_has_atom_key?",
]

NORMALIZER_FUNCS = [
    "normalized_domain",
    "domain_version",
    "maybe_put_domain_version",
    "domain_fingerprint",
    "maybe_put_domain_fingerprint",
    "schema_version",
    "normalize_schema_version",
    "parse_schema_version",
    "unsupported_schema_version_warning",
    "invalid_schema_version_warning",
    "section_shape_warnings",
    "shape_warnings",
    "invalid_section_shape_warning",
    "name?",
    "domain_version?",
    "domain_fingerprint?",
]


def read_lines() -> list[str]:
    return DOMAIN.read_text().splitlines(keepends=True)


def extract_functions(lines: list[str], names: list[str]) -> dict[str, str]:
    pattern = re.compile(r"^  def(p?) ([a-zA-Z0-9_?!]+)\(")
    all_starts: list[tuple[int, str]] = []
    first_starts: dict[str, int] = {}

    for idx, line in enumerate(lines):
        match = pattern.match(line)
        if match:
            name = match.group(2)
            all_starts.append((idx, name))
            first_starts.setdefault(name, idx)

    extracted: dict[str, str] = {}

    for name in names:
        if name not in first_starts:
            raise SystemExit(f"missing function {name}")
        start = first_starts[name]
        end = len(lines)
        for idx, other_name in all_starts:
            if idx > start and other_name != name:
                end = idx
                break
        chunk = "".join(lines[start:end])
        if end == len(lines):
            trimmed_lines = chunk.splitlines()
            if trimmed_lines and trimmed_lines[-1].strip() == "end":
                chunk = "\n".join(trimmed_lines[:-1]) + "\n"
        extracted[name] = chunk.rstrip() + "\n"
    return extracted


def to_public(body: str) -> str:
    return body.replace("  defp ", "  def ")


CORE_DELEGATES = [
    "field_id",
    "map_value",
    "fetch_key",
    "value_type",
    "safe_existing_atom",
    "relation_field_ref",
]


def prefix_all_map_helpers(body: str) -> str:
    body = prefix_calls(body, "MapHelpers", SHARED_MAP_FUNCS)
    body = prefix_calls(body, "MapHelpers", CORE_DELEGATES)
    return body


def prefix_calls(body: str, prefix: str, names: list[str]) -> str:
    lines = body.splitlines(keepends=True)
    out = []
    for line in lines:
        if re.match(r"^  def(?:p)? ", line) and ", do:" not in line:
            out.append(line)
            continue

        updated = line
        for name in sorted(names, key=len, reverse=True):
            updated = re.sub(
                rf"(?<![\w\.]){re.escape(name)}\(",
                f"{prefix}.{name}(",
                updated,
            )
            updated = re.sub(
                rf"&{re.escape(name)}/",
                f"&{prefix}.{name}/",
                updated,
            )
        out.append(updated)
    return "".join(out)


def build_shared_map(extracted: dict[str, str]) -> str:
    body = "".join(to_public(extracted[name]) for name in SHARED_MAP_FUNCS)
    return f"""defmodule Selecto.Domain.Shared.Map do
  @moduledoc false

  alias Selecto.Domain.Contract.Shared.Core

  defdelegate field_id(field), to: Core
  defdelegate map_value(map, key), to: Core
  defdelegate fetch_key(map, key), to: Core
  defdelegate value_type(value), to: Core
  defdelegate safe_existing_atom(value), to: Core
  defdelegate relation_field_ref(relation_id, field), to: Core

{body}end
"""


def build_field_bindings(extracted: dict[str, str]) -> str:
    body = ""
    for name in FIELD_BINDINGS_FUNCS:
        chunk = prefix_all_map_helpers(extracted[name])
        body += to_public(chunk)
    return f"""defmodule Selecto.Domain.FieldBindings do
  @moduledoc false

  alias Selecto.Domain.Shared.Map, as: MapHelpers

{body}end
"""


def build_inspector(extracted: dict[str, str]) -> str:
    body = ""
    for name in INSPECTOR_FUNCS:
        chunk = prefix_all_map_helpers(extracted[name])
        chunk = prefix_calls(chunk, "FieldBindings", FIELD_BINDINGS_FUNCS)
        body += to_public(chunk)
    return f"""defmodule Selecto.Domain.Inspector do
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

{body}end
"""


def fill_template(template: str, body: str) -> str:
    return template.replace("__BODY__", body)


def build_projector(extracted: dict[str, str]) -> str:
    body = ""
    for name in PROJECTOR_FUNCS:
        chunk = prefix_all_map_helpers(extracted[name])
        chunk = prefix_calls(chunk, "FieldBindings", FIELD_BINDINGS_FUNCS)
        body += to_public(chunk)
    template = """defmodule Selecto.Domain.Projector do
  @moduledoc false

  use Selecto.Domain.Constants

  alias Selecto.Domain.Shared.Map, as: MapHelpers
  alias Selecto.Domain.FieldBindings

  @projections [:query, :write, :ui, :api, :query_contract]
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
    field_choice_bindings = FieldBindings.field_choice_bindings(normalized)

    %{
      schema_version: Map.fetch!(normalized, :schema_version),
      name: MapHelpers.map_section(Map.fetch!(normalized, :domain), :name),
      projection: :query_contract,
      source: query_contract_source(normalized),
      fields: query_contract_fields(normalized, field_choice_bindings),
      joins: query_contract_joins(normalized),
      defaults: query_contract_defaults(query),
      filters: query_contract_filters(MapHelpers.map_value(query, :filters)),
      functions: query_contract_functions(MapHelpers.map_value(query, :functions)),
      query_members: query_contract_query_members(MapHelpers.map_value(query, :query_members)),
      published_views: query_contract_published_views(MapHelpers.map_value(query, :published_views)),
      source_relationships:
        query_contract_source_relationships(Map.get(normalized, :source_relationships, %{})),
      choice_sources: query_contract_choice_sources(Map.get(normalized, :choice_sources, %{})),
      field_choice_bindings: query_contract_choice_bindings(field_choice_bindings),
      capability_ids: MapHelpers.sorted_keys(Map.get(normalized, :capabilities, %{}))
    }
    |> MapHelpers.maybe_put(:domain_version, Map.get(normalized, :domain_version))
    |> MapHelpers.maybe_put(:domain_fingerprint, Map.get(normalized, :domain_fingerprint))
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

__BODY__end
"""
    return fill_template(template, body)


def build_compose(extracted: dict[str, str]) -> str:
    body = ""
    for name in COMPOSE_FUNCS:
        chunk = prefix_all_map_helpers(extracted[name])
        chunk = prefix_calls(chunk, "Shorthand", SHORTHAND_FUNCS)
        body += to_public(chunk)
    template = """defmodule Selecto.Domain.Compose do
  @moduledoc false

  alias Selecto.Domain.Diagnostics
  alias Selecto.Domain.Shared.Map, as: MapHelpers
  alias Selecto.Domain.Shorthand

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
  @known_sections [
    :schema_version,
    :domain_version,
    :domain_fingerprint,
    :name | @map_sections ++ [
      :default_selected,
      :required_selected,
      :required_filters,
      :required_order_by,
      :required_group_by,
      :redact_fields,
      :extensions
    ]
  ]
  @collision_warning_sections [
    :actions,
    :capabilities,
    :source_relationships,
    :choice_sources
  ]

  @spec compose(map(), list(), (map() -> {:ok, map(), map()} | {:error, map()})) ::
          {:ok, map(), map()} | {:error, map()}
  def compose(domain, overlays, normalize_fun) do
    with {:ok, normalized, _diagnostics} <- normalize_fun.(domain),
         {:ok, overlays} <- domain_overlays(overlays) do
      {composed_domain, composition_warnings} =
        overlays
        |> Enum.with_index()
        |> Enum.reduce({normalized.domain, []}, fn {overlay, index}, {acc, warnings} ->
          overlay = Shorthand.normalize_authoring_shorthand(overlay)

          {
            merge_domain_maps(acc, overlay),
            warnings ++ composition_collision_warnings(acc, overlay, index)
          }
        end)

      composed_domain = apply_composed_extensions(composed_domain)

      with {:ok, normalized, diagnostics} <- normalize_fun.(composed_domain) do
        {:ok, normalized, %{diagnostics | warnings: composition_warnings ++ diagnostics.warnings}}
      end
    end
  end

__BODY__end
"""
    return fill_template(template, body)


def build_shorthand(extracted: dict[str, str]) -> str:
    body = ""
    for name in SHORTHAND_FUNCS:
        chunk = prefix_all_map_helpers(extracted[name])
        body += to_public(chunk)
    return f"""defmodule Selecto.Domain.Shorthand do
  @moduledoc false

  alias Selecto.Domain.Shared.Map, as: MapHelpers

{body}end
"""


def build_orchestrator(extracted: dict[str, str]) -> str:
    normalizer_body = ""
    for name in NORMALIZER_FUNCS:
        chunk = prefix_all_map_helpers(extracted[name])
        chunk = prefix_calls(chunk, "Projector", ["query_sections", "projection_sections"])
        normalizer_body += to_public(chunk)

    template = """defmodule Selecto.Domain do
  @moduledoc \"\"\"
  Compatibility-safe normalization entry point for Selecto domains.

  This module does not participate in `Selecto.configure/3` yet. It provides a
  read-only normalization boundary for callers that want a stable, diagnostic
  view of authored domain maps while existing runtime behavior remains
  unchanged.
  \"\"\"

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

  @doc \"\"\"
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
  \"\"\"
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

  @doc \"\"\"
  Normalizes an authored domain and validates it against the first-wave
  canonical contract.

  This is still a compatibility-safe entry point: it does not participate in
  `Selecto.configure/3` unless a caller opts in elsewhere.
  \"\"\"
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

  @doc \"\"\"
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
  \"\"\"
  @spec compose(term(), term()) :: {:ok, map(), Diagnostics.t()} | {:error, Diagnostics.t()}
  def compose(domain, overlays \\\\ []) do
    Compose.compose(domain, overlays, &normalize/1)
  end

  @doc \"\"\"
  Returns structured inspection output for an authored or normalized domain.

  The inspection map is intentionally compact and deterministic so generators,
  Studio, docs, and tests can reason about the normalized contract without
  walking the whole domain map directly.
  \"\"\"
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

  @doc \"\"\"
  Returns the constrained query contract projection for an authored or normalized
  domain.

  This is a convenience wrapper around `normalize/1` and
  `project(normalized, :query_contract)` for Components, AI tooling, and other
  consumers that should not need to walk the normalized domain map directly.
  \"\"\"
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

  @doc \"\"\"
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
  \"\"\"
  @spec project(map(), :query | :write | :ui | :api | :query_contract) :: map()
  defdelegate project(normalized, projection), to: Projector

__BODY__end
"""
    return fill_template(template, normalizer_body)


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def main() -> None:
    lines = read_lines()
    all_names = (
        SHARED_MAP_FUNCS
        + FIELD_BINDINGS_FUNCS
        + INSPECTOR_FUNCS
        + PROJECTOR_FUNCS
        + COMPOSE_FUNCS
        + SHORTHAND_FUNCS
        + NORMALIZER_FUNCS
    )
    extracted = extract_functions(lines, all_names)

    write(OUT_DIR / "shared" / "map.ex", build_shared_map(extracted))
    write(OUT_DIR / "field_bindings.ex", build_field_bindings(extracted))
    write(OUT_DIR / "shorthand.ex", build_shorthand(extracted))
    write(OUT_DIR / "compose.ex", build_compose(extracted))
    write(OUT_DIR / "inspector.ex", build_inspector(extracted))
    write(OUT_DIR / "projector.ex", build_projector(extracted))
    write(DOMAIN, build_orchestrator(extracted))
    print("Split Selecto.Domain into submodules.")


if __name__ == "__main__":
    main()
