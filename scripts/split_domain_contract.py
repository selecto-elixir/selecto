#!/usr/bin/env python3
"""Mechanically split Selecto.Domain.Contract into section modules."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "lib/selecto/domain/contract"
ORCHESTRATOR = ROOT / "lib/selecto/domain/contract.ex"


def contract_source_lines() -> list[str]:
    import subprocess

    source = ROOT / "lib/selecto/domain/contract.ex"
    if source.exists() and source.stat().st_size > 20_000:
        return source.read_text().splitlines(keepends=True)

    original = subprocess.check_output(
        ["git", "show", "HEAD:lib/selecto/domain/contract.ex"],
        cwd=ROOT,
        text=True,
    )
    return original.splitlines(keepends=True)

CORE_HELPERS = [
    "error",
    "value_type",
    "map_value",
    "has_key?",
    "fetch_key",
    "fetch_map_value",
    "field_id",
    "known_field?",
    "field_in_list?",
    "field_ref?",
    "valid_static_source_path?",
    "valid_choice_source_path?",
    "enum_value?",
    "non_empty_atom_or_string?",
    "non_empty_string?",
    "valid_arity?",
    "safe_existing_atom",
    "relation_field_ref",
    "relation_fields",
    "field_index",
]

STATIC_FILTER_HELPERS = [
    "validate_static_filter_expression",
    "validate_static_filter_parts",
    "validate_static_logical_filter",
    "validate_static_field_filter",
    "invalid_static_filter_operator",
    "invalid_static_filter_operands",
    "invalid_static_filter_expression",
    "static_known_filter_op?",
    "static_logical_filter_op?",
    "static_unary_filter_op?",
    "static_field_filter_op?",
    "static_filter_operator_value?",
    "static_filter_owner",
    "static_filter_subject",
    "static_filter_attrs",
    "static_filter_error_code",
]


def read_lines() -> list[str]:
    return contract_source_lines()


def slice_lines(lines: list[str], start: int, end: int) -> str:
    return "".join(lines[start - 1 : end])


def prefix_helpers(body: str, prefix: str, names: list[str]) -> str:
    for name in sorted(names, key=len, reverse=True):
        body = re.sub(rf"(?<![\w\.]){re.escape(name)}\(", f"{prefix}.{name}(", body)
        body = re.sub(
            rf"&{re.escape(name)}/",
            f"&{prefix}.{name}/",
            body,
        )
    return body


def to_public_defs(body: str) -> str:
    return body.replace("  defp ", "  def ")


def write_module(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def build_module(
    name: str,
    uses: list[str],
    attrs: list[str],
    public_api: str,
    body: str,
    core: bool = False,
    static: bool = False,
    id_value: bool = False,
    field_ref: bool = False,
) -> str:
    parts = [f"defmodule {name} do", "  @moduledoc false", ""]
    for use in uses:
        parts.append(f"  {use}")
    if uses:
        parts.append("")
    for attr in attrs:
        parts.append(f"  {attr}")
    if attrs:
        parts.append("")

    body = to_public_defs(body)

    if core:
        processed = body
    elif static:
        processed = prefix_helpers(body, "Core", CORE_HELPERS)
    elif id_value or field_ref:
        processed = prefix_helpers(body, "Core", CORE_HELPERS)
    else:
        processed = prefix_helpers(body, "Core", CORE_HELPERS)
        processed = prefix_helpers(processed, "StaticFilters", STATIC_FILTER_HELPERS)
        processed = prefix_helpers(processed, "IdValue", ["validate_id_value"])
        processed = prefix_helpers(processed, "FieldReference", ["validate_field_reference"])

    return "\n".join(parts) + public_api + processed + "end\n"


def main() -> None:
    lines = read_lines()

    write_module(
        OUT_DIR / "shared/core.ex",
        build_module(
            "Selecto.Domain.Contract.Shared.Core",
            ["use Selecto.Domain.Constants"],
            [],
            "",
            slice_lines(lines, 5514, 5704),
            core=True,
        ),
    )

    write_module(
        OUT_DIR / "shared/static_filters.ex",
        build_module(
            "Selecto.Domain.Contract.Shared.StaticFilters",
            [
                "use Selecto.Domain.Constants",
                "alias Selecto.Domain.Contract.Shared.Core",
            ],
            ["@logical_filter_ops [:and, :or]", "@unary_filter_ops [:not]"],
            "",
            slice_lines(lines, 4585, 4817),
            static=True,
        ),
    )

    write_module(
        OUT_DIR / "shared/id_value.ex",
        build_module(
            "Selecto.Domain.Contract.Shared.IdValue",
            ["alias Selecto.Domain.Contract.Shared.Core"],
            [],
            "",
            slice_lines(lines, 5179, 5195),
            id_value=True,
        ),
    )

    write_module(
        OUT_DIR / "shared/field_reference.ex",
        build_module(
            "Selecto.Domain.Contract.Shared.FieldReference",
            ["alias Selecto.Domain.Contract.Shared.Core"],
            [],
            "",
            slice_lines(lines, 3060, 3074),
            field_ref=True,
        ),
    )

    write_module(
        OUT_DIR / "relations.ex",
        build_module(
            "Selecto.Domain.Contract.Relations",
            ["use Selecto.Domain.Constants", "alias Selecto.Domain.Contract.Shared.Core"],
            ["@relation_required_keys [:source_table, :primary_key, :fields, :columns]"],
            """
  def validate(errors, source, schemas) do
    errors
    |> validate_relation(:source, source, [:source])
    |> validate_schemas(schemas)
  end

""",
            slice_lines(lines, 129, 325),
        ),
    )

    write_module(
        OUT_DIR / "joins.ex",
        build_module(
            "Selecto.Domain.Contract.Joins",
            ["alias Selecto.Domain.Contract.Shared.Core"],
            [],
            """
  def validate(errors, joins, source, schemas) do
    validate_joins(errors, joins, source, schemas)
  end

""",
            slice_lines(lines, 326, 498),
        ),
    )

    write_module(
        OUT_DIR / "query.ex",
        build_module(
            "Selecto.Domain.Contract.Query",
            [
                "use Selecto.Domain.Constants",
                "alias Selecto.Domain.Contract.Shared.Core",
                "alias Selecto.Domain.Contract.Shared.FieldReference, as: FieldReference",
            ],
            [
                "@logical_filter_ops [:and, :or]",
                "@unary_filter_ops [:not]",
                "@query_order_directions [:asc, :desc, :asc_nulls_first, :asc_nulls_last, :desc_nulls_first, :desc_nulls_last]",
                "@query_group_wrappers [:rollup, :grouping_set]",
            ],
            """
  def validate(errors, query, field_index) do
    errors
    |> validate_query_field_lists(query, field_index)
    |> validate_filters(query, field_index)
    |> validate_functions(query)
  end

""",
            slice_lines(lines, 499, 1857),
        ),
    )

    write_module(
        OUT_DIR / "query_members.ex",
        build_module(
            "Selecto.Domain.Contract.QueryMembers",
            ["use Selecto.Domain.Constants", "alias Selecto.Domain.Contract.Shared.Core"],
            [],
            """
  def validate(errors, query) do
    validate_query_members(errors, query)
  end

""",
            slice_lines(lines, 1858, 2474),
        ),
    )

    write_module(
        OUT_DIR / "published_views.ex",
        build_module(
            "Selecto.Domain.Contract.PublishedViews",
            ["alias Selecto.Domain.Contract.Shared.Core"],
            [],
            """
  def validate(errors, query) do
    validate_published_views(errors, query)
  end

""",
            slice_lines(lines, 2475, 2796),
        ),
    )

    write_module(
        OUT_DIR / "detail_actions.ex",
        build_module(
            "Selecto.Domain.Contract.DetailActions",
            ["use Selecto.Domain.Constants", "alias Selecto.Domain.Contract.Shared.Core"],
            [],
            """
  def validate(errors, detail_actions, field_index) do
    validate_detail_actions(errors, detail_actions, field_index)
  end

""",
            slice_lines(lines, 2797, 3059),
        ),
    )

    write_module(
        OUT_DIR / "writes.ex",
        build_module(
            "Selecto.Domain.Contract.Writes",
            ["alias Selecto.Domain.Contract.Shared.Core"],
            [],
            """
  def validate(errors, writes, field_index) do
    validate_writes(errors, writes, field_index)
  end

""",
            slice_lines(lines, 3076, 3212),
        ),
    )

    write_module(
        OUT_DIR / "capabilities.ex",
        build_module(
            "Selecto.Domain.Contract.Capabilities",
            [
                "use Selecto.Domain.Constants",
                "alias Selecto.Domain.Contract.Shared.Core",
            ],
            [],
            """
  def validate(errors, capabilities) do
    validate_capabilities(errors, capabilities)
  end

  def validate_query_references(errors, query, detail_actions, capabilities) do
    validate_query_capability_references(errors, query, detail_actions, capabilities)
  end

""",
            slice_lines(lines, 3213, 3531),
        ),
    )

    write_module(
        OUT_DIR / "actions.ex",
        build_module(
            "Selecto.Domain.Contract.Actions",
            ["alias Selecto.Domain.Contract.Shared.Core"],
            [],
            """
  def validate(errors, actions, capabilities, writes, field_index) do
    validate_actions(errors, actions, capabilities, writes, field_index)
  end

""",
            slice_lines(lines, 3532, 3949),
        ),
    )

    write_module(
        OUT_DIR / "source_relationships.ex",
        build_module(
            "Selecto.Domain.Contract.SourceRelationships",
            [
                "alias Selecto.Domain.Contract.Shared.Core",
                "alias Selecto.Domain.Contract.Shared.IdValue, as: IdValue",
                "alias Selecto.Domain.Contract.Shared.StaticFilters, as: StaticFilters",
            ],
            [],
            """
  def validate(errors, source_relationships, field_index) do
    validate_source_relationships(errors, source_relationships, field_index)
  end

""",
            slice_lines(lines, 3950, 4361),
        ),
    )

    choice_body = slice_lines(lines, 4362, 4584) + slice_lines(lines, 4819, 5177)
    write_module(
        OUT_DIR / "choice_sources.ex",
        build_module(
            "Selecto.Domain.Contract.ChoiceSources",
            [
                "use Selecto.Domain.Constants",
                "alias Selecto.Domain.Contract.Shared.Core",
                "alias Selecto.Domain.Contract.Shared.IdValue, as: IdValue",
                "alias Selecto.Domain.Contract.Shared.StaticFilters, as: StaticFilters",
            ],
            [
                "@choice_source_path_keys [:source_path, :value_source, :caption_source, :description_source]",
                "@choice_source_presentation_controls [:select, :autocomplete, :table_picker]",
                "@choice_source_presentation_modes [:static, :searchable, :async, :inline]",
                "@choice_source_presentation_cardinalities [:one, :many]",
                "@choice_source_constraint_policy_keys [:source_relationship, :choice_source, :domain_of_interest]",
                "@choice_source_constraint_policy_modes [:best_effort, :fail_closed]",
                "@order_directions [:asc, :desc]",
            ],
            """
  def validate(errors, choice_sources, source_relationships, capabilities) do
    validate_choice_sources(errors, choice_sources, source_relationships, capabilities)
  end

""",
            choice_body,
        ),
    )

    write_module(
        OUT_DIR / "field_bindings.ex",
        build_module(
            "Selecto.Domain.Contract.FieldBindings",
            [
                "alias Selecto.Domain.Contract.Shared.Core",
                "alias Selecto.Domain.Contract.Shared.IdValue, as: IdValue",
            ],
            [],
            """
  def validate(errors, source, schemas, projection, choice_sources, field_index) do
    validate_field_choice_source_bindings(
      errors,
      source,
      schemas,
      projection,
      choice_sources,
      field_index
    )
  end

""",
            slice_lines(lines, 5197, 5513),
        ),
    )

    orchestrator = '''defmodule Selecto.Domain.Contract do
  @moduledoc """
  First-wave canonical domain contract checks.

  This module validates the normalized shape produced by `Selecto.Domain`.
  It is intentionally small: it covers the required core sections and the first
  strict subschemas for `source`, `schemas`, `joins`, and filter references.
  Existing runtime configuration does not call this module unless a caller opts
  into normalized validation.
  """

  use Selecto.Domain.Constants

  alias Selecto.Domain.Contract.Actions
  alias Selecto.Domain.Contract.Capabilities
  alias Selecto.Domain.Contract.ChoiceSources
  alias Selecto.Domain.Contract.DetailActions
  alias Selecto.Domain.Contract.FieldBindings
  alias Selecto.Domain.Contract.Joins
  alias Selecto.Domain.Contract.PublishedViews
  alias Selecto.Domain.Contract.Query
  alias Selecto.Domain.Contract.QueryMembers, as: QueryMembersValidator
  alias Selecto.Domain.Contract.Relations
  alias Selecto.Domain.Contract.Shared.Core
  alias Selecto.Domain.Contract.SourceRelationships
  alias Selecto.Domain.Contract.Writes

  @required_sections [:source, :schemas]

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
    field_index = Core.field_index(source, schemas, projection)

    []
    |> validate_required_sections(authored_domain)
    |> Relations.validate(source, schemas)
    |> Joins.validate(joins, source, schemas)
    |> Query.validate(query, field_index)
    |> QueryMembersValidator.validate(query)
    |> PublishedViews.validate(query)
    |> DetailActions.validate(detail_actions, field_index)
    |> Writes.validate(writes, field_index)
    |> Capabilities.validate(capabilities)
    |> Capabilities.validate_query_references(query, detail_actions, capabilities)
    |> Actions.validate(actions, capabilities, writes, field_index)
    |> SourceRelationships.validate(source_relationships, field_index)
    |> ChoiceSources.validate(choice_sources, source_relationships, capabilities)
    |> FieldBindings.validate(source, schemas, projection, choice_sources, field_index)
    |> Enum.reverse()
  end

  def errors(_normalized_domain) do
    [
      Core.error(
        :invalid_normalized_domain,
        [],
        "expected a normalized Selecto domain from Selecto.Domain.normalize/1"
      )
    ]
  end

  defp validate_required_sections(errors, authored_domain) do
    Enum.reduce(@required_sections, errors, fn section, acc ->
      if Core.has_key?(authored_domain, section) do
        acc
      else
        [
          Core.error(
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
end
'''

    ORCHESTRATOR.write_text(orchestrator)
    print("Split complete.")


if __name__ == "__main__":
    main()
