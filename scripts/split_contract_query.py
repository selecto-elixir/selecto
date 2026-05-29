#!/usr/bin/env python3
"""Mechanically split Selecto.Domain.Contract.Query into submodules."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
QUERY = ROOT / "lib/selecto/domain/contract/query.ex"
OUT_DIR = ROOT / "lib/selecto/domain/contract/query"

FIELD_LISTS_FUNCS = [
    "validate_query_field_lists",
    "validate_query_selection_list",
    "validate_query_order_list",
    "validate_query_group_list",
    "invalid_query_list",
    "validate_query_selection_entry",
    "validate_query_order_entry",
    "validate_query_group_entry",
    "validate_query_selector_reference",
    "validate_query_function_reference",
    "query_function_not_found_error",
    "validate_query_function_call_site",
    "validate_query_function_args",
    "query_function_spec_args",
    "validate_query_function_arg_count",
    "validate_query_function_selector_args",
    "validate_query_function_arg",
    "validate_query_function_selector_arg",
    "validate_query_field_reference",
    "invalid_query_field_reference",
    "query_order_direction?",
]

FILTERS_FUNCS = [
    "validate_filters",
    "validate_filter_registry",
    "validate_filter_id",
    "validate_filter_config",
    "validate_filter_config_field",
    "validate_filter_config_type",
    "validate_required_filters",
    "validate_filter_expression",
]

FUNCTIONS_FUNCS = [
    "validate_functions",
    "validate_function_id",
    "validate_function_spec",
    "validate_function_kind",
    "validate_function_sql_name",
    "validate_function_allowed_in",
    "validate_function_call_site",
    "validate_function_args",
    "validate_function_arg_spec",
    "validate_function_arg_name",
    "validate_function_arg_type",
    "validate_function_arg_source",
    "validate_function_returns",
    "validate_predicate_function_returns",
    "validate_table_function_returns",
    "validate_scalar_function_returns",
]


def read_lines() -> list[str]:
    return QUERY.read_text().splitlines(keepends=True)


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


def build_module(module_name: str, uses: str, attrs: list[str], body: str) -> str:
    parts = [f"defmodule {module_name} do", "  @moduledoc false", "", uses, ""]
    parts.extend(attrs)
    if attrs:
        parts.append("")
    parts.append(body.rstrip())
    parts.append("end\n")
    return "\n".join(parts)


def main() -> None:
    lines = read_lines()
    all_names = FIELD_LISTS_FUNCS + FILTERS_FUNCS + FUNCTIONS_FUNCS
    extracted = extract_functions(lines, all_names)

    field_body = ""
    for name in FIELD_LISTS_FUNCS:
        field_body += extracted[name]

    filters_body = ""
    for name in FILTERS_FUNCS:
        filters_body += extracted[name]

    functions_body = ""
    for name in FUNCTIONS_FUNCS:
        functions_body += extracted[name]

    uses_core = """  use Selecto.Domain.Constants

  alias Selecto.Domain.Contract.Shared.Core"""

    uses_filters = """  use Selecto.Domain.Constants

  alias Selecto.Domain.Contract.Shared.Core
  alias Selecto.Domain.Contract.Shared.FieldReference, as: FieldReference"""

    field_lists = build_module(
        "Selecto.Domain.Contract.Query.FieldLists",
        uses_core,
        [
            "  @query_order_directions [:asc, :desc, :asc_nulls_first, :asc_nulls_last, :desc_nulls_first, :desc_nulls_last]",
            "  @query_group_wrappers [:rollup, :grouping_set]",
        ],
        field_body,
    )

    filters = build_module(
        "Selecto.Domain.Contract.Query.Filters",
        uses_filters,
        [
            "  @logical_filter_ops [:and, :or]",
            "  @unary_filter_ops [:not]",
        ],
        filters_body,
    )

    functions = build_module(
        "Selecto.Domain.Contract.Query.Functions",
        uses_core,
        [],
        functions_body,
    )

    orchestrator = """defmodule Selecto.Domain.Contract.Query do
  @moduledoc false

  alias Selecto.Domain.Contract.Query.FieldLists
  alias Selecto.Domain.Contract.Query.Filters
  alias Selecto.Domain.Contract.Query.Functions

  def validate(errors, query, field_index) do
    errors
    |> FieldLists.validate_query_field_lists(query, field_index)
    |> Filters.validate_filters(query, field_index)
    |> Functions.validate_functions(query)
  end
end
"""

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "field_lists.ex").write_text(field_lists)
    (OUT_DIR / "filters.ex").write_text(filters)
    (OUT_DIR / "functions.ex").write_text(functions)
    QUERY.write_text(orchestrator)
    print("Split Selecto.Domain.Contract.Query into submodules.")


if __name__ == "__main__":
    main()
