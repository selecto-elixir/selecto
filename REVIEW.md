# Selecto Library Review

Date: 2025-08-07
Branch: non-ecto

## Overview
Selecto is a Postgrex-backed composable query builder assembling SQL from a domain (schema + joins) description and a mutable (yet immutable-struct–style) selection/filter/order/group state. Core struct: `%Selecto{postgrex_opts, domain, config, set}` with builder helpers (`select/2`, `filter/2`, `order_by/2`, `group_by/2`, `execute/2`).

## Strengths
- Clear separation between domain configuration (structural metadata) and query state (user-selected operations).
- Flexible join DSL anticipating advanced patterns (tagging, hierarchical, star, snowflake dimensions).
- Composable selector language (literals, funcs, case, aggregates, subquery stubs) implemented in `Selecto.Builder.Sql.Select`.
- Reasonable identifier sanitization helpers (`check_string/1`, wrapping functions) for core generated pieces.
- Simple, predictable state transformation API enabling pipelines.

## Gaps / Risks
### Feature Completeness vs. Documentation
Advanced join types (hierarchical, tagging, star/snowflake) set metadata but SQL builder currently treats most as simple left joins; recursive CTE logic, closure-table aggregation, and many-to-many through-table specifics are incomplete. Docs may over-promise.

### Safety / Injection Surface
- Sentinel substitution (`^SelectoParam^`) is brittle; accidental collisions possible.
- Custom columns (`:custom_columns` / raw `select` fragments) bypass sanitization—trusted-only assumption must be explicit.
- LIKE / ILIKE patterns passed directly without escaping user `%` / `_` unless caller handles it.
- Minimal type coercion (only integer/id); booleans, dates rely on caller correctness.

### API / Ergonomics
- Only `execute/2` (raising via `query!`); no non-raising `execute` returning `{:ok, ...} | {:error, ...}`.
- No compile step / validation pass for domain configuration to fail fast on structural errors.
- Missing typespecs for public functions (hinders Dialyzer adoption).
- No explicit contract for selector / predicate shapes (harder for users to extend safely).

### Correctness / Edge Cases
- Join dependency resolver has no cycle detection; potential infinite recursion if misconfigured domain.
- Hierarchical helper SQL references `#{field}_name` style columns that may not exist (placeholder risk).
- Parameter list concatenation recalculates combined list twice in builder (minor inefficiency).
- Duplicate alias handling not enforced; collisions possible.
- `configure_domain` merging of fields may have precedence ambiguities when same colid appears in multiple joins.
- Custom filters (`:custom_filters`) defined in joins not integrated into final `filters` map.

### Performance
- Repeated list concatenations (`++`) in reducers (non-critical but can be optimized with cons + reverse for large queries).
- No caching or memoization of compiled SQL for repeat shapes differing only in params.

### Testing
Current tests cover only:
- Configuration
- Basic select
- Simple join
- Basic filter
Missing coverage for:
- group_by / order_by
- Aggregates (count / functions)
- Selector edge cases (case, extract, coalesce, subqueries)
- All advanced join types
- Parameter numbering sequence integrity
- Safety (invalid identifiers rejected)
- Between / list membership / null checks

### Documentation
- `@moduledoc` good initial example but mismatch with implemented advanced joins.
- Lacking: security assumptions, selector reference, extension guidelines, failure modes.

## Prioritized Recommendations
1. Safety & Parameters: Replace sentinel substitution with linear builder (iodata) inserting `$n` as params accumulate; ensures no accidental collisions. Provide escaping for LIKE or explicit literal vs pattern API.
2. Validation Layer: Add `Selecto.validate_domain!/1` (or during `configure/2`) to check joins exist, detect cycles, ensure columns/associations referenced actually exist, verify advanced join required keys.
3. Types & Specs: Introduce `@type t`, `@type selector`, `@type predicate`, `@type join_type`, with `@spec` annotations across public API; add Dialyzer instructions.
4. Execution API: Add non-raising `execute/2` returning tagged tuple; keep `execute!/2` for current behavior.
5. Clarify Advanced Joins: Either implement promised behaviors (CTEs for adjacency, closure-table counting, intermediate many-to-many join) or mark them experimental in docs.
6. Custom Column Safety: Require explicit `trusted_sql: true` flag for raw SQL; otherwise restrict to structured selector tuples.
7. Tests Expansion: Add suites for group/order, aggregates, hierarchical placeholder rejection, parameter numbering, injection attempts, and error conditions.
8. Performance Micro-Optimizations: Refactor reducers to accumulate in reverse; optional until large-scale usage.
9. Alias Management: Allow user-defined aliases or stable deterministic aliases for predictable column mapping.
10. Compilation API: Expose `compile(selecto, opts) :: {sql, params, meta}` for logging and plan caching without execution.

## Potential Enhancements (Future)
- CTE builder DSL (`with/3`) integrated with selector language for hierarchical queries.
- Adapter layer abstraction (start with Postgrex, future: MySQL / SQLite limited subset).
- Macro-based domain declaration (`defselecto do ... end`) with compile-time validation.
- Query shape caching keyed by normalized AST to reduce rebuild overhead.
- Pluggable filter transformers (e.g., automatically expand date shortcuts, fuzzy text search normalization).

## Minor Cleanups
- Remove duplicate params recomputation in `Selecto.Builder.Sql.build/2`.
- Handle duplicate joins gracefully (dedupe earlier) before join order resolution.
- Provide helper for safe LIKE literal (`Selecto.like_literal("foo%bar")`).
- Fold `Selecto.Schema` placeholder or remove.

## Security Posture (Suggested Statement)
Domain configuration and custom column definitions are assumed trusted application code. Only user-provided filter values become query parameters; identifiers and raw SQL fragments should never derive from untrusted input. Future work: enforce structured selectors for untrusted paths.

## Quick Win Implementation Order
(1) Parameter builder refactor → (2) domain validation + cycle detect → (3) typespecs → (4) non-raising execute → (5) test expansion.

## Summary
Selecto presents a solid, extensible foundation with a powerful composable selector model. The largest immediate needs are hardening (parameterization + validation), delivering (or clearly scoping) advanced join features, and broadening test/doc coverage to match the ambition. Addressing these will move it from promising to production-ready.

---
Generated review – adjust focus areas as project goals evolve.
