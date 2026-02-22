# Cross-Database Support Plan (MySQL, MSSQL, SQLite)

## Context

Selecto currently contains partial adapter branching in SQL builders and execution paths, but the adapter layer is incomplete in the current tree. There are references to modules like `Selecto.DB.PostgreSQL`, `Selecto.DB.MySQL`, and `Selecto.Adapters.PostgreSQL` without a consistent implemented adapter contract.

This plan restores a clean adapter foundation and delivers practical cross-database support in phases, starting with core query functionality.

## Goals

1. Make Selecto generate and execute core queries on PostgreSQL, MySQL, MSSQL, and SQLite.
2. Keep PostgreSQL compatibility stable for existing users.
3. Provide explicit capability checks for PostgreSQL-only features.
4. Add clear tests and documentation for supported behavior by adapter.

## Non-Goals (Initial Delivery)

1. Full feature parity for PostgreSQL-only features (JSONB operators, array operators, PG-specific OLAP hints/functions).
2. Automatic SQL rewrites that emulate all PostgreSQL-specific semantics in other databases.
3. Performance tuning for every adapter beyond correctness and safe defaults.

## Scope Definition

Initial cross-db support means:

- `select`, `filter`, `order_by`, `group_by`, joins, and pagination generate valid SQL per adapter.
- Parameters are bound using adapter-specific placeholders.
- Execution works through adapter modules using a unified result shape.
- Unsupported features fail fast with structured `Selecto.Error` messages.

## Design Direction

Use a first-class adapter behavior under `Selecto.DB.*` and make SQL generation/execution adapter-driven instead of scattered `case` statements.

### Adapter Contract (proposed)

Each adapter module should implement:

- `name/0`
- `connect/1`
- `execute/4`
- `placeholder/1` (or equivalent placeholder strategy)
- `quote_identifier/1`
- `supports?/1` for feature checks
- `normalize_result/1` (or equivalent helper) to return `%{rows: rows, columns: cols}`

## Extension Architecture (PostGIS and Similar Features)

### Principles

1. Keep adapters focused on database engines (PostgreSQL, MySQL, MSSQL, SQLite).
2. Model database extensions (PostGIS, TimescaleDB-specific features, etc.) as optional extension packages.
3. Keep core Selecto lightweight and avoid mandatory dependencies for extension-specific capabilities.

### Core Extension API (proposed)

Add an extension behavior in core (for example `Selecto.Extension`) with callbacks such as:

- `name/0`
- `supports_adapter?/1`
- `capabilities/0`
- `validate_config/1`
- `compile_selector/3`
- `compile_filter/3`
- `normalize_param/2`

### Configuration Shape (proposed)

Allow extensions on `Selecto.configure/3`:

```elixir
selecto =
  Selecto.configure(domain, conn,
    adapter: Selecto.DB.PostgreSQL,
    extensions: [SelectoPostGIS.Extension]
  )
```

### Package Strategy

1. Create `selecto_postgis` as a separate package.
2. Keep PostGIS dependencies optional and isolated to that package.
3. Expose helper constructors for geospatial selectors/filters while compiling through Selecto extension hooks.

### Runtime Safety

1. If adapter is not supported by an extension, return structured `unsupported_feature` errors.
2. Optionally verify extension installation during initialization (for PostgreSQL: check `pg_extension` for `postgis`).
3. Fail fast with adapter/feature metadata, never with malformed SQL.

### Test Strategy for Extensions

1. Core: unit tests for extension registration, dispatch, and error handling.
2. Package: SQL compilation tests for geospatial operators and parameter encoding.
3. Optional integration tests against a PostGIS-enabled PostgreSQL service.

### Initial PostGIS Capability Set (recommended)

1. Predicates: `ST_Intersects`, `ST_Contains`, `ST_Within`, `ST_DWithin`.
2. Measurements: `ST_Distance`, `ST_Area`, `ST_Length`.
3. Output helpers: `ST_AsText`, `ST_AsGeoJSON`.
4. Input helpers: `ST_GeomFromText`, `ST_GeomFromGeoJSON`, SRID helpers.

## Implementation Phases

## Phase 0 - Baseline and Inventory

1. Capture current behavior with tests for SQL generation and executor adapter path.
2. Inventory all adapter checks and PostgreSQL assumptions in:
   - `lib/selecto/builder/sql/*.ex`
   - `lib/selecto/sql/params.ex`
   - `lib/selecto/executor.ex`
   - `lib/selecto/configuration.ex`
   - `lib/selecto/connection_pool.ex`
3. Normalize adapter naming to one namespace (`Selecto.DB.*`).

Deliverable: no behavior change yet, only test coverage and adapter naming cleanup map.

## Phase 1 - Adapter Foundation

1. Add adapter behavior module (for example `lib/selecto/db/adapter.ex`).
2. Add built-in adapters:
   - `lib/selecto/db/postgresql.ex`
   - `lib/selecto/db/mysql.ex`
   - `lib/selecto/db/mssql.ex`
   - `lib/selecto/db/sqlite.ex`
3. Update defaults in configuration to use implemented module references.
4. Remove references to non-existent adapter namespaces (`Selecto.Adapters.*`).

Deliverable: Selecto boots with explicit adapter modules and no missing-module references.

## Phase 2 - SQL Portability Core

1. Centralize identifier quoting via adapter callbacks.
2. Centralize placeholder generation in `Selecto.SQL.Params`.
3. Fix `IN`/`NOT IN` parameter handling so non-PostgreSQL adapters produce valid placeholder lists.
4. Keep SQL keyword tests case-insensitive.

Deliverable: core builders generate adapter-valid SQL strings for baseline query shapes.

## Phase 3 - Execution and Connection Path

1. Make `Selecto.Executor` adapter-first for non-PostgreSQL execution.
2. Keep PostgreSQL backward compatibility (`Postgrex` direct and Ecto repo path).
3. Align/repair connection pooling behavior so module references are valid and behavior is explicit per adapter.
4. Keep consistent error wrapping into `Selecto.Error`.

Deliverable: execute path returns normalized results across adapters and stable errors.

## Phase 4 - Feature Capability Gating

1. Add feature constants (example: `:jsonb_ops`, `:array_ops`, `:pg_text_search`, `:pg_olap_hints`).
2. Guard builder branches for adapter-incompatible features.
3. Return clear `unsupported_feature` errors with adapter + feature metadata.

Deliverable: unsupported paths fail safely and predictably.

## Phase 5 - Test Matrix

1. Unit tests for each adapter:
   - placeholder style
   - identifier quoting
   - `IN`/`NOT IN` rendering
2. SQL generation snapshots for common query patterns per adapter.
3. Executor tests using fake adapter modules for success/error/timeout behavior.
4. Optional integration tests (enabled only when adapter deps and services are present).

Deliverable: deterministic core test coverage plus opt-in integration coverage.

## Phase 6 - Docs and Release

1. Update README with adapter support matrix.
2. Add migration notes for users relying on PostgreSQL-only features.
3. Add changelog entry describing scope and known limitations.

Deliverable: release-ready docs and clear upgrade guidance.

## Proposed Support Matrix (Initial)

- PostgreSQL: full current functionality (including PG-specific features).
- MySQL: core query generation and execution; PG-only features gated.
- MSSQL: core query generation and execution; PG-only features gated.
- SQLite: core query generation and execution; PG-only features gated.

## Acceptance Criteria

1. `mix test` passes with no new warnings.
2. Core query builders pass adapter SQL tests for PostgreSQL/MySQL/MSSQL/SQLite.
3. Unsupported features return structured errors, not malformed SQL.
4. No references remain to missing adapter modules.
5. Existing PostgreSQL behavior remains backward compatible.

## Risks and Mitigations

1. Divergent SQL syntax edge cases.
   - Mitigation: adapter-owned rendering helpers and targeted fixture tests.
2. Optional dependency sprawl.
   - Mitigation: keep non-Postgres integrations optional and test-gated.
3. Regression risk in PostgreSQL behavior.
   - Mitigation: lock baseline tests first and run full suite each phase.

## Execution Order Recommendation

1. Land Phases 0-2 first (safe, mostly SQL generation).
2. Land Phase 3 separately (execution and pool behavior changes).
3. Land Phase 4 + docs in final hardening PR.

This staged order keeps risk low while making visible progress early.
