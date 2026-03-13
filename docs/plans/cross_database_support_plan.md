# Cross-Database Adapter Support Plan

## Context

Selecto now has a real adapter namespace under `Selecto.DB.*` and basic coverage
for PostgreSQL, MySQL, MariaDB, MSSQL, and SQLite. That foundation is useful,
but it should not be mistaken for full backend parity. The current adapters prove
out placeholder handling, identifier quoting, connection bootstrap, normalized
execution results, and limited baseline query execution. They do not yet
guarantee that all builder features, execution paths, or advanced query shapes
behave consistently across engines.

This document updates the original plan to match the current codebase and to
define the next steps needed to turn the adapter layer from "present" into
"dependable".

## Current Status

### What Exists Today

1. Built-in adapter modules exist for:
   - `Selecto.DB.PostgreSQL`
   - `Selecto.DB.MySQL`
   - `Selecto.DB.MariaDB`
   - `Selecto.DB.MSSQL`
   - `Selecto.DB.SQLite`
2. A shared adapter behavior exists at `lib/selecto/db/adapter.ex`.
3. SQL placeholder generation and identifier quoting are adapter-driven.
4. `Selecto.Configuration` accepts an explicit adapter and initializes
   non-PostgreSQL connections through that adapter.
5. Baseline tests now cover:
   - adapter contract shape
   - SQL param placeholder behavior
   - cross-db smoke execution and simple query-shape checks
   - explicit stream capability errors for adapters that do not implement
     `stream/4`

### What Is Still Incomplete

1. "Adapter support" currently means baseline support, not full feature parity.
2. Many SQL builder branches still reflect PostgreSQL-first assumptions even when
   adapter hooks exist.
3. Capability gating is incomplete for advanced PostgreSQL-only features.
4. Non-PostgreSQL streaming is contract-ready but mostly unimplemented.
5. Integration coverage is still narrower than the effective surface area of the
   query builder.

## Planning Principle

Treat adapter support as a capability matrix, not a boolean. An adapter should
only be considered supported for the feature set that is both implemented and
tested.

That means:

- "module exists" is not enough
- "can run `SELECT 1`" is not enough
- docs must distinguish baseline support from advanced support
- unsupported features must fail explicitly, not degrade into malformed SQL

## Goals

1. Keep PostgreSQL stable as the most complete backend.
2. Make baseline query generation and execution dependable across the current
   built-in adapters.
3. Introduce explicit capability gating for backend-specific limitations.
4. Expand tests so claims in the README are backed by real coverage.
5. Provide a clean path for additional adapters, including external or
   experimental ones such as DuckDB.

## Non-Goals

1. Full semantic parity across all backends.
2. Automatic emulation of every PostgreSQL-specific feature.
3. Shipping every possible database as a built-in adapter.
4. Claiming production readiness for an adapter without corresponding coverage.

## Definitions

### Baseline Support

An adapter has baseline support when all of the following are true:

1. It implements the `Selecto.DB.Adapter` contract.
2. It can connect and execute parameterized SQL through Selecto.
3. It can render and execute common query shapes:
   - select
   - filter
   - order by
   - group by
   - pagination
   - simple joins
4. Unsupported features return structured errors.
5. Baseline tests pass in CI or in an opt-in integration job.

### Advanced Support

An adapter has advanced support only after specific higher-level features are
verified individually, for example:

- recursive CTEs
- complex subfilters
- window functions
- compound queries
- streaming
- extension-driven features

## Adapter Model

The adapter contract is already established in `lib/selecto/db/adapter.ex` and
should remain the core abstraction:

- `name/0`
- `connect/1`
- `execute/4`
- `stream/4` (optional)
- `placeholder/1`
- `quote_identifier/1`
- `supports?/1`

Result normalization should continue to converge on `%{rows: rows, columns:
columns}` regardless of backend driver.

## Capability Model

We should formalize feature checks around a stable set of capability names,
rather than letting support be inferred from scattered conditionals.

Recommended capability buckets:

1. Core SQL shape capabilities
   - `:basic_select`
   - `:joins`
   - `:group_by`
   - `:pagination`
2. Advanced query capabilities
   - `:cte`
   - `:recursive_cte`
   - `:window_functions`
   - `:compound_queries`
   - `:stream`
3. PostgreSQL-specific capabilities
   - `:jsonb_ops`
   - `:array_ops`
   - `:pg_text_search`
   - `:pg_rollup_fix`
4. Extension capabilities
   - `:postgis`
   - `:timescaledb`

Builder and executor code should query capabilities through adapter and
extension boundaries instead of relying on backend name checks whenever
practical.

## Extension Strategy

Extensions should remain separate from engine adapters.

Principles:

1. Adapters represent database engines.
2. Extensions represent optional engine features.
3. Core Selecto should stay usable without extension-specific dependencies.

Examples:

- PostGIS belongs in `selecto_postgis`
- TimescaleDB-specific behavior belongs in a companion package
- future DuckDB-specific extras should not be conflated with SQLite support

## Built-In vs External Adapters

Not every adapter needs to live in core.

### Keep Built In

These are reasonable built-in targets because they already exist and match the
current test/dependency model:

- PostgreSQL
- MySQL
- MariaDB
- MSSQL
- SQLite

### Candidate External Adapters

Backends such as DuckDB should be evaluated as external adapters first unless
they reach a high-confidence support level and justify ongoing maintenance in
core.

Reasons:

1. Driver maturity and Elixir ecosystem support vary.
2. SQL semantics may overlap partially with SQLite but are not identical.
3. Maintenance cost should scale with actual test coverage and user demand.

For DuckDB specifically, the recommended path is:

1. define a standalone `Selecto.DB.DuckDB` adapter package or internal
   experimental adapter
2. implement the adapter contract
3. validate baseline query-shape coverage
4. document unsupported features explicitly
5. only consider core inclusion after stable integration coverage exists

## Workstreams

## Phase 1 - Honest Support Classification

1. Update docs to distinguish:
   - foundation in place
   - baseline supported
   - advanced supported
2. Remove any wording that implies full parity where only smoke coverage exists.
3. Publish a capability matrix per adapter.

Deliverable: docs reflect actual support level instead of aspirational support.

## Phase 2 - Capability Audit

1. Inventory PostgreSQL-specific assumptions in:
   - `lib/selecto/builder/sql/*.ex`
   - `lib/selecto/executor.ex`
   - `lib/selecto/configuration.ex`
   - `lib/selecto/connection_pool.ex`
   - `lib/selecto/sql/params.ex`
2. Tag each assumption as one of:
   - portable
   - adapter-specific
   - extension-specific
   - unsupported outside PostgreSQL
3. Convert ad hoc backend checks into capability checks where practical.

Deliverable: a concrete map of remaining PostgreSQL-first behavior.

## Phase 3 - Baseline Query Parity

1. Expand cross-db query-shape tests beyond smoke assertions.
2. Cover at minimum:
   - select
   - filter combinations
   - `IN` and `NOT IN`
   - order by
   - group by
   - joins
   - pagination
3. Validate generated SQL with adapter-aware expectations.
4. Execute representative queries against real services where available.

Deliverable: baseline support means more than placeholder correctness.

## Phase 4 - Fail-Fast Capability Gating

1. Add explicit unsupported-feature errors for advanced PostgreSQL-only paths.
2. Ensure errors include adapter and feature metadata.
3. Prefer structured validation failures before query execution whenever
   possible.

Deliverable: non-portable features fail safely and predictably.

## Phase 5 - Connection and Streaming Hardening

1. Verify pooling behavior per adapter and document which paths are supported.
2. Keep PostgreSQL direct-connection streaming as the reference implementation.
3. Either implement real streaming per adapter or clearly mark it unsupported.
4. Avoid pseudo-streaming that materializes full result sets while claiming
   cursor semantics.

Deliverable: connection behavior and stream claims are explicit and testable.

## Phase 6 - External Adapter Onboarding Path

1. Document how to add a new adapter outside core.
2. Provide a checklist for experimental adapters such as DuckDB:
   - driver dependency strategy
   - placeholder strategy
   - identifier quoting
   - normalized result shape
   - feature capability declaration
   - baseline integration test set
3. Add guidance for when an external adapter is mature enough for core
   consideration.

Deliverable: new adapter work can progress without overcommitting core support.

## Testing Strategy

### Unit Coverage

1. Adapter contract tests for each built-in adapter.
2. Placeholder and identifier quoting tests.
3. Builder tests for adapter-specific SQL rendering.
4. Error-shape tests for unsupported capabilities.

### Integration Coverage

1. Continue using adapter-tagged integration tests.
2. Expand baseline query-shape assertions per backend.
3. Keep optional service-backed runs for databases that require external setup.
4. Validate SQL keywords with case-insensitive assertions.

### Release Gate

Before claiming adapter improvements in release notes:

1. `mix compile`
2. focused adapter unit tests
3. focused cross-db baseline tests
4. updated docs and support matrix

## Documentation Requirements

The README and changelog should always distinguish among:

1. adapter foundation exists
2. baseline execute support exists
3. advanced features supported
4. unsupported features and known gaps

This distinction matters because users will reasonably interpret "supports X"
to mean more than "there is a module for X".

## Acceptance Criteria

This plan is considered complete when:

1. Adapter support language is accurate across docs.
2. Built-in adapters have explicit capability classification.
3. Baseline cross-db tests cover representative query shapes, not only smoke
   execution.
4. PostgreSQL-only features fail with structured errors on unsupported adapters.
5. External adapter guidance exists for future backends such as DuckDB.
6. No adapter is presented as feature-complete without matching tests.

## Recommended Near-Term Order

1. Fix the documentation and support matrix first.
2. Complete the capability audit next.
3. Expand baseline cross-db tests after the audit.
4. Add fail-fast gating for advanced PostgreSQL-only features.
5. Document the external adapter path for DuckDB and similar backends.

That sequence keeps public claims honest while steadily improving the real
adapter surface.
