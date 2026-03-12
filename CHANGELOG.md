
# Selecto Library Changelog

## V 0.3.15 - Join Label Resolution Hardening
---------------------------------------------------------

#### Changed
- Updated joined-column label naming in `Selecto.Schema.Column` to always prefer
  assigned join/field names (`:name` or `"name"`) before falling back to
  `humanize/1`.
- Hardened assigned-name resolution so `nil` does not bypass fallback handling,
  and added regression coverage for atom-assigned names.
- Bumped package version to `0.3.15`.

## V 0.3.14 - Join Label Name Fallback Fix
---------------------------------------------------------

#### Added
- Added focused regression coverage in `test/selecto_schema_column_test.exs`
  for joined-column display labels honoring join config names across
  atom/string join keys.

#### Changed
- Updated `Selecto.Schema.Column` join label resolution to honor join config
  `:name`/`"name"` before falling back to `humanize(join)` when building
  joined column display names.
- Added atom/string join-key lookup fallback when resolving join metadata for
  display labels.
- Bumped package version to `0.3.14`.

## V 0.3.13 - Overlay Join/Schema DSL and OLAP Key Mapping Fixes
---------------------------------------------------------

#### Added
- Added `defjoin/2` and `defschema/2` to `Selecto.Config.OverlayDSL` so
  overlays can define top-level `joins` and `schemas` entries without
  replacing entire sections.
- Added overlay merge coverage and DSL tests for additive/deep merge behavior
  across `joins` and `schemas`.
- Added OLAP regression coverage ensuring `:star_dimension` and
  `:snowflake_dimension` joins honor explicit `owner_key`/`my_key`, including
  a snowflake normalization-chain case.

#### Changed
- Updated `Selecto.Config.Overlay.merge/2` to deep-merge top-level `joins` and
  `schemas` the same way query-member registries are merged.
- Updated OLAP join SQL generation to prefer explicit
  `dimension_key`/`owner_key` and `my_key` when building ON clauses instead of
  defaulting to `<join>_id = <alias>.id`.
- Bumped package version to `0.3.13`.

## V 0.3.12 - Package Metadata and Documentation Links
---------------------------------------------------------

#### Changed
- Updated package metadata description to better reflect Selecto's core query
  capabilities (joins, CTEs, OLAP, and hierarchical patterns).
- Added package links for SQL pattern references and the hosted demo
  (`https://seeken.github.io/selecto-sql-patterns`,
  `https://testselecto.fly.dev`).
- Bumped package version to `0.3.12`.

## V 0.3.11 - Subquery Parameter Binding Reliability
---------------------------------------------------------

#### Added
- Added focused regression coverage for parameterized subquery predicates in
  `test/subquery_param_binding_regression_test.exs`.
- Expanded `Selecto.Builder.Sql.Where` tests to cover placeholder binding for
  parameterized `IN` and `EXISTS` subquery filters.

#### Changed
- Updated SQL WHERE compilation so parameterized subquery predicates
  (`{:subquery, :in, ...}`, `{:subquery, :any|:all, ...}`, and
  `{:exists, ..., params}`) now convert inline `$n` placeholders into iodata
  param markers before final SQL parameterization.
- Fixed placeholder/param ordering so subquery predicate params are preserved in
  `Selecto.to_sql/1` output and no longer collide with outer-query parameters.
- Bumped package version to `0.3.11`.

## V 0.3.10 - CTE/VALUES Auto-Join and Syntax Cleanup
---------------------------------------------------------

#### Added
- Added CTE auto-join shortcuts and join inference for `with_cte/4`,
  `with_recursive_cte/5`, and `with_ctes/3`.
- Added `with_values/3` join shortcuts so inline `VALUES` clauses can be
  attached as joins with inferred fields.

#### Changed
- Removed legacy bracket field notation fallbacks and standardized joined-field
  path handling around dot notation.
- Removed legacy filter/literal selector fallback paths to keep query
  normalization behavior explicit and predictable.
- Bumped package version to `0.3.10`.

## V 0.3.9 - OTP Runtime and Execution Hardening
---------------------------------------------------------

#### Added
- Added supervised application boot via `Selecto.Application`, including
  managed task and connection-pool runtime children.
- Added typed public option validation via `Selecto.OptionsValidator`
  (`NimbleOptions`) for configure/execute/SQL-format/diagnostic entry points.
- Added query execution telemetry spans (`[:selecto, :query, :execution, ...]`)
  with stop metadata for status, row count, and error type.
- Added focused tests for option validation, telemetry span lifecycle coverage,
  and counter-backed cache stats behavior.

#### Changed
- Hardened query cache lifecycle with idempotent startup, safe fallback behavior
  when cache process is unavailable, ETS fast-path reads, and immediate
  post-write visibility.
- Moved query-cache hot stats paths to `:counters` for lower contention under
  high read/write load.
- Reworked connection pool manager lifecycle around
  `Registry + DynamicSupervisor` with adapter-aware pool naming.
- Replaced process-dictionary runtime state in hooks/pool transient flows with
  ETS-backed process-scoped storage for safer cross-process behavior.
- Bumped package version to `0.3.9`.

## V 0.3.8 - Property Testing and Tagging Hardening
---------------------------------------------------------

#### Added
- Added StreamData-backed property testing scaffolding under
  `test/property/`, including deterministic SQL generation checks,
  builder immutability checks, invalid-field failure-path checks, and an
  opt-in PostgreSQL-backed property suite.
- Added property-testing run documentation to the README, including
  `:requires_db` execution guidance and optional PostgreSQL env overrides.

#### Changed
- Updated tagging SQL builder paths to preserve many-to-many join metadata,
  improving compatibility for domains that rely on relationship metadata
  during tagging resolution.
- Updated tagging behavior/tests for Northwind-oriented scenarios in
  `Selecto.Builder.Sql.Tagging`.
- Bumped package version to `0.3.8`.

## V 0.3.7 - Cross-DB Baseline Expansion and PG18 Rollup Gating
---------------------------------------------------------

#### Added
- Added `test/cross_db_baseline_test.exs` with adapter-tagged baseline
  execution checks for PostgreSQL, MySQL, MariaDB, MSSQL, and SQLite.
- Expanded cross-DB baseline checks with adapter-executed query-shape coverage
  for select/filter/order/group/pagination paths.
- Added dedicated cross-database CI baseline jobs in
  `.github/workflows/ci.yml` for PostgreSQL, MySQL, MariaDB, MSSQL, and
  SQLite.
- Added concrete `Exqlite`-backed connect/execute support in
  `Selecto.DB.SQLite` (with structured fallback when dependency is unavailable).

#### Changed
- Updated CI dialyzer invocation to use current Dialyxir CLI behavior
  (`mix dialyzer` without deprecated `--halt-exit-status`).
- Stopped tracking generated Dialyzer PLT artifacts under `priv/plts/` and
  updated gitignore rules so CI/build environments generate fresh compatible
  PLTs per runtime.
- Updated `mneme` dependency to be available in `:dev` and `:test` so
  `mix format --check-formatted` works in CI when formatter imports dep rules.
- Added test-only adapter driver dependencies (`myxql`, `tds`, `exqlite`) to
  support cross-database baseline integration runs.
- Updated adapter stream capability errors to include structured
  `unsupported_feature: :stream` details when adapters do not support
  streaming.
- Updated ROLLUP ORDER BY compatibility behavior to auto-disable `rollupfix`
  wrapping on PostgreSQL 18+ while preserving wrapper safety for older
  PostgreSQL versions.
- Bumped package version to `0.3.7`.

## V 0.3.6 - Usage Rules and Multi-Tenant Plan Expansion
---------------------------------------------------------

#### Added
- Added `usage-rules.md` with concise guidance for agentic workflows and
  automated rule aggregation tooling.
- Expanded `docs/plans/multi_tenant_usage_patterns.md` with concrete
  implementation phases, API sketches, cross-package coordination points, and
  phase-level acceptance criteria.
- Added first-class tenant helpers in `Selecto.Tenant` and top-level API
  delegates (`with_tenant/2`, `tenant/1`, `apply_tenant_scope/2`,
  `require_tenant_filter/2|3`) for runtime tenant scoping.
- Added runtime tenant coverage tests for context normalization, required
  filters, SQL generation, and execution option prefix merging.
- Added explicit database adapter modules under `Selecto.DB.*`
  (`PostgreSQL`, `MySQL`, `MariaDB`, `MSSQL`, `SQLite`) plus shared
  `Selecto.DB.Adapter` behavior.
- Added prioritized delivery roadmap at
  `docs/plans/prioritized_delivery_roadmap.md`.
- Added `Selecto.execute_stream/2` and `Selecto.Executor.execute_stream/2` for
  incremental row consumption, with PostgreSQL cursor-backed streaming for
  direct connections and adapter `stream/4` integration support.
- Added stream-contract coverage for unsupported contexts (`:pool`,
  `:ecto_repo`, and adapters without `stream/4`) and receive-timeout behavior
  for stalled cursor producers.
- Added/expanded subfilter join-path resilience for via joins, including
  top-level case-insensitive `AND` parsing, reordered predicate selection, and
  deduplication of duplicate join steps.
- Added compound subfilter join-path batch resolution in registry flows to
  reduce repeated resolution overhead and improve consistency.
- Added first-class multi-arg function selector support via `{:func, ...}` DSL,
  including distinct/filter options and alias handling in select builder paths.
- Added explicit adapter stream capability contract expectations in execution
  paths (`supports?(:stream)` + `stream/4`) with validation coverage.

#### Changed
- Clarified tenant-isolation recommendations for shared-table, schema-prefix,
  dedicated database, and hybrid deployment models.
- Updated query/filter pipelines and SQL WHERE compilation to include runtime
  `set.required_filters` alongside domain required filters, with deduping.
- Updated execute paths (`execute/2`, `execute_with_metadata/2`,
  `execute_one/2`) to inject tenant-derived `:prefix` when no explicit prefix
  option is provided.
- Updated SQL param placeholder resolution to use adapter module
  `placeholder/1` when available.
- Replaced legacy `Selecto.Adapters.PostgreSQL` references in connection pool
  setup with `Selecto.DB.PostgreSQL`.
- Completed pooled execution success handling in `Selecto.Executor` for
  connection-pool execution paths and improved generic adapter pool startup
  behavior to fail safely for unsupported adapters.
- Updated default `execute/2` path to route through
  `Selecto.Performance.Hooks.with_hooks/3`, including hook propagation across
  timeout task boundaries and tenant-aware cache key namespacing.
- Added tenant scope policy helpers (`tenant_required?/2`,
  `validate_scope/2`, `ensure_scope!/2`) and wired fail-fast validation into
  execute paths and `query_filters/2` derivation by default.
- Improved Ecto `has_through` association introspection to derive
  `owner_key`/`related_key` from through-path metadata instead of `:id`
  fallbacks.
- Added PostgreSQL stream support for pooled connection references when a
  streamable pool connection handle is available.
- Bumped package version to `0.3.6`.

## V 0.3.5 - Query Tooling, Diagnostics, and Livebook Ergonomics
---------------------------------------------------------

#### Added
- Added explicit pre/post pivot filter APIs:
  - `Selecto.pre_pivot_filter/2`
  - `Selecto.post_pivot_filter/2`
- Added query filter introspection helpers:
  - `Selecto.pre_pivot_filters/1`
  - `Selecto.post_pivot_filters/1`
  - `Selecto.query_filters/2`
- Added SQL output utilities:
  - `Selecto.SQL.Formatter`
  - `Selecto.format_sql/2`
  - `Selecto.highlight_sql/2`
- Added SQL generation options to `Selecto.to_sql/2`:
  - `pretty: true`
  - `highlight: :ansi | :markdown`
- Added query diagnostics helpers via `Selecto.Diagnostics`:
  - `Selecto.explain/2`
  - `Selecto.explain_analyze/2`
- Added notebook execution helper module `Selecto.Livebook` with shared
  explain/run helpers for demos and livebooks.
- Added tests for query enhancement APIs and diagnostics SQL generation.

#### Changed
- Improved selection-shape handling so known JSON computed aliases can be used
  in `select_shape/2` and resolved into explicit field selectors.
- Improved missing-field errors to provide better guidance when a selection
  references a computed alias.
- Updated livebook examples to use shared filter extraction through
  `Selecto.query_filters/1` where read-query filters are reused in write flows.
- Bumped package version to `0.3.5`.

## V 0.3.4 - Structured Selection Shapes
---------------------------------------------------------

#### Added
- Added `Selecto.select_shape/2` to compile nested list/tuple selection shapes
  into regular selectors plus correlated subselects.
- Added `Selecto.execute_shape/2` to execute shaped queries and materialize each
  row back into the original nested list/tuple structure.
- Added `Selecto.SelectionShape` with shape parsing, subselect inference for
  single-join nested containers, and row materialization helpers.
- Added focused coverage for shape compilation, SQL generation, and nested
  materialization behavior.

#### Changed
- Bumped package version to `0.3.4`.

## V 0.3.3 - Extension Framework & PostGIS Package Extraction
---------------------------------------------------------

#### Added
- Added a first-class extension contract via `Selecto.Extension` and shared
  extension dispatch/normalization helpers via `Selecto.Extensions`.
- Added extension loading from domain definitions (`:extensions`) during
  `Selecto.configure/3`, with normalized extension specs persisted on
  `Selecto` and processed configuration state.
- Added extension hook support to `Selecto.Config.OverlayDSL`, including
  extension DSL imports, compile-time setup callbacks, and overlay fragment
  merging.
- Added extension callback surfaces for companion packages
  (`components_views/2`, `updato_domain/2`, `ecto_type_to_selecto_type/2`) to
  support ecosystem integration.
- Added/expanded tests for extension normalization, callback dispatch, and
  overlay integration behavior.

#### Changed
- Extracted PostGIS extension implementation from `selecto` core into the
  dedicated `selecto_postgis` package while keeping the runtime module contract
  as `Selecto.Extensions.PostGIS`.
- Moved PostGIS-specific Ecto custom type mapping out of `Selecto.EctoAdapter`
  core matching and into extension callback dispatch
  (`ecto_type_to_selecto_type/2`).
- Updated extension-related docs and examples to clarify package boundaries and
  extension loading patterns.

## V 0.3.2 - Path Syntax Consistency & Documentation Expansion
---------------------------------------------------------

#### Fixed
- Standardized nested join field-path handling around dot notation for
  consistency in schema join resolution and tests.

#### Added
- Expanded ecosystem references in the README for Livebooks, tutorials, and
  hosted demo usage.
- Added a cross-database support planning document (MySQL, MSSQL, SQLite).
- Expanded cross-database planning docs with extension architecture guidance
  and PostGIS considerations.
- Added multi-tenant usage pattern documentation.

#### Changed
- Released package version `0.3.2`.
- Updated documentation/repository references to the `selecto-elixir`
  organization for published project docs.

## V 0.3.1 - CTE Module Naming Compatibility
---------------------------------------------------------

#### Fixed
- Resolved case-insensitive filesystem conflicts (notably on macOS) caused by
  having both `Selecto.Builder.Cte` and `Selecto.Builder.CTE`.

#### Changed
- Renamed the advanced CTE SQL builder module from `Selecto.Builder.CTE` to
  `Selecto.Builder.CteSql`.
- Updated internal SQL compilation flow to call `Selecto.Builder.CteSql`.

#### Compatibility
- Kept `Selecto.Builder.Cte` as the compatibility/legacy API surface for
  helper-style CTE functions.

## V 0.3.0 - Security & Validation Overhaul
---------------------------------------------------------

#### Removed (API Surface Cleanup)
- Removed unused modules from the published API surface:
  - `Selecto.Connection`
  - `Selecto.OptionProvider`
  - `Selecto.QueryTimeoutMonitor`
  - `Selecto.PhoenixHelpers`
  - `Selecto.Performance.Optimizer`
- Removed obsolete examples/tests tied to those modules.
- Notes:
  - `Selecto.AutoPivot`, `Selecto.Config.Overlay`, and `Selecto.Performance.Hooks` remain supported.
  - Workspace runtime and test suites were revalidated after cleanup.

### PHASE 4: Domain Validation Layer Implementation

#### Added
- **`Selecto.DomainValidator` module** - Comprehensive domain configuration validation
  - `validate_domain!/1` - Raising validation function
  - `validate_domain/1` - Non-raising validation function  
  - `Selecto.DomainValidator.ValidationError` exception type
  - Detailed error formatting with clear diagnostic messages

#### Enhanced
- **`Selecto.configure/3`** - Added optional `validate: true` parameter
  - When enabled, validates domain before processing
  - Backwards compatible - validation disabled by default
  - Updated documentation with validation examples and usage

#### Validation Features
- **Domain Structure Validation**
  - Required top-level keys (source, schemas)
  - Schema structural integrity (required keys, column definitions)
  - Association queryable reference validation
  - Join reference validation (associations must exist)

- **Advanced Validation Logic**
  - **Join dependency cycle detection** - Prevents infinite recursion
  - **Advanced join type validation** - Required keys for specialized joins:
    - `:dimension` joins require `dimension` key
    - `:hierarchical` materialized path requires `path_field`
    - `:hierarchical` closure table requires `closure_table`, `ancestor_field`, `descendant_field`
    - `:snowflake_dimension` requires non-empty `normalization_joins`
  - **Field reference validation** - Basic existence checking for filters/selectors

#### Tests Added
- **16 comprehensive validation test cases** covering all error conditions and success paths
- Integration testing with main Selecto API
- Real circular dependency cycle detection
- Advanced join requirement validation

#### Files Added
- `lib/selecto/domain_validator.ex` - Main validation module
- `test/selecto_domain_validator_test.exs` - Comprehensive test suite

### PHASES 1-3: Complete SQL Parameterization Security Refactor

#### Security Enhancement - Complete SQL Injection Prevention

**PHASE 1: WHERE Clause Parameterization**
- Replaced string interpolation with iodata parameterization in WHERE builders
- Eliminated sentinel pattern `^SelectoParam^` usage in WHERE clauses
- Added `Selecto.SQL.Params` module for safe parameter handling

**PHASE 2: GROUP BY & ORDER BY Parameterization** 
- Extended iodata parameterization to GROUP BY and ORDER BY clauses
- Added `Params.finalize/1` for converting `{:param, value}` markers to `$N` placeholders
- Comprehensive test coverage for new parameterization system

**PHASE 3: SELECT & FROM Parameterization**
- Refactored SELECT builder to use iodata parameterization
- Enhanced FROM builder with structured parameter handling  
- All SQL generation clauses now use safe iodata-based approach

**PHASE 4: Legacy Code Elimination**
- Removed all legacy string-based SQL building functions (76+ lines removed)
- Consolidated iodata functions as primary implementations
- Eliminated dual-path logic throughout the system
- Complete removal of sentinel pattern handling

#### Security Benefits Achieved
- ✅ **Zero SQL injection vectors** - No string interpolation paths remain
- ✅ **Complete parameterization** - All user values use structured parameters  
- ✅ **No sentinel artifacts** - `^SelectoParam^` patterns completely eliminated
- ✅ **Type safety** - Values preserve types through parameter pipeline
- ✅ **Production ready** - Robust validation prevents configuration errors

#### Files Modified (Security Refactor)
- `lib/selecto/builder/sql.ex` - Complete refactor with legacy code removal
- `lib/selecto/builder/sql/select.ex` - SELECT builder iodata conversion
- `lib/selecto/builder/sql/where.ex` - WHERE parameterization
- `lib/selecto/builder/sql/group.ex` - GROUP BY parameterization
- `lib/selecto/builder/sql/order.ex` - ORDER BY parameterization
- `lib/selecto/sql/params.ex` - Parameter handling utilities
- `lib/selecto.ex` - Enhanced with validation integration

#### Files Added (Security & Testing)
- `test/selecto_where_iodata_test.exs` - WHERE clause iodata tests
- `test/selecto_sql_params_test.exs` - Parameter handling tests  
- `test/selecto_group_order_test.exs` - GROUP/ORDER BY tests
- `test/selecto_select_from_test.exs` - SELECT/FROM iodata tests
- `test/selecto_integration_test.exs` - End-to-end parameterization tests
- `PHASE2_COMPLETE.md` - GROUP/ORDER BY documentation
- `PHASE3_COMPLETE.md` - SELECT/FROM documentation  
- `PHASE4_COMPLETE.md` - Legacy elimination documentation

#### Test Results
- **39 total tests** - All parameterization and validation tests passing ✅
- **23 parameterization tests** - Complete SQL generation coverage
- **16 validation tests** - Domain configuration validation coverage

---

CHANGES (Legacy)
=======

- refactor configuration system- joins, filters, columns

V 0.2.6
-------

- prep for move to org

V 0.2.4
-------

- support for filter form updates

V 0.2.3 TODO
------------

- bug fixes
- fix for rollup sorts
- date helpers

V 0.2.2 TODO
------------

- bug fixes
- update some where handlers

V 0.2.1
-------

- upd to keep sync with comp
- remove unused ecto query builder

V 0.2.0
-------

- Support custom filters
- switch to build SQL directly

V 0.1.3
-------

- support for subqueries fragments in select

V 0.1.2
-------

- Support for custom cols

V 0.1.0
-------

Initial Release
