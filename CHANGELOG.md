
# Selecto Library Changelog

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
