
# Selecto Library Changelog

## V 0.3.0 - Security & Validation Overhaul (Current)
---------------------------------------------------------

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
