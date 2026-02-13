# Disabled Tests

Date reviewed: 2026-02-13

This file tracks intentionally disabled tests, why they are disabled, and the exact
conditions needed to re-enable them.

## Disabled Files

None currently.

## Recently Re-enabled

### `test/selecto_cte_integration_test.exs`
- Re-enabled on: 2026-02-13
- Validation command: `mix test test/selecto_cte_integration_test.exs`
- Result: `11 tests, 0 failures`
- Compatibility note: this suite depends on legacy `Selecto.Builder.Cte` helpers, now provided for backward compatibility.

### `test/selecto_test.exs`
- Re-enabled on: 2026-02-13
- Safety model: tagged with `@moduletag :requires_db`, and excluded by default in `test/test_helper.exs`.
- Default behavior: `mix test` skips this suite.
- To run intentionally:
1. Ensure PostgreSQL is reachable with:
- host: `localhost`
- port: `5432`
- database: `selecto_test`
- username/password: `postgres` / `password`
2. Execute with DB tests enabled:
- `SELECTO_RUN_DB_TESTS=1 mix test test/selecto_test.exs --include requires_db`

## Policy

When disabled files are re-enabled, they must use explicit tags:
- DB-dependent suites: `@moduletag :requires_db`
- Transitional/incomplete suites: `@moduletag :skip` with a linked tracking issue
