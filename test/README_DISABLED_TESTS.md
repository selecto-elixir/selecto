# Disabled Tests

Date reviewed: 2026-02-13

This file tracks intentionally disabled tests, why they are disabled, and the exact
conditions needed to re-enable them.

## Disabled Files

### `test/selecto_test.exs.disabled`
- Classification: `:requires_db`
- Reason: hard dependency on a live PostgreSQL instance and mutable DDL/DML setup.
- Deterministic prerequisites:
1. PostgreSQL reachable at `localhost:5432`
2. Database `selecto_test`
3. Credentials: `postgres` / `password`
- What it does: creates/drops `users`, `posts`, `post_tags` and runs end-to-end query execution tests.
- Re-enable steps:
1. Rename to `test/selecto_test.exs`
2. Add `@moduletag :requires_db`
3. Run with DB available: `mix test test/selecto_test.exs`

## Recently Re-enabled

### `test/selecto_cte_integration_test.exs`
- Re-enabled on: 2026-02-13
- Validation command: `mix test test/selecto_cte_integration_test.exs`
- Result: `11 tests, 0 failures`
- Compatibility note: this suite depends on legacy `Selecto.Builder.Cte` helpers, now provided for backward compatibility.

## Policy

When disabled files are re-enabled, they must use explicit tags:
- DB-dependent suites: `@moduletag :requires_db`
- Transitional/incomplete suites: `@moduletag :skip` with a linked tracking issue
