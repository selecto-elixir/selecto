# Prioritized Delivery Roadmap (P0/P1/P2)

This roadmap captures the remaining high-impact gaps and acceptance checks for
the next delivery cycles.

## P0 (Correctness and Safety)

1. **Cross-DB adapter foundation**
   - Add first-class adapter modules under `Selecto.DB.*` with a shared
     behavior contract.
   - Ensure SQL placeholder strategy is adapter-driven.
   - Acceptance:
     - `mix test test/selecto_db_adapters_test.exs`
     - `mix test test/selecto_sql_params_test.exs`

2. **Non-PostgreSQL pool execution path**
   - Complete successful execution handling for pooled queries.
   - Remove crash-prone generic non-PostgreSQL pool path behavior.
   - Acceptance:
     - `mix test test/connection_pool_test.exs`
     - `mix test test/connection_pool_additional_test.exs`
     - `mix test test/executor_test.exs`

3. **Tenant enforcement parity**
   - Enforce tenant-required policy for read and write derivation paths.
   - Keep tenant required filters non-removable and fail early when missing.
   - Acceptance:
     - `mix test test/selecto_tenant_test.exs`
     - downstream write-path tests in companion packages.

4. **Integrate performance hooks/cache into main execute path**
   - Route normal execute flow through hook orchestration.
   - Include tenant namespace in cache keys.
   - Acceptance:
     - `mix test test/performance/hooks_test.exs`
     - `mix test test/performance/query_cache_test.exs`

## P1 (Feature Hardening)

1. **Subfilter path and join-sequence robustness**
   - Replace simplistic ON-clause decomposition and optimize multi-path merges.
   - Acceptance:
     - `mix test test/selecto/subfilter/join_path_resolver_test.exs`
     - `mix test test/selecto/subfilter/sql_test.exs`

2. **Selector/function DSL completion**
   - Complete richer aggregate/function argument forms and mixed literal/field
     signatures.
   - Acceptance:
     - `mix test test/selecto/select_prep_selector_test.exs`
     - `mix test test/sql_functions_test.exs`

3. **Ecto through-association key introspection**
   - Remove `:id` fallback assumptions for `has_through` owner/related keys.
   - Acceptance:
     - dedicated association introspection tests.

## P2 (Scale and Release Confidence)

1. **True database cursor streaming**
   - Add DB-level stream execution APIs; avoid full-result materialization before
     stream transforms.

2. **Cross-DB CI matrix**
   - Run baseline suites against PostgreSQL, MySQL/MariaDB, MSSQL, and SQLite
     in CI.

3. **Release documentation parity**
   - Keep docs/changelog synchronized with delivered adapter and tenant support.

## Current Progress Snapshot

- Completed: P0.1 adapter module extraction and adapter-driven placeholders.
- Completed: P0.2 pooled execution success path and generic adapter pool safety.
- Completed: P0.4 hook orchestration integration and tenant-aware cache keying
  in default execute path.
- Completed (core): P0.3 tenant enforcement parity for read execution and
  query-filter derivation fail-fast validation in `selecto`.
