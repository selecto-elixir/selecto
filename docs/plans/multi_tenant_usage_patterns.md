# Multi-Tenant Usage Patterns for Selecto Ecosystem

## Purpose

Capture recommended multi-tenant patterns for `selecto`, `selecto_updato`, and related components so implementation work can be resumed later.

## Tenant Models and Fit

### 1) Separate database/login per tenant

- Isolation: strongest (security and operational blast radius).
- Cost/complexity: highest (many credentials, pools, migrations, observability streams).
- How to use with Selecto:
  - Resolve tenant at request boundary.
  - Route to tenant-specific repo/connection before `Selecto.configure/3`.
  - Run reads and writes through the same tenant repo for consistency.

### 2) Schema-per-tenant (same DB, separate schemas)

- Isolation: high (clear namespace separation).
- Cost/complexity: medium.
- How to use with Selecto/Updato:
  - Use tenant schema prefix for operations.
  - For writes, pass `prefix: tenant_schema` through `SelectoUpdato.execute/3` opts.
  - For reads, use tenant-bound connection/search path or repo routing.

### 3) Shared tables with `tenant_id`

- Isolation: app/DB-policy dependent.
- Cost/complexity: lowest.
- How to use with Selecto/Updato:
  - Enforce tenant scope server-side via required filters.
  - For all writes, include tenant scope in update/delete filters.
  - Use tenant-aware uniqueness and upsert conflict targets (for example: `tenant_id + external_id`).

### 4) Shared tables with PostgreSQL RLS

- Isolation: strong when policy + session context are enforced.
- Cost/complexity: medium.
- How to use:
  - Set tenant context at connection/transaction boundary.
  - Keep app-level tenant required filters as defense-in-depth.
  - Prefer this model over plain shared-table for sensitive SaaS workloads.

### 5) Hybrid strategy

- Practical default for growth:
  - Shared+RLS for most tenants.
  - Dedicated DB/schema for enterprise/compliance tenants.

## Current Capabilities in This Workspace

- `Selecto.configure/3` already allows per-tenant connection routing.
- `Selecto.Config.Overlay` supports runtime tenant-specific domain overlays.
- `required_filters` are merged into the generated WHERE clause and are not user-optional.
- `SelectoUpdato.execute/3` forwards repo options, enabling prefix-based writes in schema models.
- Saved views already support a context key that can be tenant-scoped.

## Recommended Baseline Architecture

1. Create a `TenantContext` at request entry:
   - `tenant_id`
   - `tenant_mode` (`:dedicated_db | :schema | :shared_rls | :shared_column`)
   - `repo_or_conn`
   - `db_prefix` (when applicable)
2. Build Selecto using tenant-routed repo/connection.
3. Apply tenant overlay that injects non-removable scope filters.
4. Execute writes with explicit tenant scope and prefix options when needed.
5. Namespace user artifacts by tenant (`saved views`, `filter sets`, report configs).

## Security Rules

1. Never trust UI filters for tenant isolation.
2. Enforce tenant boundaries in server-built query state.
3. Add DB-level controls (RLS or strict credentials) for defense in depth.
4. Ensure cache keys include tenant identity to prevent cross-tenant cache bleed.
5. Prefer composite unique indexes that include `tenant_id` in shared-table models.

## Data/Index Guidance

- Shared table model:
  - Include `tenant_id` on all tenant-owned tables.
  - Add indexes with tenant leading key for common filters and joins.
  - Use composite uniqueness with tenant key.
- Schema model:
  - Keep schema migration/versioning consistent across tenant schemas.
- Dedicated DB model:
  - Standardize provisioning and rotation for tenant credentials.

## Operational Concerns

- Connection pools:
  - Dedicated DB model may require bounded per-tenant pools or on-demand pooling.
- Migrations:
  - Schema/dedicated models need orchestrated fan-out migrations.
- Observability:
  - Include tenant tags in logs/metrics/traces (without leaking sensitive values).
- Backups and restore:
  - Define tenant-level restore strategy for each model.

## Implementation Blueprint (Detailed)

### Phase 1: Tenant Context Contract

Deliver a consistent tenant context shape for read paths:

```elixir
%{
  tenant_id: "acme",
  tenant_mode: :shared_rls | :shared_column | :schema | :dedicated_db,
  repo_or_conn: MyApp.Repo,
  prefix: "tenant_acme",
  cache_namespace: "tenant:acme"
}
```

Planned API surface:

1. `Selecto.with_tenant/2` to attach tenant context to query state.
2. `Selecto.tenant/1` to inspect tenant context during execution/debugging.
3. `Selecto.apply_tenant_scope/2` helper for server-enforced required filters.

Acceptance criteria:

- Tenant context persists through `select`, `filter`, `pivot`, `subselect`, and
  `select_shape` query flows.
- Tenant context is available to diagnostics and explain helpers.

### Phase 2: Read Prefix Support

Add read-path prefix support for schema-per-tenant parity with write-path
prefix options in companion packages.

Planned behavior:

1. For `tenant_mode: :schema`, execute read SQL with prefix-aware table
   references or connection-level search path setup.
2. Preserve backward compatibility for existing non-prefix usage.
3. Return explicit error messages when prefix mode is requested but unsupported
   by execution backend.

Acceptance criteria:

- Prefix-scoped read queries produce SQL/exec behavior isolated to tenant
  schema.
- Prefix mode is no-op in dedicated-db mode.

### Phase 3: Enforced Tenant Filters

Provide a first-class helper for required tenant filters in shared-table models.

Planned API sketch:

```elixir
selecto
|> Selecto.with_tenant(%{tenant_id: "acme", tenant_mode: :shared_column})
|> Selecto.require_tenant_filter("tenant_id")
```

Expected semantics:

- Required tenant filters are non-removable by user-provided filters.
- Missing tenant values fail early with structured validation errors.

Acceptance criteria:

- Update/delete style query derivations cannot be executed without tenant scope.
- `query_filters/1` includes required tenant filters so downstream write tools
  can reuse them safely.

### Phase 4: Tenant-Aware Caching and Telemetry

Add tenant identity into query cache and telemetry metadata to prevent
cross-tenant bleed.

Planned behavior:

1. Include tenant namespace in generated cache keys.
2. Include tenant identity metadata in telemetry events (without sensitive
   payloads).
3. Document redaction guidance for tenant metadata logging.

Acceptance criteria:

- Same SQL + params under two tenants produces different cache keys.
- Cache hits never cross tenant boundaries in shared process tests.

### Phase 5: Ecosystem Coordination and Docs

Coordinate tenant behavior with companion packages:

- `selecto_updato`: read/write tenant parity and prefix propagation.
- `selecto_components`: tenant-scoped saved views/filter sets and UI context.
- `selecto_mix`: generated templates with tenant-aware defaults.
- `selecto_absinthe` / `selecto_ash` / `selecto_phoenix`: explicit tenant
  context handoff patterns.

Acceptance criteria:

- Each ecosystem package has a multi-tenant usage plan document and
  package-specific recommendations.
- End-to-end sample demonstrates tenant-safe read + write workflow.

## Test Matrix

Run matrix scenarios for each tenant mode:

1. Shared table + required `tenant_id` filter.
2. Shared table + RLS session context.
3. Schema prefix read/write parity.
4. Dedicated database per tenant.

Minimum checks per mode:

- Correct SQL generation and parameterization.
- Isolation between tenant A and tenant B under concurrent execution.
- Safe failure modes for missing/invalid tenant context.

## Decision Guidance

- Early-stage SaaS: shared table + RLS.
- Mid-stage with stronger isolation needs: schema-per-tenant.
- High-compliance/enterprise tenants: dedicated DB/login.
- At scale: hybrid portfolio with tiered tenant placement.

---

This document is now a concrete implementation blueprint and should be updated
as phases are delivered.
