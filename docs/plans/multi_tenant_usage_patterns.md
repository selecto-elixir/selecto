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

## Suggested Next Implementation Steps (When Revisited)

1. Add first-class tenant context support to Selecto execution options.
2. Add read-path support for schema prefixes (parity with Updato write-path options).
3. Add helper API to inject tenant required filters safely.
4. Add cache-key extension point including tenant identity.
5. Add docs/examples for all four major models with security checklists.

## Decision Guidance

- Early-stage SaaS: shared table + RLS.
- Mid-stage with stronger isolation needs: schema-per-tenant.
- High-compliance/enterprise tenants: dedicated DB/login.
- At scale: hybrid portfolio with tiered tenant placement.

---

This document is intentionally a planning snapshot for later implementation.
