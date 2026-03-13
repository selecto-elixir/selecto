# Adapter Migration

Selecto now expects app-owned database adapter packages.

## What This Means

- `selecto` provides the query builder and shared adapter contract
- applications add the adapter package they actually use
- adapter packages live in separate Hex modules such as:
  - `selecto_db_postgresql`
  - `selecto_db_mysql`
  - `selecto_db_mariadb`
  - `selecto_db_mssql`
  - `selecto_db_sqlite`

## Installation Pattern

Add `selecto` plus the adapter package your app needs:

```elixir
def deps do
  [
    {:selecto, "~> 0.4.0"},
    {:selecto_db_postgresql, "~> 0.4.0"}
  ]
end
```

Then configure Selecto with that adapter module:

```elixir
Selecto.configure(domain, db_opts, adapter: SelectoDBPostgreSQL.Adapter)
```

## Current Direction

PostgreSQL remains the reference backend, but the long-term direction is for all
database-specific adapters to live outside core `selecto`, including
PostgreSQL itself via `selecto_db_postgresql`.

The legacy `postgrex_opts` field and parameter name still exist in parts of the
core API for backward compatibility, but applications should think of them as
generic connection inputs rather than Postgrex-only configuration.
