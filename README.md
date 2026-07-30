# Selecto

> Alpha software. Expect API churn and breaking changes while the core package is still being hardened.

Selecto includes bounded formal-verification suites for tenant query scope and
provider/consumer domain compatibility. Run `mix selecto.verify`; see
[`docs/formal_verification.md`](docs/formal_verification.md) for the exact proof
boundary and JSON artifact support.

`selecto` is the core query engine in the Selecto ecosystem.

It gives you:

- domain-driven query configuration
- safe select/filter/group/order composition
- automatic join resolution from configured relationships
- aggregate and OLAP-style query support
- CTEs, lateral joins, and other advanced SQL shapes
- expression helpers and a named-function registry for reusable query AST

## Ecosystem

Use `selecto` with companion packages when you need more than the core engine:

- `selecto_components` for Phoenix LiveView query UI
- `selecto_mix` for domain generation and installation tasks
- `selecto_updato` for write operations over Selecto domains
- adapter packages such as `selecto_db_postgresql`, `selecto_db_mysql`, `selecto_db_sqlite`, and others
- `selecto_postgis` for spatial/map extension support

## Installation

Add `selecto` and the adapter package your app uses:

```elixir
def deps do
  [
    {:selecto, ">= 0.4.6 and < 0.6.0"},
    {:selecto_db_postgresql, ">= 0.4.4 and < 0.6.0"}
  ]
end
```

## Quick Start

Define a domain:

```elixir
domain = %{
  name: "Orders",
  source: %{
    source_table: "orders",
    primary_key: :id,
    fields: [:id, :total, :customer_id, :created_at],
    columns: %{
      id: %{type: :integer},
      total: %{type: :decimal},
      customer_id: %{type: :integer},
      created_at: %{type: :utc_datetime}
    },
    associations: %{
      customer: %{queryable: :customers, field: :customer, owner_key: :customer_id, related_key: :id}
    }
  },
  schemas: %{
    customers: %{
      source_table: "customers",
      fields: [:id, :name],
      columns: %{
        id: %{type: :integer},
        name: %{type: :string}
      }
    }
  },
  joins: %{
    customer: %{type: :star_dimension, display_field: :name}
  }
}
```

Build and run a query:

```elixir
selecto = Selecto.configure(domain, Repo)

{:ok, {rows, columns, aliases}} =
  selecto
  |> Selecto.select(["id", "total", "customer.name"])
  |> Selecto.filter([{"total", {:gt, 100}}])
  |> Selecto.order_by(["created_at"])
  |> Selecto.execute()
```

## Expression Helpers

Use `Selecto.Expr` when query structure is assembled dynamically in Elixir:

```elixir
alias Selecto.Expr, as: X

query =
  selecto
  |> Selecto.filter(
    X.compact_and([
      X.eq("status", "active"),
      X.when_present(search, &X.ilike("customer.name", "%#{&1}%")),
      X.gte("total", 100)
    ])
  )
  |> Selecto.select([
    X.field("id"),
    X.field("customer.name"),
    X.as(X.count("*"), "row_count")
  ])
```

For lighter authoring, Selecto also ships macro and sigil support. See `docs/expression_dsl.md` for the detailed guide.

## Named Functions (UDF Registry)

Selecto domains can register named database functions under `domain[:functions]`.

That lets you reuse typed scalar, predicate, and table-function definitions instead of scattering raw SQL fragments through your code.

```elixir
domain = %{
  # ...
  functions: %{
    "name_lower" => %{
      kind: :scalar,
      sql_name: "lower",
      returns: :string,
      allowed_in: [:select, :order_by],
      args: [%{name: :value, type: :string, source: :selector}]
    }
  }
}

query =
  selecto
  |> Selecto.select([
    "name",
    Selecto.Expr.as(Selecto.udf("name_lower", ["name"]), "normalized_name")
  ])
```

UDF-backed custom columns are also supported through `custom_columns[*].select`.

## Extensions

Selecto supports extension packages through the `:extensions` key on domains.

Example with PostGIS:

```elixir
domain = %{
  # ...
  extensions: [Selecto.Extensions.PostGIS]
}
```

Use extensions when a package needs to contribute domain metadata, overlay DSL, adapter type mapping, or companion-package integrations.

## Status

Current `0.4.x` scope:

- core query building is usable but not stable
- advanced subfilter internals are still high-risk/experimental
- adapter support exists across multiple databases, but PostgreSQL remains the most complete path
- schema/domain generation and UI are intentionally outside this package and live in companion repos

## Demos And Tutorials

- `selecto_livebooks` for guided notebooks
- `selecto_northwind` for tutorial-style examples
- `testselecto.fly.dev` for a hosted demo app

## Developer Guides

- `docs/developer_contracts.md` for authored domain contracts, capabilities,
  choice sources, published views, writes, and domain actions.
- `docs/domain_schema_v1.md` for the normalized domain schema contract.
- `docs/expression_dsl.md` for dynamic expression helpers, macros, and sigils.

## Related Repos

- `selecto_components`
- `selecto_mix`
- `selecto_updato`
- `selecto_postgis`
- `selecto_test`
