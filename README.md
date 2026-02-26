# Selecto

**Advanced Query Builder for Elixir (Alpha)**

> ⚠️ **Alpha Quality Software**
>
> `selecto` is currently alpha quality and under active development. Expect
> breaking API changes, behavior changes, incomplete features, and potentially
> severe bugs. Do not treat current releases as production-hardened without
> your own validation, testing, and risk controls.

Selecto is a query building system that allows you to construct complex SQL
queries within configured domains. It supports advanced join patterns,
hierarchical relationships, OLAP dimensions, and Common Table Expressions
(CTEs).

## 📘 Livebooks, Tutorials, and Demo

- [selecto-elixir/selecto_livebooks](https://github.com/selecto-elixir/selecto_livebooks) contains a Livebook that walks through many Selecto query features.
- [seeken/selecto_northwind](https://github.com/seeken/selecto_northwind) contains tutorials for building Selecto queries and workflows.
- [testselecto.fly.dev](https://testselecto.fly.dev) runs the `selecto_test` app as a hosted Selecto demo.

## 📌 Release Status (0.3.x)

- **Alpha**: Core query building, join handling, CTE support, and standard
  filter/select/order flows are usable but not yet stable; breaking changes may
  occur between minor releases.
- **High Risk / Experimental**: Advanced subfilter APIs
  (`Selecto.Subfilter.Parser`, `Selecto.Subfilter.Registry`,
  `Selecto.Subfilter.SQL`) are still being hardened for broad domain coverage.
- **Not Included**: Schema/domain code generation and UI components are not
  part of `selecto` and are provided by companion packages (`selecto_mix`,
  `selecto_components`).

## ⚠️ Known Limitations (Advanced Subfilters)

- Multi-hop subfilter paths must be explicit or unambiguous in domain join config; complex paths can return `:unresolvable_path`.
- SQL compound operation rendering is intentionally simplified and currently combines top-level compound groups with `AND`.
- Some advanced subfilter SQL builders still assume film-domain style correlation keys (for example `film_id`) and may need customization for non-film domain schemas.

## 🚀 Key Features

- **Enhanced Join Types**: Self-joins, lateral joins, cross joins, full outer joins, conditional joins
- **Advanced Join Patterns**: Star/snowflake schemas, hierarchical relationships, many-to-many tagging
- **Enhanced Field Resolution**: Smart disambiguation, error handling, and field suggestions
- **OLAP Support**: Optimized for analytics with dimension tables and aggregation-friendly queries  
- **Hierarchical Data**: Adjacency lists, materialized paths, closure tables with recursive CTEs
- **Safe Parameterization**: 100% parameterized queries with iodata-based SQL generation
- **Complex Relationships**: Many-to-many joins with aggregation and faceted filtering
- **CTE Support**: Both simple and recursive Common Table Expressions
- **Domain Configuration**: Declarative schema definitions with automatic join resolution
- **Alpha Lifecycle**: Active development with frequent internal and API changes

## 🧭 Field Path Syntax (0.3.2+)

Selecto examples and tests now standardize on dot notation for joined paths:

- `customer.name`
- `customer.region.name`
- `items.product.category.name`

Use this notation consistently across `select`, `filter`, `group_by`, and
`order_by` field references.

## 🧩 Extensions (0.3.3+)

Selecto supports package-provided extensions through the `:extensions` key in
your domain config.

### Install

```elixir
def deps do
  [
    {:selecto, "~> 0.3.3"},
    # Optional extension package for spatial/map support
    {:selecto_postgis, "~> 0.1"}
  ]
end
```

### Enable an extension in a domain

```elixir
domain = %{
  name: "Places",
  source: %{
    source_table: "places",
    primary_key: :id,
    fields: [:id, :name, :location],
    columns: %{location: %{type: :geometry}},
    associations: %{}
  },
  schemas: %{},
  joins: %{},
  extensions: [
    Selecto.Extensions.PostGIS
  ]
}
```

### Optional extension DSL in overlays

```elixir
defmodule MyApp.Overlays.PlacesOverlay do
  use Selecto.Config.OverlayDSL,
    extensions: [Selecto.Extensions.PostGIS]

  defmap_view do
    geometry_field("location")
    popup_field("name")
    default_zoom(11)
    center({41.2, -87.6})
  end
end
```

### Authoring your own extension

Implement `Selecto.Extension` and opt into only the callbacks you need:

- `merge_domain/2` for domain defaults/metadata
- `overlay_dsl_modules/1`, `overlay_setup/2`, `overlay_fragment/2` for overlay
  DSL integration
- `components_views/2` for `selecto_components` view registration
- `updato_domain/2` for `selecto_updato` integration

## 📋 Quick Start

```elixir
# Configure your domain
domain = %{
  name: "E-commerce Analytics",
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
      customer: %{queryable: :customers, field: :customer, owner_key: :customer_id, related_key: :id},
      items: %{queryable: :order_items, field: :items, owner_key: :id, related_key: :order_id}
    }
  },
  schemas: %{
    customers: %{
      name: "Customer",
      source_table: "customers", 
      fields: [:id, :name, :region_id],
      columns: %{
        id: %{type: :integer},
        name: %{type: :string},
        region_id: %{type: :integer}
      }
    },
    order_items: %{
      name: "Order Item",
      source_table: "order_items",
      fields: [:id, :quantity, :product_id, :order_id],
      columns: %{
        id: %{type: :integer},
        quantity: %{type: :integer}, 
        product_id: %{type: :integer},
        order_id: %{type: :integer}
      }
    }
  },
  joins: %{
    customer: %{type: :star_dimension, display_field: :name},
    items: %{type: :left}
  }
}

# Create and configure Selecto
selecto = Selecto.configure(domain, postgrex_connection)

# Build queries with automatic join resolution
result = selecto
  |> Selecto.select(["id", "total", "customer.name", "items.quantity"])
  |> Selecto.filter([{"total", {:gt, 100}}, {"customer.name", {:like, "John%"}}])
  |> Selecto.order_by(["created_at"])
  |> Selecto.execute()
```

## 🏗️ Advanced Join Patterns

### OLAP Dimensions (Star Schema)

Perfect for analytics and business intelligence:

```elixir
joins: %{
  customer: %{type: :star_dimension, display_field: :full_name},
  product: %{type: :star_dimension, display_field: :name},
  time: %{type: :star_dimension, display_field: :date}
}
```

### Snowflake Schema (Normalized Dimensions)

For normalized dimension tables requiring additional joins:

```elixir
joins: %{
  region: %{
    type: :snowflake_dimension,
    display_field: :name,
    normalization_joins: [%{table: "countries", alias: "co"}]
  }
}
```

### Hierarchical Relationships

Support for tree structures with multiple implementation patterns:

```elixir
# Adjacency List Pattern
joins: %{
  parent_category: %{
    type: :hierarchical,
    hierarchy_type: :adjacency_list,
    depth_limit: 5
  }
}

# Materialized Path Pattern  
joins: %{
  parent_category: %{
    type: :hierarchical,
    hierarchy_type: :materialized_path,
    path_field: :path,
    path_separator: "/"
  }
}

# Closure Table Pattern
joins: %{
  parent_category: %{
    type: :hierarchical, 
    hierarchy_type: :closure_table,
    closure_table: "category_closure",
    ancestor_field: :ancestor_id,
    descendant_field: :descendant_id
  }
}
```

### Many-to-Many Tagging

Automatic aggregation and faceted filtering:

```elixir
joins: %{
  tags: %{
    type: :tagging,
    tag_field: :name,
    name: "Post Tags"
  }
}

# Automatically creates:
# - Aggregated tag lists: string_agg(tags.name, ', ')
# - Faceted filters for individual tag selection
```

## 🔧 Common Table Expressions (CTEs)

Build complex queries with CTEs using familiar Selecto syntax:

```elixir
alias Selecto.Builder.Cte

# Simple CTE
active_users = selecto
  |> Selecto.select(["id", "name"])
  |> Selecto.filter([{"active", true}])

{cte_iodata, params} = Cte.build_cte_from_selecto("active_users", active_users)

# Recursive CTE for hierarchies
base_case = selecto
  |> Selecto.select(["id", "name", "parent_id", {:literal, 0, "level"}])
  |> Selecto.filter([{"parent_id", nil}])

recursive_case = selecto  
  |> Selecto.select(["c.id", "c.name", "c.parent_id", "h.level + 1"])
  |> Selecto.filter([{"h.level", {:lt, 5}}])

{recursive_cte, params} = Cte.build_recursive_cte_from_selecto("hierarchy", base_case, recursive_case)
```

For advanced spec-based CTE SQL generation, use `Selecto.Builder.CteSql`.

## 📊 Advanced Selection Features

### Custom SQL with Field Validation

```elixir
# Safe custom SQL with automatic field validation
selecto |> Selecto.select([
  {:custom_sql, "COALESCE({{customer_name}}, 'Unknown')", %{
    customer_name: "customer.name"
  }}
])
```

### Complex Aggregations

```elixir
selecto |> Selecto.select([
  {:func, "count", ["*"]},
  {:func, "avg", ["total"]}, 
  {:array, "product_names", ["items.product_name"]},
  {:case, "status", %{
    "high_value" => [{"total", {:gt, 1000}}],
    "else" => [{:literal, "standard"}]
  }}
])
```

## 🔍 Advanced Filtering

### Logical Operators

```elixir
selecto |> Selecto.filter([
  {:and, [
    {"active", true},
    {:or, [
      {"customer.region", "West"},
      {"customer.region", "East"}
    ]}
  ]},
  {"total", {:between, 100, 1000}}
])
```

### Subqueries and Text Search

```elixir
selecto |> Selecto.filter([
  {"customer_id", {:subquery, :in, "SELECT id FROM vip_customers", []}},
  {"description", {:text_search, "elixir postgresql"}}
])
```

## 🎯 Domain Configuration

### Complete Domain Structure

```elixir
domain = %{
  name: "Domain Name",
  source: %{
    source_table: "main_table",
    primary_key: :id,
    fields: [:id, :field1, :field2],
    redact_fields: [:sensitive_field],
    columns: %{
      id: %{type: :integer},
      field1: %{type: :string}
    },
    associations: %{
      related_table: %{
        queryable: :related_schema,
        field: :related,
        owner_key: :foreign_key,
        related_key: :id
      }
    }
  },
  schemas: %{
    related_schema: %{
      name: "Related Schema",
      source_table: "related_table", 
      # ... schema definition
    }
  },
  joins: %{
    related_table: %{type: :left, name: "Related Items"}
  },
  default_selected: ["id", "name"],
  required_filters: [{"active", true}]
}
```

## 🧪 Testing and Quality

- **Broad test coverage**: Includes unit/integration coverage for core paths and
  many edge cases.
- **Alpha caveat**: Passing tests do not guarantee production readiness.
- **Major bugs still possible**: Validate behavior against your own schema,
  workload, and safety requirements.
- **Safe parameterization goals**: SQL generation is designed around
  parameterized query construction.

## 📚 Documentation

- [Join Patterns Guide](guides/joins.md) - Comprehensive database join patterns
- [Phase Implementation History](PHASE4_COMPLETE.md) - Development progression
- [Advanced Usage Examples](guides/advanced_usage.md) - Complex query examples
- [API Reference](docs/api_reference.md) - Complete function documentation

## 🚦 System Requirements

- Elixir 1.10+  
- PostgreSQL 12+ (for advanced features like CTEs and window functions)
- Postgrex connection

## 📦 Installation

```elixir
def deps do
  [
    {:selecto, "~> 0.3.3"}
  ]
end
```

For local multi-repo development against vendored ecosystem packages, set:

```bash
SELECTO_ECOSYSTEM_USE_LOCAL=true
```

This is the shared local-development switch used across Selecto ecosystem repos.

## 🤝 Contributing

Selecto has evolved through multiple development phases:

- **Phase 1**: Foundation and CTE support
- **Phase 2**: Hierarchical joins 
- **Phase 3**: Many-to-many tagging
- **Phase 4**: OLAP dimension optimization
- **Phase 5**: Ongoing testing and documentation

The codebase uses modern Elixir practices, but remains alpha software and is
not presented as production-hardened.

## 📄 License

[MIT](LICENSE)

---

**Selecto** - From simple queries to complex analytics, Selecto helps model
database relationships while the project continues to mature.
