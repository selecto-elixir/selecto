# Selecto

**Advanced Query Builder for Elixir with Enterprise-Grade Join Support**

Selecto is a powerful, production-ready query building system that allows you to construct complex SQL queries within configured domains. It features comprehensive support for advanced join patterns, hierarchical relationships, OLAP dimensions, and Common Table Expressions (CTEs).

## 🚀 Key Features

- **Advanced Join Patterns**: Star/snowflake schemas, hierarchical relationships, many-to-many tagging
- **OLAP Support**: Optimized for analytics with dimension tables and aggregation-friendly queries  
- **Hierarchical Data**: Adjacency lists, materialized paths, closure tables with recursive CTEs
- **Safe Parameterization**: 100% parameterized queries with iodata-based SQL generation
- **Complex Relationships**: Many-to-many joins with aggregation and faceted filtering
- **CTE Support**: Both simple and recursive Common Table Expressions
- **Domain Configuration**: Declarative schema definitions with automatic join resolution
- **Production Ready**: Comprehensive test coverage (81.52%) and battle-tested architecture

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
  |> Selecto.select(["id", "total", "customer[name]", "items[quantity]"])
  |> Selecto.filter([{"total", {:gt, 100}}, {"customer[name]", {:like, "John%"}}])
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
# - Aggregated tag lists: string_agg(tags[name], ', ')
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

## 📊 Advanced Selection Features

### Custom SQL with Field Validation

```elixir
# Safe custom SQL with automatic field validation
selecto |> Selecto.select([
  {:custom_sql, "COALESCE({{customer_name}}, 'Unknown')", %{
    customer_name: "customer[name]"
  }}
])
```

### Complex Aggregations

```elixir
selecto |> Selecto.select([
  {:func, "count", ["*"]},
  {:func, "avg", ["total"]}, 
  {:array, "product_names", ["items[product_name]"]},
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
      {"customer[region]", "West"},
      {"customer[region]", "East"}
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

- **81.52% Test Coverage**: Comprehensive test suite covering all advanced features
- **Production Ready**: Battle-tested with complex real-world scenarios
- **Safe Parameterization**: 100% parameterized queries prevent SQL injection
- **Performance Optimized**: Efficient join ordering and dependency resolution

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
    {:selecto, "~> 0.2.6"}
  ]
end
```

## 🤝 Contributing

Selecto has evolved through multiple development phases:

- **Phase 1**: Foundation and CTE support
- **Phase 2**: Hierarchical joins 
- **Phase 3**: Many-to-many tagging
- **Phase 4**: OLAP dimension optimization
- **Phase 5**: Testing and documentation (81.52% coverage achieved)

The codebase uses modern Elixir practices with comprehensive test coverage and is ready for production use.

## 📄 License

[Add your license information here]

---

**Selecto** - From simple queries to complex analytics, Selecto handles your database relationships with enterprise-grade reliability.