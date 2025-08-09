# Complex Join Patterns Guide

This guide provides comprehensive examples and patterns for implementing complex joins using Selecto's advanced join infrastructure.

## Table of Contents

- [Star Schema Dimensions](#star-schema-dimensions)
- [Hierarchical Joins](#hierarchical-joins)
- [Many-to-Many Tagging](#many-to-many-tagging)
- [Self-Referencing Hierarchies](#self-referencing-hierarchies)
- [Mixed Join Patterns](#mixed-join-patterns)
- [Performance Considerations](#performance-considerations)
- [Advanced CTE Integration](#advanced-cte-integration)

## Star Schema Dimensions

Star schema dimensions are optimized for analytical queries, providing denormalized access to dimension data with automatic display field handling.

### Basic Star Dimension Setup

```elixir
# Domain configuration for star schema
star_domain = %{
  name: "Analytics Star Schema",
  source: %{
    source_table: "sales_facts",
    primary_key: :id,
    fields: [:id, :sale_amount, :customer_id, :product_id, :date_id],
    columns: %{
      id: %{type: :integer},
      sale_amount: %{type: :decimal},
      customer_id: %{type: :integer},
      product_id: %{type: :integer},
      date_id: %{type: :integer}
    },
    associations: %{
      customer: %{queryable: :customers, field: :customer, owner_key: :customer_id, related_key: :id},
      product: %{queryable: :products, field: :product, owner_key: :product_id, related_key: :id},
      date: %{queryable: :dates, field: :date, owner_key: :date_id, related_key: :id}
    }
  },
  schemas: %{
    customers: %{
      name: "Customer Dimension",
      source_table: "customer_dim",
      fields: [:id, :name, :segment, :region, :country],
      columns: %{
        id: %{type: :integer},
        name: %{type: :string},
        segment: %{type: :string},
        region: %{type: :string},
        country: %{type: :string}
      }
    },
    products: %{
      name: "Product Dimension",
      source_table: "product_dim",
      fields: [:id, :name, :category, :subcategory, :brand],
      columns: %{
        id: %{type: :integer},
        name: %{type: :string},
        category: %{type: :string},
        subcategory: %{type: :string},
        brand: %{type: :string}
      }
    },
    dates: %{
      name: "Date Dimension",
      source_table: "date_dim",
      fields: [:id, :date_value, :year, :quarter, :month, :day_name],
      columns: %{
        id: %{type: :integer},
        date_value: %{type: :date},
        year: %{type: :integer},
        quarter: %{type: :integer},
        month: %{type: :integer},
        day_name: %{type: :string}
      }
    }
  },
  joins: %{
    # Star dimensions with automatic display field resolution
    customer: %{type: :star_dimension, display_field: :name},
    product: %{type: :star_dimension, display_field: :name},
    date: %{type: :star_dimension, display_field: :date_value}
  }
}

# Configure and query
selecto = Selecto.configure(star_domain, conn)

# Query with star dimension fields
sales_by_region = selecto
  |> Selecto.select([
    "customer_display",          # Automatic display field (customer.name)
    "customer[segment]",         # Dimension attribute
    "customer[region]",          # Dimension attribute
    "product_display",           # Automatic display field (product.name)
    "product[category]",         # Dimension attribute
    "date[year]",               # Time dimension
    {:func, "sum", ["sale_amount"]},
    {:func, "count", ["*"]}
  ])
  |> Selecto.filter([
    {"date[year]", 2024},
    {"customer[segment]", {:in, ["Premium", "Enterprise"]}},
    {"product[category]", {:not_eq, "Discontinued"}}
  ])
  |> Selecto.group_by([
    "customer[region]",
    "product[category]", 
    "date[year]"
  ])
  |> Selecto.execute()
```

### Nested Star Dimensions

```elixir
# Extended star schema with snowflake dimension normalization
nested_star_domain = %{
  # ... base configuration ...
  schemas: %{
    # ... existing schemas ...
    regions: %{
      name: "Region Dimension",
      source_table: "region_dim",
      fields: [:id, :name, :country_id],
      columns: %{
        id: %{type: :integer},
        name: %{type: :string},
        country_id: %{type: :integer}
      },
      associations: %{
        country: %{queryable: :countries, field: :country, owner_key: :country_id, related_key: :id}
      }
    },
    countries: %{
      name: "Country Dimension",
      source_table: "country_dim", 
      fields: [:id, :name, :continent],
      columns: %{
        id: %{type: :integer},
        name: %{type: :string},
        continent: %{type: :string}
      }
    }
  },
  joins: %{
    customer: %{
      type: :star_dimension,
      display_field: :name,
      joins: %{
        # Snowflake pattern: dimension joins to other dimensions
        region: %{
          type: :star_dimension,
          display_field: :name,
          joins: %{
            country: %{type: :star_dimension, display_field: :name}
          }
        }
      }
    }
  }
}

# Query with nested star dimensions
geographic_analysis = selecto
  |> Selecto.select([
    "customer[region][country_display]",  # Nested dimension display
    "customer[region][country][continent]", # Deep dimension attribute
    {:func, "sum", ["sale_amount"]},
    {:func, "count", ["DISTINCT", "customer_id"]}
  ])
  |> Selecto.filter([
    {"customer[region][country][continent]", {:in, ["North America", "Europe"]}},
    {"sale_amount", {:gt, 1000}}
  ])
  |> Selecto.group_by([
    "customer[region][country_display]",
    "customer[region][country][continent]"
  ])
  |> Selecto.execute()
```

## Hierarchical Joins

Hierarchical joins handle tree-like data structures using either adjacency lists or materialized paths, with automatic CTE generation for traversal.

### Adjacency List Hierarchies

```elixir
# Domain with adjacency list hierarchy
hierarchy_domain = %{
  name: "Organizational Hierarchy",
  source: %{
    source_table: "employees",
    primary_key: :id,
    fields: [:id, :name, :position, :department_id, :manager_id],
    columns: %{
      id: %{type: :integer},
      name: %{type: :string}, 
      position: %{type: :string},
      department_id: %{type: :integer},
      manager_id: %{type: :integer}
    },
    associations: %{
      manager: %{queryable: :employees, field: :manager, owner_key: :manager_id, related_key: :id},
      department: %{queryable: :departments, field: :department, owner_key: :department_id, related_key: :id}
    }
  },
  schemas: %{
    departments: %{
      name: "Department",
      source_table: "departments",
      fields: [:id, :name, :parent_id],
      columns: %{
        id: %{type: :integer},
        name: %{type: :string},
        parent_id: %{type: :integer}
      }
    }
  },
  joins: %{
    # Self-referencing hierarchy (management chain)
    manager: %{
      type: :hierarchical,
      hierarchy_type: :adjacency_list,
      depth_limit: 6,
      path_separator: " > "
    },
    # Department hierarchy  
    department: %{
      type: :hierarchical,
      hierarchy_type: :adjacency_list,
      depth_limit: 4
    }
  }
}

selecto = Selecto.configure(hierarchy_domain, conn)

# Query management hierarchy
management_analysis = selecto
  |> Selecto.select([
    "name",
    "position",
    "manager_path",           # Full path to CEO: "CEO > VP > Director > Manager"
    "manager_level",          # Depth in hierarchy (0 = CEO, 1 = VP, etc.)
    "manager_path_array",     # Array of manager IDs in path
    "department[name]",
    "department_path",        # Department hierarchy path
    "department_level"        # Department depth
  ])
  |> Selecto.filter([
    {"manager_level", {:between, 2, 4}},      # Middle management layers
    {"department_level", {:lte, 2}},          # Top 2 department levels
    {"position", {:like, "%Manager%"}}
  ])
  |> Selecto.order_by([
    "department_level",
    "manager_level", 
    "name"
  ])
  |> Selecto.execute()
```

### Materialized Path Hierarchies

```elixir
# Domain with materialized path hierarchy (more efficient for deep trees)
path_hierarchy_domain = %{
  name: "Content Categories",
  source: %{
    source_table: "articles",
    primary_key: :id,
    fields: [:id, :title, :content, :category_id],
    columns: %{
      id: %{type: :integer},
      title: %{type: :string},
      content: %{type: :text},
      category_id: %{type: :integer}
    },
    associations: %{
      category: %{queryable: :categories, field: :category, owner_key: :category_id, related_key: :id}
    }
  },
  schemas: %{
    categories: %{
      name: "Category",
      source_table: "categories",
      fields: [:id, :name, :path, :parent_id],
      columns: %{
        id: %{type: :integer},
        name: %{type: :string}, 
        path: %{type: :string},    # e.g., "/technology/web-dev/frontend"
        parent_id: %{type: :integer}
      }
    }
  },
  joins: %{
    category: %{
      type: :hierarchical,
      hierarchy_type: :materialized_path,
      path_field: :path,
      path_separator: "/",
      depth_limit: 8
    }
  }
}

selecto = Selecto.configure(path_hierarchy_domain, conn)

# Query with path-based hierarchy
category_analysis = selecto
  |> Selecto.select([
    "title",
    "category[name]",
    "category_path",          # Full category path
    "category_level",         # Depth (calculated from path)
    "category_ancestors",     # Array of ancestor category names
    {:func, "count", ["*"]}
  ])
  |> Selecto.filter([
    # Find all articles in "Technology" branch (any level)
    {"category_path", {:like, "%/technology/%"}},
    # Or specific depth
    {"category_level", {:eq, 3}}
  ])
  |> Selecto.group_by([
    "category[name]", 
    "category_path",
    "category_level"
  ])
  |> Selecto.execute()
```

## Many-to-Many Tagging

Many-to-many tagging joins provide faceted filtering and tag aggregation for flexible categorization systems.

### Basic Tagging Setup

```elixir
# Domain with many-to-many tagging
tagging_domain = %{
  name: "Content Tagging System",
  source: %{
    source_table: "articles",
    primary_key: :id,
    fields: [:id, :title, :content, :author_id, :published_at],
    columns: %{
      id: %{type: :integer},
      title: %{type: :string},
      content: %{type: :text}, 
      author_id: %{type: :integer},
      published_at: %{type: :utc_datetime}
    },
    associations: %{
      author: %{queryable: :users, field: :author, owner_key: :author_id, related_key: :id},
      tags: %{queryable: :article_tags, field: :tags, owner_key: :id, related_key: :article_id}
    }
  },
  schemas: %{
    users: %{
      name: "User",
      source_table: "users",
      fields: [:id, :name, :email],
      columns: %{
        id: %{type: :integer},
        name: %{type: :string},
        email: %{type: :string}
      }
    },
    article_tags: %{
      name: "Article Tag",
      source_table: "article_tags",
      fields: [:id, :name, :article_id, :weight],
      columns: %{
        id: %{type: :integer},
        name: %{type: :string},
        article_id: %{type: :integer},
        weight: %{type: :integer}  # Tag relevance weight
      }
    }
  },
  joins: %{
    author: %{type: :left, display_field: :name},
    tags: %{
      type: :tagging,
      tag_field: :name,
      weight_field: :weight,     # Optional: for weighted tagging
      aggregation: :string_agg   # How to combine multiple tags
    }
  }
}

selecto = Selecto.configure(tagging_domain, conn)

# Query with tag filtering and aggregation
tagged_content = selecto
  |> Selecto.select([
    "title",
    "author_display",
    "tags_list",              # Aggregated tag string: "tech,web,frontend" 
    "tags_count",             # Number of tags
    "tags_weight_sum",        # Sum of tag weights
    "published_at"
  ])
  |> Selecto.filter([
    # Faceted tag filtering
    {"tags_filter", "technology"},        # Has "technology" tag
    {"tags_any", ["web", "mobile"]},      # Has any of these tags
    {"tags_all", ["frontend", "react"]},  # Has all of these tags
    {"tags_weight_min", 5},               # Minimum tag weight
    {"published_at", {:gte, ~D[2024-01-01]}}
  ])
  |> Selecto.order_by([
    {:desc, "tags_weight_sum"},  # Order by tag relevance
    {:desc, "published_at"}
  ])
  |> Selecto.execute()
```

### Advanced Tagging Patterns

```elixir
# Multi-dimensional tagging (categories + user tags)
multi_tag_domain = %{
  # ... base configuration ...
  associations: %{
    # ... existing associations ...
    user_tags: %{queryable: :user_article_tags, field: :user_tags, owner_key: :id, related_key: :article_id},
    system_tags: %{queryable: :system_tags, field: :system_tags, owner_key: :id, related_key: :article_id}
  },
  schemas: %{
    # ... existing schemas ...
    user_article_tags: %{
      name: "User Tag",
      source_table: "user_article_tags", 
      fields: [:id, :name, :article_id, :user_id, :created_at],
      columns: %{
        id: %{type: :integer},
        name: %{type: :string},
        article_id: %{type: :integer},
        user_id: %{type: :integer},
        created_at: %{type: :utc_datetime}
      }
    },
    system_tags: %{
      name: "System Tag",
      source_table: "system_tags",
      fields: [:id, :name, :article_id, :confidence],
      columns: %{
        id: %{type: :integer},
        name: %{type: :string},
        article_id: %{type: :integer},
        confidence: %{type: :float}  # AI-generated tag confidence
      }
    }
  },
  joins: %{
    # ... existing joins ...
    user_tags: %{
      type: :tagging,
      tag_field: :name,
      aggregation: :array_agg    # Keep as array for complex filtering
    },
    system_tags: %{
      type: :tagging,
      tag_field: :name,
      weight_field: :confidence,
      min_weight: 0.7           # Only high-confidence system tags
    }
  }
}

# Query with multiple tag dimensions
multi_dimensional_tags = selecto
  |> Selecto.select([
    "title",
    "user_tags_array",        # User-generated tags as array
    "system_tags_list",       # AI tags as string (high confidence only)
    "system_tags_avg_confidence", # Average AI confidence
    {:func, "count", ["DISTINCT", "user_tags[user_id]"]} # Unique taggers
  ])
  |> Selecto.filter([
    {"user_tags_count", {:gte, 3}},           # At least 3 user tags
    {"system_tags_filter", "programming"},    # AI detected "programming"
    {"system_tags_avg_confidence", {:gte, 0.8}} # High AI confidence
  ])
  |> Selecto.group_by(["id", "title"])
  |> Selecto.execute()
```

## Self-Referencing Hierarchies

Self-referencing hierarchies handle cases where entities reference other entities of the same type, such as management structures or threaded comments.

### Comment Threading

```elixir
# Domain for threaded comments
comment_domain = %{
  name: "Threaded Comments",
  source: %{
    source_table: "comments",
    primary_key: :id,
    fields: [:id, :content, :author_id, :parent_id, :article_id, :created_at],
    columns: %{
      id: %{type: :integer},
      content: %{type: :text},
      author_id: %{type: :integer},
      parent_id: %{type: :integer},    # Self-reference
      article_id: %{type: :integer},
      created_at: %{type: :utc_datetime}
    },
    associations: %{
      parent: %{queryable: :comments, field: :parent, owner_key: :parent_id, related_key: :id},
      author: %{queryable: :users, field: :author, owner_key: :author_id, related_key: :id},
      article: %{queryable: :articles, field: :article, owner_key: :article_id, related_key: :id}
    }
  },
  # ... schemas for users, articles ...
  joins: %{
    parent: %{
      type: :hierarchical,
      hierarchy_type: :adjacency_list,
      depth_limit: 10,  # Limit thread depth
      self_reference: true
    },
    author: %{type: :left, display_field: :name},
    article: %{type: :left, display_field: :title}
  }
}

selecto = Selecto.configure(comment_domain, conn)

# Query comment threads with hierarchy
comment_threads = selecto
  |> Selecto.select([
    "content",
    "author_display",
    "parent_path",            # Path to root comment
    "parent_level",           # Thread depth (0 = root, 1 = reply, etc.)
    "thread_root_id",         # ID of root comment in thread
    "created_at"
  ])
  |> Selecto.filter([
    {"article_id", {:eq, 123}},
    {"parent_level", {:lte, 5}}  # Limit thread depth for display
  ])
  |> Selecto.order_by([
    "thread_root_id",         # Group by thread
    "parent_level",           # Order by depth
    "created_at"              # Then by time
  ])
  |> Selecto.execute()
```

### Organizational Reporting

```elixir
# Query direct reports and team sizes
org_structure = selecto
  |> Selecto.select([
    "name",
    "position", 
    "manager_display",        # Direct manager name
    "manager_level",          # Levels from CEO
    {:func, "count", ["subordinates.id"]}, # Direct reports count
    "team_size_total",        # Total team size (all levels down)
    "manager_path"            # Full reporting chain to CEO
  ])
  |> Selecto.filter([
    {"manager_level", {:between, 1, 4}},  # Skip CEO, limit depth
    {"active", true}
  ])
  |> Selecto.group_by([
    "id", "name", "position", 
    "manager_display", "manager_level"
  ])
  |> Selecto.order_by([
    "manager_level",
    {:desc, {:func, "count", ["subordinates.id"]}}
  ])
  |> Selecto.execute()
```

## Mixed Join Patterns

Complex domains often combine multiple join patterns for comprehensive data relationships.

### E-commerce with Mixed Patterns

```elixir
# Domain combining star dimensions, hierarchies, and tagging
ecommerce_mixed = %{
  name: "E-commerce Analytics",
  source: %{
    source_table: "orders",
    primary_key: :id,
    fields: [:id, :total, :customer_id, :created_at, :status],
    # ... columns ...
    associations: %{
      customer: %{queryable: :customers, field: :customer, owner_key: :customer_id, related_key: :id},
      items: %{queryable: :order_items, field: :items, owner_key: :id, related_key: :order_id}
    }
  },
  schemas: %{
    customers: %{
      # ... customer schema with region association ...
    },
    order_items: %{
      # ... order item schema with product association ...
    },
    products: %{
      # ... product schema with category and tags associations ...
    },
    categories: %{
      # ... hierarchical categories with path field ...
    }
  },
  joins: %{
    # Star dimension for customer analytics
    customer: %{
      type: :star_dimension,
      display_field: :name,
      joins: %{
        region: %{type: :star_dimension, display_field: :name}
      }
    },
    # Left join to order items with nested patterns
    items: %{
      type: :left,
      joins: %{
        # Star dimension for product analytics
        product: %{
          type: :star_dimension,
          display_field: :name,
          joins: %{
            # Hierarchical category structure
            category: %{
              type: :hierarchical,
              hierarchy_type: :materialized_path,
              path_field: :path,
              path_separator: "/"
            },
            # Many-to-many product tags
            tags: %{type: :tagging, tag_field: :name}
          }
        }
      }
    }
  }
}

selecto = Selecto.configure(ecommerce_mixed, conn)

# Complex query using all join patterns
comprehensive_analysis = selecto
  |> Selecto.select([
    # Star dimension fields
    "customer_display",
    "customer[region_display]",
    
    # Hierarchical category analysis
    "items[product][category_path]",
    "items[product][category_level]",
    
    # Tagging analysis
    "items[product][tags_list]",
    "items[product][tags_count]",
    
    # Aggregated measures
    {:func, "sum", ["total"]},
    {:func, "avg", ["items[quantity]"]},
    {:func, "count", ["DISTINCT", "items[product_id]"]}
  ])
  |> Selecto.filter([
    # Star dimension filters
    {"customer[region][name]", {:in, ["North America", "Europe"]}},
    
    # Hierarchical filters
    {"items[product][category_level]", {:lte, 3}},
    {"items[product][category_path]", {:like, "%electronics%"}},
    
    # Tagging filters
    {"items[product][tags_filter]", "premium"},
    {"items[product][tags_count]", {:gte, 2}},
    
    # Standard filters
    {"status", "completed"},
    {"created_at", {:gte, ~D[2024-01-01]}}
  ])
  |> Selecto.group_by([
    "customer[region_display]",
    "items[product][category_path]",
    "items[product][category_level]"
  ])
  |> Selecto.order_by([
    {:desc, {:func, "sum", ["total"]}},
    "items[product][category_level]"
  ])
  |> Selecto.execute()
```

## Performance Considerations

### Join Ordering Optimization

```elixir
# Optimal join order for performance
def optimize_join_order(selecto) do
  selecto
  # 1. Apply most selective filters first (reduces dataset early)
  |> Selecto.filter([
    {"created_at", {:between, ~D[2024-01-01], ~D[2024-03-31]}}, # Date range
    {"status", "active"},                                        # Selective filter
    {"customer[segment]", "enterprise"}                          # Dimension filter
  ])
  # 2. Select specific fields (avoid SELECT *)
  |> Selecto.select([
    "customer_display",
    "items[product][category_path]",
    {:func, "sum", ["total"]}
  ])
  # 3. Group by dimension fields only
  |> Selecto.group_by(["customer_display", "items[product][category_path]"])
  # 4. Order by aggregated values for efficient top-N
  |> Selecto.order_by([{:desc, {:func, "sum", ["total"]}}])
  |> Selecto.limit(100)
end
```

### Index Recommendations

```sql
-- Recommended indexes for join patterns

-- Star dimension joins
CREATE INDEX idx_orders_customer_date ON orders(customer_id, created_at);
CREATE INDEX idx_customers_segment_region ON customers(segment, region_id);

-- Hierarchical joins
CREATE INDEX idx_categories_parent_path ON categories(parent_id, path);
CREATE INDEX idx_employees_manager_level ON employees(manager_id) WHERE active = true;

-- Tagging joins  
CREATE INDEX idx_product_tags_product_name ON product_tags(product_id, name);
CREATE INDEX idx_article_tags_name_weight ON article_tags(name, weight) WHERE weight >= 0.5;

-- Composite indexes for common filter combinations
CREATE INDEX idx_orders_status_date_customer ON orders(status, created_at, customer_id);
```

## Advanced CTE Integration

### Custom CTE with Join Patterns

```elixir
alias Selecto.Builder.Cte

# Build custom CTE that leverages join patterns
def build_customer_segment_analysis(conn) do
  # Base selecto with join patterns
  base_selecto = Selecto.configure(ecommerce_mixed, conn)
    |> Selecto.select([
      "customer_id",
      "customer[segment]", 
      "customer[region_display]",
      "items[product][category_level]",
      {:func, "sum", ["total"]},
      {:func, "count", ["*"]}
    ])
    |> Selecto.filter([
      {"created_at", {:gte, ~D[2024-01-01]}},
      {"status", "completed"}
    ])
    |> Selecto.group_by([
      "customer_id",
      "customer[segment]",
      "customer[region_display]", 
      "items[product][category_level]"
    ])
  
  # Convert to CTE
  {customer_cte, params} = Cte.build_cte_from_selecto("customer_analysis", base_selecto)
  
  # Main query using the CTE
  main_query = [
    "SELECT ",
      "customer_segment, region_display, category_level, ",
      "SUM(total_sales) as segment_sales, ",
      "AVG(order_count) as avg_orders_per_customer, ",
      "COUNT(DISTINCT customer_id) as customer_count ",
    "FROM customer_analysis ",
    "GROUP BY customer_segment, region_display, category_level ",
    "ORDER BY segment_sales DESC"
  ]
  
  {complete_query, combined_params} = Cte.integrate_ctes_with_query(
    [{customer_cte, params}],
    main_query,
    []
  )
  
  {sql, final_params} = Selecto.SQL.Params.finalize(complete_query)
  Postgrex.query(conn, sql, final_params)
end
```

This comprehensive guide demonstrates how to effectively use Selecto's complex join patterns for real-world data analysis scenarios. Each pattern is optimized for specific use cases and can be combined to handle sophisticated data relationships.