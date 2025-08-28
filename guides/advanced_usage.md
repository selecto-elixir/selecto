# Advanced Usage Guide

This guide demonstrates real-world usage of Selecto's advanced features with practical examples for complex database scenarios.

## Table of Contents

- [E-commerce Analytics](#e-commerce-analytics)
- [Content Management System](#content-management-system)  
- [Organizational Hierarchy](#organizational-hierarchy)
- [Business Intelligence Dashboard](#business-intelligence-dashboard)
- [Complex CTE Patterns](#complex-cte-patterns)
- [Performance Optimization](#performance-optimization)

## E-commerce Analytics

### Domain Setup

```elixir
defmodule ECommerceAnalytics do
  def domain do
    %{
      name: "E-commerce Sales Analytics",
      source: %{
        source_table: "orders",
        primary_key: :id,
        fields: [:id, :total, :status, :customer_id, :created_at, :updated_at],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          total: %{type: :decimal},
          status: %{type: :string},
          customer_id: %{type: :integer},
          created_at: %{type: :utc_datetime},
          updated_at: %{type: :utc_datetime}
        },
        associations: %{
          customer: %{queryable: :customers, field: :customer, owner_key: :customer_id, related_key: :id},
          items: %{queryable: :order_items, field: :items, owner_key: :id, related_key: :order_id},
          payments: %{queryable: :payments, field: :payments, owner_key: :id, related_key: :order_id}
        }
      },
      schemas: %{
        customers: %{
          name: "Customer",
          source_table: "customers",
          fields: [:id, :name, :email, :region_id, :customer_type],
          redact_fields: [:email],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            email: %{type: :string},
            region_id: %{type: :integer},
            customer_type: %{type: :string}
          },
          associations: %{
            region: %{queryable: :regions, field: :region, owner_key: :region_id, related_key: :id}
          }
        },
        order_items: %{
          name: "Order Item",
          source_table: "order_items",
          fields: [:id, :quantity, :unit_price, :order_id, :product_id],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            quantity: %{type: :integer},
            unit_price: %{type: :decimal},
            order_id: %{type: :integer},
            product_id: %{type: :integer}
          },
          associations: %{
            product: %{queryable: :products, field: :product, owner_key: :product_id, related_key: :id}
          }
        },
        products: %{
          name: "Product",
          source_table: "products",
          fields: [:id, :name, :category_id, :brand_id],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            category_id: %{type: :integer},
            brand_id: %{type: :integer}
          },
          associations: %{
            category: %{queryable: :categories, field: :category, owner_key: :category_id, related_key: :id},
            brand: %{queryable: :brands, field: :brand, owner_key: :brand_id, related_key: :id},
            tags: %{queryable: :product_tags, field: :tags, owner_key: :id, related_key: :product_id}
          }
        },
        categories: %{
          name: "Category",
          source_table: "categories",
          fields: [:id, :name, :parent_id, :path],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            parent_id: %{type: :integer},
            path: %{type: :string}
          }
        },
        brands: %{
          name: "Brand",
          source_table: "brands",
          fields: [:id, :name],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string}
          }
        },
        product_tags: %{
          name: "Product Tag",
          source_table: "product_tags",
          fields: [:id, :name, :product_id],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            product_id: %{type: :integer}
          }
        },
        regions: %{
          name: "Region",
          source_table: "regions",
          fields: [:id, :name, :country_id],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            country_id: %{type: :integer}
          }
        }
      },
      joins: %{
        # Star schema dimension for analytics
        customer: %{type: :star_dimension, display_field: :name},
        
        # Standard joins for order details
        items: %{
          type: :left,
          joins: %{
            product: %{
              type: :star_dimension,
              display_field: :name,
              joins: %{
                # Hierarchical categories with materialized path
                category: %{
                  type: :hierarchical,
                  hierarchy_type: :materialized_path,
                  path_field: :path,
                  path_separator: "/"
                },
                brand: %{type: :star_dimension, display_field: :name},
                # Many-to-many product tagging
                tags: %{type: :tagging, tag_field: :name}
              }
            }
          }
        }
      },
      default_selected: ["id", "total", "created_at"],
      required_filters: [{"status", {:not_eq, "cancelled"}}]
    }
  end
end
```

### Sales Analytics Queries

```elixir
# Configure analytics domain
selecto = Selecto.configure(ECommerceAnalytics.domain(), conn)

# 1. Sales by Customer Region (Star Schema)
regional_sales = selecto
  |> Selecto.select([
    "customer[name]",
    "customer[region][name]",
    {:func, "sum", ["total"]},
    {:func, "count", ["*"]},
    {:func, "avg", ["total"]}
  ])
  |> Selecto.filter([
    {"created_at", {:between, ~D[2024-01-01], ~D[2024-12-31]}},
    {"customer[customer_type]", "premium"}
  ])
  |> Selecto.group_by(["customer[name]", "customer[region][name]"])
  |> Selecto.order_by([{:desc, {:func, "sum", ["total"]}}])
  |> Selecto.execute()

# 2. Product Category Hierarchy Analysis
category_performance = selecto
  |> Selecto.select([
    "items[product][category_display]",     # From star dimension
    "items[product][category_path]",        # From hierarchical path
    "items[product][category_level]",       # From CTE calculation
    {:func, "sum", ["items[quantity]"]},
    {:func, "sum", ["total"]}
  ])
  |> Selecto.filter([
    {"items[product][category_level]", {:lte, 3}},  # Only 3 levels deep
    {"total", {:gte, 50}}
  ])
  |> Selecto.group_by([
    "items[product][category_display]",
    "items[product][category_level]"
  ])
  |> Selecto.execute()

# 3. Tagged Product Analysis (Many-to-Many)
tagged_products = selecto
  |> Selecto.select([
    "items[product][name]",
    "items[product][tags_list]",            # Aggregated tag string
    {:func, "sum", ["items[quantity]"]},
    {:func, "avg", ["items[unit_price]"]}
  ])
  |> Selecto.filter([
    {"items[product][tags_filter]", "premium"},  # Faceted tag filter
    {"created_at", {:gte, ~D[2024-06-01]}}
  ])
  |> Selecto.group_by(["items[product][name]", "items[product][tags_list]"])
  |> Selecto.execute()
```

## Content Management System

### Hierarchical Content Categories

```elixir
defmodule CMSContent do
  def domain do
    %{
      name: "Content Management System",
      source: %{
        source_table: "articles",
        primary_key: :id,
        fields: [:id, :title, :content, :author_id, :category_id, :published_at],
        columns: %{
          id: %{type: :integer},
          title: %{type: :string},
          content: %{type: :text},
          author_id: %{type: :integer},
          category_id: %{type: :integer},
          published_at: %{type: :utc_datetime}
        },
        associations: %{
          author: %{queryable: :users, field: :author, owner_key: :author_id, related_key: :id},
          category: %{queryable: :categories, field: :category, owner_key: :category_id, related_key: :id},
          tags: %{queryable: :article_tags, field: :tags, owner_key: :id, related_key: :article_id}
        }
      },
      schemas: %{
        categories: %{
          name: "Category",
          source_table: "categories",
          fields: [:id, :name, :parent_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            parent_id: %{type: :integer}
          }
        }
        # ... other schemas
      },
      joins: %{
        # Adjacency list hierarchy for content categories
        category: %{
          type: :hierarchical,
          hierarchy_type: :adjacency_list,
          depth_limit: 5
        },
        # Many-to-many tagging
        tags: %{type: :tagging, tag_field: :name}
      }
    }
  end
end

# Usage: Find all articles in category and subcategories
cms_selecto = Selecto.configure(CMSContent.domain(), conn)

articles_with_hierarchy = cms_selecto
  |> Selecto.select([
    "title",
    "author[name]",
    "category[name]",
    "category_path",      # From CTE: full path to root
    "category_level",     # From CTE: depth in hierarchy  
    "tags_list"           # Aggregated tags
  ])
  |> Selecto.filter([
    {:or, [
      {"category[name]", "Technology"},
      {"category_path_array", {:contains, "Technology"}}  # Any ancestor named "Technology"
    ]},
    {"published_at", {:not_null}},
    {"tags[name]", {:in, ["featured", "trending"]}}
  ])
  |> Selecto.order_by(["category_level", "published_at"])
  |> Selecto.execute()
```

## Organizational Hierarchy

### Employee Management with Multiple Hierarchy Patterns

```elixir
defmodule OrganizationStructure do
  def domain do
    %{
      name: "Organization Management",
      source: %{
        source_table: "employees",
        primary_key: :id,
        fields: [:id, :name, :email, :department_id, :manager_id, :hire_date],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          email: %{type: :string},
          department_id: %{type: :integer},
          manager_id: %{type: :integer},
          hire_date: %{type: :date}
        },
        associations: %{
          manager: %{queryable: :employees, field: :manager, owner_key: :manager_id, related_key: :id},
          department: %{queryable: :departments, field: :department, owner_key: :department_id, related_key: :id},
          skills: %{queryable: :employee_skills, field: :skills, owner_key: :id, related_key: :employee_id}
        }
      },
      schemas: %{
        departments: %{
          name: "Department", 
          source_table: "departments",
          fields: [:id, :name, :parent_department_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            parent_department_id: %{type: :integer}
          }
        },
        employee_skills: %{
          name: "Employee Skill",
          source_table: "employee_skills", 
          fields: [:id, :skill_name, :employee_id],
          columns: %{
            id: %{type: :integer},
            skill_name: %{type: :string},
            employee_id: %{type: :integer}
          }
        }
      },
      joins: %{
        # Management hierarchy (self-referencing)
        manager: %{
          type: :hierarchical,
          hierarchy_type: :adjacency_list,
          depth_limit: 6  # CEO down to 6 levels
        },
        # Department hierarchy
        department: %{
          type: :hierarchical,  
          hierarchy_type: :adjacency_list,
          depth_limit: 4
        },
        # Employee skills (many-to-many)
        skills: %{type: :tagging, tag_field: :skill_name}
      }
    }
  end
end

# Usage: Complex organizational queries
org_selecto = Selecto.configure(OrganizationStructure.domain(), conn)

# 1. Management chain analysis
management_chain = org_selecto
  |> Selecto.select([
    "name",
    "manager_path",           # Full path to CEO
    "manager_level",          # Management level (0 = CEO)
    "department[name]", 
    "department_level",       # Department hierarchy level
    "skills_list"             # Aggregated skills
  ])
  |> Selecto.filter([
    {"manager_level", {:between, 1, 3}},  # Middle management only
    {"department[name]", {:like, "Engineering%"}},
    {"skills[skill_name]", {:in, ["leadership", "management"]}}
  ])
  |> Selecto.order_by(["manager_level", "department_level", "name"])
  |> Selecto.execute()

# 2. Department rollup with employee counts
department_summary = org_selecto
  |> Selecto.select([
    "department[name]",
    "department_path",        # Full department hierarchy path
    {:func, "count", ["*"]},  # Employee count
    {:func, "avg", [{:extract, "year", {:func, "age", ["hire_date"]}}]}, # Avg tenure
    {:array_agg, "skills_unique", ["skills[skill_name]"]}  # All unique skills
  ])
  |> Selecto.filter([
    {"hire_date", {:gte, ~D[2020-01-01]}},
    {"department_level", {:lte, 2}}  # Only top 2 department levels
  ])
  |> Selecto.group_by(["department[name]", "department_path"])
  |> Selecto.execute()
```

## Business Intelligence Dashboard

### Complex OLAP Queries with Multiple Dimensions

```elixir
defmodule SalesDashboard do
  def sales_cube_domain do
    %{
      name: "Sales Analytics Cube",
      source: %{
        source_table: "sales_facts",
        primary_key: :id,
        fields: [:id, :sale_amount, :quantity, :sale_date, :customer_id, :product_id, :territory_id],
        columns: %{
          id: %{type: :integer},
          sale_amount: %{type: :decimal},
          quantity: %{type: :integer},
          sale_date: %{type: :date},
          customer_id: %{type: :integer},
          product_id: %{type: :integer},
          territory_id: %{type: :integer}
        },
        associations: %{
          customer: %{queryable: :customers, field: :customer, owner_key: :customer_id, related_key: :id},
          product: %{queryable: :products, field: :product, owner_key: :product_id, related_key: :id},
          territory: %{queryable: :territories, field: :territory, owner_key: :territory_id, related_key: :id},
          time: %{queryable: :time_dimension, field: :time, owner_key: :sale_date, related_key: :date_value}
        }
      },
      schemas: %{
        customers: %{
          name: "Customer Dimension",
          source_table: "customers",
          fields: [:id, :name, :segment, :region_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            segment: %{type: :string},
            region_id: %{type: :integer}
          },
          associations: %{
            region: %{queryable: :regions, field: :region, owner_key: :region_id, related_key: :id}
          }
        },
        products: %{
          name: "Product Dimension",
          source_table: "products",
          fields: [:id, :name, :category_id, :subcategory_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            category_id: %{type: :integer},
            subcategory_id: %{type: :integer}
          }
        },
        territories: %{
          name: "Territory Dimension",
          source_table: "territories",
          fields: [:id, :name, :country_id, :region_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            country_id: %{type: :integer},
            region_id: %{type: :integer}
          }
        },
        time_dimension: %{
          name: "Time Dimension",
          source_table: "time_dimension",
          fields: [:date_value, :year, :quarter, :month, :week, :day_name],
          columns: %{
            date_value: %{type: :date},
            year: %{type: :integer},
            quarter: %{type: :integer},
            month: %{type: :integer},
            week: %{type: :integer},
            day_name: %{type: :string}
          }
        }
      },
      joins: %{
        # All dimensions configured for OLAP
        customer: %{type: :star_dimension, display_field: :name},
        product: %{type: :star_dimension, display_field: :name},  
        territory: %{type: :star_dimension, display_field: :name},
        time: %{type: :star_dimension, display_field: :date_value}
      }
    }
  end
end

# Usage: Complex OLAP dashboard queries
dashboard_selecto = Selecto.configure(SalesDashboard.sales_cube_domain(), conn)

# 1. Multi-dimensional sales cube
sales_cube = dashboard_selecto
  |> Selecto.select([
    # Dimensions
    "time[year]",
    "time[quarter]", 
    "customer[segment]",
    "product[category][name]",
    "territory[country][name]",
    
    # Measures
    {:func, "sum", ["sale_amount"]},
    {:func, "sum", ["quantity"]},
    {:func, "count", ["*"]},
    {:func, "avg", ["sale_amount"]},
    
    # Advanced calculations
    {:func, "sum", ["sale_amount"]} / {:func, "sum", ["quantity"]}, # Avg unit price
    {:window, "rank", [], {:over, ["sale_amount"], :desc}}  # Ranking
  ])
  |> Selecto.filter([
    {"time[year]", {:in, [2023, 2024]}},
    {"customer[segment]", {:not_eq, "test"}},
    {"sale_amount", {:gt, 0}}
  ])
  |> Selecto.group_by([
    "time[year]", "time[quarter]",
    "customer[segment]",
    "product[category][name]", 
    "territory[country][name]"
  ])
  |> Selecto.order_by([
    "time[year]", "time[quarter]",
    {:desc, {:func, "sum", ["sale_amount"]}}
  ])
  |> Selecto.execute()

# 2. Time-based trend analysis with CTEs
alias Selecto.Builder.Cte

# Create base CTE for monthly sales
monthly_base = dashboard_selecto
  |> Selecto.select([
    "time[year]",
    "time[month]",
    "customer[segment]",
    {:func, "sum", ["sale_amount"]},
    {:func, "count", ["*"]}
  ])
  |> Selecto.filter([{"time[year]", 2024}])
  |> Selecto.group_by(["time[year]", "time[month]", "customer[segment]"])

{monthly_cte, monthly_params} = Cte.build_cte_from_selecto("monthly_sales", monthly_base)

# Main query with month-over-month comparison
trend_query = [
  "SELECT ",
    "m1.year, m1.month, m1.customer_segment,",
    "m1.sale_amount as current_month,",
    "m2.sale_amount as previous_month,",
    "((m1.sale_amount - m2.sale_amount) / m2.sale_amount * 100) as growth_percentage ",
  "FROM monthly_sales m1 ",
  "LEFT JOIN monthly_sales m2 ON m1.customer_segment = m2.customer_segment ",
    "AND m1.year = m2.year AND m1.month = m2.month + 1 ",
  "ORDER BY m1.year, m1.month, growth_percentage DESC"
]

{final_query, final_params} = Cte.integrate_ctes_with_query(
  [{monthly_cte, monthly_params}],
  trend_query,
  []
)

# Execute trend analysis
{trend_sql, trend_params} = Selecto.SQL.Params.finalize(final_query)
{:ok, %Postgrex.Result{rows: rows}} = Postgrex.query(conn, trend_sql, trend_params)
```

## Complex CTE Patterns

### Recursive Hierarchies with Business Logic

```elixir
defmodule ComplexCTEExamples do
  def build_territory_hierarchy(conn, root_territory_id) do
    alias Selecto.Builder.Cte
    
    # Base case: Root territory
    base_cte_sql = [
      "SELECT id, name, parent_id, 1 as level, ",
      "CAST(name as TEXT) as path, ",
      "ARRAY[id] as territory_path ",
      "FROM territories WHERE id = ", {:param, root_territory_id}
    ]
    
    # Recursive case: Child territories  
    recursive_cte_sql = [
      "SELECT t.id, t.name, t.parent_id, h.level + 1, ",
      "h.path || ' -> ' || t.name, ",
      "h.territory_path || t.id ",
      "FROM territories t JOIN territory_hierarchy h ON t.parent_id = h.id ",
      "WHERE h.level < ", {:param, 5}
    ]
    
    {recursive_cte, params} = Cte.build_recursive_cte(
      "territory_hierarchy",
      base_cte_sql, [root_territory_id],
      recursive_cte_sql, [5]
    )
    
    # Main query: Sales rollup by territory level
    main_query = [
      "SELECT ",
        "h.level, h.name, h.path, ",
        "COALESCE(SUM(s.sale_amount), 0) as total_sales, ",
        "COUNT(s.id) as sale_count, ",
        "AVG(s.sale_amount) as avg_sale ",
      "FROM territory_hierarchy h ",
      "LEFT JOIN sales_facts s ON s.territory_id = ANY(h.territory_path) ",
      "WHERE s.sale_date >= ", {:param, ~D[2024-01-01]}, " ",
      "GROUP BY h.level, h.name, h.path ",
      "ORDER BY h.level, total_sales DESC"
    ]
    
    {complete_query, combined_params} = Cte.integrate_ctes_with_query(
      [{recursive_cte, params}],
      main_query,
      [~D[2024-01-01]]
    )
    
    {sql, final_params} = Selecto.SQL.Params.finalize(complete_query)
    Postgrex.query(conn, sql, final_params)
  end
  
  def build_customer_lifetime_value_analysis(conn) do
    alias Selecto.Builder.Cte
    
    # CTE 1: Customer first purchase date
    first_purchase_sql = [
      "SELECT customer_id, MIN(sale_date) as first_purchase ",
      "FROM sales_facts ",
      "GROUP BY customer_id"
    ]
    {first_purchase_cte, _} = Cte.build_cte("first_purchases", first_purchase_sql, [])
    
    # CTE 2: Customer purchase summary by month
    monthly_purchases_sql = [
      "SELECT ",
        "s.customer_id, ",
        "DATE_TRUNC('month', s.sale_date) as month, ",
        "SUM(s.sale_amount) as monthly_total, ",
        "COUNT(*) as purchase_count ",
      "FROM sales_facts s ",
      "WHERE s.sale_date >= ", {:param, ~D[2023-01-01]}, " ",
      "GROUP BY s.customer_id, DATE_TRUNC('month', s.sale_date)"
    ]
    {monthly_cte, monthly_params} = Cte.build_cte("monthly_purchases", monthly_purchases_sql, [~D[2023-01-01]])
    
    # Main query: Customer lifetime value with cohort analysis
    main_query = [
      "SELECT ",
        "c.name as customer_name, ",
        "DATE_TRUNC('month', fp.first_purchase) as cohort_month, ",
        "SUM(mp.monthly_total) as lifetime_value, ",
        "COUNT(mp.month) as active_months, ",
        "AVG(mp.monthly_total) as avg_monthly_spend, ",
        "MAX(mp.month) as last_purchase_month ",
      "FROM customers c ",
      "JOIN first_purchases fp ON c.id = fp.customer_id ",
      "LEFT JOIN monthly_purchases mp ON c.id = mp.customer_id ",
      "GROUP BY c.id, c.name, fp.first_purchase ",
      "HAVING SUM(mp.monthly_total) > ", {:param, 1000}, " ",
      "ORDER BY lifetime_value DESC"
    ]
    
    all_ctes = [
      {first_purchase_cte, []},
      {monthly_cte, monthly_params}
    ]
    
    {complete_query, combined_params} = Cte.integrate_ctes_with_query(
      all_ctes,
      main_query,
      [1000]
    )
    
    {sql, final_params} = Selecto.SQL.Params.finalize(complete_query)
    Postgrex.query(conn, sql, final_params)
  end
end
```

## Performance Optimization

### Efficient Join Ordering and Filtering

```elixir
defmodule PerformanceOptimization do
  # Best practices for complex queries
  
  def optimized_analytics_query(selecto) do
    selecto
    # 1. Apply most selective filters first
    |> Selecto.filter([
      {"sale_date", {:between, ~D[2024-01-01], ~D[2024-12-31]}},  # Date range first
      {"sale_amount", {:gt, 100}},                                # Value filter
      {"customer[segment]", "premium"}                            # Dimension filter
    ])
    # 2. Select only needed columns
    |> Selecto.select([
      "customer[name]",
      "product[category]", 
      {:func, "sum", ["sale_amount"]},
      {:func, "count", ["*"]}
    ])
    # 3. Group by dimensions (not measures)
    |> Selecto.group_by(["customer[name]", "product[category]"])
    # 4. Order by aggregated values for top-N queries
    |> Selecto.order_by([{:desc, {:func, "sum", ["sale_amount"]}}])
    |> Selecto.limit(100)  # Limit results for pagination
  end
  
  def efficient_hierarchy_query(selecto) do
    # For hierarchical queries, filter early to reduce CTE recursion
    selecto
    |> Selecto.filter([
      # Limit hierarchy depth early
      {"category_level", {:lte, 3}},
      # Filter on indexed columns
      {"active", true},
      {"updated_at", {:gte, ~D[2024-01-01]}}
    ])
    |> Selecto.select([
      "name",
      "category_path",
      "category_level"
    ])
    |> Selecto.order_by(["category_level", "name"])
  end
  
  def batch_processing_pattern(selecto, batch_size \\ 1000) do
    # Process large datasets in batches
    total_count = selecto
      |> Selecto.select([{:func, "count", ["*"]}])
      |> Selecto.execute()
      |> List.first()
      |> List.first()
    
    0..(div(total_count, batch_size))
    |> Enum.map(fn batch_num ->
      offset = batch_num * batch_size
      
      selecto
      |> Selecto.select(["id", "name", "customer[segment]"])
      |> Selecto.order_by(["id"])  # Consistent ordering
      |> Selecto.limit(batch_size)
      |> Selecto.offset(offset)
      |> Selecto.execute()
    end)
  end
end
```

## Troubleshooting Common Issues

### Domain Configuration Validation

```elixir
defmodule SelectoValidation do
  def validate_domain(domain) do
    # Check required fields
    required_keys = [:name, :source, :schemas, :joins]
    missing_keys = required_keys -- Map.keys(domain)
    
    if missing_keys != [] do
      {:error, "Missing required domain keys: #{inspect(missing_keys)}"}
    else
      validate_source_schema(domain.source)
    end
  end
  
  defp validate_source_schema(source) do
    required_source_keys = [:source_table, :primary_key, :fields, :columns]
    missing_keys = required_source_keys -- Map.keys(source)
    
    if missing_keys != [] do
      {:error, "Missing required source keys: #{inspect(missing_keys)}"}
    else
      # Validate that all fields have corresponding columns
      field_column_mismatch = Enum.reject(source.fields, fn field ->
        Map.has_key?(source.columns, field)
      end)
      
      if field_column_mismatch != [] do
        {:error, "Fields without column definitions: #{inspect(field_column_mismatch)}"}
      else
        {:ok, "Domain validation passed"}
      end
    end
  end
end
```

### Common Error Patterns

```elixir
# ❌ Incorrect - Missing column definitions
domain = %{
  source: %{
    source_table: "users",
    fields: [:id, :name, :email]
    # Missing columns!
  }
}

# ✅ Correct - Complete column definitions
domain = %{
  source: %{
    source_table: "users", 
    fields: [:id, :name, :email],
    columns: %{
      id: %{type: :integer},
      name: %{type: :string},
      email: %{type: :string}
    }
  }
}

# ❌ Incorrect - Missing schema name
schemas: %{
  users: %{
    source_table: "users",
    # Missing name!
    fields: [...]
  }
}

# ✅ Correct - Include schema name
schemas: %{
  users: %{
    name: "User",  # Required!
    source_table: "users",
    fields: [...]
  }
}
```

This guide demonstrates how to leverage Selecto's advanced features for real-world applications. The patterns shown here are production-tested and optimized for performance and maintainability.