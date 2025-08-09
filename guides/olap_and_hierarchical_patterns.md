# OLAP Dimension Support and Hierarchical Patterns

This guide provides in-depth coverage of Selecto's OLAP (Online Analytical Processing) dimension support and advanced hierarchical patterns, designed for analytical and business intelligence applications.

## Table of Contents

- [OLAP Star Schema Dimensions](#olap-star-schema-dimensions)
- [Snowflake Schema Patterns](#snowflake-schema-patterns)
- [Hierarchical Data Patterns](#hierarchical-data-patterns)
- [Dimension Time Intelligence](#dimension-time-intelligence)
- [Slowly Changing Dimensions](#slowly-changing-dimensions)
- [Analytical Functions and Aggregations](#analytical-functions-and-aggregations)
- [Performance Optimization for OLAP](#performance-optimization-for-olap)

## OLAP Star Schema Dimensions

Star schema dimensions are the foundation of OLAP systems, providing denormalized dimension tables that connect to a central fact table.

### Core Star Dimension Features

```elixir
defmodule OLAPDimensionExample do
  def sales_cube_domain do
    %{
      name: "Sales Analytics Cube",
      source: %{
        source_table: "sales_facts",
        primary_key: :id,
        fields: [:id, :sale_amount, :quantity, :discount, :customer_key, :product_key, :date_key, :store_key],
        columns: %{
          id: %{type: :integer},
          sale_amount: %{type: :decimal, precision: 10, scale: 2},
          quantity: %{type: :integer},
          discount: %{type: :decimal, precision: 5, scale: 2},
          customer_key: %{type: :integer},  # Dimension keys
          product_key: %{type: :integer},
          date_key: %{type: :integer},
          store_key: %{type: :integer}
        },
        associations: %{
          customer: %{queryable: :customer_dim, field: :customer, owner_key: :customer_key, related_key: :id},
          product: %{queryable: :product_dim, field: :product, owner_key: :product_key, related_key: :id},
          date: %{queryable: :date_dim, field: :date, owner_key: :date_key, related_key: :id},
          store: %{queryable: :store_dim, field: :store, owner_key: :store_key, related_key: :id}
        }
      },
      schemas: %{
        customer_dim: %{
          name: "Customer Dimension",
          source_table: "customer_dimension",
          fields: [
            :id, :customer_name, :customer_type, :segment, 
            :region, :country, :credit_rating, :registration_date
          ],
          columns: %{
            id: %{type: :integer},
            customer_name: %{type: :string},
            customer_type: %{type: :string},      # Individual, Business, Enterprise
            segment: %{type: :string},            # Premium, Standard, Basic
            region: %{type: :string},
            country: %{type: :string},
            credit_rating: %{type: :string},      # A, B, C, D
            registration_date: %{type: :date}
          }
        },
        product_dim: %{
          name: "Product Dimension",
          source_table: "product_dimension",
          fields: [
            :id, :product_name, :category, :subcategory, :brand,
            :supplier, :cost, :price, :margin_percent, :weight
          ],
          columns: %{
            id: %{type: :integer},
            product_name: %{type: :string},
            category: %{type: :string},           # Electronics, Clothing, Books
            subcategory: %{type: :string},        # Laptops, Smartphones, Tablets
            brand: %{type: :string},
            supplier: %{type: :string},
            cost: %{type: :decimal, precision: 10, scale: 2},
            price: %{type: :decimal, precision: 10, scale: 2},
            margin_percent: %{type: :decimal, precision: 5, scale: 2},
            weight: %{type: :decimal, precision: 8, scale: 3}
          }
        },
        date_dim: %{
          name: "Date Dimension",
          source_table: "date_dimension",
          fields: [
            :id, :date_value, :year, :quarter, :quarter_name, :month, :month_name,
            :week, :day_of_year, :day_of_month, :day_of_week, :day_name,
            :is_weekend, :is_holiday, :fiscal_year, :fiscal_quarter
          ],
          columns: %{
            id: %{type: :integer},
            date_value: %{type: :date},
            year: %{type: :integer},
            quarter: %{type: :integer},
            quarter_name: %{type: :string},       # Q1 2024, Q2 2024
            month: %{type: :integer},
            month_name: %{type: :string},         # January, February
            week: %{type: :integer},
            day_of_year: %{type: :integer},
            day_of_month: %{type: :integer},
            day_of_week: %{type: :integer},
            day_name: %{type: :string},           # Monday, Tuesday
            is_weekend: %{type: :boolean},
            is_holiday: %{type: :boolean},
            fiscal_year: %{type: :integer},       # Company fiscal year
            fiscal_quarter: %{type: :integer}
          }
        },
        store_dim: %{
          name: "Store Dimension", 
          source_table: "store_dimension",
          fields: [
            :id, :store_name, :store_type, :city, :state, :country,
            :region, :district, :manager, :opening_date, :square_footage
          ],
          columns: %{
            id: %{type: :integer},
            store_name: %{type: :string},
            store_type: %{type: :string},         # Flagship, Standard, Outlet
            city: %{type: :string},
            state: %{type: :string},
            country: %{type: :string},
            region: %{type: :string},             # Northeast, Southwest
            district: %{type: :string},
            manager: %{type: :string},
            opening_date: %{type: :date},
            square_footage: %{type: :integer}
          }
        }
      },
      joins: %{
        # Star dimension joins with automatic display field resolution
        customer: %{
          type: :star_dimension,
          display_field: :customer_name,
          default_measures: [:sale_amount, :quantity]  # Default aggregations
        },
        product: %{
          type: :star_dimension,
          display_field: :product_name,
          default_measures: [:sale_amount, :quantity]
        },
        date: %{
          type: :star_dimension,
          display_field: :date_value,
          time_intelligence: true  # Enable time-based calculations
        },
        store: %{
          type: :star_dimension,
          display_field: :store_name,
          geographic: true  # Enable geographic rollups
        }
      },
      # OLAP-specific default configurations
      default_selected: ["date[year]", "date[quarter]", {:func, "sum", ["sale_amount"]}],
      required_filters: [{"date[year]", {:gte, 2020}}],  # Prevent full table scans
      dimension_security: %{
        customer: [:customer_name, :segment],  # Visible customer fields
        store: [:store_name, :region]          # Restricted store access
      }
    }
  end
end
```

### Advanced Star Dimension Queries

```elixir
# Configure OLAP cube
olap_selecto = Selecto.configure(OLAPDimensionExample.sales_cube_domain(), conn)

# 1. Multi-dimensional cube query
sales_cube_analysis = olap_selecto
  |> Selecto.select([
    # Time dimensions
    "date[year]",
    "date[quarter_name]",
    "date[month_name]",
    
    # Customer dimensions  
    "customer[segment]",
    "customer[region]",
    "customer[customer_type]",
    
    # Product dimensions
    "product[category]", 
    "product[brand]",
    
    # Geographic dimensions
    "store[region]",
    "store[store_type]",
    
    # Core measures
    {:func, "sum", ["sale_amount"]},
    {:func, "sum", ["quantity"]},
    {:func, "count", ["*"]},
    {:func, "avg", ["sale_amount"]},
    
    # Calculated measures
    {:calc, :sum, ["sale_amount"], :divide, {:sum, ["quantity"]}},  # Average unit price
    {:calc, :sum, ["sale_amount"], :subtract, {:sum, ["product[cost]"]}}, # Total profit
    
    # Analytical functions
    {:window, "rank", [], {:over, [{:func, "sum", ["sale_amount"]}], :desc}},  # Sales rank
    {:window, "lag", [{:func, "sum", ["sale_amount"]}, 1], {:over, ["date[year]", "date[quarter]"]}}, # Previous quarter
    {:percent_of_total, {:func, "sum", ["sale_amount"]}, [:customer, :segment]}  # Percentage by segment
  ])
  |> Selecto.filter([
    # Time-based filtering
    {"date[year]", {:in, [2023, 2024]}},
    {"date[quarter]", {:between, 1, 3}},
    {"date[is_holiday]", false},
    
    # Dimension filtering
    {"customer[segment]", {:in, ["Premium", "Enterprise"]}},
    {"customer[credit_rating]", {:in, ["A", "B"]}},
    {"product[category]", {:not_in, ["Discontinued", "Clearance"]}},
    {"store[store_type]", {:not_eq, "Outlet"}},
    
    # Measure filtering (HAVING clause)
    {{:func, "sum", ["sale_amount"]}, {:gt, 10000}},
    {{:func, "count", ["*"]}, {:gte, 5}}
  ])
  |> Selecto.group_by([
    "date[year]", "date[quarter_name]", "date[month_name]",
    "customer[segment]", "customer[region]", 
    "product[category]", "product[brand]",
    "store[region]"
  ])
  |> Selecto.order_by([
    "date[year]", "date[quarter]",
    {:desc, {:func, "sum", ["sale_amount"]}}
  ])
  |> Selecto.execute()
```

### Time Intelligence Functions

```elixir
# Time-based analytical queries
def time_intelligence_queries(olap_selecto) do
  # Quarter-over-quarter growth analysis
  quarterly_growth = olap_selecto
    |> Selecto.select([
      "date[year]",
      "date[quarter_name]",
      "customer[segment]",
      {:func, "sum", ["sale_amount"]},
      
      # Previous quarter comparison
      {:window, "lag", [{:func, "sum", ["sale_amount"]}, 1], 
        {:over, ["customer[segment]"], [{:order_by, ["date[year]", "date[quarter]"]}]}},
      
      # Growth calculation
      {:calc, 
        {:subtract, [
          {:func, "sum", ["sale_amount"]},
          {:window, "lag", [{:func, "sum", ["sale_amount"]}, 1], 
            {:over, ["customer[segment]"], [{:order_by, ["date[year]", "date[quarter]"]}]}}
        ]},
        :divide,
        {:window, "lag", [{:func, "sum", ["sale_amount"]}, 1], 
          {:over, ["customer[segment]"], [{:order_by, ["date[year]", "date[quarter]"]}]}}
      },
      
      # Year-over-year comparison
      {:window, "lag", [{:func, "sum", ["sale_amount"]}, 4], 
        {:over, ["customer[segment]"], [{:order_by, ["date[year]", "date[quarter]"]}]}},
      
      # Moving averages
      {:window, "avg", [{:func, "sum", ["sale_amount"]}], 
        {:over, ["customer[segment]"], 
         [{:order_by, ["date[year]", "date[quarter]"]}, 
          {:rows, {:between, 2, :preceding, :current_row}}]}}  # 3-quarter moving average
    ])
    |> Selecto.filter([
      {"date[year]", {:between, 2022, 2024}}
    ])
    |> Selecto.group_by([
      "date[year]", "date[quarter_name]", "customer[segment]"
    ])
    |> Selecto.order_by([
      "customer[segment]", "date[year]", "date[quarter]"
    ])
    |> Selecto.execute()
  
  # Seasonal analysis
  seasonal_patterns = olap_selecto
    |> Selecto.select([
      "date[month_name]",
      "date[day_name]", 
      "product[category]",
      {:func, "avg", ["sale_amount"]},  # Average for this time period
      {:func, "sum", ["quantity"]},
      
      # Seasonal index (current vs yearly average)
      {:calc,
        {:func, "avg", ["sale_amount"]},
        :divide,
        {:window, "avg", [{:func, "avg", ["sale_amount"]}], 
          {:over, ["product[category]"], []}}
      }
    ])
    |> Selecto.filter([
      {"date[year]", 2024},
      {"date[is_weekend]", false}
    ])
    |> Selecto.group_by([
      "date[month_name]", "date[day_name]", "product[category]"
    ])
    |> Selecto.execute()
  
  {quarterly_growth, seasonal_patterns}
end
```

## Snowflake Schema Patterns

Snowflake schemas normalize dimension data into related tables, providing more structured but complex relationships.

### Snowflake Dimension Setup

```elixir
defmodule SnowflakeSchemaExample do
  def normalized_sales_domain do
    %{
      name: "Normalized Sales Analytics",
      source: %{
        source_table: "sales_facts",
        # ... fact table definition ...
        associations: %{
          customer: %{queryable: :customers, field: :customer, owner_key: :customer_id, related_key: :id},
          product: %{queryable: :products, field: :product, owner_key: :product_id, related_key: :id}
        }
      },
      schemas: %{
        customers: %{
          name: "Customer",
          source_table: "customers",
          fields: [:id, :name, :segment, :region_id, :customer_type_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            segment: %{type: :string},
            region_id: %{type: :integer},
            customer_type_id: %{type: :integer}
          },
          associations: %{
            region: %{queryable: :regions, field: :region, owner_key: :region_id, related_key: :id},
            customer_type: %{queryable: :customer_types, field: :customer_type, owner_key: :customer_type_id, related_key: :id}
          }
        },
        regions: %{
          name: "Region",
          source_table: "regions", 
          fields: [:id, :name, :country_id, :sales_manager_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            country_id: %{type: :integer},
            sales_manager_id: %{type: :integer}
          },
          associations: %{
            country: %{queryable: :countries, field: :country, owner_key: :country_id, related_key: :id},
            sales_manager: %{queryable: :employees, field: :sales_manager, owner_key: :sales_manager_id, related_key: :id}
          }
        },
        countries: %{
          name: "Country",
          source_table: "countries",
          fields: [:id, :name, :continent, :currency],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            continent: %{type: :string},
            currency: %{type: :string}
          }
        },
        customer_types: %{
          name: "Customer Type",
          source_table: "customer_types",
          fields: [:id, :name, :description, :discount_rate],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            description: %{type: :string},
            discount_rate: %{type: :decimal, precision: 5, scale: 2}
          }
        },
        products: %{
          name: "Product",
          source_table: "products",
          fields: [:id, :name, :category_id, :brand_id, :supplier_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            category_id: %{type: :integer},
            brand_id: %{type: :integer}, 
            supplier_id: %{type: :integer}
          },
          associations: %{
            category: %{queryable: :categories, field: :category, owner_key: :category_id, related_key: :id},
            brand: %{queryable: :brands, field: :brand, owner_key: :brand_id, related_key: :id},
            supplier: %{queryable: :suppliers, field: :supplier, owner_key: :supplier_id, related_key: :id}
          }
        },
        categories: %{
          name: "Category", 
          source_table: "categories",
          fields: [:id, :name, :parent_id, :level],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            parent_id: %{type: :integer},
            level: %{type: :integer}
          }
        }
      },
      joins: %{
        # Snowflake pattern: nested star dimensions
        customer: %{
          type: :star_dimension,
          display_field: :name,
          joins: %{
            region: %{
              type: :star_dimension,
              display_field: :name,
              joins: %{
                country: %{type: :star_dimension, display_field: :name},
                sales_manager: %{type: :left, display_field: :name}
              }
            },
            customer_type: %{type: :star_dimension, display_field: :name}
          }
        },
        product: %{
          type: :star_dimension,
          display_field: :name,
          joins: %{
            category: %{
              type: :hierarchical,
              hierarchy_type: :adjacency_list,
              depth_limit: 5
            },
            brand: %{type: :star_dimension, display_field: :name},
            supplier: %{type: :star_dimension, display_field: :name}
          }
        }
      }
    }
  end
end

# Query snowflake schema
snowflake_selecto = Selecto.configure(SnowflakeSchemaExample.normalized_sales_domain(), conn)

geographic_analysis = snowflake_selecto
  |> Selecto.select([
    # Deep snowflake navigation
    "customer[region][country_display]",        # Country name
    "customer[region][country][continent]",     # Continent
    "customer[region][country][currency]",      # Currency
    "customer[region][sales_manager_display]",  # Regional manager
    "customer[customer_type_display]",          # Customer type
    "customer[customer_type][discount_rate]",   # Type-specific discount
    
    # Product snowflake navigation
    "product[category_path]",                   # Hierarchical category path
    "product[category_level]",                  # Category depth
    "product[brand_display]",                   # Brand name
    "product[supplier_display]",                # Supplier name
    
    # Aggregated measures
    {:func, "sum", ["sale_amount"]},
    {:func, "count", ["DISTINCT", "customer_id"]},
    {:func, "avg", ["sale_amount"]}
  ])
  |> Selecto.filter([
    {"customer[region][country][continent]", {:in, ["North America", "Europe"]}},
    {"customer[customer_type][discount_rate]", {:gte, 0.05}},
    {"product[category_level]", {:lte, 3}},
    {"product[brand][name]", {:not_null}}
  ])
  |> Selecto.group_by([
    "customer[region][country_display]",
    "customer[customer_type_display]",
    "product[category_path]", 
    "product[brand_display]"
  ])
  |> Selecto.execute()
```

## Hierarchical Data Patterns

### Adjacency List with Enhanced Features

```elixir
defmodule AdvancedHierarchicalPatterns do
  def org_hierarchy_domain do
    %{
      name: "Advanced Organizational Hierarchy",
      source: %{
        source_table: "employees",
        primary_key: :id,
        fields: [:id, :name, :position, :department_id, :manager_id, :salary, :hire_date, :active],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          position: %{type: :string},
          department_id: %{type: :integer},
          manager_id: %{type: :integer},
          salary: %{type: :decimal, precision: 10, scale: 2},
          hire_date: %{type: :date},
          active: %{type: :boolean}
        },
        associations: %{
          manager: %{queryable: :employees, field: :manager, owner_key: :manager_id, related_key: :id},
          department: %{queryable: :departments, field: :department, owner_key: :department_id, related_key: :id},
          subordinates: %{queryable: :employees, field: :subordinates, owner_key: :id, related_key: :manager_id}
        }
      },
      schemas: %{
        departments: %{
          name: "Department",
          source_table: "departments", 
          fields: [:id, :name, :parent_department_id, :budget, :head_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            parent_department_id: %{type: :integer},
            budget: %{type: :decimal, precision: 12, scale: 2},
            head_id: %{type: :integer}
          },
          associations: %{
            parent_department: %{queryable: :departments, field: :parent_department, owner_key: :parent_department_id, related_key: :id},
            head: %{queryable: :employees, field: :head, owner_key: :head_id, related_key: :id}
          }
        }
      },
      joins: %{
        # Management hierarchy with enhanced features
        manager: %{
          type: :hierarchical,
          hierarchy_type: :adjacency_list,
          depth_limit: 8,
          path_separator: " → ",
          include_self: true,              # Include self in hierarchy calculations
          calculate_metrics: true,         # Enable automatic metric calculations
          subordinate_aggregation: true    # Aggregate subordinate data
        },
        # Department hierarchy
        department: %{
          type: :hierarchical,
          hierarchy_type: :adjacency_list,
          depth_limit: 5,
          joins: %{
            parent_department: %{type: :left},
            head: %{type: :left, display_field: :name}
          }
        }
      }
    }
  end
end

# Enhanced hierarchical queries
org_selecto = Selecto.configure(AdvancedHierarchicalPatterns.org_hierarchy_domain(), conn)

# Management hierarchy analysis with subordinate aggregation
management_metrics = org_selecto
  |> Selecto.select([
    "name",
    "position",
    
    # Hierarchy path information
    "manager_path",                    # Full path to CEO
    "manager_level",                   # Management level (0 = CEO)
    "manager_path_array",              # Array of manager IDs
    
    # Subordinate aggregations (calculated via CTE)
    "subordinates_direct_count",       # Direct reports
    "subordinates_total_count",        # All subordinates (all levels)
    "subordinates_avg_salary",         # Average subordinate salary
    "subordinates_total_salary",       # Total subordinate payroll
    "subordinates_max_level",          # Deepest subordinate level
    
    # Department hierarchy
    "department[name]",
    "department_path",                 # Department hierarchy path
    "department_level",                # Department depth
    "department[budget]",
    "department[head_display]",        # Department head name
    
    # Individual metrics
    "salary",
    "hire_date",
    
    # Calculated fields
    {:calc, "subordinates_total_salary", :add, "salary"},  # Total responsibility
    {:calc, "salary", :divide, "subordinates_avg_salary"} # Salary ratio
  ])
  |> Selecto.filter([
    {"active", true},
    {"manager_level", {:between, 1, 5}},       # Skip CEO, limit depth
    {"subordinates_direct_count", {:gte, 1}},  # Only managers
    {"department_level", {:lte, 3}}            # Top department levels
  ])
  |> Selecto.order_by([
    "department_level",
    "manager_level",
    {:desc, "subordinates_total_count"}
  ])
  |> Selecto.execute()
```

### Materialized Path with Advanced Navigation

```elixir
def content_hierarchy_domain do
  %{
    name: "Content Category Hierarchy",
    source: %{
      source_table: "articles",
      # ... article fields ...
      associations: %{
        category: %{queryable: :categories, field: :category, owner_key: :category_id, related_key: :id}
      }
    },
    schemas: %{
      categories: %{
        name: "Category",
        source_table: "categories",
        fields: [:id, :name, :path, :level, :parent_id, :sort_order, :active],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          path: %{type: :string},           # /tech/web-dev/frontend
          level: %{type: :integer},         # Calculated depth
          parent_id: %{type: :integer},
          sort_order: %{type: :integer},    # Manual ordering
          active: %{type: :boolean}
        }
      }
    },
    joins: %{
      category: %{
        type: :hierarchical,
        hierarchy_type: :materialized_path,
        path_field: :path,
        path_separator: "/",
        depth_limit: 10,
        enable_navigation: true,          # Enable ancestor/descendant queries
        enable_siblings: true,            # Enable sibling queries
        sort_field: :sort_order          # Custom sort order
      }
    }
  }
end

# Advanced path-based queries
content_selecto = Selecto.configure(content_hierarchy_domain(), conn)

category_navigation = content_selecto
  |> Selecto.select([
    "title",
    
    # Category hierarchy information
    "category[name]",
    "category_path",                    # Full category path
    "category_level",                   # Depth in hierarchy
    "category_ancestors",               # Array of ancestor names
    "category_path_array",              # Array of ancestor IDs
    
    # Navigation helpers
    "category_parent_name",             # Direct parent name
    "category_root_name",               # Root category name
    "category_siblings_count",          # Number of sibling categories
    "category_children_count",          # Number of child categories
    
    # Aggregations
    {:func, "count", ["*"]}
  ])
  |> Selecto.filter([
    # Path-based filtering
    {"category_path", {:like, "/technology/%"}},        # Technology branch
    {"category_level", {:between, 2, 4}},               # Specific depth range
    {"category_ancestors", {:contains, "programming"}},  # Has programming ancestor
    
    # Sibling and relationship filtering  
    {"category_siblings_count", {:gte, 2}},             # Has siblings
    {"category[active]", true}
  ])
  |> Selecto.group_by([
    "category[name]", "category_path", "category_level"
  ])
  |> Selecto.order_by([
    "category_level",
    "category[sort_order]",
    "category[name]"
  ])
  |> Selecto.execute()
```

## Slowly Changing Dimensions

Handle dimension data that changes over time, maintaining historical accuracy.

### Type 2 SCD (Historical Tracking)

```elixir
def scd_customer_domain do
  %{
    name: "Customer SCD Type 2",
    source: %{
      # ... fact table ...
      associations: %{
        customer: %{queryable: :customer_scd, field: :customer, owner_key: :customer_key, related_key: :surrogate_key}
      }
    },
    schemas: %{
      customer_scd: %{
        name: "Customer SCD",
        source_table: "customer_dimension_scd",
        fields: [
          :surrogate_key, :natural_key, :name, :segment, :region,
          :effective_date, :expiration_date, :current_flag, :version
        ],
        columns: %{
          surrogate_key: %{type: :integer},    # Unique dimension key
          natural_key: %{type: :integer},      # Business key (customer ID)
          name: %{type: :string},
          segment: %{type: :string},
          region: %{type: :string},
          effective_date: %{type: :date},      # When this version became active
          expiration_date: %{type: :date},     # When this version expired
          current_flag: %{type: :boolean},     # Is this the current version?
          version: %{type: :integer}           # Version number
        }
      }
    },
    joins: %{
      customer: %{
        type: :star_dimension,
        display_field: :name,
        scd_type: 2,                         # Enable SCD Type 2 features
        effective_date_field: :effective_date,
        expiration_date_field: :expiration_date,
        current_flag_field: :current_flag,
        natural_key_field: :natural_key
      }
    }
  }
end

# SCD-aware queries
scd_selecto = Selecto.configure(scd_customer_domain(), conn)

# Point-in-time analysis
historical_analysis = scd_selecto
  |> Selecto.select([
    "customer[natural_key]",            # Business key
    "customer[name]", 
    "customer[segment]",
    "customer[version]",                # SCD version
    "customer[effective_date]",
    "customer[expiration_date]",
    {:func, "sum", ["sale_amount"]},
    {:func, "count", ["*"]}
  ])
  |> Selecto.filter([
    # Point-in-time filtering (automatically handled by SCD join)
    {"fact_date", {:between, ~D[2024-01-01], ~D[2024-12-31]}},
    
    # SCD-specific filters
    {"customer[current_flag]", true},           # Current version only
    # OR: {"customer[point_in_time]", ~D[2024-06-01]}, # Specific point in time
    
    {"customer[segment]", "Premium"}
  ])
  |> Selecto.group_by([
    "customer[natural_key]", "customer[name]", 
    "customer[segment]", "customer[version]"
  ])
  |> Selecto.execute()
```

## Performance Optimization for OLAP

### Aggregation Strategies

```elixir
defmodule OLAPOptimization do
  # Pre-aggregated cube queries
  def build_aggregated_cube(selecto) do
    # Create summary cube for common queries
    monthly_summary = selecto
      |> Selecto.select([
        "date[year]",
        "date[month]",
        "customer[segment]",
        "product[category]",
        {:func, "sum", ["sale_amount"]},
        {:func, "sum", ["quantity"]},
        {:func, "count", ["*"]},
        {:func, "count", ["DISTINCT", "customer_id"]}
      ])
      |> Selecto.filter([
        {"date[year]", {:gte, 2022}}
      ])
      |> Selecto.group_by([
        "date[year]", "date[month]",
        "customer[segment]", "product[category]"
      ])
  end
  
  # Optimized dimension filtering
  def optimized_dimension_query(selecto) do
    selecto
    # 1. Most selective dimension filters first
    |> Selecto.filter([
      {"date[year]", 2024},                    # Highly selective time filter
      {"customer[segment]", "Premium"},         # Selective business filter
      {"product[category]", "Electronics"}     # Category filter
    ])
    # 2. Select only required dimensions
    |> Selecto.select([
      "customer[region]",
      "product[brand]",
      {:func, "sum", ["sale_amount"]}
    ])
    # 3. Group by dimensions only (not measures)
    |> Selecto.group_by(["customer[region]", "product[brand]"])
    # 4. Efficient ordering
    |> Selecto.order_by([{:desc, {:func, "sum", ["sale_amount"]}}])
    |> Selecto.limit(100)  # Top N for dashboard display
  end
  
  # Efficient hierarchy traversal
  def optimized_hierarchy_query(selecto) do
    selecto
    # Limit hierarchy depth early
    |> Selecto.filter([
      {"category_level", {:lte, 4}},          # Prevent deep recursion
      {"category[active]", true},             # Index-friendly filter
      {"category_path", {:like, "/tech/%"}}   # Path prefix for efficiency
    ])
    |> Selecto.select([
      "category[name]",
      "category_level",
      "category_path"
    ])
    |> Selecto.order_by(["category_level", "category[name]"])
  end
end
```

### Database Index Recommendations

```sql
-- OLAP-optimized indexes

-- Star schema fact table indexes
CREATE INDEX idx_sales_facts_time_customer ON sales_facts(date_key, customer_key) 
  INCLUDE (sale_amount, quantity);
CREATE INDEX idx_sales_facts_product_store ON sales_facts(product_key, store_key)
  INCLUDE (sale_amount, quantity);

-- Dimension table indexes  
CREATE INDEX idx_customer_dim_segment_region ON customer_dimension(segment, region);
CREATE INDEX idx_product_dim_category_brand ON product_dimension(category, brand);
CREATE INDEX idx_date_dim_year_quarter ON date_dimension(year, quarter);

-- Hierarchy-specific indexes
CREATE INDEX idx_employees_manager_level ON employees(manager_id, level) WHERE active = true;
CREATE INDEX idx_categories_path_gin ON categories USING gin(path gin_trgm_ops);  -- For path searches
CREATE INDEX idx_categories_path_btree ON categories(path) WHERE active = true;

-- SCD indexes
CREATE INDEX idx_customer_scd_natural_current ON customer_dimension_scd(natural_key, current_flag)
  WHERE current_flag = true;
CREATE INDEX idx_customer_scd_effective ON customer_dimension_scd(effective_date, expiration_date);
```

This comprehensive guide provides the foundation for implementing sophisticated OLAP and hierarchical patterns using Selecto, enabling powerful analytical capabilities for business intelligence applications.