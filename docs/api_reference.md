# Selecto API Reference

Complete API documentation for Selecto's advanced query builder with comprehensive join support.

## Core Functions

### `Selecto.configure/2`

Configures a Selecto instance with domain configuration and database connection.

```elixir
configure(domain, connection) :: %Selecto{}
```

**Parameters:**
- `domain` - Domain configuration map containing source, schemas, and join definitions
- `connection` - Postgrex connection for database operations

**Returns:** Configured Selecto struct ready for query building

**Example:**
```elixir
selecto = Selecto.configure(domain_config, postgrex_conn)
```

### `Selecto.select/2`

Specifies fields to select in the query with automatic join resolution.

```elixir
select(selecto, fields) :: %Selecto{}
```

**Parameters:**
- `selecto` - Selecto struct
- `fields` - List of field specifications

**Field Types:**
- `"field_name"` - Simple field selection
- `"join[field]"` - Field from joined table
- `"join[nested][field]"` - Field from nested joins
- `{:func, "function_name", [args]}` - SQL function call
- `{:literal, value}` - Literal value
- `{:custom_sql, sql, field_map}` - Custom SQL with field validation
- `{:case, name, conditions}` - CASE expression
- `{:array, name, [fields]}` - Array aggregation

**Example:**
```elixir
selecto
|> Selecto.select([
  "id", 
  "customer[name]",
  {:func, "count", ["*"]},
  {:case, "status", %{"active" => [{:literal, true}], "else" => [{:literal, false}]}}
])
```

### `Selecto.filter/2`

Applies WHERE conditions with automatic join dependency resolution.

```elixir
filter(selecto, conditions) :: %Selecto{}
```

**Parameters:**
- `selecto` - Selecto struct
- `conditions` - List of filter conditions

**Condition Types:**
- `{field, value}` - Equality comparison
- `{field, {:gt, value}}` - Greater than
- `{field, {:gte, value}}` - Greater than or equal
- `{field, {:lt, value}}` - Less than
- `{field, {:lte, value}}` - Less than or equal
- `{field, {:like, pattern}}` - LIKE pattern matching
- `{field, {:in, [values]}}` - IN list
- `{field, {:between, min, max}}` - BETWEEN range
- `{field, {:not_null}}` - NOT NULL check
- `{field, {:null}}` - NULL check
- `{:and, [conditions]}` - AND logical operator
- `{:or, [conditions]}` - OR logical operator
- `{field, {:subquery, :in, sql, params}}` - Subquery
- `{field, {:text_search, query}}` - Full text search (PostgreSQL)

**Example:**
```elixir
selecto
|> Selecto.filter([
  {"active", true},
  {:and, [
    {"total", {:gt, 100}},
    {:or, [
      {"customer[region]", "West"},
      {"customer[type]", "premium"}
    ]}
  ]},
  {"created_at", {:between, ~D[2024-01-01], ~D[2024-12-31]}}
])
```

### `Selecto.group_by/2`

Groups results by specified fields.

```elixir
group_by(selecto, fields) :: %Selecto{}
```

**Parameters:**
- `selecto` - Selecto struct
- `fields` - List of field names for grouping

**Example:**
```elixir
selecto
|> Selecto.group_by(["customer[region]", "product[category]"])
```

### `Selecto.order_by/2`

Orders results by specified fields with direction.

```elixir
order_by(selecto, fields) :: %Selecto{}
```

**Parameters:**
- `selecto` - Selecto struct  
- `fields` - List of field specifications with optional direction

**Field Types:**
- `"field_name"` - Ascending order (default)
- `{:desc, "field_name"}` - Descending order
- `{:asc, "field_name"}` - Explicit ascending order
- `{:desc, {:func, "sum", ["total"]}}` - Order by function result

**Example:**
```elixir
selecto
|> Selecto.order_by([
  "customer[name]",
  {:desc, "created_at"},
  {:desc, {:func, "sum", ["total"]}}
])
```

### `Selecto.limit/2` and `Selecto.offset/2`

Pagination controls for result sets.

```elixir
limit(selecto, count) :: %Selecto{}
offset(selecto, count) :: %Selecto{}
```

**Example:**
```elixir
selecto
|> Selecto.limit(50)
|> Selecto.offset(100)  # Skip first 100, return next 50
```

### `Selecto.execute/1`

Executes the query and returns results.

```elixir
execute(selecto) :: {:ok, [row]} | {:error, term}
```

**Returns:** Query results as list of tuples/maps depending on configuration

## Join Configuration API

### Standard Join Types

#### Left Join
```elixir
joins: %{
  association_name: %{type: :left}
}
```

#### Inner Join  
```elixir
joins: %{
  association_name: %{type: :inner}
}
```

#### Right Join
```elixir
joins: %{
  association_name: %{type: :right}
}
```

### Advanced Join Types

#### Star Dimension (OLAP)
For business intelligence and analytics queries with optimized aggregation.

```elixir
joins: %{
  dimension_name: %{
    type: :star_dimension,
    display_field: :name,           # Primary display field
    aggregation_friendly: true     # Optimize for GROUP BY
  }
}
```

**Generated Fields:**
- `dimension_name_display` - Display field value
- `dimension_name_id` - Foreign key value  
- Additional fields from schema

#### Snowflake Dimension (Normalized OLAP)
For normalized dimension tables requiring additional joins.

```elixir
joins: %{
  dimension_name: %{
    type: :snowflake_dimension,
    display_field: :name,
    normalization_joins: [
      %{table: "countries", alias: "co", join_field: :country_id},
      %{table: "regions", alias: "re", join_field: :region_id}
    ]
  }
}
```

#### Hierarchical Joins
For tree-structured data with multiple implementation patterns.

##### Adjacency List Pattern
```elixir
joins: %{
  parent_category: %{
    type: :hierarchical,
    hierarchy_type: :adjacency_list,
    depth_limit: 5,                 # Maximum recursion depth
    parent_field: :parent_id        # Self-reference field
  }
}
```

**Generated Fields:**
- `category_path` - Full path from root as string
- `category_level` - Depth in hierarchy (0 = root)
- `category_path_array` - Path as PostgreSQL array

##### Materialized Path Pattern
```elixir
joins: %{
  parent_category: %{
    type: :hierarchical,
    hierarchy_type: :materialized_path,
    path_field: :path,              # Field containing path
    path_separator: "/",            # Path separator character
    depth_limit: 10
  }
}
```

**Generated Fields:**
- `category_ancestors` - Array of ancestor IDs
- `category_level` - Calculated depth from path
- `category_path_components` - Split path components

##### Closure Table Pattern
```elixir
joins: %{
  parent_category: %{
    type: :hierarchical,
    hierarchy_type: :closure_table,
    closure_table: "category_closure",
    ancestor_field: :ancestor_id,
    descendant_field: :descendant_id,
    depth_field: :depth
  }
}
```

#### Many-to-Many Tagging
For flexible tagging and categorization with aggregation support.

```elixir
joins: %{
  tags: %{
    type: :tagging,
    tag_field: :name,               # Field containing tag value
    junction_table: "post_tags",    # Optional: explicit junction table
    aggregation_separator: ", "     # Separator for string aggregation
  }
}
```

**Generated Fields:**
- `tags_list` - Comma-separated string of all tags
- `tags_array` - PostgreSQL array of tag values
- `tags_count` - Count of associated tags
- `tags_filter` - Special filter field for faceted search

**Special Filtering:**
```elixir
# Filter by any tag
{"tags[name]", "programming"}

# Filter by tag aggregation
{"tags_filter", "elixir"}  # Has "elixir" tag

# Filter by tag count
{"tags_count", {:gte, 3}}  # Has 3+ tags
```

## Common Table Expression (CTE) API

### `Selecto.Builder.Cte`

Module for building CTEs from Selecto queries.

#### `build_cte_from_selecto/2`
Creates a simple CTE from a Selecto struct.

```elixir
build_cte_from_selecto(name, selecto) :: {iodata, [params]}
```

**Example:**
```elixir
alias Selecto.Builder.Cte

active_users = selecto
  |> Selecto.select(["id", "name", "email"])
  |> Selecto.filter([{"active", true}])

{cte_iodata, params} = Cte.build_cte_from_selecto("active_users", active_users)
```

#### `build_recursive_cte_from_selecto/3`
Creates a recursive CTE for hierarchical queries.

```elixir
build_recursive_cte_from_selecto(name, base_case, recursive_case) :: {iodata, [params]}
```

**Example:**
```elixir
# Base case: root categories
base_case = selecto
  |> Selecto.select(["id", "name", "parent_id", {:literal, 0, "level"}])
  |> Selecto.filter([{"parent_id", nil}])

# Recursive case: child categories  
recursive_case = selecto
  |> Selecto.select(["c.id", "c.name", "c.parent_id", "h.level + 1"])
  |> Selecto.from("categories c")
  |> Selecto.join("hierarchy h", "c.parent_id = h.id")
  |> Selecto.filter([{"h.level", {:lt, 5}}])

{recursive_cte, params} = Cte.build_recursive_cte_from_selecto(
  "category_hierarchy",
  base_case,
  recursive_case
)
```

#### `build_cte/3`
Creates CTE from raw SQL with parameters.

```elixir
build_cte(name, sql_iodata, params) :: {iodata, [params]}
```

#### `build_recursive_cte/5`
Creates recursive CTE from raw SQL components.

```elixir
build_recursive_cte(name, base_sql, base_params, recursive_sql, recursive_params) :: {iodata, [params]}
```

#### `integrate_ctes_with_query/3`
Combines multiple CTEs with a main query.

```elixir
integrate_ctes_with_query(cte_list, main_query, main_params) :: {iodata, [params]}
```

**Example:**
```elixir
all_ctes = [
  {user_cte, user_params},
  {post_cte, post_params}
]

main_query = [
  "SELECT u.name, p.title ",
  "FROM active_users u ",
  "JOIN popular_posts p ON u.id = p.author_id"
]

{complete_query, all_params} = Cte.integrate_ctes_with_query(
  all_ctes,
  main_query,
  []
)
```

## Parameter Handling API

### `Selecto.SQL.Params`

Handles safe parameter substitution and SQL finalization.

#### `finalize/1`
Converts parameterized iodata to executable SQL with placeholders.

```elixir
finalize(iodata_with_params) :: {sql_string, [param_values]}
```

**Example:**
```elixir
# iodata with embedded {:param, value} markers
parameterized_query = ["SELECT * FROM users WHERE id = ", {:param, 123}]

{sql, params} = Selecto.SQL.Params.finalize(parameterized_query)
# Result: {"SELECT * FROM users WHERE id = $1", [123]}
```

## Domain Configuration Reference

### Complete Domain Structure
```elixir
domain = %{
  # Required: Domain identification
  name: "Domain Display Name",
  
  # Required: Primary source table configuration
  source: %{
    source_table: "primary_table_name",
    primary_key: :id,
    fields: [:id, :field1, :field2, :field3],
    redact_fields: [:sensitive_field],  # Optional: fields to exclude
    columns: %{
      id: %{type: :integer},
      field1: %{type: :string},
      field2: %{type: :decimal}, 
      field3: %{type: :utc_datetime}
    },
    associations: %{
      related_table: %{
        queryable: :schema_key,         # Key in schemas map
        field: :association_name,       # Field name for selection
        owner_key: :foreign_key_field,  # FK field in source table
        related_key: :id               # PK field in related table
      }
    }
  },
  
  # Required: Related table schema definitions
  schemas: %{
    schema_key: %{
      name: "Human Readable Name",
      source_table: "actual_table_name",
      fields: [:id, :name, :other_field],
      redact_fields: [],              # Optional
      columns: %{
        id: %{type: :integer},
        name: %{type: :string},
        other_field: %{type: :boolean}
      },
      associations: %{
        # Nested associations for chained joins
        nested_relation: %{
          queryable: :another_schema,
          field: :nested_field,
          owner_key: :nested_id,
          related_key: :id
        }
      }
    }
  },
  
  # Required: Join configuration
  joins: %{
    related_table: %{
      type: :left,                    # or :inner, :right, :star_dimension, etc.
      name: "Human Readable Name",    # Optional display name
      # Additional type-specific options...
    }
  },
  
  # Optional: Default query configuration
  default_selected: ["id", "name"],           # Always selected fields
  required_filters: [{"active", true}]       # Always applied filters
}
```

### Column Types
Supported column types for type validation and casting:

- `:integer` - Integer numbers
- `:decimal` - Decimal/numeric values  
- `:float` - Floating point numbers
- `:string` - Text/varchar fields
- `:text` - Long text fields
- `:boolean` - Boolean true/false
- `:date` - Date only
- `:time` - Time only  
- `:utc_datetime` - Timestamp with timezone
- `:naive_datetime` - Timestamp without timezone
- `:binary` - Binary data
- `:array` - PostgreSQL arrays
- `:map` - JSON/JSONB fields

## Error Handling

### Common Errors

#### Configuration Errors
- **Missing required keys**: Domain missing name, source, schemas, or joins
- **Invalid column types**: Unsupported column type specified
- **Missing schema references**: Join references non-existent schema
- **Circular dependencies**: Join configuration creates circular reference

#### Query Building Errors  
- **Invalid field reference**: Field doesn't exist in domain or schema
- **Unsupported join type**: Join type not implemented
- **Parameter type mismatch**: Parameter doesn't match expected column type
- **Depth limit exceeded**: Hierarchical query exceeds configured depth limit

#### Execution Errors
- **SQL syntax error**: Generated SQL is invalid
- **Database connection error**: Connection lost or invalid
- **Parameter binding error**: Parameter count mismatch or type conversion failure

### Error Examples
```elixir
# Configuration validation
case Selecto.configure(domain, conn) do
  {:ok, selecto} -> selecto
  {:error, "Missing required domain keys: [:joins]"} -> handle_config_error()
end

# Query execution  
case Selecto.execute(selecto) do
  {:ok, results} -> process_results(results)
  {:error, %Postgrex.Error{} = error} -> handle_db_error(error)
end
```

## Performance Considerations

### Query Optimization Tips

1. **Filter Early**: Apply most selective filters first
   ```elixir
   selecto
   |> Selecto.filter([
     {"date_range", {:between, start_date, end_date}},  # Selective date filter first
     {"status", "active"},                               # Then status
     {"customer[type]", "premium"}                       # Finally join filters
   ])
   ```

2. **Limit Join Depth**: Avoid excessive nested joins
   ```elixir
   # Good: Limited nesting
   "customer[region][name]"
   
   # Avoid: Deep nesting
   "order[customer][company][region][country][continent][name]"
   ```

3. **Use Appropriate Join Types**: 
   - Use `star_dimension` for analytics/aggregation
   - Use `left` joins when related data may not exist
   - Use `inner` joins for required relationships

4. **Batch Large Queries**: Process large datasets in chunks
   ```elixir
   selecto
   |> Selecto.order_by(["id"])      # Consistent ordering
   |> Selecto.limit(1000)          # Process in batches
   |> Selecto.offset(batch * 1000)
   ```

5. **Index Support**: Ensure database indexes support your query patterns
   - Index foreign key fields used in joins
   - Index commonly filtered fields  
   - Consider composite indexes for multi-field filters

This API reference provides comprehensive documentation for all Selecto features with practical examples and configuration guidance.