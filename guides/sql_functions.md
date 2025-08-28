# SQL Functions Guide

Selecto provides comprehensive support for SQL functions across multiple categories. This includes both standard functions that work with most SQL databases and advanced functions with specialized capabilities.

## Function Categories

### String Functions

String manipulation and processing functions:

```elixir
# Substring extraction
{:substr, "description", 1, 50}       # Extract first 50 characters
{:substr, "name", 5}                  # Extract from position 5 onwards

# String cleaning
{:trim, "name"}                       # Remove leading/trailing whitespace  
{:ltrim, "name"}                      # Remove leading whitespace
{:rtrim, "name"}                      # Remove trailing whitespace

# Case conversion
{:upper, "category"}                  # Convert to uppercase
{:lower, "category"}                  # Convert to lowercase

# String analysis
{:length, "name"}                     # Get string length
{:position, "substring", "full_string"} # Find position of substring

# String manipulation
{:replace, "description", "old", "new"}  # Replace text
{:split_part, "email", "@", 1}          # Split and extract part
```

### Mathematical Functions

Numeric calculations and transformations:

```elixir
# Basic math
{:abs, "balance"}                     # Absolute value
{:ceil, "price"}                      # Round up
{:floor, "price"}                     # Round down
{:round, "price"}                     # Round to nearest integer
{:round, "price", 2}                  # Round to 2 decimal places

# Advanced math
{:power, "base", 2}                   # Exponentiation
{:sqrt, "area"}                       # Square root
{:mod, "id", 10}                      # Modulo operation
{:random}                             # Random number generation
```

### Date/Time Functions

Temporal data processing:

```elixir
# Current time
{:now}                                # Current timestamp

# Date manipulation
{:date_trunc, "month", "created_at"}  # Truncate to month
{:age, "created_at"}                  # Age from current time
{:age, "end_date", "start_date"}      # Age between dates
{:date_part, "year", "created_at"}    # Extract date part

# Intervals
{:interval, "1 day"}                  # Time interval from string
{:interval, {7, "days"}}              # Time interval from tuple
```

### Array Functions

Array manipulation and aggregation:

```elixir
# Array aggregation
{:array_agg, "category"}              # Aggregate values into array

# Array analysis
{:array_length, "tags"}               # Get array length
{:array_to_string, "tags", ", "}      # Convert array to string
{:string_to_array, "csv_data", ","}   # Convert string to array

# Array operations
{:unnest, "tags"}                     # Expand array to rows
{:array_cat, "array1", "array2"}      # Concatenate arrays
```

### Window Functions

Advanced analytical functions with partitioning and ordering:

```elixir
# Row numbering
{:window, {:row_number}, over: [
  partition_by: ["category"], 
  order_by: ["price"]
]}

# Ranking
{:window, {:rank}, over: [order_by: ["score"]]}
{:window, {:dense_rank}, over: [order_by: ["score"]]}

# Value access
{:window, {:lag, "price"}, over: [
  partition_by: ["product_id"],
  order_by: ["date"]
]}
{:window, {:lead, "price", 2}, over: [order_by: ["date"]]}

# Percentiles
{:window, {:ntile, 4}, over: [order_by: ["revenue"]]}

# Aggregates with window
{:window, {:sum, "amount"}, over: [
  partition_by: ["customer_id"],
  order_by: ["date"]
]}
```

### Conditional Functions

Logic and branching:

```elixir
# Simple if-then-else
{:iif, 
  {"price", :gt, {:literal, 100}}, 
  {:literal, "expensive"}, 
  {:literal, "affordable"}
}

# Oracle-style decode (multiple conditions)
{:decode, "status", [
  {"active", "Currently Active"},
  {"inactive", "Not Active"},
  {"pending", "Awaiting Approval"}
]}
```

## Advanced Usage Patterns

### Combining Functions

Functions can be nested and combined:

```elixir
# Nested functions
{:upper, {:substr, "name", 1, 1}}     # First letter uppercase

# Functions in calculations
{:round, {:power, "radius", 2}, 2}    # Area calculation with rounding
```

### Window Functions with Complex Partitioning

```elixir
# Running totals
selecto = Selecto.configure(domain, connection)
|> Selecto.select([
  "date",
  "amount", 
  {:window, {:sum, "amount"}, over: [
    partition_by: ["account_id"],
    order_by: ["date"]
  ], alias: "running_total"}
])

# Ranking within groups
selecto = Selecto.configure(domain, connection)
|> Selecto.select([
  "product_name",
  "category",
  "sales",
  {:window, {:row_number}, over: [
    partition_by: ["category"],
    order_by: [{"sales", :desc}]
  ], alias: "rank_in_category"}
])
```

### Custom SQL with Function Integration

For specialized needs, combine with custom SQL:

```elixir
# Custom SQL with function parameters
{:custom_sql, 
  "CASE WHEN ? > 1000 THEN 'high' ELSE 'normal' END",
  [{"revenue_amount", :sum}]
}
```

## Performance Considerations

### Function Efficiency

- **String functions**: Generally fast, but consider indexing for LIKE operations
- **Math functions**: Very efficient, can often be computed in-place
- **Date functions**: Moderate cost, consider date indexing for frequent operations
- **Array functions**: Can be expensive with large arrays
- **Window functions**: Powerful but can be costly; use appropriate partitioning

### Window Function Optimization

```elixir
# Good: Efficient partitioning
{:window, {:row_number}, over: [
  partition_by: ["customer_id", "region"],  # Reduces partition size
  order_by: ["date"]
]}

# Avoid: Very large partitions
{:window, {:row_number}, over: [
  partition_by: [],                         # Single massive partition
  order_by: ["date"]
]}
```

### Index Recommendations

For frequently used function patterns:

```sql
-- For date truncation queries
CREATE INDEX idx_orders_month ON orders (date_trunc('month', created_at));

-- For string pattern matching
CREATE INDEX idx_names_upper ON customers (upper(name));

-- For array operations
CREATE INDEX idx_tags_gin ON posts USING GIN (tags);
```

## Error Handling

Functions include built-in error handling:

```elixir
# Safe division with fallback
{:coalesce, [
  {:nullif, {"revenue", :div, "quantity"}, 0},
  {:literal, 0}
]}

# Safe string operations
{:coalesce, [
  {:trim, "user_input"},
  {:literal, "default_value"}
]}
```

## Integration Examples

### E-commerce Analytics

```elixir
# Product performance analysis
selecto = Selecto.configure(ecommerce_domain, connection)
|> Selecto.select([
  "product_name",
  {:upper, "category"},
  {:round, "avg_price", 2},
  {:window, {:rank}, over: [
    partition_by: ["category"],
    order_by: [{"total_sales", :desc}]
  ], alias: "category_rank"},
  {:array_to_string, "tags", ", "}
])
|> Selecto.filter([
  {"created_at", :gte, {:date_trunc, "month", {:now}}}
])
```

### Financial Reporting  

```elixir
# Monthly revenue trends with calculations
selecto = Selecto.configure(finance_domain, connection)
|> Selecto.select([
  {:date_trunc, "month", "transaction_date", alias: "month"},
  {:sum, "amount", alias: "total_revenue"},
  {:window, {:lag, {:sum, "amount"}}, over: [
    order_by: [{:date_trunc, "month", "transaction_date"}]
  ], alias: "prev_month_revenue"},
  {:round, {:mul, 
    {:div, {:sub, {:sum, "amount"}, {:lag, {:sum, "amount"}}}, {:lag, {:sum, "amount"}}},
    100
  }, 2, alias: "growth_percentage"}
])
|> Selecto.group_by([{:date_trunc, "month", "transaction_date"}])
|> Selecto.order_by([{:date_trunc, "month", "transaction_date"}])
```

This comprehensive function support enables sophisticated data analysis and reporting directly in your Selecto queries, reducing the need for post-processing in application code.