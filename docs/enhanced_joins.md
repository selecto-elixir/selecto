# Enhanced Joins in Selecto

This guide covers the enhanced join types and improved field resolution capabilities introduced in Selecto Phase 3.

## Overview

Selecto now supports advanced join types beyond the standard LEFT/INNER joins, along with enhanced field resolution for complex join scenarios with improved disambiguation and error handling.

## New Join Types

### Self-Joins
Join a table to itself with different aliases for comparison or hierarchical relationships.

```elixir
joins: %{
  manager: %{
    type: :self_join,
    self_key: :manager_id,
    target_key: :id,
    alias: "mgr",
    condition_type: :left
  }
}
```

**Use Cases:**
- Employee-manager relationships
- Category hierarchies 
- Product variants comparison
- User referral systems

### Lateral Joins
Correlated subqueries that can reference columns from preceding tables in the FROM clause.

```elixir
joins: %{
  recent_orders: %{
    type: :lateral_join,
    lateral_query: "SELECT * FROM orders o WHERE o.customer_id = customers.id ORDER BY o.created_at DESC LIMIT 5",
    alias: "recent"
  }
}
```

**Use Cases:**
- Top N records per group
- Complex correlated subqueries
- Dynamic filtering based on main table values
- Reporting with ranked results

### Cross Joins
Cartesian product between tables (use with caution for performance).

```elixir
joins: %{
  product_variants: %{
    type: :cross_join,
    source: "product_options",
    alias: "variants"
  }
}
```

**Use Cases:**
- Product configuration matrices
- Calendar/scheduling combinations
- Test data generation
- Mathematical combinations

**⚠️ Performance Warning:** Cross joins can produce very large result sets. Always apply appropriate filters.

### Full Outer Joins
Complete outer join that returns all rows from both tables.

```elixir
joins: %{
  all_transactions: %{
    type: :full_outer_join,
    source: "transactions", 
    left_key: :account_id,
    right_key: :account_id,
    alias: "trans"
  }
}
```

**Use Cases:**
- Data reconciliation
- Comparing datasets for differences
- Audit reports showing all records
- Data migration validation

### Conditional Joins
Dynamic join conditions based on field values or runtime parameters.

```elixir
joins: %{
  applicable_discounts: %{
    type: :conditional_join,
    source: "discounts",
    conditions: [
      {:field_comparison, "orders.total", :gte, "discounts.minimum_amount"},
      {:date_range, "orders.created_at", "discounts.valid_from", "discounts.valid_to"}
    ],
    condition_type: :left
  }
}
```

**Condition Types:**
- `:field_comparison` - Compare fields with operators (`:eq`, `:ne`, `:gt`, `:gte`, `:lt`, `:lte`)
- `:date_range` - Check if a date falls within a range
- `:custom_sql` - Custom SQL condition

**Use Cases:**
- Business rule enforcement
- Dynamic pricing calculations
- Time-based associations
- Complex eligibility checks

## Enhanced Field Resolution

The enhanced field resolution system provides better error handling, disambiguation, and field suggestions.

### Basic Field Resolution

```elixir
# Simple field from source table
{:ok, field_info} = Selecto.resolve_field(selecto, "user_name")

# Qualified field from specific join
{:ok, field_info} = Selecto.resolve_field(selecto, "posts.title")

# Field with custom alias
{:ok, field_info} = Selecto.resolve_field(selecto, {:field, "name", alias: "display_name"})
```

### Disambiguation

When field names are ambiguous across multiple tables:

```elixir
# Explicitly disambiguate
{:ok, field_info} = Selecto.resolve_field(selecto, {:disambiguated_field, "id", from: "users"})

# Or use qualified syntax
{:ok, field_info} = Selecto.resolve_field(selecto, "users.id")
```

### Field Introspection

```elixir
# Get all available fields
all_fields = Selecto.available_fields(selecto)

# Check for ambiguity
is_ambiguous = Selecto.FieldResolver.is_ambiguous_field?(selecto, "name")

# Get suggestions for partial matches
suggestions = Selecto.field_suggestions(selecto, "user")
# Returns: ["user_name", "user_email", "users.id", ...]

# Get disambiguation options
options = Selecto.FieldResolver.get_disambiguation_options(selecto, "id")
```

### Error Handling

Enhanced error messages with context and suggestions:

```elixir
case Selecto.resolve_field(selecto, "invalid_field") do
  {:ok, field_info} -> 
    # Use field_info
  {:error, %Selecto.Error{type: :field_resolution_error} = error} ->
    # error.message contains descriptive error
    # error.details.suggestions contains similar field names
    # error.details.available_fields contains all valid options
end
```

## SQL Generation

Enhanced joins integrate seamlessly with Selecto's SQL generation:

```elixir
selecto = MyDomain.configure()
|> Selecto.select(["name", "manager.name", "recent_orders.total"])
|> Selecto.filter([{"status", "active"}])

{sql, params} = Selecto.to_sql(selecto)
```

Generated SQL will include the appropriate enhanced join syntax:

```sql
SELECT users.name, mgr.name, recent.total
FROM users selecto_root
LEFT JOIN users mgr ON selecto_root.manager_id = mgr.id
LEFT JOIN LATERAL (
  SELECT * FROM orders o 
  WHERE o.customer_id = users.id 
  ORDER BY o.created_at DESC LIMIT 5
) recent ON true
WHERE selecto_root.status = $1
```

## Configuration Reference

### Common Configuration Options

All enhanced join types support these base options:

- `alias`: Custom table alias (string)
- `name`: Display name for the join (optional)
- `filters`: Custom filters specific to this join
- `custom_columns`: Additional computed columns
- `additional_fields`: Extra fields beyond the standard schema

### Self-Join Configuration

- `self_key`: Field in source table that references target (default: `:parent_id`)
- `target_key`: Field in target table being referenced (default: `:id`)
- `condition_type`: Join type (`:inner`, `:left`, `:right`, `:full`)

### Lateral Join Configuration

- `lateral_query`: SQL query string (required)
- `alias`: Table alias for the lateral results

### Cross Join Configuration

- `source`: Target table name
- `alias`: Table alias

### Full Outer Join Configuration

- `left_key`: Field from source table
- `right_key`: Field from target table

### Conditional Join Configuration

- `conditions`: Array of condition tuples (required)
- `condition_type`: Join type (`:inner`, `:left`, `:right`, `:full`)

## Performance Considerations

1. **Cross Joins**: Can produce large result sets. Always apply filters.
2. **Lateral Joins**: May be expensive for large datasets. Consider indexing referenced columns.
3. **Conditional Joins**: Complex conditions may impact query planning. Test with EXPLAIN.
4. **Self-Joins**: Ensure proper indexing on both sides of the relationship.

## Migration Guide

### From Basic Joins

Old configuration:
```elixir
joins: %{
  manager: %{
    # Standard association-based join
  }
}
```

New enhanced configuration:
```elixir
joins: %{
  manager: %{
    type: :self_join,
    self_key: :manager_id,
    target_key: :id,
    alias: "mgr"
  }
}
```

### Field Resolution Updates

Legacy field access:
```elixir
field_info = Selecto.field(selecto, "field_name")
# Returns nil if not found
```

Enhanced field resolution:
```elixir
case Selecto.resolve_field(selecto, "field_name") do
  {:ok, field_info} -> field_info
  {:error, error} -> 
    # Handle with suggestions and context
    suggest_alternatives(error.details.suggestions)
end
```

## Examples

### Employee Hierarchy with Manager Lookup

```elixir
domain = %{
  name: "Employees",
  source: %{
    source_table: "employees",
    fields: [:id, :name, :manager_id, :department],
    # ... other config
  },
  joins: %{
    manager: %{
      type: :self_join,
      self_key: :manager_id,
      target_key: :id,
      alias: "mgr"
    }
  }
}

selecto = Selecto.configure(domain, db_connection)
|> Selecto.select(["name", "department", "manager.name"])
|> Selecto.filter([{"department", "Engineering"}])
```

### Top 5 Orders per Customer

```elixir
joins: %{
  recent_orders: %{
    type: :lateral_join,
    lateral_query: """
      SELECT order_id, total, created_at 
      FROM orders o 
      WHERE o.customer_id = customers.id 
      ORDER BY o.created_at DESC 
      LIMIT 5
    """,
    alias: "recent"
  }
}
```

### Dynamic Discount Application

```elixir
joins: %{
  applicable_discounts: %{
    type: :conditional_join,
    source: "discounts",
    conditions: [
      {:field_comparison, "orders.total", :gte, "discounts.minimum_amount"},
      {:date_range, "orders.created_at", "discounts.valid_from", "discounts.valid_to"},
      {:custom_sql, "discounts.active = true"}
    ],
    condition_type: :left
  }
}
```

## Testing

Enhanced joins include comprehensive test coverage:

- Unit tests for each join type configuration
- SQL generation validation
- Field resolution error handling
- Integration tests with real domain configurations

Run the test suite:
```bash
mix test test/enhanced_joins_test.exs
mix test test/field_resolver_test.exs
```

## Troubleshooting

### Common Issues

1. **"Field not found" errors**: Use `Selecto.available_fields/1` to see all available fields
2. **Ambiguous field references**: Use qualified field names like `"table.field"`
3. **Join configuration errors**: Ensure all required fields are provided for the join type
4. **Performance issues**: Add appropriate database indexes for join keys

### Debugging

Enable SQL logging to see generated queries:
```elixir
{sql, params} = Selecto.to_sql(selecto)
IO.puts("Generated SQL: #{sql}")
IO.inspect(params, label: "Parameters")
```

Use field resolution debugging:
```elixir
case Selecto.resolve_field(selecto, field_name) do
  {:error, error} ->
    IO.puts("Error: #{error.message}")
    IO.inspect(error.details.suggestions, label: "Suggestions")
end
```