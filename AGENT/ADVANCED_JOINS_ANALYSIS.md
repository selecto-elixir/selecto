# Advanced Joins Analysis: Implementation Gaps

## Executive Summary

The Selecto library's advanced join functionality is **fundamentally broken**. While extensive configuration logic exists for 4 advanced join types, the actual SQL generation completely fails to implement the promised behavior. All advanced joins fall back to basic LEFT JOINs, making the advanced features non-functional.

## Current State Assessment

### ✅ What's Implemented (Configuration Layer)

**File: `lib/selecto/schema/join.ex`**
- Extensive configuration logic for 4 advanced join types
- Proper validation and metadata generation
- Custom column and filter definitions
- Join dependency tracking

**Advanced Join Types Configured:**
1. **`:tagging`** - Many-to-many relationships through join tables
2. **`:hierarchical`** - Self-referencing hierarchies with 3 subtypes:
   - `:adjacency_list` - Parent-child relationships
   - `:materialized_path` - Path-based hierarchies 
   - `:closure_table` - Ancestor-descendant relationship tables
3. **`:star_dimension`** - OLAP star schema optimized joins
4. **`:snowflake_dimension`** - Normalized dimension tables with additional joins

### ❌ What's Missing (SQL Generation Layer)

**File: `lib/selecto/builder/sql.ex`**
- **Lines 121-148**: All advanced join types fall back to basic LEFT JOIN
- **No recursive CTEs** for hierarchical queries
- **No specialized SQL patterns** for OLAP dimensions
- **No closure table handling**
- **No tagging aggregation**

**File: `lib/selecto/builder/sql/select.ex`** 
- **Lines 161-164**: Custom column SQL treated as unsafe literal strings
- **Invalid SQL generation** in hierarchy builders (undefined field references)
- **No coordination** between JOIN and SELECT builders for complex SQL

## Critical Gaps Analysis

### 1. **Hierarchical Joins - Completely Broken**

**Current Code (Lines 162-178 in `sql.ex`):**
```elixir
:hierarchical_adjacency ->
  build_hierarchical_adjacency_join(selecto, join, config, fc, p)
  # Falls back to basic LEFT JOIN - NO RECURSIVE CTE GENERATION
```

**Missing SQL Patterns:**

**A. Adjacency Lists** - Need recursive CTEs:
```sql
WITH RECURSIVE hierarchy AS (
  SELECT id, name, parent_id, 0 as level, CAST(name AS TEXT) as path
  FROM categories WHERE parent_id IS NULL
  UNION ALL
  SELECT c.id, c.name, c.parent_id, h.level + 1, h.path || ' > ' || c.name
  FROM categories c JOIN hierarchy h ON c.parent_id = h.id
  WHERE h.level < 5
)
SELECT * FROM hierarchy WHERE id = ?
```

**B. Materialized Paths** - Path-based queries:
```sql
SELECT *, (length(path) - length(replace(path, '/', ''))) as depth
FROM categories WHERE path LIKE 'root/electronics%'
```

**C. Closure Tables** - Multi-table relationship queries:
```sql
SELECT c.*, cl.depth, 
  (SELECT COUNT(*) FROM category_closure cl2 WHERE cl2.ancestor_id = c.id) as descendant_count
FROM categories c
JOIN category_closure cl ON c.id = cl.descendant_id
WHERE cl.ancestor_id = ?
```

### 2. **Custom Column SQL Integration - Invalid Code**

**Current Problem (Lines 493-507 in `join.ex`):**
```elixir
defp build_hierarchy_path_sql(field, table, depth_limit) do
  # This generates INVALID SQL - #{field}_name doesn't exist
  "WITH RECURSIVE path_cte AS (
    SELECT id, #{field}_name, CAST(#{field}_name AS TEXT) as path, 0 as level
    FROM #{table}
    WHERE parent_id IS NULL
    ...
  )"
end
```

**Issues:**
- References undefined `#{field}_name` columns
- No field validation against actual schema
- Invalid SQL that would fail at runtime
- No coordination with SELECT builder for complex expressions

### 3. **Many-to-Many Tagging - Missing Core Logic**

**Current Code:** Basic LEFT JOIN only
**Missing Requirements:**
```sql
-- Tag aggregation
SELECT p.*, string_agg(t.name, ', ') as tag_list
FROM products p
LEFT JOIN product_tags pt ON p.id = pt.product_id  
LEFT JOIN tags t ON pt.tag_id = t.id
GROUP BY p.id

-- Faceted filtering  
SELECT p.* FROM products p
WHERE EXISTS (
  SELECT 1 FROM product_tags pt 
  JOIN tags t ON pt.tag_id = t.id
  WHERE pt.product_id = p.id AND t.name = ANY($1)
)
```

### 4. **OLAP Dimensions - No Optimization**

**Current:** Basic LEFT JOINs with no OLAP features
**Missing:**
- Fact table detection and optimization
- Aggregation-friendly SQL patterns  
- Snowflake normalization join chains
- Dimension table caching hints

## Root Cause Analysis

### **1. Architecture Gap**
- **Configuration layer** is sophisticated and well-designed
- **SQL generation layer** completely ignores advanced join types
- **No bridge** between configuration metadata and SQL builders

### **2. SQL Builder Limitations**
- **Single-pass generation** can't handle complex multi-statement SQL
- **No CTE support** in the iodata parameterization system
- **Linear join processing** can't handle hierarchical relationships
- **No subquery integration** for complex filters

### **3. Custom Column Integration Issues**
- **Unsafe SQL handling** - custom columns bypass parameterization
- **No field validation** - references to non-existent columns
- **No coordination** between JOIN and SELECT builders
- **Broken helper functions** generate invalid SQL

## Implementation Priority Matrix

### **🔴 Critical (Blocking Production Use)**
1. **Fix hierarchical CTE generation** - Current code generates invalid SQL
2. **Implement proper many-to-many joins** - Core tagging functionality broken
3. **Fix custom column SQL integration** - SELECT builder fails on complex SQL
4. **Add CTE support to iodata system** - Required for recursive queries

### **🟡 High Priority (Feature Completeness)**
5. **Implement closure table patterns** - Complex ancestor-descendant queries
6. **Add OLAP dimension optimizations** - Star/snowflake schema performance
7. **Implement faceted filtering** - Dynamic tag-based filtering
8. **Add comprehensive testing** - No tests exist for advanced joins

### **🟢 Medium Priority (Enhancement)**
9. **Performance optimizations** - Query plan improvements
10. **Additional hierarchy types** - Nested sets, linear hierarchies
11. **Dynamic CTE configuration** - Runtime depth limits, custom recursion

## Next Steps

1. **Create detailed implementation plan** with specific code changes
2. **Design CTE integration** with existing iodata parameterization
3. **Implement SQL pattern library** for each advanced join type
4. **Build comprehensive test suite** for all advanced join scenarios
5. **Update documentation** to reflect actual capabilities

## Files Requiring Major Changes

### **Core SQL Generation**
- `lib/selecto/builder/sql.ex` - Add CTE support, fix join builders
- `lib/selecto/builder/sql/select.ex` - Fix custom column integration
- `lib/selecto/sql/params.ex` - Extend for CTE parameterization

### **Join Configuration**  
- `lib/selecto/schema/join.ex` - Fix invalid SQL generation functions
- Add field validation for custom columns

### **New Files Needed**
- `lib/selecto/builder/cte.ex` - CTE generation utilities
- `lib/selecto/builder/sql/hierarchy.ex` - Hierarchical SQL patterns
- `lib/selecto/builder/sql/tagging.ex` - Many-to-many SQL patterns
- `lib/selecto/builder/sql/olap.ex` - OLAP dimension SQL patterns

### **Testing Infrastructure**
- `test/advanced_joins/` - Directory for advanced join tests
- Comprehensive test domains with realistic hierarchical data
- SQL pattern validation tests
- Performance regression tests

The advanced joins implementation represents a significant undertaking that will fundamentally change how Selecto generates SQL for complex relationships. However, the current state is so broken that any implementation would be better than the existing non-functional code.