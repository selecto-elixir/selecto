# Advanced Joins Implementation Plan

## Project Overview

**Goal**: Implement functional advanced join SQL generation to match the existing configuration promises in Selecto.

**Scope**: 4 advanced join types with 7 distinct SQL patterns  
**Effort**: Large (3-5 phases, ~2000 lines of new code)  
**Impact**: Transforms Selecto from basic query builder to enterprise-grade OLAP/hierarchy tool

## Implementation Phases

### **PHASE 1: Foundation & CTE Support** 
*Prerequisites for all advanced joins*

#### **Objectives**
- Add CTE (Common Table Expression) support to iodata parameterization system
- Create base hierarchy SQL generation infrastructure
- Fix broken custom column integration

#### **Deliverables**
1. **`lib/selecto/builder/cte.ex`** - CTE generation utilities
   - `build_cte/3` - Generate parameterized CTEs with iodata
   - `build_recursive_cte/4` - Recursive CTE with UNION ALL
   - `integrate_ctes_with_query/2` - Prepend CTEs to main query

2. **Extend `lib/selecto/sql/params.ex`** - CTE parameterization
   - Support `{:cte, name, cte_iodata}` markers
   - Handle CTE parameter scoping and collision avoidance
   - Update `finalize/1` to process CTE sections

3. **Fix `lib/selecto/builder/sql/select.ex`** - Custom column safety
   - Add field validation for custom SQL expressions  
   - Coordinate with JOIN builder for complex SQL
   - Proper parameterization of custom column references

#### **Key Changes**
```elixir
# New CTE builder
defmodule Selecto.Builder.Cte do
  def build_recursive_cte(name, base_query, recursive_query, params) do
    {[
      "WITH RECURSIVE ", name, " AS (",
        base_query, 
        " UNION ALL ",
        recursive_query,
      ")"
    ], params}
  end
end

# Extended SQL params
defmodule Selecto.SQL.Params do
  def finalize({:cte, name, cte_iodata}) do
    # Handle CTE markers in iodata
  end
end
```

#### **Success Criteria**
- CTE generation works with iodata parameterization
- Custom columns can reference CTE results safely
- All existing tests pass with new CTE infrastructure

---

### **PHASE 2: Hierarchical Joins Implementation**
*Self-referencing tree structures*

#### **Objectives**  
- Implement 3 hierarchical join patterns
- Generate valid recursive SQL for tree traversal
- Replace broken hierarchy builders with functional code

#### **Deliverables**
1. **`lib/selecto/builder/sql/hierarchy.ex`** - Hierarchical SQL patterns
   - `build_adjacency_list_cte/3` - Recursive parent-child traversal
   - `build_materialized_path_query/3` - Path-based hierarchy queries  
   - `build_closure_table_join/3` - Ancestor-descendant relationship queries

2. **Update `lib/selecto/builder/sql.ex`** - Hierarchy join integration
   - Replace fallback LEFT JOINs with proper hierarchy builders
   - Integrate CTE generation with main query building
   - Handle hierarchy-specific JOIN ordering

3. **Fix `lib/selecto/schema/join.ex`** - Valid SQL generation
   - Remove broken `build_hierarchy_path_sql/3` functions
   - Add proper field validation for hierarchy references
   - Generate valid custom column SQL

#### **SQL Patterns to Implement**

**A. Adjacency List Pattern:**
```sql
WITH RECURSIVE hierarchy AS (
  SELECT id, name, parent_id, 0 as level, 
         CAST(id AS TEXT) as path,
         ARRAY[id] as path_array
  FROM {{table}} WHERE {{root_condition}}
  UNION ALL
  SELECT c.id, c.name, c.parent_id, h.level + 1,
         h.path || '>' || CAST(c.id AS TEXT),
         h.path_array || c.id
  FROM {{table}} c
  JOIN hierarchy h ON c.parent_id = h.id
  WHERE h.level < {{depth_limit}}
)
SELECT h.*, main.* 
FROM {{main_table}} main
JOIN hierarchy h ON main.{{join_field}} = h.id
WHERE {{conditions}}
```

**B. Materialized Path Pattern:**
```sql
SELECT c.*, 
       (length(c.{{path_field}}) - length(replace(c.{{path_field}}, '{{separator}}', ''))) as depth,
       string_to_array(c.{{path_field}}, '{{separator}}') as path_array
FROM {{table}} c
WHERE c.{{path_field}} LIKE {{pattern}} || '%'
```

**C. Closure Table Pattern:**
```sql
SELECT c.*, cl.depth,
       (SELECT COUNT(*) FROM {{closure_table}} cl2 
        WHERE cl2.{{ancestor_field}} = c.id) as descendant_count
FROM {{table}} c
JOIN {{closure_table}} cl ON c.id = cl.{{descendant_field}}
WHERE cl.{{ancestor_field}} = {{root_id}}
```

#### **Success Criteria**
- All 3 hierarchical patterns generate valid SQL
- CTEs properly integrated with main queries  
- Hierarchy depth limits enforced
- Custom hierarchy columns work in SELECT clauses

---

### **PHASE 3: Many-to-Many Tagging Implementation**
*Join table relationships with aggregation*

#### **Objectives**
- Implement proper many-to-many JOIN handling
- Add tag aggregation and faceted filtering
- Support intermediate join table operations

#### **Deliverables**
1. **`lib/selecto/builder/sql/tagging.ex`** - Many-to-many patterns
   - `build_tagging_join/3` - Intermediate join table handling
   - `build_tag_aggregation/3` - string_agg for tag lists
   - `build_faceted_filter/3` - EXISTS subqueries for tag filtering

2. **Update WHERE builder** - Faceted filtering support
   - Add EXISTS subquery generation for tag filters
   - Handle array parameter binding for tag lists
   - Optimize tag filtering performance

#### **SQL Patterns to Implement**

**A. Basic Many-to-Many Join:**
```sql
SELECT main.*, 
       string_agg(tags.{{tag_field}}, ', ') as {{tag_list_alias}},
       COUNT(DISTINCT tags.id) as {{tag_count_alias}}
FROM {{main_table}} main
LEFT JOIN {{join_table}} jt ON main.{{main_key}} = jt.{{main_foreign_key}}
LEFT JOIN {{tag_table}} tags ON jt.{{tag_foreign_key}} = tags.{{tag_key}}
GROUP BY main.{{main_key}}, main.*
```

**B. Faceted Tag Filtering:**
```sql
SELECT main.* FROM {{main_table}} main
WHERE EXISTS (
  SELECT 1 FROM {{join_table}} jt
  JOIN {{tag_table}} tags ON jt.{{tag_foreign_key}} = tags.{{tag_key}}
  WHERE jt.{{main_foreign_key}} = main.{{main_key}} 
    AND tags.{{tag_field}} = ANY({{tag_array_param}})
)
```

**C. Tag Count Filtering:**
```sql
SELECT main.* FROM {{main_table}} main
WHERE (
  SELECT COUNT(*) FROM {{join_table}} jt
  WHERE jt.{{main_foreign_key}} = main.{{main_key}}
) >= {{min_tag_count}}
```

#### **Success Criteria**
- Many-to-many joins generate proper intermediate table SQL
- Tag aggregation works with GROUP BY
- Faceted filtering supports tag arrays
- Performance acceptable for large tag datasets

---

### **PHASE 4: OLAP Dimension Optimization**
*Star and snowflake schema performance*

#### **Objectives**
- Implement OLAP-optimized join patterns
- Add star/snowflake dimension handling
- Optimize for aggregation query performance

#### **Deliverables**
1. **`lib/selecto/builder/sql/olap.ex`** - OLAP patterns
   - `build_star_dimension_join/3` - Optimized dimension joins
   - `build_snowflake_join_chain/3` - Normalized dimension chains
   - `build_fact_table_optimization/3` - Fact table join hints

2. **Dimension-aware query optimization**
   - Detect fact vs dimension table patterns
   - Optimize JOIN ordering for star schemas
   - Add dimension table join hints

#### **SQL Patterns to Implement**

**A. Star Schema Optimization:**
```sql
-- Fact table first, dimensions joined efficiently
SELECT f.*, 
       d1.{{display_field}} as {{d1_alias}},
       d2.{{display_field}} as {{d2_alias}}
FROM {{fact_table}} f
LEFT JOIN {{dim1_table}} d1 ON f.{{dim1_key}} = d1.{{dim1_pk}}
LEFT JOIN {{dim2_table}} d2 ON f.{{dim2_key}} = d2.{{dim2_pk}}
WHERE {{fact_filters}} -- Fact filters first for performance
```

**B. Snowflake Normalization Chain:**
```sql
-- Handle normalized dimension hierarchies
SELECT f.*, d1.name as category, d2.name as subcategory, d3.name as product_type
FROM {{fact_table}} f
LEFT JOIN {{dim1_table}} d1 ON f.{{dim1_key}} = d1.id
LEFT JOIN {{dim2_table}} d2 ON d1.{{norm_key1}} = d2.id  
LEFT JOIN {{dim3_table}} d3 ON d2.{{norm_key2}} = d3.id
```

#### **Success Criteria**
- Star schema queries optimized for aggregation
- Snowflake normalization chains work correctly
- JOIN ordering optimized for OLAP workloads
- Dimension filters applied efficiently

---

### **PHASE 5: Testing & Documentation**
*Comprehensive validation and docs*

#### **Objectives**
- Create comprehensive test suite for all advanced joins
- Update documentation to reflect actual capabilities
- Performance testing and optimization

#### **Deliverables**
1. **Test Infrastructure**
   - `test/advanced_joins/hierarchy_test.exs` - All hierarchy patterns
   - `test/advanced_joins/tagging_test.exs` - Many-to-many patterns
   - `test/advanced_joins/olap_test.exs` - OLAP dimension patterns
   - `test/advanced_joins/integration_test.exs` - End-to-end scenarios

2. **Documentation Updates**
   - Update `lib/selecto/schema/join.ex` module docs
   - Add advanced join examples to main documentation
   - Performance tuning guide for complex joins

3. **Performance Testing**
   - Benchmark hierarchical query performance
   - Test with large datasets (10K+ nodes in hierarchies)
   - Optimize query plans for production usage

#### **Test Scenarios**
- **Hierarchy Tests**: 5-level deep trees, circular dependency detection, path traversal
- **Tagging Tests**: 100+ tags per item, faceted filtering, tag clouds
- **OLAP Tests**: Multi-dimensional aggregations, time-series analysis
- **Integration Tests**: Mixed join types, complex WHERE clauses, performance regression

#### **Success Criteria**
- 100% test coverage for all advanced join patterns
- Performance acceptable for production datasets
- Documentation accurate and complete
- All existing functionality preserved

---

## Architecture Changes Required

### **1. SQL Generation Pipeline Enhancement**
```elixir
# Current: Simple linear join processing
def build_from(selecto, joins) do
  Enum.reduce(joins, {[], []}, fn join, {fc, p} ->
    # Basic LEFT JOIN only
  end)
end

# New: Advanced join pattern detection
def build_from(selecto, joins) do
  Enum.reduce(joins, {[], []}, fn join, {fc, p} ->
    case detect_join_pattern(selecto, join) do
      {:hierarchy, pattern} -> build_hierarchy_join(selecto, join, pattern, fc, p)
      {:tagging, config} -> build_tagging_join(selecto, join, config, fc, p)
      {:olap, type} -> build_olap_join(selecto, join, type, fc, p)
      :basic -> build_basic_join(selecto, join, fc, p)
    end
  end)
end
```

### **2. CTE Integration Architecture**
```elixir
# Query structure enhancement
%{
  ctes: [%{name: "hierarchy", sql: cte_iodata, params: cte_params}],
  main_query: main_iodata,
  final_params: combined_params
}
```

### **3. Custom Column Safety Architecture**
```elixir
# Safe custom column handling
def validate_custom_column_sql(sql, available_fields, cte_fields) do
  # Validate field references exist
  # Ensure no SQL injection
  # Coordinate with JOIN builders
end
```

## Risk Assessment

### **High Risk**
- **CTE parameterization complexity** - May require significant changes to iodata system
- **Performance impact** - Recursive CTEs can be slow on large datasets  
- **SQL compatibility** - Some patterns may not work on all PostgreSQL versions

### **Medium Risk**  
- **Custom column safety** - Balancing flexibility with security
- **JOIN ordering optimization** - Complex dependency resolution
- **Test data complexity** - Realistic test scenarios for hierarchies/OLAP

### **Low Risk**
- **Configuration backward compatibility** - Existing config should work unchanged
- **Documentation updates** - Clear patterns to follow
- **Incremental deployment** - Can be rolled out by join type

## Success Metrics

### **Functional Metrics**
- ✅ All 4 advanced join types generate valid SQL
- ✅ 100% test coverage for advanced join patterns  
- ✅ Zero regression in existing functionality
- ✅ Performance acceptable on realistic datasets

### **Quality Metrics**
- ✅ All SQL injection vectors eliminated
- ✅ Proper parameterization maintained
- ✅ Error messages clear and actionable
- ✅ Documentation accurate and complete

### **Performance Metrics**
- ✅ Hierarchy queries: <100ms for 1000-node trees
- ✅ Tagging queries: <200ms for 10K tags
- ✅ OLAP queries: <500ms for typical star schema
- ✅ Memory usage: <2x increase over basic joins

## Timeline Estimate

- **Phase 1 (Foundation)**: 2-3 weeks - CTE support, custom column fixes
- **Phase 2 (Hierarchical)**: 2-3 weeks - 3 hierarchy patterns  
- **Phase 3 (Tagging)**: 1-2 weeks - Many-to-many implementation
- **Phase 4 (OLAP)**: 1-2 weeks - Dimension optimizations
- **Phase 5 (Testing/Docs)**: 1-2 weeks - Comprehensive validation

**Total Estimate: 7-12 weeks** for complete implementation

This represents a significant undertaking that will fundamentally transform Selecto's capabilities, but the result will be a production-ready advanced query builder that can handle enterprise-scale hierarchical and OLAP workloads.