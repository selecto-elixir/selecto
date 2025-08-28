# Advanced Joins Implementation Plan

**Priority #5 from REVIEW.md**: Clarify Advanced Joins - Implement promised behaviors for hierarchical, tagging, and OLAP dimension joins.

## Current State Analysis

The Selecto codebase currently has:
- ✅ Domain configuration support for advanced join types
- ✅ Type definitions for hierarchical, tagging, dimension joins  
- ✅ Basic join processing infrastructure
- ❌ **Missing**: Actual SQL generation for advanced join patterns
- ❌ **Missing**: Recursive CTE logic for hierarchical queries
- ❌ **Missing**: Closure table support
- ❌ **Missing**: Many-to-many through-table handling
- ❌ **Missing**: OLAP dimension optimization

## Implementation Strategy

### Phase 1: Hierarchical Joins 🌳
**Goal**: Implement recursive CTEs for adjacency list and closure table patterns

#### 1.1 Adjacency List Pattern
```sql
-- Target output for depth-limited hierarchical query
WITH RECURSIVE hierarchy AS (
  -- Base case: root nodes
  SELECT id, parent_id, name, 0 as depth, ARRAY[id] as path
  FROM categories 
  WHERE parent_id IS NULL
  
  UNION ALL
  
  -- Recursive case: child nodes
  SELECT c.id, c.parent_id, c.name, h.depth + 1, h.path || c.id
  FROM categories c
  INNER JOIN hierarchy h ON c.parent_id = h.id
  WHERE h.depth < ? -- depth_limit parameter
)
SELECT * FROM hierarchy ORDER BY path;
```

**Implementation Tasks**:
- [ ] Extend `Selecto.Builder.Sql.Hierarchy` with recursive CTE generation
- [ ] Add depth limiting logic with parameterization  
- [ ] Handle cycle detection with path arrays
- [ ] Support custom parent/child field names
- [ ] Add root condition filtering

#### 1.2 Materialized Path Pattern  
```sql
-- For path-based hierarchical data like "/electronics/computers/laptops/"
SELECT * FROM categories 
WHERE path LIKE ? || '%'  -- path prefix matching
ORDER BY path;
```

**Implementation Tasks**:
- [ ] Add path prefix query generation
- [ ] Support custom path separators  
- [ ] Handle path depth limiting
- [ ] Add path-based filtering and sorting

#### 1.3 Closure Table Pattern
```sql
-- For pre-computed ancestor/descendant relationships
WITH hierarchical_data AS (
  SELECT d.*, ct.depth
  FROM category_data d
  INNER JOIN category_closure ct ON d.id = ct.descendant_id  
  WHERE ct.ancestor_id = ? -- root parameter
  AND ct.depth <= ? -- depth_limit parameter
)
SELECT * FROM hierarchical_data ORDER BY depth, name;
```

**Implementation Tasks**:
- [ ] Add closure table join generation
- [ ] Support ancestor/descendant filtering
- [ ] Handle depth-based queries
- [ ] Custom closure table configuration

### Phase 2: Tagging/Many-to-Many Joins 🏷️
**Goal**: Implement proper many-to-many relationships with aggregation

#### 2.1 Basic Many-to-Many Through Tables
```sql
-- Posts with their tags via junction table
SELECT p.*, string_agg(t.name, ', ') as tag_names
FROM posts p
LEFT JOIN post_tags pt ON p.id = pt.post_id
LEFT JOIN tags t ON pt.tag_id = t.id  
WHERE p.active = true
GROUP BY p.id, p.title, p.content
ORDER BY p.created_at DESC;
```

**Implementation Tasks**:
- [ ] Detect many-to-many associations in domain config
- [ ] Generate proper junction table joins
- [ ] Add tag aggregation (string_agg, array_agg, count)
- [ ] Support tag filtering and searching
- [ ] Handle tag weight/scoring systems

#### 2.2 Advanced Tagging Features
```sql
-- Posts with tag filtering and weighting
WITH weighted_posts AS (
  SELECT p.id, p.title, 
         sum(coalesce(pt.weight, 1)) as tag_score,
         array_agg(t.name ORDER BY pt.weight DESC) as tags
  FROM posts p
  INNER JOIN post_tags pt ON p.id = pt.post_id  
  INNER JOIN tags t ON pt.tag_id = t.id
  WHERE t.name = ANY(?) -- tag filter array
  GROUP BY p.id, p.title
  HAVING count(DISTINCT t.id) >= ? -- minimum tag count
)
SELECT * FROM weighted_posts ORDER BY tag_score DESC;
```

**Implementation Tasks**:
- [ ] Tag weight/score aggregation
- [ ] Tag inclusion/exclusion filtering  
- [ ] Minimum tag count requirements
- [ ] Tag hierarchy support (parent/child tags)
- [ ] Tag synonym handling

### Phase 3: OLAP Dimension Joins 📊  
**Goal**: Implement star and snowflake schema optimizations

#### 3.1 Star Schema Dimensions
```sql
-- Fact table with denormalized dimension lookups
SELECT 
  f.measure_value,
  d1.dimension_name as product_name,
  d2.dimension_name as customer_name, 
  d3.dimension_name as time_period
FROM fact_sales f
LEFT JOIN dim_product d1 ON f.product_key = d1.dimension_key
LEFT JOIN dim_customer d2 ON f.customer_key = d2.dimension_key  
LEFT JOIN dim_time d3 ON f.time_key = d3.dimension_key
WHERE d3.year = ? AND d1.category = ?;
```

**Implementation Tasks**:
- [ ] Detect star schema patterns in domain config
- [ ] Generate dimension table joins automatically
- [ ] Add dimension filtering and grouping
- [ ] Support time dimension granularities (day/week/month/year)
- [ ] Handle slowly changing dimensions (SCD Type 1/2)

#### 3.2 Snowflake Schema Dimensions  
```sql
-- Normalized dimensions with sub-dimensions
SELECT 
  f.measure_value,
  p.product_name,
  pc.category_name,
  pb.brand_name
FROM fact_sales f
LEFT JOIN dim_product p ON f.product_key = p.product_key
LEFT JOIN dim_product_category pc ON p.category_key = pc.category_key
LEFT JOIN dim_product_brand pb ON p.brand_key = pb.brand_key
WHERE pc.category_name = ? AND pb.brand_name = ?;
```

**Implementation Tasks**:
- [ ] Handle normalized dimension chains
- [ ] Generate multi-level dimension joins
- [ ] Support dimension hierarchy navigation
- [ ] Add dimension rollup/drill-down capabilities
- [ ] Optimize join order for performance

### Phase 4: SQL Builder Integration 🔧
**Goal**: Integrate advanced join logic into existing SQL builders

#### 4.1 Builder Architecture Updates
**Files to modify**:
- `lib/selecto/builder/sql.ex` - Main orchestration
- `lib/selecto/builder/sql/hierarchy.ex` - Hierarchical patterns  
- `lib/selecto/builder/sql/tagging.ex` - Many-to-many patterns
- `lib/selecto/builder/sql/olap.ex` - OLAP dimension patterns

**Implementation Tasks**:
- [ ] Extend join type detection in `build_select/1`
- [ ] Add CTE generation pipeline in `build/2`
- [ ] Integrate advanced joins with filtering
- [ ] Handle parameter coordination between CTEs and main query
- [ ] Add join optimization hints

#### 4.2 CTE Builder Enhancements
**File**: `lib/selecto/builder/cte.ex`

**Implementation Tasks**:
- [ ] Add recursive CTE support (`WITH RECURSIVE`)
- [ ] Implement CTE parameter threading
- [ ] Add CTE optimization (materialization hints)
- [ ] Support multiple CTEs in single query
- [ ] Handle CTE dependency ordering

### Phase 5: Configuration & Types 📋
**Goal**: Enhance domain configuration for advanced joins

#### 5.1 Enhanced Type Definitions
**File**: `lib/selecto/types.ex`

**New types needed**:
```elixir
@type hierarchy_type :: :adjacency_list | :materialized_path | :closure_table
@type aggregation_type :: :string_agg | :array_agg | :count | :sum | :avg
@type dimension_type :: :star | :snowflake | :time | :slowly_changing
@type join_optimization :: :nested_loop | :hash | :sort_merge
```

#### 5.2 Domain Configuration Extensions
**Enhanced join config format**:
```elixir
joins: %{
  categories: %{
    type: :hierarchical,
    hierarchy_type: :adjacency_list,
    parent_field: :parent_id,
    depth_limit: 5,
    root_condition: %{parent_id: nil},
    path_separator: "/",
    cycle_detection: true
  },
  tags: %{
    type: :tagging, 
    tag_field: :name,
    through_table: :post_tags,
    weight_field: :weight,
    aggregation: :string_agg,
    separator: ", ",
    min_weight: 0.1
  },
  product_dim: %{
    type: :dimension,
    dimension_type: :snowflake,
    fact_table: :sales,
    dimension_key: :product_key,
    normalization_joins: [
      %{table: :product_categories, key: :category_key},
      %{table: :product_brands, key: :brand_key}
    ]
  }
}
```

### Phase 6: Testing & Documentation 🧪
**Goal**: Comprehensive testing and clear documentation

#### 6.1 Test Coverage
**New test files**:
- [ ] `test/hierarchical_joins_test.exs` - All hierarchy patterns
- [ ] `test/tagging_joins_test.exs` - Many-to-many with aggregation  
- [ ] `test/olap_joins_test.exs` - Star/snowflake schemas
- [ ] `test/advanced_joins_integration_test.exs` - Multi-pattern queries

#### 6.2 Documentation Updates
- [ ] Update main module docs with advanced join examples
- [ ] Create `guides/hierarchical_patterns.md`
- [ ] Create `guides/tagging_and_many_to_many.md`  
- [ ] Create `guides/olap_dimensions.md`
- [ ] Add performance tuning guide for complex joins

## Implementation Order & Timeline

### Sprint 1: Hierarchical Foundations (Week 1)
1. Adjacency list recursive CTEs
2. Basic hierarchy builder infrastructure
3. Core hierarchy tests

### Sprint 2: Hierarchy Completion (Week 1.5)  
1. Materialized path support
2. Closure table support  
3. Comprehensive hierarchy testing

### Sprint 3: Tagging Infrastructure (Week 2)
1. Basic many-to-many through tables
2. Tag aggregation (string_agg, array_agg)
3. Tag filtering and weighting

### Sprint 4: Advanced Tagging (Week 2.5)
1. Tag hierarchies and synonyms
2. Complex tag filtering logic
3. Tag scoring algorithms

### Sprint 5: OLAP Foundations (Week 3)
1. Star schema dimension detection
2. Basic dimension joins
3. Time dimension support

### Sprint 6: OLAP Completion (Week 3.5)
1. Snowflake schema normalization
2. Dimension hierarchy navigation
3. Performance optimizations

### Sprint 7: Integration & Polish (Week 4)
1. End-to-end testing
2. Performance benchmarking
3. Documentation completion

## Success Criteria

✅ **Functional Requirements**:
- All promised advanced join types generate correct SQL
- Recursive hierarchies work with cycle detection  
- Many-to-many aggregation produces expected results
- OLAP dimensions support filtering and grouping
- Performance acceptable for typical datasets (<100k rows)

✅ **Quality Requirements**:
- >90% test coverage for advanced join paths
- All Dialyzer type checks pass
- Comprehensive documentation with examples
- Backward compatibility maintained

✅ **Developer Experience**:
- Clear configuration format for each join type
- Helpful error messages for misconfigurations
- Performance guidance for large datasets
- Migration path from simple to advanced joins

## Risk Mitigation

**🚨 SQL Complexity Risk**: Advanced joins can generate very complex SQL
- **Mitigation**: Extensive testing with real datasets, query plan analysis

**🚨 Performance Risk**: Recursive CTEs and multiple joins can be slow
- **Mitigation**: Benchmarking suite, optimization guidelines, query hints

**🚨 Configuration Complexity**: Advanced joins have many options
- **Mitigation**: Sensible defaults, validation, clear examples

**🚨 Backward Compatibility**: Changes might break existing simple joins
- **Mitigation**: Comprehensive regression testing, gradual rollout

## Next Steps

1. **Review & Approval**: Get stakeholder buy-in on this plan
2. **Environment Setup**: Prepare test databases with hierarchical/tagging data  
3. **Sprint 1 Kickoff**: Begin with adjacency list recursive CTEs
4. **Progress Tracking**: Weekly demos and progress reviews

---

Ready to implement the future of advanced SQL joins in Selecto! 🚀