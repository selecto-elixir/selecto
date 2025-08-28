# PHASE 1: Foundation & CTE Support - Technical Specification

## Overview

**Phase 1** establishes the foundation for all advanced join implementations by adding Common Table Expression (CTE) support to Selecto's iodata parameterization system and fixing critical custom column integration issues.

## Objectives

1. **Add CTE support** to the existing iodata parameterization system
2. **Fix broken custom column integration** that generates invalid SQL
3. **Create base infrastructure** for hierarchical SQL generation
4. **Maintain backward compatibility** with all existing functionality

## Technical Architecture

### **1. CTE Builder Module**

**New File: `lib/selecto/builder/cte.ex`**

```elixir
defmodule Selecto.Builder.Cte do
  @moduledoc """
  Common Table Expression (CTE) builder for advanced join patterns.
  
  Generates parameterized CTEs using iodata for safe SQL construction.
  Supports both simple and recursive CTEs with proper parameter scoping.
  """
  
  @doc """
  Build a simple CTE with parameterized content.
  
  Returns: {cte_iodata, params}
  """
  def build_cte(name, query_iodata, params) when is_binary(name) do
    cte_iodata = [
      name, " AS (",
      query_iodata,
      ")"
    ]
    {cte_iodata, params}
  end
  
  @doc """
  Build a recursive CTE with base case and recursive case.
  
  Returns: {recursive_cte_iodata, combined_params}
  """
  def build_recursive_cte(name, base_query_iodata, base_params, recursive_query_iodata, recursive_params) do
    recursive_cte_iodata = [
      name, " AS (",
      base_query_iodata,
      " UNION ALL ",
      recursive_query_iodata,
      ")"
    ]
    combined_params = base_params ++ recursive_params
    {recursive_cte_iodata, combined_params}
  end
  
  @doc """
  Combine multiple CTEs into a single WITH clause.
  
  Returns: {with_clause_iodata, combined_params}
  """
  def build_with_clause(ctes) when is_list(ctes) do
    case ctes do
      [] -> {[], []}
      [{first_cte, first_params} | rest] ->
        {cte_parts, all_params} = 
          Enum.reduce(rest, {[first_cte], first_params}, fn {cte_iodata, params}, {acc_ctes, acc_params} ->
            {acc_ctes ++ [", ", cte_iodata], acc_params ++ params}
          end)
        
        with_clause = ["WITH ", cte_parts, " "]
        {with_clause, all_params}
    end
  end
  
  @doc """
  Prepend CTEs to a main query with proper parameter coordination.
  
  Returns: {complete_query_iodata, combined_params}
  """
  def integrate_ctes_with_query([], main_query_iodata, main_params) do
    {main_query_iodata, main_params}
  end
  
  def integrate_ctes_with_query(ctes, main_query_iodata, main_params) when is_list(ctes) do
    {with_clause, cte_params} = build_with_clause(ctes)
    
    complete_query = [
      with_clause,
      main_query_iodata
    ]
    
    combined_params = cte_params ++ main_params
    {complete_query, combined_params}
  end
end
```

### **2. Extended Parameter Handling**

**Update File: `lib/selecto/sql/params.ex`**

Add CTE marker support to the existing parameter system:

```elixir
# Add to existing Selecto.SQL.Params module

@doc """
Handle CTE markers in iodata structures.
CTE markers are processed before main query finalization.
"""
def finalize_with_ctes(iodata_with_ctes) do
  {cte_sections, main_iodata, all_params} = extract_ctes(iodata_with_ctes)
  
  # Process CTEs first
  {processed_ctes, cte_params} = 
    Enum.map_reduce(cte_sections, [], fn {cte_name, cte_iodata}, acc_params ->
      {cte_sql, cte_specific_params} = finalize(cte_iodata)
      {{cte_name, cte_sql}, acc_params ++ cte_specific_params}
    end)
  
  # Process main query with adjusted parameter numbering
  param_offset = length(cte_params)
  {main_sql, main_specific_params} = finalize(main_iodata, param_offset)
  
  # Combine everything
  final_params = cte_params ++ main_specific_params ++ all_params
  {processed_ctes, main_sql, final_params}
end

defp extract_ctes(iodata) do
  # Extract {:cte, name, cte_iodata} markers from iodata structure
  # Return {cte_list, cleaned_main_iodata, extracted_params}
end
```

### **3. Custom Column Safety Fix**

**Update File: `lib/selecto/builder/sql/select.ex`**

Fix the broken custom column integration that currently generates invalid SQL:

```elixir
# Add to existing Selecto.Builder.Sql.Select module

@doc """
Safely handle custom column SQL with field validation and parameterization.
"""
def prep_selector(selecto, {:custom_sql, sql_template, field_mappings}) do
  # Validate that all referenced fields exist
  available_fields = get_available_fields(selecto)
  validate_field_references(sql_template, field_mappings, available_fields)
  
  # Replace field placeholders with actual field references
  safe_sql = substitute_field_references(sql_template, field_mappings, selecto)
  
  # Return as safe iodata
  {[safe_sql], :selecto_root, []}
end

defp get_available_fields(selecto) do
  # Get all available fields from source and joins
  source_fields = Map.keys(selecto.config.columns || %{})
  join_fields = get_join_fields(selecto.config.joins || %{})
  cte_fields = get_cte_fields(selecto) # New: CTE field availability
  
  source_fields ++ join_fields ++ cte_fields
end

defp validate_field_references(sql_template, field_mappings, available_fields) do
  # Ensure all field references in mappings exist
  Enum.each(field_mappings, fn {placeholder, field_ref} ->
    case validate_field_exists(field_ref, available_fields) do
      :ok -> :ok
      {:error, reason} -> 
        raise ArgumentError, "Invalid field reference '#{field_ref}' in custom SQL: #{reason}"
    end
  end)
end

defp substitute_field_references(sql_template, field_mappings, selecto) do
  # Safely replace {{field}} placeholders with actual field references
  Enum.reduce(field_mappings, sql_template, fn {placeholder, field_ref}, acc_sql ->
    safe_field_reference = build_safe_field_reference(field_ref, selecto)
    String.replace(acc_sql, "{{#{placeholder}}}", safe_field_reference)
  end)
end
```

### **4. Main SQL Builder Integration**

**Update File: `lib/selecto/builder/sql.ex`**

Integrate CTE support into the main SQL building process:

```elixir
# Update existing build/2 function

def build(selecto, opts) do
  # Existing SELECT, WHERE, GROUP BY, ORDER BY building...
  {aliases, sel_joins, select_iodata, select_params} = build_select(selecto)
  {filter_joins, where_iolist, _where_params} = build_where(selecto)
  {group_by_joins, group_by_iodata, _group_by_params} = build_group_by(selecto)
  {order_by_joins, order_by_iodata, _order_by_params} = build_order_by(selecto)
  
  joins_in_order = Selecto.Builder.Join.get_join_order(
    Selecto.joins(selecto),
    List.flatten(sel_joins ++ filter_joins ++ group_by_joins ++ order_by_joins)
  )
  
  # NEW: Check for advanced joins that require CTEs
  {from_iodata, from_params, required_ctes} = build_from_with_ctes(selecto, joins_in_order)
  
  # Existing WHERE, GROUP BY, ORDER BY finalization...
  
  # NEW: Build complete query with CTEs
  base_query_iodata = [
    "\n        select ", select_iodata,
    "\n        from ", from_iodata,
    where_iodata_section,
    group_by_iodata_section, 
    order_by_iodata_section
  ]
  
  # NEW: Integrate CTEs with main query
  {final_query_iodata, final_params} = 
    Selecto.Builder.Cte.integrate_ctes_with_query(required_ctes, base_query_iodata, all_params)
  
  {sql, combined_params} = Selecto.SQL.Params.finalize(final_query_iodata)
  {sql, aliases, combined_params}
end

# NEW: Enhanced FROM builder with CTE detection
defp build_from_with_ctes(selecto, joins) do
  Enum.reduce(joins, {[], [], []}, fn 
    :selecto_root, {fc, p, ctes} ->
      root_table = Selecto.source_table(selecto)
      root_alias = build_join_string(selecto, "selecto_root")
      {fc ++ [[root_table, " ", root_alias]], p, ctes}
    
    join, {fc, p, ctes} ->
      config = Selecto.joins(selecto)[join]
      
      case detect_advanced_join_pattern(config) do
        {:hierarchy, pattern} -> 
          build_hierarchy_join_with_cte(selecto, join, config, pattern, fc, p, ctes)
        
        {:tagging, _} -> 
          build_tagging_join(selecto, join, config, fc, p, ctes)
        
        {:olap, type} ->
          build_olap_join(selecto, join, config, type, fc, p, ctes)
          
        :basic ->
          # Existing basic join logic
          {fc ++ [build_basic_join_iodata(selecto, join, config)], p, ctes}
      end
  end)
end

defp detect_advanced_join_pattern(config) do
  case Map.get(config, :join_type) do
    :hierarchical_adjacency -> {:hierarchy, :adjacency_list}
    :hierarchical_materialized_path -> {:hierarchy, :materialized_path}  
    :hierarchical_closure_table -> {:hierarchy, :closure_table}
    :many_to_many -> {:tagging, nil}
    :star_dimension -> {:olap, :star}
    :snowflake_dimension -> {:olap, :snowflake}
    _ -> :basic
  end
end
```

### **5. Hierarchy SQL Generation Stubs**

**New File: `lib/selecto/builder/sql/hierarchy.ex`**

Create the foundation for hierarchical SQL generation (to be completed in Phase 2):

```elixir
defmodule Selecto.Builder.Sql.Hierarchy do
  @moduledoc """
  Hierarchical SQL pattern generation for self-referencing relationships.
  
  Supports adjacency lists, materialized paths, and closure table patterns
  using recursive CTEs and specialized SQL constructs.
  """
  
  alias Selecto.Builder.Cte
  
  @doc """
  Build hierarchical join with appropriate CTE pattern.
  Returns: {from_clause_iodata, params, [ctes]}
  """
  def build_hierarchy_join_with_cte(selecto, join, config, pattern, fc, p, ctes) do
    case pattern do
      :adjacency_list ->
        build_adjacency_list_join(selecto, join, config, fc, p, ctes)
      
      :materialized_path ->
        build_materialized_path_join(selecto, join, config, fc, p, ctes)
      
      :closure_table ->
        build_closure_table_join(selecto, join, config, fc, p, ctes)
    end
  end
  
  # Phase 1: Create stubs that return basic joins
  # Phase 2: Implement full CTE generation
  
  defp build_adjacency_list_join(selecto, join, config, fc, p, ctes) do
    # Phase 1: Stub implementation - return basic join
    basic_join_iodata = [
      " left join ", config.source, " ", build_join_string(selecto, join),
      " on ", build_selector_string(selecto, join, config.my_key),
      " = ", build_selector_string(selecto, config.requires_join, config.owner_key)
    ]
    
    # TODO Phase 2: Replace with recursive CTE
    # {hierarchy_cte, cte_params} = build_adjacency_cte(selecto, join, config)
    # new_ctes = ctes ++ [{hierarchy_cte, cte_params}]
    
    {fc ++ [basic_join_iodata], p, ctes}
  end
  
  defp build_materialized_path_join(selecto, join, config, fc, p, ctes) do
    # Phase 1: Stub - Phase 2 will implement path-based queries
    basic_join_iodata = [
      " left join ", config.source, " ", build_join_string(selecto, join),
      " on ", build_selector_string(selecto, join, config.my_key),
      " = ", build_selector_string(selecto, config.requires_join, config.owner_key)
    ]
    {fc ++ [basic_join_iodata], p, ctes}
  end
  
  defp build_closure_table_join(selecto, join, config, fc, p, ctes) do
    # Phase 1: Stub - Phase 2 will implement closure table patterns  
    basic_join_iodata = [
      " left join ", config.source, " ", build_join_string(selecto, join),
      " on ", build_selector_string(selecto, join, config.my_key),
      " = ", build_selector_string(selecto, config.requires_join, config.owner_key)
    ]
    {fc ++ [basic_join_iodata], p, ctes}
  end
end
```

## Testing Strategy

### **Phase 1 Test Requirements**

**New File: `test/cte_builder_test.exs`**

```elixir
defmodule Selecto.CteBuilderTest do
  use ExUnit.Case
  alias Selecto.Builder.Cte
  
  describe "build_cte/3" do
    test "builds simple CTE with parameterization" do
      query_iodata = ["SELECT id, name FROM users WHERE active = ", {:param, true}]
      params = [true]
      
      {cte_iodata, result_params} = Cte.build_cte("active_users", query_iodata, params)
      
      assert result_params == [true]
      # Verify iodata structure is correct
    end
  end
  
  describe "build_recursive_cte/5" do
    test "builds recursive CTE with proper UNION ALL structure" do
      base_iodata = ["SELECT id, name, 0 as level FROM categories WHERE parent_id IS NULL"]
      recursive_iodata = [
        "SELECT c.id, c.name, p.level + 1 FROM categories c ", 
        "JOIN category_tree p ON c.parent_id = p.id WHERE p.level < ", {:param, 5}
      ]
      
      {cte_iodata, params} = Cte.build_recursive_cte(
        "category_tree", base_iodata, [], recursive_iodata, [5]
      )
      
      assert params == [5]
      # Verify recursive structure
    end
  end
  
  describe "integration with main query" do
    test "CTEs properly prepended to main query" do
      cte1 = {["users_cte AS (SELECT * FROM users)"], []}
      cte2 = {["posts_cte AS (SELECT * FROM posts)"], []} 
      main_query = ["SELECT * FROM users_cte JOIN posts_cte ON users_cte.id = posts_cte.user_id"]
      
      {final_query, params} = Cte.integrate_ctes_with_query([cte1, cte2], main_query, [])
      
      # Verify WITH clause structure and main query combination
    end
  end
end
```

**Update File: `test/selecto_integration_test.exs`**

Add tests for CTE integration with existing functionality:

```elixir
# Add to existing integration test file

test "CTE integration preserves existing functionality" do
  # Ensure all existing tests still pass with CTE infrastructure
  domain = build_test_domain_with_basic_joins()
  selecto = Selecto.configure(domain, :mock_connection)
  
  {sql, aliases, params} = Selecto.gen_sql(selecto, [])
  
  # Verify no regression in existing functionality
  assert String.contains?(sql, "select")
  assert String.contains?(sql, "from") 
  refute String.contains?(sql, "WITH") # No CTEs for basic joins
end

test "advanced join detection triggers CTE preparation" do
  # Test that advanced joins are detected but fallback to basic (Phase 1)
  domain = build_test_domain_with_hierarchy_join()
  selecto = Selecto.configure(domain, :mock_connection)
  
  {sql, aliases, params} = Selecto.gen_sql(selecto, [])
  
  # Phase 1: Should still generate basic LEFT JOIN but with CTE infrastructure ready
  assert String.contains?(sql, "left join")
  refute String.contains?(sql, "WITH") # CTEs not yet implemented in Phase 1
end
```

## Backward Compatibility

### **Guarantees**
1. **All existing tests must pass** - No regression in current functionality
2. **Existing join configurations work unchanged** - Basic joins continue to work
3. **API compatibility maintained** - No breaking changes to public functions
4. **Parameter system unchanged** - Existing parameterization behavior preserved

### **Migration Path**
- **Phase 1**: Infrastructure added, advanced joins still use basic fallbacks
- **Phase 2+**: Advanced joins progressively gain CTE functionality
- **No breaking changes** - Users can upgrade immediately and benefit incrementally

## Success Criteria for Phase 1

### **Functional Requirements**
- ✅ CTE builder module creates valid iodata structures
- ✅ Parameter system handles CTE markers correctly  
- ✅ Custom column safety prevents invalid SQL generation
- ✅ All existing tests pass without modification
- ✅ Advanced join detection framework in place

### **Quality Requirements** 
- ✅ No SQL injection vectors introduced
- ✅ Proper error handling for invalid configurations
- ✅ Clear error messages for debugging
- ✅ Code follows existing project patterns

### **Performance Requirements**
- ✅ No performance regression for basic joins
- ✅ CTE infrastructure adds <5% overhead when unused  
- ✅ Parameter processing remains efficient

### **Documentation Requirements**
- ✅ CTE builder module fully documented
- ✅ Custom column safety patterns explained
- ✅ Phase 2+ roadmap clearly outlined

## Phase 1 Completion Definition

Phase 1 is complete when:

1. **All new modules compile and test successfully**
2. **Entire existing test suite passes unchanged**
3. **CTE infrastructure can generate valid WITH clauses**
4. **Custom column SQL validation prevents invalid references**
5. **Advanced join detection correctly identifies join types**
6. **Code review approved by maintainers**

This foundation phase is critical for the success of all subsequent phases, as it establishes the architectural patterns and safety mechanisms that the advanced join implementations will depend on.