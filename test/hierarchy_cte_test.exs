defmodule Selecto.HierarchyCteTest do
  use ExUnit.Case
  alias Selecto.Builder.Sql.Hierarchy

  test "build_adjacency_list_cte generates valid recursive CTE" do
    # Mock selecto struct
    selecto = %{postgrex_opts: :mock_connection}
    
    # Mock join configuration
    join = :categories
    
    # Mock config with hierarchy settings
    config = %{
      source: "categories",
      hierarchy_depth: 5,
      id_field: "id",
      name_field: "name", 
      parent_field: "parent_id"
    }
    
    # Generate adjacency list CTE
    result = Hierarchy.build_adjacency_list_cte(selecto, join, config)
    
    # Should return a tuple
    assert is_tuple(result)
    {cte_iodata, cte_params} = result
    
    # Convert iodata to string for testing
    cte_sql = IO.iodata_to_binary(cte_iodata)
    
    # Verify CTE structure
    assert String.contains?(cte_sql, "WITH RECURSIVE categories_hierarchy AS (")
    assert String.contains?(cte_sql, "UNION ALL")
    
    # Verify params is a list
    assert is_list(cte_params)
    
    # Basic validation - should contain recursive CTE pattern
    assert String.contains?(cte_sql, "(")
    assert String.contains?(cte_sql, ")")
  end
  
  test "hierarchy_cte_name generates correct CTE name" do
    # Test the helper function directly via module attribute access
    result = Hierarchy.hierarchy_cte_name(:categories)
    assert result == "categories_hierarchy"
    
    result = Hierarchy.hierarchy_cte_name(:employees) 
    assert result == "employees_hierarchy"
  end
  
  test "build_adjacency_list_cte handles different configuration options" do
    selecto = %{postgrex_opts: :mock_connection}
    join = :employees
    
    # Test custom field names and depth limit
    config = %{
      source: "staff",
      hierarchy_depth: 10,
      id_field: "employee_id",
      name_field: "full_name",
      parent_field: "manager_id"
    }
    
    {cte_iodata, cte_params} = Hierarchy.build_adjacency_list_cte(selecto, join, config)
    cte_sql = IO.iodata_to_binary(cte_iodata)
    
    # Verify custom CTE name
    assert String.contains?(cte_sql, "employees_hierarchy")
    
    # Verify custom depth limit is used
    assert 10 in cte_params
    
    # Verify it generates valid SQL structure
    assert String.contains?(cte_sql, "WITH RECURSIVE")
    assert String.contains?(cte_sql, "UNION ALL")
  end
  
  test "build_adjacency_list_cte with default values" do
    selecto = %{postgrex_opts: :mock_connection}
    join = :nodes
    
    # Minimal config - should use defaults
    config = %{
      source: "tree_nodes"
    }
    
    {cte_iodata, cte_params} = Hierarchy.build_adjacency_list_cte(selecto, join, config)
    cte_sql = IO.iodata_to_binary(cte_iodata)
    
    # Should use default depth limit of 5
    assert 5 in cte_params
    
    # Should generate valid SQL
    assert String.contains?(cte_sql, "WITH RECURSIVE")
    assert String.contains?(cte_sql, "nodes_hierarchy")
  end
  
  test "build_materialized_path_query generates path-based SQL" do
    selecto = %{postgrex_opts: :mock_connection}
    join = :categories
    
    config = %{
      source: "categories",
      path_field: "path",
      path_separator: "/",
      root_path: "root"
    }
    
    {query_iodata, query_params} = Hierarchy.build_materialized_path_query(selecto, join, config)
    query_sql = IO.iodata_to_binary(query_iodata)
    
    # Should generate materialized path CTE
    assert String.contains?(query_sql, "WITH categories_materialized_path AS")
    
    # Should include depth calculation
    assert String.contains?(query_sql, "length(path)")
    assert String.contains?(query_sql, "replace(path, '/', '')")
    
    # Should include path array
    assert String.contains?(query_sql, "string_to_array(path, '/')")
    
    # Should filter by path pattern
    assert String.contains?(query_sql, "WHERE path LIKE $1")
    
    # Should have path pattern parameter
    assert ["root/%"] == query_params
  end
  
  test "build_materialized_path_query with default values" do
    selecto = %{postgrex_opts: :mock_connection}
    join = :items
    
    config = %{
      source: "menu_items"
    }
    
    {query_iodata, query_params} = Hierarchy.build_materialized_path_query(selecto, join, config)
    query_sql = IO.iodata_to_binary(query_iodata)
    
    # Should use default path field
    assert String.contains?(query_sql, "path LIKE $1")
    
    # Should match any path when no root specified
    assert ["%"] == query_params
    
    # Should generate valid CTE name
    assert String.contains?(query_sql, "items_materialized_path")
  end
  
  test "build_closure_table_query generates closure table SQL" do
    selecto = %{postgrex_opts: :mock_connection}
    join = :categories
    
    config = %{
      source: "categories",
      closure_table: "category_closure",
      ancestor_field: "ancestor_id", 
      descendant_field: "descendant_id",
      depth_field: "depth"
    }
    
    {query_iodata, query_params} = Hierarchy.build_closure_table_query(selecto, join, config)
    query_sql = IO.iodata_to_binary(query_iodata)
    
    # Should generate closure table CTE
    assert String.contains?(query_sql, "WITH categories_closure AS")
    
    # Should join main table with closure table
    assert String.contains?(query_sql, "FROM categories c")
    assert String.contains?(query_sql, "JOIN category_closure cl")
    
    # Should include configured fields
    assert String.contains?(query_sql, "cl.depth")
    assert String.contains?(query_sql, "cl.descendant_id")
    
    # Should include descendant count subquery
    assert String.contains?(query_sql, "COUNT(*) FROM category_closure cl2")
    assert String.contains?(query_sql, "cl2.ancestor_id = c.id")
    
    # Basic closure queries don't need parameters
    assert [] == query_params
  end
  
  test "build_closure_table_query with default table name" do
    selecto = %{postgrex_opts: :mock_connection}
    join = :nodes
    
    config = %{
      source: "tree_nodes"
    }
    
    {query_iodata, query_params} = Hierarchy.build_closure_table_query(selecto, join, config)
    query_sql = IO.iodata_to_binary(query_iodata)
    
    # Should default closure table name to source + _closure
    assert String.contains?(query_sql, "tree_nodes_closure")
    
    # Should use default field names  
    assert String.contains?(query_sql, "ancestor_id")
    assert String.contains?(query_sql, "descendant_id")
    assert String.contains?(query_sql, "cl.depth")
    
    assert [] == query_params
  end
end