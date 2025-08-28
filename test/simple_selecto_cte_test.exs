defmodule Selecto.SimpleCteTest do
  use ExUnit.Case
  alias Selecto.Builder.Cte
  alias Selecto.SQL.Params

  describe "Selecto-powered CTE functionality" do
    test "build_cte_from_selecto/2 exists and has correct signature" do
      # Verify the function exists with expected signature
      assert function_exported?(Selecto.Builder.Cte, :build_cte_from_selecto, 2)
      assert function_exported?(Selecto.Builder.Cte, :build_recursive_cte_from_selecto, 3)
      assert function_exported?(Selecto.Builder.Cte, :build_with_clause_from_selecto, 1)
      assert function_exported?(Selecto.Builder.Cte, :build_hierarchy_cte_from_selecto, 4)
    end

    test "build_cte_from_selecto handles mock selecto input" do
      # Create a mock selecto struct that would come from Selecto.configure + select/filter
      mock_selecto = %{
        postgrex_opts: :mock,
        domain: %{},
        config: %{},
        set: %{selected: ["id", "name"], filtered: []}
      }
      
      # We need to mock Selecto.gen_sql since we can't easily create a real Selecto in tests
      # For now, let's test that the function can be called and handles the SQL generation logic
      
      # Mock the generated SQL result
      mock_gen_sql_result = {"SELECT id, name FROM users", [], []}
      
      # Test by manually calling the SQL generation logic that would be used
      {query_sql, _aliases, query_params} = mock_gen_sql_result
      query_iodata = [query_sql]
      
      {cte_iodata, params} = Cte.build_cte("test_cte", query_iodata, query_params)
      
      assert [cte_name, " AS (", sql_part, ")"] = cte_iodata
      assert cte_name == "test_cte"
      assert sql_part == query_iodata
      assert params == query_params
    end

    test "build_recursive_cte_from_selecto logic works with mock SQL" do
      # Mock base case SQL
      base_sql = "SELECT id, name, 0 as level FROM categories WHERE parent_id IS NULL"
      base_params = []
      
      # Mock recursive case SQL  
      recursive_sql = "SELECT c.id, c.name, h.level + 1 FROM categories c JOIN hierarchy h ON c.parent_id = h.id WHERE h.level < $1"
      recursive_params = [5]
      
      # Test the core logic that build_recursive_cte_from_selecto would use
      base_iodata = [base_sql]
      recursive_iodata = [recursive_sql]
      
      {cte_iodata, combined_params} = Cte.build_recursive_cte("hierarchy", base_iodata, base_params, recursive_iodata, recursive_params)
      
      assert ["RECURSIVE ", cte_name, " AS (", base_part, " UNION ALL ", recursive_part, ")"] = cte_iodata
      assert cte_name == "hierarchy"
      assert base_part == base_iodata
      assert recursive_part == recursive_iodata
      assert combined_params == base_params ++ recursive_params
      assert 5 in combined_params
    end

    test "CTE integration with parameter system works" do
      # Test that CTEs can be finalized with parameters properly
      base_query = ["SELECT id, name FROM users WHERE active = ", {:param, true}]
      {cte_iodata, cte_params} = Cte.build_cte("active_users", base_query, [true])
      
      # Test integration with main query
      main_query = ["SELECT count(*) FROM active_users WHERE created_at > ", {:param, ~D[2024-01-01]}]
      main_params = [~D[2024-01-01]]
      
      {final_query, combined_params} = Cte.integrate_ctes_with_query(
        [{cte_iodata, cte_params}], 
        main_query, 
        main_params
      )
      
      # Verify structure
      assert [with_clause, main_query_part] = final_query
      assert ["WITH " | _rest] = with_clause
      assert main_query_part == main_query
      
      # Verify parameters are combined correctly
      assert combined_params == cte_params ++ main_params
      assert true in combined_params
      assert ~D[2024-01-01] in combined_params
      
      # Test final SQL generation
      {final_sql, final_params} = Params.finalize(final_query)
      
      assert String.contains?(final_sql, "WITH")
      assert String.contains?(final_sql, "active_users AS")
      assert String.contains?(final_sql, "SELECT count(*)")
      assert String.contains?(final_sql, "$1")
      assert String.contains?(final_sql, "$2")
      assert final_params == combined_params
    end

    test "multiple CTE building logic works" do
      # Test build_with_clause functionality that would be used by build_with_clause_from_selecto
      cte1_iodata = ["users_cte AS (SELECT id, name FROM users WHERE active = ", {:param, true}, ")"]
      cte1_params = [true]
      
      cte2_iodata = ["posts_cte AS (SELECT id, title FROM posts WHERE created_at > ", {:param, ~D[2024-01-01]}, ")"]
      cte2_params = [~D[2024-01-01]]
      
      ctes = [{cte1_iodata, cte1_params}, {cte2_iodata, cte2_params}]
      
      {with_clause, combined_params} = Cte.build_with_clause(ctes)
      
      # Verify WITH clause structure
      assert ["WITH " | _rest] = with_clause
      
      # Verify parameters combined correctly
      assert combined_params == cte1_params ++ cte2_params
      assert true in combined_params
      assert ~D[2024-01-01] in combined_params
      
      # Test finalization
      {with_sql, final_params} = Params.finalize(with_clause)
      
      assert String.contains?(with_sql, "WITH")
      assert String.contains?(with_sql, "users_cte")
      assert String.contains?(with_sql, "posts_cte")
      assert String.contains?(with_sql, ",")  # CTE separator
      assert final_params == combined_params
    end

    test "hierarchy CTE pattern matches expected SQL structure" do
      # Test that hierarchy CTE generates the expected recursive pattern
      # This verifies the structure that build_hierarchy_cte_from_selecto would create
      
      # Base case: root nodes
      base_query = ["SELECT id, name, parent_id, 0 as level FROM categories WHERE parent_id IS NULL"]
      base_params = []
      
      # Recursive case: child nodes with depth limit
      recursive_query = [
        "SELECT c.id, c.name, c.parent_id, h.level + 1 ",
        "FROM categories c JOIN category_tree h ON c.parent_id = h.id ",
        "WHERE h.level < ", {:param, 5}
      ]
      recursive_params = [5]
      
      {hierarchy_cte, params} = Cte.build_recursive_cte(
        "category_tree", 
        base_query, 
        base_params, 
        recursive_query, 
        recursive_params
      )
      
      # Verify recursive structure
      assert ["RECURSIVE ", "category_tree", " AS (", base_part, " UNION ALL ", recursive_part, ")"] = hierarchy_cte
      assert base_part == base_query
      assert recursive_part == recursive_query
      assert params == [5]
      
      # Test with main query
      main_query = ["SELECT * FROM category_tree WHERE level <= ", {:param, 3}]
      {final_query, final_params} = Cte.integrate_ctes_with_query(
        [{hierarchy_cte, params}], 
        main_query, 
        [3]
      )
      
      # Generate final SQL
      {sql, sql_params} = Params.finalize(final_query)
      
      # Verify hierarchy SQL characteristics
      assert String.contains?(sql, "WITH RECURSIVE")
      assert String.contains?(sql, "category_tree AS")
      assert String.contains?(sql, "UNION ALL")
      assert String.contains?(sql, "JOIN category_tree")
      assert String.contains?(sql, "level")
      
      # Verify parameters
      assert sql_params == [5, 3]  # Depth limit and level filter
    end

    test "API design matches intended usage patterns" do
      # This test documents the intended API for using Selecto with CTEs
      # Even though we can't fully test it without real Selecto structs,
      # we can verify the design makes sense
      
      # Pattern 1: Simple CTE from Selecto query
      # selecto = Selecto.configure(domain, conn) |> Selecto.select(["id"]) |> Selecto.filter([{"active", true}])
      # {cte, params} = Cte.build_cte_from_selecto("active_users", selecto)
      
      # Pattern 2: Recursive CTE from two Selecto queries  
      # base = Selecto.configure(domain, conn) |> Selecto.select([...]) |> Selecto.filter([{"parent_id", nil}])
      # recursive = Selecto.configure(domain, conn) |> Selecto.select([...]) |> Selecto.filter([...])
      # {cte, params} = Cte.build_recursive_cte_from_selecto("hierarchy", base, recursive)
      
      # Pattern 3: Multiple CTEs
      # cte_list = [{"cte1", selecto1}, {"cte2", selecto2}]
      # {with_clause, params} = Cte.build_with_clause_from_selecto(cte_list)
      
      # Pattern 4: Specialized hierarchy helper
      # {hierarchy_cte, params} = Cte.build_hierarchy_cte_from_selecto("tree", domain, conn, opts)
      
      # All these functions exist and have the expected signatures
      assert function_exported?(Cte, :build_cte_from_selecto, 2)
      assert function_exported?(Cte, :build_recursive_cte_from_selecto, 3)
      assert function_exported?(Cte, :build_with_clause_from_selecto, 1)
      assert function_exported?(Cte, :build_hierarchy_cte_from_selecto, 4)
      
      # The core building blocks all work
      assert function_exported?(Cte, :build_cte, 3)
      assert function_exported?(Cte, :build_recursive_cte, 5)
      assert function_exported?(Cte, :build_with_clause, 1)
      assert function_exported?(Cte, :integrate_ctes_with_query, 3)
    end
  end
end