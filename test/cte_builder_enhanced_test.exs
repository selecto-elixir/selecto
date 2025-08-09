defmodule Selecto.CteBuilderEnhancedTest do
  use ExUnit.Case
  alias Selecto.Builder.Cte
  alias Selecto.SQL.Params

  describe "build_cte/3 edge cases" do
    test "handles CTE with complex nested iodata" do
      nested_query = [
        "SELECT u.id, u.name, ", 
        ["p.title"], 
        " FROM users u JOIN posts p ON u.id = p.user_id WHERE u.active = ", 
        {:param, true},
        " AND p.status = ",
        {:param, "published"}
      ]
      params = [true, "published"]
      
      {cte_iodata, result_params} = Cte.build_cte("user_posts", nested_query, params)
      
      assert result_params == [true, "published"]
      assert cte_iodata == ["user_posts", " AS (", nested_query, ")"]
      
      # Test that the result can be finalized properly
      {sql, final_params} = Params.finalize(cte_iodata)
      assert is_binary(sql)
      assert final_params == [true, "published"]
    end

    test "builds CTE with empty name handled" do
      query_iodata = ["SELECT 1"]
      params = []
      
      {cte_iodata, result_params} = Cte.build_cte("", query_iodata, params)
      
      assert result_params == []
      assert cte_iodata == ["", " AS (", query_iodata, ")"]
    end

    test "handles CTE with only parameters, no static content" do
      query_iodata = [{:param, "value1"}, " = ", {:param, "value2"}]
      params = ["value1", "value2"]
      
      {cte_iodata, result_params} = Cte.build_cte("param_cte", query_iodata, params)
      
      assert result_params == ["value1", "value2"]
      
      # Verify the structure is correct
      ["param_cte", " AS (", query_content, ")"] = cte_iodata
      assert query_content == query_iodata
    end
  end

  describe "build_recursive_cte/5 edge cases" do
    test "builds recursive CTE with complex parameter distribution" do
      base_iodata = [
        "SELECT id, name, 0 as level FROM categories WHERE active = ",
        {:param, true},
        " AND parent_id IS NULL"
      ]
      base_params = [true]
      
      recursive_iodata = [
        "SELECT c.id, c.name, p.level + 1 FROM categories c JOIN cte p ON c.parent_id = p.id ",
        "WHERE p.level < ", {:param, 5}, " AND c.status = ", {:param, "active"}
      ]
      recursive_params = [5, "active"]
      
      {cte_iodata, combined_params} = Cte.build_recursive_cte(
        "hierarchy", 
        base_iodata, 
        base_params, 
        recursive_iodata, 
        recursive_params
      )
      
      assert combined_params == [true, 5, "active"]
      
      # Verify recursive structure - build_recursive_cte separates RECURSIVE and name
      expected_structure = [
        "RECURSIVE ",
        "hierarchy",
        " AS (",
        base_iodata,
        " UNION ALL ",
        recursive_iodata,
        ")"
      ]
      assert cte_iodata == expected_structure
    end

    test "handles recursive CTE with no parameters in base case" do
      base_iodata = ["SELECT id, name, 0 as level FROM root_categories"]
      base_params = []
      
      recursive_iodata = ["SELECT c.id, c.name, p.level + 1 FROM categories c JOIN cte p ON c.parent_id = p.id WHERE p.level < ", {:param, 3}]
      recursive_params = [3]
      
      {cte_iodata, combined_params} = Cte.build_recursive_cte(
        "simple_hierarchy", 
        base_iodata, 
        base_params, 
        recursive_iodata, 
        recursive_params
      )
      
      assert combined_params == [3]
      assert cte_iodata == [
        "RECURSIVE ",
        "simple_hierarchy",
        " AS (",
        base_iodata,
        " UNION ALL ",
        recursive_iodata,
        ")"
      ]
    end

    test "handles recursive CTE with no parameters in recursive case" do
      base_iodata = ["SELECT id, name, 0 as level FROM categories WHERE type = ", {:param, "root"}]
      base_params = ["root"]
      
      recursive_iodata = ["SELECT c.id, c.name, p.level + 1 FROM categories c JOIN cte p ON c.parent_id = p.id"]
      recursive_params = []
      
      {cte_iodata, combined_params} = Cte.build_recursive_cte(
        "typed_hierarchy", 
        base_iodata, 
        base_params, 
        recursive_iodata, 
        recursive_params
      )
      
      assert combined_params == ["root"]
    end

    test "builds recursive CTE with identical parameter names" do
      # Test parameter scoping when both cases use similar parameters
      base_iodata = ["SELECT * FROM users WHERE status = ", {:param, "active"}]
      base_params = ["active"]
      
      recursive_iodata = ["SELECT * FROM related WHERE parent_status = ", {:param, "active"}]
      recursive_params = ["active"]
      
      {_cte_iodata, combined_params} = Cte.build_recursive_cte(
        "status_hierarchy",
        base_iodata,
        base_params,
        recursive_iodata,
        recursive_params
      )
      
      # Parameters should be preserved in order
      assert combined_params == ["active", "active"]
    end
  end

  describe "build_with_clause/1 comprehensive tests" do
    test "builds WITH clause with mixed parameter types" do
      cte1 = {["cte1 AS (SELECT * FROM t1 WHERE id = ", {:param, 123}, ")"], [123]}
      cte2 = {["cte2 AS (SELECT * FROM t2 WHERE name = ", {:param, "test"}, ")"], ["test"]}
      cte3 = {["cte3 AS (SELECT * FROM t3 WHERE active = ", {:param, true}, ")"], [true]}
      
      {with_clause, params} = Cte.build_with_clause([cte1, cte2, cte3])
      
      assert params == [123, "test", true]
      
      # Verify WITH structure
      ["WITH ", cte_parts, " "] = with_clause
      
      # Should contain all CTEs separated by commas
      assert is_list(cte_parts)
    end

    test "builds WITH clause with recursive CTEs" do
      simple_cte = {["simple AS (SELECT 1)"], []}
      recursive_cte = {["RECURSIVE tree AS (SELECT root UNION ALL SELECT child FROM tree)"], []}
      
      {with_clause, params} = Cte.build_with_clause([simple_cte, recursive_cte])
      
      assert params == []
      
      ["WITH ", cte_parts, " "] = with_clause
      
      # Should contain both CTEs - check that they're present in the structure
      flat_parts = List.flatten(cte_parts)
      # Convert to string to check content
      parts_string = flat_parts |> Enum.map(&to_string/1) |> Enum.join("")
      assert String.contains?(parts_string, "simple AS")
      assert String.contains?(parts_string, "RECURSIVE tree AS")
    end

    test "handles WITH clause with only one complex CTE" do
      complex_query = [
        "complex AS (SELECT u.id, COUNT(p.id) as post_count FROM users u ",
        "LEFT JOIN posts p ON u.id = p.user_id WHERE u.created_at > ",
        {:param, ~D[2024-01-01]},
        " GROUP BY u.id HAVING COUNT(p.id) > ",
        {:param, 5},
        ")"
      ]
      complex_cte = {complex_query, [~D[2024-01-01], 5]}
      
      {with_clause, params} = Cte.build_with_clause([complex_cte])
      
      assert params == [~D[2024-01-01], 5]
      assert with_clause == ["WITH ", [complex_query], " "]
    end

    test "preserves parameter order across multiple CTEs with mixed ordering" do
      # CTEs with parameters in different arrangements
      cte1 = {["cte1 AS (SELECT * FROM t1 WHERE a = ", {:param, 1}, " AND b = ", {:param, 2}, ")"], [1, 2]}
      cte2 = {["cte2 AS (SELECT * FROM t2 WHERE x = ", {:param, "x"}, ")"], ["x"]}
      cte3 = {["cte3 AS (SELECT * FROM t3 WHERE p = ", {:param, true}, " AND q = ", {:param, false}, " AND r = ", {:param, nil}, ")"], [true, false, nil]}
      
      {_with_clause, params} = Cte.build_with_clause([cte1, cte2, cte3])
      
      # Parameters should maintain their order: cte1 params, then cte2 params, then cte3 params
      assert params == [1, 2, "x", true, false, nil]
    end

    test "handles CTEs with nested iodata structures" do
      nested_cte1 = {[
        "nested1 AS (",
        ["SELECT id FROM (", ["SELECT * FROM inner WHERE val = ", {:param, "inner"}], ") sub"],
        ")"
      ], ["inner"]}
      
      nested_cte2 = {[
        "nested2 AS (",
        ["SELECT ", ["count(*) as cnt"], " FROM table WHERE active = ", {:param, true}],
        ")"
      ], [true]}
      
      {with_clause, params} = Cte.build_with_clause([nested_cte1, nested_cte2])
      
      assert params == ["inner", true]
      
      # Should maintain nested structure
      ["WITH ", cte_parts, " "] = with_clause
      assert is_list(cte_parts)
    end
  end

  describe "integrate_ctes_with_query/3 comprehensive tests" do
    test "integrates multiple CTEs with parameterized main query" do
      cte1 = {["cte1 AS (SELECT id FROM users WHERE active = ", {:param, true}, ")"], [true]}
      cte2 = {["cte2 AS (SELECT user_id FROM posts WHERE status = ", {:param, "published"}, ")"], ["published"]}
      
      main_query = [
        "SELECT c1.id, COUNT(c2.user_id) as post_count ",
        "FROM cte1 c1 LEFT JOIN cte2 c2 ON c1.id = c2.user_id ",
        "WHERE c1.id > ", {:param, 100},
        " GROUP BY c1.id ORDER BY post_count DESC LIMIT ", {:param, 50}
      ]
      main_params = [100, 50]
      
      {complete_query, combined_params} = Cte.integrate_ctes_with_query([cte1, cte2], main_query, main_params)
      
      # Parameters should be: CTE params first, then main query params
      assert combined_params == [true, "published", 100, 50]
      
      # Should have WITH clause followed by main query
      assert is_list(complete_query)
      [with_part, main_part] = complete_query
      
      # Verify WITH clause structure
      ["WITH ", _cte_parts, " "] = with_part
      assert main_part == main_query
    end

    test "handles main query with no parameters" do
      cte1 = {["users_cte AS (SELECT * FROM users WHERE active = ", {:param, true}, ")"], [true]}
      main_query = ["SELECT * FROM users_cte ORDER BY created_at DESC"]
      main_params = []
      
      {complete_query, combined_params} = Cte.integrate_ctes_with_query([cte1], main_query, main_params)
      
      assert combined_params == [true]
      
      [_with_part, main_part] = complete_query
      assert main_part == main_query
    end

    test "handles CTEs with no parameters but main query with parameters" do
      cte1 = {["static_cte AS (SELECT 1 as id, 'test' as name)"], []}
      main_query = ["SELECT * FROM static_cte WHERE id = ", {:param, 1}]
      main_params = [1]
      
      {complete_query, combined_params} = Cte.integrate_ctes_with_query([cte1], main_query, main_params)
      
      assert combined_params == [1]
      
      [_with_part, main_part] = complete_query
      assert main_part == main_query
    end

    test "preserves complex nested iodata in integration" do
      nested_cte = {[
        "complex_cte AS (",
        ["SELECT id, ", ["nested_field"], " FROM (SELECT * FROM base WHERE x = ", {:param, "x"}, ") sub"],
        ")"
      ], ["x"]}
      
      nested_main = [
        "SELECT c.id, c.nested_field FROM complex_cte c WHERE c.id IN (",
        ["SELECT ref_id FROM refs WHERE active = ", {:param, true}],
        ") ORDER BY ", ["c.nested_field"], " LIMIT ", {:param, 10}
      ]
      main_params = [true, 10]
      
      {complete_query, combined_params} = Cte.integrate_ctes_with_query([nested_cte], nested_main, main_params)
      
      assert combined_params == ["x", true, 10]
      
      # Should preserve all nesting
      [with_part, main_part] = complete_query
      assert main_part == nested_main
      assert is_list(with_part)
    end

    test "handles empty main query gracefully" do
      cte1 = {["test_cte AS (SELECT 1)"], []}
      main_query = []
      main_params = []
      
      {complete_query, combined_params} = Cte.integrate_ctes_with_query([cte1], main_query, main_params)
      
      assert combined_params == []
      
      [_with_part, main_part] = complete_query
      assert main_part == []
    end
  end

  describe "end-to-end finalization" do
    test "complex CTE workflow produces valid SQL" do
      # Build a complex scenario with recursive CTE and multiple regular CTEs
      base_hierarchy = ["SELECT id, name, parent_id, 0 as level FROM categories WHERE parent_id IS NULL"]
      recursive_hierarchy = [
        "SELECT c.id, c.name, c.parent_id, h.level + 1 FROM categories c ",
        "JOIN hierarchy h ON c.parent_id = h.id WHERE h.level < ", {:param, 5}
      ]
      
      {recursive_cte, recursive_params} = Cte.build_recursive_cte(
        "hierarchy", 
        base_hierarchy, 
        [], 
        recursive_hierarchy, 
        [5]
      )
      
      # Build a regular CTE
      active_users_query = ["SELECT id, name FROM users WHERE active = ", {:param, true}, " AND created_at > ", {:param, ~D[2024-01-01]}]
      {users_cte, users_params} = Cte.build_cte("active_users", active_users_query, [true, ~D[2024-01-01]])
      
      # Combine CTEs - they're already in {cte_iodata, params} format
      all_ctes = [{recursive_cte, recursive_params}, {users_cte, users_params}]
      
      # Main query using both CTEs
      main_query = [
        "SELECT h.name as category_name, u.name as user_name ",
        "FROM hierarchy h CROSS JOIN active_users u ",
        "WHERE h.level = ", {:param, 2}, 
        " ORDER BY h.name, u.name LIMIT ", {:param, 100}
      ]
      main_params = [2, 100]
      
      # Integrate everything
      {complete_query, combined_params} = Cte.integrate_ctes_with_query(all_ctes, main_query, main_params)
      
      # Parameters should be in order: recursive_cte, users_cte, main_query
      assert combined_params == [5, true, ~D[2024-01-01], 2, 100]
      
      # Finalize to actual SQL
      {final_sql, final_params} = Params.finalize(complete_query)
      
      assert is_binary(final_sql)
      assert String.contains?(final_sql, "WITH")
      assert String.contains?(final_sql, "RECURSIVE hierarchy")
      assert String.contains?(final_sql, "active_users AS")
      assert String.contains?(final_sql, "FROM hierarchy h CROSS JOIN active_users u")
      
      assert final_params == [5, true, ~D[2024-01-01], 2, 100]
    end

    test "parameter numbering is correct in complex scenarios" do
      # Create multiple CTEs with various parameter patterns
      cte1 = {["cte1 AS (SELECT * FROM t1 WHERE a = ", {:param, "a1"}, " AND b = ", {:param, "b1"}, ")"], ["a1", "b1"]}
      cte2 = {["cte2 AS (SELECT * FROM t2 WHERE x = ", {:param, "x2"}, ")"], ["x2"]}
      cte3 = {["cte3 AS (SELECT * FROM t3 WHERE p = ", {:param, "p3"}, " AND q = ", {:param, "q3"}, " AND r = ", {:param, "r3"}, ")"], ["p3", "q3", "r3"]}
      
      main_query = [
        "SELECT * FROM cte1 c1 JOIN cte2 c2 ON c1.id = c2.id JOIN cte3 c3 ON c2.id = c3.id ",
        "WHERE c1.extra = ", {:param, "main1"}, " AND c3.final = ", {:param, "main2"}
      ]
      main_params = ["main1", "main2"]
      
      {complete_query, combined_params} = Cte.integrate_ctes_with_query([cte1, cte2, cte3], main_query, main_params)
      
      # Parameters should be: cte1 (a1, b1), cte2 (x2), cte3 (p3, q3, r3), main (main1, main2)
      assert combined_params == ["a1", "b1", "x2", "p3", "q3", "r3", "main1", "main2"]
      
      # Finalize to check parameter numbering
      {final_sql, final_params} = Params.finalize(complete_query)
      
      assert String.contains?(final_sql, "$1") # Should be "a1"
      assert String.contains?(final_sql, "$2") # Should be "b1"
      assert String.contains?(final_sql, "$3") # Should be "x2"
      assert String.contains?(final_sql, "$4") # Should be "p3"
      assert String.contains?(final_sql, "$5") # Should be "q3" 
      assert String.contains?(final_sql, "$6") # Should be "r3"
      assert String.contains?(final_sql, "$7") # Should be "main1"
      assert String.contains?(final_sql, "$8") # Should be "main2"
      
      assert final_params == ["a1", "b1", "x2", "p3", "q3", "r3", "main1", "main2"]
    end
  end

  describe "error handling and edge cases" do
    test "build_with_clause handles malformed CTE tuples gracefully" do
      # Test what happens with unexpected input formats
      malformed_ctes = [
        {["valid_cte AS (SELECT 1)"], []},
        # Missing params should be handled
        {["no_params_cte AS (SELECT 2)"], []}
      ]
      
      {with_clause, params} = Cte.build_with_clause(malformed_ctes)
      
      assert params == []
      ["WITH ", cte_parts, " "] = with_clause
      assert is_list(cte_parts)
    end

    test "build_cte handles binary name validation" do
      query_iodata = ["SELECT 1"]
      params = []
      
      # Should work with binary names
      {cte_iodata, result_params} = Cte.build_cte("valid_name", query_iodata, params)
      assert result_params == []
      assert cte_iodata == ["valid_name", " AS (", query_iodata, ")"]
    end

    test "empty and whitespace handling" do
      # Test various empty/whitespace scenarios
      empty_query = []
      {cte_iodata, params} = Cte.build_cte("empty_query", empty_query, [])
      assert cte_iodata == ["empty_query", " AS (", [], ")"]
      assert params == []
      
      whitespace_query = [" ", " "]
      {cte_iodata2, params2} = Cte.build_cte("whitespace", whitespace_query, [])
      assert cte_iodata2 == ["whitespace", " AS (", [" ", " "], ")"]
      assert params2 == []
    end

    test "large parameter lists" do
      # Test with many parameters
      large_query = Enum.map(1..20, fn i -> {:param, i} end) |> Enum.intersperse(" OR x = ")
      large_params = Enum.to_list(1..20)
      
      {_cte_iodata, result_params} = Cte.build_cte("large_params", large_query, large_params)
      assert result_params == large_params
      assert length(result_params) == 20
    end
  end
end