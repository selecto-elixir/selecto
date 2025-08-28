defmodule Selecto.CteBuilderTest do
  use ExUnit.Case
  alias Selecto.Builder.Cte
  alias Selecto.SQL.Params
  
  describe "build_cte/3" do
    test "builds simple CTE with parameterization" do
      query_iodata = ["SELECT id, name FROM users WHERE active = ", {:param, true}]
      params = [true]
      
      {cte_iodata, result_params} = Cte.build_cte("active_users", query_iodata, params)
      
      assert result_params == [true]
      
      # Verify iodata structure contains expected CTE format
      assert cte_iodata == ["active_users", " AS (", query_iodata, ")"]
    end
    
    test "builds CTE with no parameters" do
      query_iodata = ["SELECT id, name FROM users"]
      params = []
      
      {cte_iodata, result_params} = Cte.build_cte("all_users", query_iodata, params)
      
      assert result_params == []
      assert cte_iodata == ["all_users", " AS (", query_iodata, ")"]
    end
    
    test "builds CTE with multiple parameters" do
      query_iodata = [
        "SELECT id, name FROM users WHERE active = ", {:param, true},
        " AND created_at > ", {:param, ~D[2024-01-01]}
      ]
      params = [true, ~D[2024-01-01]]
      
      {cte_iodata, result_params} = Cte.build_cte("filtered_users", query_iodata, params)
      
      assert result_params == [true, ~D[2024-01-01]]
      assert cte_iodata == ["filtered_users", " AS (", query_iodata, ")"]
    end
  end
  
  describe "build_recursive_cte/5" do
    test "builds recursive CTE with proper UNION ALL structure" do
      base_iodata = ["SELECT id, name, 0 as level FROM categories WHERE parent_id IS NULL"]
      base_params = []
      
      recursive_iodata = [
        "SELECT c.id, c.name, p.level + 1 FROM categories c ", 
        "JOIN category_tree p ON c.parent_id = p.id WHERE p.level < ", {:param, 5}
      ]
      recursive_params = [5]
      
      {cte_iodata, params} = Cte.build_recursive_cte(
        "category_tree", base_iodata, base_params, recursive_iodata, recursive_params
      )
      
      assert params == [5]
      
      expected_cte = [
        "RECURSIVE ", "category_tree", " AS (",
        base_iodata,
        " UNION ALL ",
        recursive_iodata,
        ")"
      ]
      assert cte_iodata == expected_cte
    end
    
    test "builds recursive CTE with parameters in both base and recursive parts" do
      base_iodata = ["SELECT id, name, 0 as level FROM categories WHERE type = ", {:param, "main"}]
      base_params = ["main"]
      
      recursive_iodata = [
        "SELECT c.id, c.name, p.level + 1 FROM categories c ", 
        "JOIN hierarchy p ON c.parent_id = p.id WHERE p.level < ", {:param, 3}
      ]
      recursive_params = [3]
      
      {cte_iodata, params} = Cte.build_recursive_cte(
        "hierarchy", base_iodata, base_params, recursive_iodata, recursive_params
      )
      
      assert params == ["main", 3]
      
      expected_cte = [
        "RECURSIVE ", "hierarchy", " AS (",
        base_iodata,
        " UNION ALL ",
        recursive_iodata,
        ")"
      ]
      assert cte_iodata == expected_cte
    end
  end
  
  describe "build_with_clause/1" do
    test "builds WITH clause for single CTE" do
      cte1 = {["users_cte AS (SELECT * FROM users)"], []}
      
      {with_clause, params} = Cte.build_with_clause([cte1])
      
      assert params == []
      assert with_clause == ["WITH ", [["users_cte AS (SELECT * FROM users)"]], " "]
    end
    
    test "builds WITH clause for multiple CTEs" do
      cte1 = {["users_cte AS (SELECT * FROM users)"], []}
      cte2 = {["posts_cte AS (SELECT * FROM posts WHERE active = ", {:param, true}, ")"], [true]} 
      
      {with_clause, params} = Cte.build_with_clause([cte1, cte2])
      
      assert params == [true]
      
      expected_with = [
        "WITH ", 
        [["users_cte AS (SELECT * FROM users)"], ", ", ["posts_cte AS (SELECT * FROM posts WHERE active = ", {:param, true}, ")"]], 
        " "
      ]
      assert with_clause == expected_with
    end
    
    test "returns empty for no CTEs" do
      {with_clause, params} = Cte.build_with_clause([])
      
      assert params == []
      assert with_clause == []
    end
    
    test "handles CTEs with complex parameters" do
      cte1 = {["cte1 AS (SELECT * FROM t1 WHERE id = ", {:param, 1}, ")"], [1]}
      cte2 = {["cte2 AS (SELECT * FROM t2 WHERE active = ", {:param, true}, " AND type = ", {:param, "test"}, ")"], [true, "test"]}
      
      {_with_clause, params} = Cte.build_with_clause([cte1, cte2])
      
      assert params == [1, true, "test"]
    end
  end
  
  describe "integrate_ctes_with_query/3" do
    test "returns main query when no CTEs provided" do
      main_query = ["SELECT * FROM users"]
      main_params = []
      
      {final_query, params} = Cte.integrate_ctes_with_query([], main_query, main_params)
      
      assert final_query == main_query
      assert params == []
    end
    
    test "prepends single CTE to main query" do
      cte1 = {["users_cte AS (SELECT * FROM users)"], []}
      main_query = ["SELECT * FROM users_cte"]
      main_params = []
      
      {final_query, params} = Cte.integrate_ctes_with_query([cte1], main_query, main_params)
      
      expected_query = [
        ["WITH ", [["users_cte AS (SELECT * FROM users)"]], " "],
        main_query
      ]
      assert final_query == expected_query
      assert params == []
    end
    
    test "handles CTEs with parameters and main query parameters" do
      cte1 = {["active_users AS (SELECT * FROM users WHERE active = ", {:param, true}, ")"], [true]}
      main_query = ["SELECT * FROM active_users WHERE created_at > ", {:param, ~D[2024-01-01]}]
      main_params = [~D[2024-01-01]]
      
      {final_query, params} = Cte.integrate_ctes_with_query([cte1], main_query, main_params)
      
      assert params == [true, ~D[2024-01-01]]
      
      expected_query = [
        ["WITH ", [["active_users AS (SELECT * FROM users WHERE active = ", {:param, true}, ")"]], " "],
        main_query
      ]
      assert final_query == expected_query
    end
    
    test "handles multiple CTEs with complex parameter coordination" do
      cte1 = {["cte1 AS (SELECT * FROM t1 WHERE id = ", {:param, 1}, ")"], [1]}
      cte2 = {["cte2 AS (SELECT * FROM t2 WHERE type = ", {:param, "test"}, ")"], ["test"]}
      main_query = ["SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id WHERE active = ", {:param, true}]
      main_params = [true]
      
      {_final_query, params} = Cte.integrate_ctes_with_query([cte1, cte2], main_query, main_params)
      
      assert params == [1, "test", true]
    end
  end
  
  describe "end-to-end CTE generation and parameterization" do
    test "recursive CTE generates valid SQL when finalized" do
      # Build a complete recursive hierarchy CTE
      base_query = ["SELECT id, name, parent_id, 0 as level FROM categories WHERE parent_id IS NULL"]
      recursive_query = [
        "SELECT c.id, c.name, c.parent_id, h.level + 1 FROM categories c ",
        "JOIN hierarchy h ON c.parent_id = h.id WHERE h.level < ", {:param, 5}
      ]
      
      {hierarchy_cte, cte_params} = Cte.build_recursive_cte(
        "hierarchy", base_query, [], recursive_query, [5]
      )
      
      main_query = ["SELECT * FROM hierarchy WHERE level <= ", {:param, 3}]
      main_params = [3]
      
      {final_query, all_params} = Cte.integrate_ctes_with_query(
        [{hierarchy_cte, cte_params}], main_query, main_params
      )
      
      # Finalize to actual SQL
      {sql, final_params} = Params.finalize(final_query)
      
      assert final_params == all_params
      assert String.contains?(sql, "WITH")
      assert String.contains?(sql, "RECURSIVE")
      assert String.contains?(sql, "hierarchy AS")
      assert String.contains?(sql, "UNION ALL")
      assert String.contains?(sql, "$1") # Parameter placeholder
      assert String.contains?(sql, "$2") # Parameter placeholder
    end
  end

  # Tests for Selecto-powered CTE functions (Phase 1.5+)
  describe "build_cte_from_selecto/2" do
    setup do
      # Create a mock domain for testing
      domain = %{
        name: "Test Domain",
        source: %{
          source_table: "users",
          primary_key: :id,
          fields: [:id, :name, :email, :active],
          redact_fields: [],
          columns: %{
            "id" => %{type: :integer, requires_join: nil},
            "name" => %{type: :string, requires_join: nil},
            "email" => %{type: :string, requires_join: nil},
            "active" => %{type: :boolean, requires_join: nil}
          },
          associations: %{}
        },
        schemas: %{},
        default_selected: ["id", "name"],
        joins: %{},
        filters: %{}
      }
      
      {:ok, domain: domain}
    end

    test "builds CTE from simple Selecto struct", %{domain: domain} do
      selecto = Selecto.configure(domain, nil)
        |> Selecto.select(["id", "name"])
        |> Selecto.filter([{"active", true}])
      
      {cte_iodata, params} = Cte.build_cte_from_selecto("active_users", selecto)
      
      # Should include the CTE name and AS clause
      assert [cte_name, " AS (", _query_sql, ")"] = cte_iodata
      assert cte_name == "active_users"
      assert params == [true]
    end

    test "builds CTE from Selecto struct with multiple filters", %{domain: domain} do
      selecto = Selecto.configure(domain, nil)
        |> Selecto.select(["id", "name", "email"])
        |> Selecto.filter([{"active", true}, {"name", {:like, "%admin%"}}])
      
      {_cte_iodata, params} = Cte.build_cte_from_selecto("admin_users", selecto)
      
      # Should preserve all filter parameters
      assert length(params) == 2
      assert true in params
      assert "%admin%" in params
    end

    test "builds CTE from Selecto struct with no filters", %{domain: domain} do
      selecto = Selecto.configure(domain, nil)
        |> Selecto.select(["id", "name"])
      
      {cte_iodata, params} = Cte.build_cte_from_selecto("all_users", selecto)
      
      # Should have no parameters for unfiltered query
      assert params == []
      assert [cte_name, " AS (", _query_sql, ")"] = cte_iodata
      assert cte_name == "all_users"
    end
  end

  describe "build_recursive_cte_from_selecto/3" do
    setup do
      # Create a hierarchical domain for testing
      domain = %{
        name: "Categories Domain",
        source: %{
          source_table: "categories",
          primary_key: :id,
          fields: [:id, :name, :parent_id],
          redact_fields: [],
          columns: %{
            "id" => %{type: :integer, requires_join: nil},
            "name" => %{type: :string, requires_join: nil}, 
            "parent_id" => %{type: :integer, requires_join: nil}
          },
          associations: %{}
        },
        schemas: %{},
        default_selected: ["id", "name"],
        joins: %{},
        filters: %{}
      }
      
      {:ok, domain: domain}
    end

    test "builds recursive CTE from two Selecto structs", %{domain: domain} do
      base_case = Selecto.configure(domain, nil)
        |> Selecto.select(["id", "name", "parent_id"])
        |> Selecto.filter([{"parent_id", nil}])
      
      recursive_case = Selecto.configure(domain, nil)
        |> Selecto.select(["id", "name", "parent_id"])
        |> Selecto.filter([{"active", true}])
      
      {recursive_cte, params} = Cte.build_recursive_cte_from_selecto(
        "category_tree", base_case, recursive_case
      )
      
      # Should include RECURSIVE keyword and UNION ALL
      assert ["RECURSIVE ", cte_name | _rest] = recursive_cte
      assert cte_name == "category_tree"
      
      # Should combine parameters from both queries
      assert true in params  # From recursive case filter
    end

    test "handles recursive CTE with no parameters", %{domain: domain} do
      base_case = Selecto.configure(domain, nil)
        |> Selecto.select(["id", "name"])
      
      recursive_case = Selecto.configure(domain, nil)
        |> Selecto.select(["id", "name"])
      
      {_recursive_cte, params} = Cte.build_recursive_cte_from_selecto(
        "simple_tree", base_case, recursive_case
      )
      
      assert params == []
    end
  end

  describe "build_with_clause_from_selecto/1" do
    setup do
      domain = %{
        name: "Users Domain",
        source: %{
          source_table: "users",
          primary_key: :id,
          fields: [:id, :name, :active],
          redact_fields: [],
          columns: %{
            "id" => %{type: :integer, requires_join: nil},
            "name" => %{type: :string, requires_join: nil},
            "active" => %{type: :boolean, requires_join: nil}
          },
          associations: %{}
        },
        schemas: %{},
        default_selected: ["id", "name"],
        joins: %{},
        filters: %{}
      }
      
      {:ok, domain: domain}
    end

    test "builds WITH clause from multiple Selecto queries", %{domain: domain} do
      active_users = Selecto.configure(domain, nil)
        |> Selecto.select(["id", "name"])
        |> Selecto.filter([{"active", true}])
      
      all_users = Selecto.configure(domain, nil)
        |> Selecto.select(["id", "name"])
      
      cte_queries = [
        {"active_users", active_users},
        {"all_users", all_users}
      ]
      
      {with_clause, params} = Cte.build_with_clause_from_selecto(cte_queries)
      
      # Should start with WITH keyword
      assert ["WITH " | _rest] = with_clause
      
      # Should include parameter from active_users filter
      assert true in params
    end

    test "handles empty CTE queries list" do
      {with_clause, params} = Cte.build_with_clause_from_selecto([])
      
      assert with_clause == []
      assert params == []
    end
  end

  describe "build_hierarchy_cte_from_selecto/4" do
    setup do
      domain = %{
        name: "Categories Domain", 
        source: %{
          source_table: "categories",
          primary_key: :id,
          fields: [:id, :name, :parent_id],
          redact_fields: [],
          columns: %{
            "id" => %{type: :integer, requires_join: nil},
            "name" => %{type: :string, requires_join: nil},
            "parent_id" => %{type: :integer, requires_join: nil}
          },
          associations: %{}
        },
        schemas: %{},
        default_selected: ["id", "name"],
        joins: %{},
        filters: %{}
      }
      
      {:ok, domain: domain}
    end

    test "builds hierarchy CTE with default options", %{domain: domain} do
      {hierarchy_cte, params} = Cte.build_hierarchy_cte_from_selecto(
        "category_hierarchy", domain, nil
      )
      
      # Should be recursive CTE
      assert ["RECURSIVE ", cte_name | _rest] = hierarchy_cte
      assert cte_name == "category_hierarchy"
      
      # Should include depth limit parameter (default 5)
      assert 5 in params
    end

    test "builds hierarchy CTE with custom options", %{domain: domain} do
      opts = %{
        id_field: "category_id",
        name_field: "title", 
        parent_field: "parent_category_id",
        depth_limit: 3,
        additional_fields: ["description"]
      }
      
      {_hierarchy_cte, params} = Cte.build_hierarchy_cte_from_selecto(
        "custom_hierarchy", domain, nil, opts
      )
      
      # Should use custom depth limit
      assert 3 in params
    end

    test "builds hierarchy CTE with custom root condition", %{domain: domain} do
      opts = %{
        root_condition: [{"parent_id", nil}, {"active", true}]
      }
      
      {_hierarchy_cte, params} = Cte.build_hierarchy_cte_from_selecto(
        "active_hierarchy", domain, nil, opts
      )
      
      # Should include parameters from root condition
      assert true in params  # From active filter
      assert 5 in params     # Default depth limit
    end
  end
end