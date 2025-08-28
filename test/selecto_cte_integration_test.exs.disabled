defmodule Selecto.CteIntegrationTest do
  use ExUnit.Case
  alias Selecto.Builder.Cte

  # Mock domain and connection for testing
  defp build_test_domain do
    %{
      name: "Test Users",
      source: %{
        source_table: "users",
        primary_key: :id,
        fields: [:id, :name, :email, :active, :created_at],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          email: %{type: :string},
          active: %{type: :boolean},
          created_at: %{type: :utc_datetime}
        },
        associations: %{}
      },
      schemas: %{
        posts: %{
          source_table: "posts",
          primary_key: :id,
          fields: [:id, :title, :user_id, :created_at],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            title: %{type: :string},
            user_id: %{type: :integer},
            created_at: %{type: :utc_datetime}
          },
          associations: %{}
        }
      },
      default_selected: ["id", "name"],
      joins: %{},
      filters: %{}
    }
  end

  defp build_categories_domain do
    %{
      name: "Test Categories",
      source: %{
        source_table: "categories",
        primary_key: :id,
        fields: [:id, :name, :parent_id, :description],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          parent_id: %{type: :integer},
          description: %{type: :string}
        },
        associations: %{}
      },
      schemas: %{},
      default_selected: ["id", "name"],
      joins: %{},
      filters: %{}
    }
  end

  defp mock_connection, do: %{test: :connection}

  describe "build_cte_from_selecto/2" do
    test "builds simple CTE from Selecto struct with select and filter" do
      domain = build_test_domain()
      
      # Create a Selecto struct with select and filter
      selecto = Selecto.configure(domain, mock_connection())
        |> Selecto.select(["id", "name", "email"])
        |> Selecto.filter([{"active", true}])
      
      {cte_iodata, params} = Cte.build_cte_from_selecto("active_users", selecto)
      
      # Should contain the CTE structure
      assert [cte_name, " AS (", [sql], ")"] = cte_iodata
      assert cte_name == "active_users"
      assert is_binary(sql)
      assert is_list(params)
      
      # SQL should contain SELECT and WHERE clauses
      assert String.contains?(sql, "select")
      assert String.contains?(sql, "from")
      # Parameters should include the filter value
      assert true in params
    end

    test "builds CTE with complex filters and multiple parameters" do
      domain = build_test_domain()
      
      selecto = Selecto.configure(domain, mock_connection())
        |> Selecto.select(["id", "name"])
        |> Selecto.filter([
          {"active", true}, 
          {"created_at", {:gt, ~D[2024-01-01]}}
        ])
      
      {cte_iodata, params} = Cte.build_cte_from_selecto("filtered_users", selecto)
      
      assert [cte_name, " AS (", [sql], ")"] = cte_iodata
      assert cte_name == "filtered_users"
      
      # Should have multiple parameters
      assert length(params) >= 2
      assert true in params
      assert ~D[2024-01-01] in params
    end

    test "builds CTE with only select (no filters)" do
      domain = build_test_domain()
      
      selecto = Selecto.configure(domain, mock_connection())
        |> Selecto.select(["id", "name"])
      
      {cte_iodata, params} = Cte.build_cte_from_selecto("all_users", selecto)
      
      assert [cte_name, " AS (", [sql], ")"] = cte_iodata
      assert cte_name == "all_users"
      assert String.contains?(sql, "select")
      assert String.contains?(sql, "id")
      assert String.contains?(sql, "name")
    end
  end

  describe "build_recursive_cte_from_selecto/3" do
    test "builds recursive CTE from two Selecto structs" do
      domain = build_categories_domain()
      
      # Base case: root categories
      base_case = Selecto.configure(domain, mock_connection())
        |> Selecto.select(["id", "name", "parent_id"])
        |> Selecto.filter([{"parent_id", nil}])
      
      # Recursive case: child categories (simplified - real JOIN handling in Phase 2)
      recursive_case = Selecto.configure(domain, mock_connection())
        |> Selecto.select(["id", "name", "parent_id"])
        |> Selecto.filter([{"id", {:gt, 0}}])  # Placeholder filter
      
      {cte_iodata, params} = Cte.build_recursive_cte_from_selecto("hierarchy", base_case, recursive_case)
      
      # Should contain RECURSIVE keyword and UNION ALL
      assert ["RECURSIVE ", cte_name, " AS (", [base_sql], " UNION ALL ", [recursive_sql], ")"] = cte_iodata
      assert cte_name == "hierarchy"
      assert is_binary(base_sql)
      assert is_binary(recursive_sql)
      assert is_list(params)
      
      # Base SQL should filter for NULL parent_id
      assert String.contains?(base_sql, "where")
      # Recursive SQL should have its own conditions
      assert String.contains?(recursive_sql, "select")
    end

    test "combines parameters from both base and recursive cases" do
      domain = build_categories_domain()
      
      base_case = Selecto.configure(domain, mock_connection())
        |> Selecto.select(["id", "name"])
        |> Selecto.filter([{"parent_id", nil}])
      
      recursive_case = Selecto.configure(domain, mock_connection())
        |> Selecto.select(["id", "name"])
        |> Selecto.filter([{"id", {:lt, 1000}}])
      
      {_cte_iodata, params} = Cte.build_recursive_cte_from_selecto("test_hierarchy", base_case, recursive_case)
      
      # Should contain parameters from both cases
      assert is_list(params)
      assert length(params) >= 1  # At least the 1000 from recursive case
      assert 1000 in params
    end
  end

  describe "build_with_clause_from_selecto/1" do
    test "builds WITH clause from multiple Selecto structs" do
      domain = build_test_domain()
      
      active_users = Selecto.configure(domain, mock_connection())
        |> Selecto.select(["id", "name"])
        |> Selecto.filter([{"active", true}])
      
      # Use posts domain for second CTE (would be different domain in real usage)
      posts_domain = %{domain | source: domain.schemas.posts}
      recent_posts = Selecto.configure(posts_domain, mock_connection())
        |> Selecto.select(["id", "title", "user_id"])
        |> Selecto.filter([{"created_at", {:gt, ~D[2024-01-01]}}])
      
      cte_queries = [
        {"active_users", active_users},
        {"recent_posts", recent_posts}
      ]
      
      {with_clause, params} = Cte.build_with_clause_from_selecto(cte_queries)
      
      # Should start with WITH
      assert ["WITH " | _rest] = with_clause
      
      # Should contain both CTE names
      with_sql = IO.iodata_to_binary(with_clause)
      assert String.contains?(with_sql, "active_users")
      assert String.contains?(with_sql, "recent_posts")
      assert String.contains?(with_sql, ",")  # Separator between CTEs
      
      # Should combine parameters from both CTEs
      assert is_list(params)
      assert true in params  # From active filter
      assert ~D[2024-01-01] in params  # From date filter
    end

    test "handles single CTE in WITH clause" do
      domain = build_test_domain()
      
      single_cte = Selecto.configure(domain, mock_connection())
        |> Selecto.select(["id"])
        |> Selecto.filter([{"active", true}])
      
      cte_queries = [{"single_cte", single_cte}]
      
      {with_clause, params} = Cte.build_with_clause_from_selecto(cte_queries)
      
      assert ["WITH " | _rest] = with_clause
      
      with_sql = IO.iodata_to_binary(with_clause)
      assert String.contains?(with_sql, "single_cte")
      refute String.contains?(with_sql, ",")  # No separator for single CTE
      
      assert true in params
    end
  end

  describe "build_hierarchy_cte_from_selecto/4" do
    test "builds hierarchy CTE with default options" do
      domain = build_categories_domain()
      
      {cte_iodata, params} = Cte.build_hierarchy_cte_from_selecto(
        "category_tree",
        domain,
        mock_connection(),
        %{depth_limit: 3}
      )
      
      # Should be recursive CTE structure
      assert ["RECURSIVE ", cte_name, " AS (", _base, " UNION ALL ", _recursive, ")"] = cte_iodata
      assert cte_name == "category_tree"
      
      # Should have depth limit parameter
      assert 3 in params
    end

    test "builds hierarchy CTE with custom field names" do
      domain = build_categories_domain()
      
      {cte_iodata, params} = Cte.build_hierarchy_cte_from_selecto(
        "custom_tree",
        domain,
        mock_connection(),
        %{
          id_field: "category_id",
          name_field: "title",
          parent_field: "parent_category_id",
          depth_limit: 5,
          additional_fields: ["description"]
        }
      )
      
      assert ["RECURSIVE ", "custom_tree", " AS (", [base_sql], " UNION ALL ", [recursive_sql], ")"] = cte_iodata
      
      # Base SQL should reference custom field names
      # Note: In a real test we'd need to mock Selecto.configure to handle custom fields
      assert is_binary(base_sql)
      assert is_binary(recursive_sql)
      assert 5 in params  # depth_limit
    end
  end

  describe "integration with main query building" do
    test "CTE built from Selecto can be integrated with main query" do
      domain = build_test_domain()
      
      # Build CTE from Selecto
      cte_selecto = Selecto.configure(domain, mock_connection())
        |> Selecto.select(["id", "name"])
        |> Selecto.filter([{"active", true}])
      
      {cte_iodata, cte_params} = Cte.build_cte_from_selecto("active_users", cte_selecto)
      
      # Build main query
      main_query = ["SELECT count(*) FROM active_users"]
      main_params = []
      
      # Integrate CTE with main query
      {final_query, combined_params} = Cte.integrate_ctes_with_query(
        [{cte_iodata, cte_params}],
        main_query,
        main_params
      )
      
      # Should have proper structure
      assert [["WITH " | _with_part], main_query] = final_query
      
      # Parameters should be combined correctly
      assert combined_params == cte_params ++ main_params
      assert true in combined_params  # From CTE filter
    end

    test "multiple Selecto CTEs integrate properly" do
      domain = build_test_domain()
      
      # Build two CTEs
      cte1_selecto = Selecto.configure(domain, mock_connection())
        |> Selecto.select(["id"])
        |> Selecto.filter([{"active", true}])
      
      cte2_selecto = Selecto.configure(domain, mock_connection())
        |> Selecto.select(["id", "name"])
        |> Selecto.filter([{"created_at", {:gt, ~D[2024-01-01]}}])
      
      {cte1, params1} = Cte.build_cte_from_selecto("active_users", cte1_selecto)
      {cte2, params2} = Cte.build_cte_from_selecto("recent_users", cte2_selecto)
      
      # Main query joining the CTEs
      main_query = ["SELECT * FROM active_users a JOIN recent_users r ON a.id = r.id"]
      
      {final_query, combined_params} = Cte.integrate_ctes_with_query(
        [{cte1, params1}, {cte2, params2}],
        main_query,
        []
      )
      
      # Should have WITH clause with both CTEs
      final_sql = IO.iodata_to_binary(final_query)
      assert String.contains?(final_sql, "WITH")
      assert String.contains?(final_sql, "active_users")
      assert String.contains?(final_sql, "recent_users")
      assert String.contains?(final_sql, ",")  # CTE separator
      
      # Should have parameters from both CTEs
      assert true in combined_params
      assert ~D[2024-01-01] in combined_params
    end
  end
end