defmodule Selecto.PerformanceTest do
  use ExUnit.Case
  alias Selecto

  @moduledoc """
  Performance tests for Selecto's advanced join patterns and OLAP functionality.
  
  These tests measure query generation performance for complex join patterns including:
  - Star schema dimensions
  - Hierarchical joins (adjacency list and materialized path)
  - Many-to-many tagging
  - Mixed join patterns
  
  Run with: mix test test/performance_test.exs --include performance:true
  """
  
  @tag :performance
  @tag timeout: 120_000  # 2 minute timeout for performance tests

  setup_all do
    # Create mock database connection for performance testing
    conn = %{
      __struct__: Postgrex.Connection,
      pid: self(),
      parameters: %{}
    }
    
    {:ok, conn: conn}
  end

  describe "Star Schema Performance" do
    test "star schema join generation performance", %{conn: conn} do
      domain = build_large_star_schema_domain(50)  # 50 dimensions
      selecto = Selecto.configure(domain, conn)
      
      # Measure performance of complex star schema query generation
      result = :timer.tc(fn ->
        for _i <- 1..100 do
          selecto
          |> Selecto.select(build_large_select_list(25))  # 25 fields
          |> Selecto.filter(build_large_filter_list(20))  # 20 filters
          |> Selecto.group_by(build_large_group_by_list(15))  # 15 group fields
          |> Selecto.to_sql()
        end
      end)
      
      {elapsed_microseconds, _result} = result
      avg_time_ms = elapsed_microseconds / 1000 / 100
      
      # Assert performance requirements (should generate under 50ms per query)
      assert avg_time_ms < 50.0, "Star schema query generation too slow: #{avg_time_ms}ms (should be < 50ms)"
      
      IO.puts("\n📊 Star Schema Performance:")
      IO.puts("  Average query generation time: #{Float.round(avg_time_ms, 2)}ms")
      IO.puts("  Total dimensions: 50")
      IO.puts("  Select fields: 25, Filters: 20, Group by: 15")
    end
    
    test "snowflake schema join generation performance", %{conn: conn} do
      domain = build_snowflake_schema_domain()
      selecto = Selecto.configure(domain, conn)
      
      result = :timer.tc(fn ->
        for _i <- 1..50 do
          selecto
          |> Selecto.select([
            "customer[region][country_display]",
            "customer[customer_type_display]", 
            "product[category][parent][name]",
            "product[brand_display]",
            "product[supplier][region][name]",
            {:func, "sum", ["sale_amount"]},
            {:func, "count", ["*"]},
            {:func, "avg", ["quantity"]}
          ])
          |> Selecto.filter([
            {"customer[region][country][continent]", "North America"},
            {"product[category][level]", {:lte, 3}},
            {"product[supplier][active]", true},
            {"sale_date", {:gte, ~D[2024-01-01]}}
          ])
          |> Selecto.group_by([
            "customer[region][country_display]",
            "product[category][parent][name]",
            "product[brand_display]"
          ])
          |> Selecto.to_sql()
        end
      end)
      
      {elapsed_microseconds, _result} = result
      avg_time_ms = elapsed_microseconds / 1000 / 50
      
      # Snowflake queries are more complex but should still be reasonable
      assert avg_time_ms < 75.0, "Snowflake schema query generation too slow: #{avg_time_ms}ms"
      
      IO.puts("\n❄️  Snowflake Schema Performance:")
      IO.puts("  Average query generation time: #{Float.round(avg_time_ms, 2)}ms")
      IO.puts("  Deep nested joins: 4-5 levels")
    end
  end

  describe "Hierarchical Join Performance" do
    test "adjacency list hierarchy performance", %{conn: conn} do
      domain = build_deep_hierarchy_domain(:adjacency_list, 10)  # 10 levels deep
      selecto = Selecto.configure(domain, conn)
      
      result = :timer.tc(fn ->
        for _i <- 1..50 do
          selecto
          |> Selecto.select([
            "name",
            "manager_path",
            "manager_level", 
            "manager_path_array",
            "subordinates_count",
            "department[name]",
            "department_path",
            {:func, "count", ["*"]}
          ])
          |> Selecto.filter([
            {"manager_level", {:between, 2, 8}},
            {"active", true},
            {"department_level", {:lte, 4}}
          ])
          |> Selecto.group_by(["manager_level", "department[name]"])
          |> Selecto.to_sql()
        end
      end)
      
      {elapsed_microseconds, _result} = result
      avg_time_ms = elapsed_microseconds / 1000 / 50
      
      # Hierarchical CTEs are complex but should generate quickly
      assert avg_time_ms < 100.0, "Adjacency list hierarchy too slow: #{avg_time_ms}ms"
      
      IO.puts("\n🌳 Adjacency List Hierarchy Performance:")
      IO.puts("  Average query generation time: #{Float.round(avg_time_ms, 2)}ms")
      IO.puts("  Hierarchy depth: 10 levels")
    end
    
    test "materialized path hierarchy performance", %{conn: conn} do
      domain = build_deep_hierarchy_domain(:materialized_path, 15)  # 15 levels deep
      selecto = Selecto.configure(domain, conn)
      
      result = :timer.tc(fn ->
        for _i <- 1..75 do
          selecto
          |> Selecto.select([
            "title",
            "category[name]",
            "category_path",
            "category_level",
            "category_ancestors",
            "category_siblings_count",
            {:func, "count", ["*"]}
          ])
          |> Selecto.filter([
            {"category_path", {:like, "/technology/web/%"}},
            {"category_level", {:between, 3, 12}},
            {"category[active]", true}
          ])
          |> Selecto.group_by(["category_path", "category_level"])
          |> Selecto.to_sql()
        end
      end)
      
      {elapsed_microseconds, _result} = result
      avg_time_ms = elapsed_microseconds / 1000 / 75
      
      # Materialized path should be faster than adjacency list
      assert avg_time_ms < 60.0, "Materialized path hierarchy too slow: #{avg_time_ms}ms"
      
      IO.puts("\n🛤️  Materialized Path Hierarchy Performance:")
      IO.puts("  Average query generation time: #{Float.round(avg_time_ms, 2)}ms")  
      IO.puts("  Hierarchy depth: 15 levels")
    end
  end

  describe "Many-to-Many Tagging Performance" do
    test "complex tagging query performance", %{conn: conn} do
      domain = build_complex_tagging_domain()
      selecto = Selecto.configure(domain, conn)
      
      result = :timer.tc(fn ->
        for _i <- 1..100 do
          selecto
          |> Selecto.select([
            "title",
            "author_display",
            "tags_list",
            "tags_count", 
            "tags_weight_sum",
            "user_tags_array",
            "system_tags_list",
            {:func, "count", ["*"]}
          ])
          |> Selecto.filter([
            {"tags_filter", "technology"},
            {"tags_any", ["web", "mobile", "api"]},
            {"tags_all", ["programming", "tutorial"]},
            {"user_tags_count", {:gte, 3}},
            {"system_tags_confidence", {:gte, 0.8}},
            {"published_at", {:gte, ~D[2024-01-01]}}
          ])
          |> Selecto.group_by(["author_display", "tags_count"])
          |> Selecto.to_sql()
        end
      end)
      
      {elapsed_microseconds, _result} = result
      avg_time_ms = elapsed_microseconds / 1000 / 100
      
      # Complex tagging with multiple dimensions
      assert avg_time_ms < 80.0, "Complex tagging query too slow: #{avg_time_ms}ms"
      
      IO.puts("\n🏷️  Many-to-Many Tagging Performance:")
      IO.puts("  Average query generation time: #{Float.round(avg_time_ms, 2)}ms")
      IO.puts("  Multiple tag dimensions with aggregation")
    end
  end

  describe "Mixed Pattern Performance" do
    test "comprehensive mixed pattern performance", %{conn: conn} do
      domain = build_comprehensive_mixed_domain()
      selecto = Selecto.configure(domain, conn)
      
      result = :timer.tc(fn ->
        for _i <- 1..25 do  # Fewer iterations for complex mixed patterns
          selecto
          |> Selecto.select([
            # Star dimension fields
            "customer_display",
            "customer[region_display]",
            
            # Hierarchical fields  
            "items[product][category_path]",
            "items[product][category_level]",
            
            # Tagging fields
            "items[product][tags_list]",
            "items[product][tags_count]",
            
            # Nested hierarchical
            "customer[organization][manager_path]",
            "customer[organization][department_path]",
            
            # Aggregations
            {:func, "sum", ["total"]},
            {:func, "avg", ["items[quantity]"]},
            {:func, "count", ["DISTINCT", "customer_id"]},
            {:func, "count", ["DISTINCT", "items[product_id]"]}
          ])
          |> Selecto.filter([
            # Star dimension filters
            {"customer[segment]", "Enterprise"},
            {"customer[region][country]", "United States"},
            
            # Hierarchical filters
            {"items[product][category_level]", {:lte, 4}},
            {"customer[organization][manager_level]", {:between, 1, 5}},
            
            # Tagging filters
            {"items[product][tags_filter]", "premium"},
            {"items[product][tags_count]", {:gte, 2}},
            
            # Standard filters
            {"status", "completed"},
            {"created_at", {:gte, ~D[2024-01-01]}}
          ])
          |> Selecto.group_by([
            "customer[region_display]",
            "items[product][category_path]",
            "customer[organization][department_path]"
          ])
          |> Selecto.to_sql()
        end
      end)
      
      {elapsed_microseconds, _result} = result
      avg_time_ms = elapsed_microseconds / 1000 / 25
      
      # Mixed patterns are most complex but should still be reasonable
      assert avg_time_ms < 150.0, "Mixed pattern query too slow: #{avg_time_ms}ms"
      
      IO.puts("\n🔀 Mixed Pattern Performance:")
      IO.puts("  Average query generation time: #{Float.round(avg_time_ms, 2)}ms")
      IO.puts("  Combined star, hierarchical, and tagging patterns")
    end
  end

  describe "CTE Generation Performance" do
    test "complex recursive CTE performance", %{conn: conn} do
      domain = build_recursive_cte_domain()
      selecto = Selecto.configure(domain, conn)
      
      result = :timer.tc(fn ->
        for _i <- 1..50 do
          selecto
          |> Selecto.select([
            "name",
            "category_hierarchy_path", 
            "category_hierarchy_level",
            "employee_hierarchy_path",
            "employee_hierarchy_level",
            {:func, "count", ["*"]}
          ])
          |> Selecto.filter([
            {"category_hierarchy_level", {:lte, 8}},
            {"employee_hierarchy_level", {:between, 1, 6}},
            {"active", true}
          ])
          |> Selecto.group_by([
            "category_hierarchy_level",
            "employee_hierarchy_level"
          ])
          |> Selecto.to_sql()
        end
      end)
      
      {elapsed_microseconds, _result} = result
      avg_time_ms = elapsed_microseconds / 1000 / 50
      
      # Complex CTEs with multiple recursive hierarchies
      assert avg_time_ms < 120.0, "Complex recursive CTE too slow: #{avg_time_ms}ms"
      
      IO.puts("\n🔄 Recursive CTE Performance:")
      IO.puts("  Average query generation time: #{Float.round(avg_time_ms, 2)}ms")
      IO.puts("  Multiple recursive CTEs")
    end
  end

  describe "Memory Usage Performance" do 
    test "memory efficiency with large domains", %{conn: conn} do
      domain = build_memory_test_domain(100)  # Very large domain
      
      # Measure memory before
      memory_before = :erlang.memory(:total)
      
      selecto = Selecto.configure(domain, conn)
      
      # Generate many queries
      for _i <- 1..50 do
        selecto
        |> Selecto.select(build_large_select_list(30))
        |> Selecto.filter(build_large_filter_list(25))
        |> Selecto.group_by(build_large_group_by_list(20))
        |> Selecto.to_sql()
      end
      
      # Measure memory after
      memory_after = :erlang.memory(:total)
      memory_diff_mb = (memory_after - memory_before) / (1024 * 1024)
      
      # Should not use excessive memory
      assert memory_diff_mb < 50.0, "Memory usage too high: #{memory_diff_mb}MB"
      
      IO.puts("\n💾 Memory Usage Performance:")
      IO.puts("  Memory increase: #{Float.round(memory_diff_mb, 2)}MB")
      IO.puts("  Domain size: 100 schemas, 50 queries generated")
    end
  end

  describe "SQL Generation Validation" do
    test "generated SQL correctness under load", %{conn: conn} do
      domain = build_validation_domain()
      selecto = Selecto.configure(domain, conn)
      
      # Generate many different query patterns
      results = for i <- 1..100 do
        query = selecto
          |> Selecto.select(generate_random_select_list(i))
          |> Selecto.filter(generate_random_filter_list(i))  
          |> Selecto.group_by(generate_random_group_by_list(i))
        
        {sql, params} = Selecto.to_sql(query)
        
        # Validate SQL structure
        assert is_binary(sql)
        assert String.contains?(sql, "SELECT")
        assert String.contains?(sql, "FROM")
        assert is_list(params)
        
        # Check for SQL injection protection
        refute String.contains?(sql, "'; DROP")
        refute String.contains?(sql, "-- ")
        refute String.contains?(sql, "/*")
        
        {sql, params}
      end
      
      # All queries should be valid
      assert length(results) == 100
      
      IO.puts("\n✅ SQL Generation Validation:")
      IO.puts("  Generated and validated 100 random queries")
      IO.puts("  All queries passed security and structure checks")
    end
  end

  # Helper functions for building test domains
  
  defp build_large_star_schema_domain(dimension_count) do
    dimensions = for i <- 1..dimension_count do
      {:"dim_#{i}", %{
        name: "Dimension #{i}",
        source_table: "dimension_#{i}",
        fields: [:id, :name, :category, :region, :active],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          category: %{type: :string},
          region: %{type: :string},
          active: %{type: :boolean}
        }
      }}
    end |> Enum.into(%{})
    
    joins = for i <- 1..dimension_count do
      {:"dim_#{i}", %{type: :star_dimension, display_field: :name}}
    end |> Enum.into(%{})
    
    associations = for i <- 1..dimension_count do
      {:"dim_#{i}", %{queryable: :"dim_#{i}", field: :"dim_#{i}", owner_key: :"dim_#{i}_id", related_key: :id}}
    end |> Enum.into(%{})
    
    %{
      name: "Large Star Schema Test",
      source: %{
        source_table: "facts",
        primary_key: :id,
        fields: [:id, :amount, :quantity, :date] ++ (for i <- 1..dimension_count, do: :"dim_#{i}_id"),
        columns: %{
          id: %{type: :integer},
          amount: %{type: :decimal},
          quantity: %{type: :integer},
          date: %{type: :date}
        } |> Map.merge(for i <- 1..dimension_count do
          {:"dim_#{i}_id", %{type: :integer}}
        end |> Enum.into(%{})),
        associations: associations
      },
      schemas: dimensions,
      joins: joins
    }
  end

  defp build_snowflake_schema_domain do
    %{
      name: "Complex Snowflake Schema",
      source: %{
        source_table: "sales_facts",
        primary_key: :id,
        fields: [:id, :sale_amount, :quantity, :customer_id, :product_id],
        columns: %{
          id: %{type: :integer},
          sale_amount: %{type: :decimal},
          quantity: %{type: :integer},
          customer_id: %{type: :integer},
          product_id: %{type: :integer}
        },
        associations: %{
          customer: %{queryable: :customers, field: :customer, owner_key: :customer_id, related_key: :id},
          product: %{queryable: :products, field: :product, owner_key: :product_id, related_key: :id}
        }
      },
      schemas: %{
        customers: %{
          name: "Customer",
          source_table: "customers",
          fields: [:id, :name, :customer_type_id, :region_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            customer_type_id: %{type: :integer},
            region_id: %{type: :integer}
          },
          associations: %{
            customer_type: %{queryable: :customer_types, field: :customer_type, owner_key: :customer_type_id, related_key: :id},
            region: %{queryable: :regions, field: :region, owner_key: :region_id, related_key: :id}
          }
        },
        customer_types: %{
          name: "Customer Type",
          source_table: "customer_types",
          fields: [:id, :name],
          columns: %{id: %{type: :integer}, name: %{type: :string}}
        },
        regions: %{
          name: "Region",
          source_table: "regions",
          fields: [:id, :name, :country_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            country_id: %{type: :integer}
          },
          associations: %{
            country: %{queryable: :countries, field: :country, owner_key: :country_id, related_key: :id}
          }
        },
        countries: %{
          name: "Country",
          source_table: "countries",
          fields: [:id, :name, :continent],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            continent: %{type: :string}
          }
        },
        products: %{
          name: "Product",
          source_table: "products",
          fields: [:id, :name, :category_id, :brand_id, :supplier_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            category_id: %{type: :integer},
            brand_id: %{type: :integer},
            supplier_id: %{type: :integer}
          },
          associations: %{
            category: %{queryable: :categories, field: :category, owner_key: :category_id, related_key: :id},
            brand: %{queryable: :brands, field: :brand, owner_key: :brand_id, related_key: :id},
            supplier: %{queryable: :suppliers, field: :supplier, owner_key: :supplier_id, related_key: :id}
          }
        },
        categories: %{
          name: "Category",
          source_table: "categories",
          fields: [:id, :name, :parent_id, :level],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            parent_id: %{type: :integer},
            level: %{type: :integer}
          }
        },
        brands: %{
          name: "Brand",
          source_table: "brands",
          fields: [:id, :name],
          columns: %{id: %{type: :integer}, name: %{type: :string}}
        },
        suppliers: %{
          name: "Supplier",
          source_table: "suppliers",
          fields: [:id, :name, :region_id, :active],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            region_id: %{type: :integer},
            active: %{type: :boolean}
          },
          associations: %{
            region: %{queryable: :regions, field: :region, owner_key: :region_id, related_key: :id}
          }
        }
      },
      joins: %{
        customer: %{
          type: :star_dimension,
          display_field: :name,
          joins: %{
            customer_type: %{type: :star_dimension, display_field: :name},
            region: %{
              type: :star_dimension,
              display_field: :name,
              joins: %{
                country: %{type: :star_dimension, display_field: :name}
              }
            }
          }
        },
        product: %{
          type: :star_dimension,
          display_field: :name,
          joins: %{
            category: %{
              type: :hierarchical,
              hierarchy_type: :adjacency_list,
              depth_limit: 5,
              joins: %{
                parent: %{type: :left}
              }
            },
            brand: %{type: :star_dimension, display_field: :name},
            supplier: %{
              type: :star_dimension,
              display_field: :name,
              joins: %{
                region: %{type: :star_dimension, display_field: :name}
              }
            }
          }
        }
      }
    }
  end
  
  defp build_deep_hierarchy_domain(type, depth) do
    case type do
      :adjacency_list -> build_adjacency_hierarchy_domain(depth)
      :materialized_path -> build_materialized_path_domain(depth)
    end
  end
  
  defp build_adjacency_hierarchy_domain(_depth) do
    %{
      name: "Deep Adjacency Hierarchy Test",
      source: %{
        source_table: "employees",
        primary_key: :id,
        fields: [:id, :name, :manager_id, :department_id, :active],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          manager_id: %{type: :integer},
          department_id: %{type: :integer},
          active: %{type: :boolean}
        },
        associations: %{
          manager: %{queryable: :employees, field: :manager, owner_key: :manager_id, related_key: :id},
          department: %{queryable: :departments, field: :department, owner_key: :department_id, related_key: :id}
        }
      },
      schemas: %{
        departments: %{
          name: "Department",
          source_table: "departments",
          fields: [:id, :name, :parent_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            parent_id: %{type: :integer}
          }
        }
      },
      joins: %{
        manager: %{
          type: :hierarchical,
          hierarchy_type: :adjacency_list,
          depth_limit: 10
        },
        department: %{
          type: :hierarchical,
          hierarchy_type: :adjacency_list,
          depth_limit: 5
        }
      }
    }
  end
  
  defp build_materialized_path_domain(_depth) do
    %{
      name: "Deep Materialized Path Test",
      source: %{
        source_table: "articles",
        primary_key: :id,
        fields: [:id, :title, :category_id],
        columns: %{
          id: %{type: :integer},
          title: %{type: :string},
          category_id: %{type: :integer}
        },
        associations: %{
          category: %{queryable: :categories, field: :category, owner_key: :category_id, related_key: :id}
        }
      },
      schemas: %{
        categories: %{
          name: "Category",
          source_table: "categories",
          fields: [:id, :name, :path, :active],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            path: %{type: :string},
            active: %{type: :boolean}
          }
        }
      },
      joins: %{
        category: %{
          type: :hierarchical,
          hierarchy_type: :materialized_path,
          path_field: :path,
          path_separator: "/",
          depth_limit: 15
        }
      }
    }
  end
  
  defp build_complex_tagging_domain do
    %{
      name: "Complex Tagging Test",
      source: %{
        source_table: "articles",
        primary_key: :id,
        fields: [:id, :title, :author_id, :published_at],
        columns: %{
          id: %{type: :integer},
          title: %{type: :string},
          author_id: %{type: :integer},
          published_at: %{type: :utc_datetime}
        },
        associations: %{
          author: %{queryable: :users, field: :author, owner_key: :author_id, related_key: :id},
          tags: %{queryable: :article_tags, field: :tags, owner_key: :id, related_key: :article_id},
          user_tags: %{queryable: :user_article_tags, field: :user_tags, owner_key: :id, related_key: :article_id},
          system_tags: %{queryable: :system_tags, field: :system_tags, owner_key: :id, related_key: :article_id}
        }
      },
      schemas: %{
        users: %{
          name: "User",
          source_table: "users",
          fields: [:id, :name],
          columns: %{id: %{type: :integer}, name: %{type: :string}}
        },
        article_tags: %{
          name: "Article Tag",
          source_table: "article_tags",
          fields: [:id, :name, :article_id, :weight],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            article_id: %{type: :integer},
            weight: %{type: :integer}
          }
        },
        user_article_tags: %{
          name: "User Tag",
          source_table: "user_article_tags",
          fields: [:id, :name, :article_id, :user_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            article_id: %{type: :integer},
            user_id: %{type: :integer}
          }
        },
        system_tags: %{
          name: "System Tag",
          source_table: "system_tags",
          fields: [:id, :name, :article_id, :confidence],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            article_id: %{type: :integer},
            confidence: %{type: :float}
          }
        }
      },
      joins: %{
        author: %{type: :left, display_field: :name},
        tags: %{type: :tagging, tag_field: :name, weight_field: :weight},
        user_tags: %{type: :tagging, tag_field: :name, aggregation: :array_agg},
        system_tags: %{type: :tagging, tag_field: :name, weight_field: :confidence, min_weight: 0.7}
      }
    }
  end
  
  defp build_comprehensive_mixed_domain do
    # This would be a large domain combining all patterns
    # Simplified for performance testing
    %{
      name: "Mixed Pattern Test",
      source: %{
        source_table: "orders",
        primary_key: :id,
        fields: [:id, :total, :customer_id, :created_at, :status],
        columns: %{
          id: %{type: :integer},
          total: %{type: :decimal},
          customer_id: %{type: :integer},
          created_at: %{type: :utc_datetime},
          status: %{type: :string}
        },
        associations: %{
          customer: %{queryable: :customers, field: :customer, owner_key: :customer_id, related_key: :id},
          items: %{queryable: :order_items, field: :items, owner_key: :id, related_key: :order_id}
        }
      },
      schemas: %{
        customers: %{
          name: "Customer",
          source_table: "customers",
          fields: [:id, :name, :segment, :organization_id, :region_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            segment: %{type: :string},
            organization_id: %{type: :integer},
            region_id: %{type: :integer}
          },
          associations: %{
            organization: %{queryable: :organizations, field: :organization, owner_key: :organization_id, related_key: :id},
            region: %{queryable: :regions, field: :region, owner_key: :region_id, related_key: :id}
          }
        },
        organizations: %{
          name: "Organization",
          source_table: "organizations",
          fields: [:id, :name, :manager_id, :department_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            manager_id: %{type: :integer},
            department_id: %{type: :integer}
          },
          associations: %{
            manager: %{queryable: :organizations, field: :manager, owner_key: :manager_id, related_key: :id},
            department: %{queryable: :departments, field: :department, owner_key: :department_id, related_key: :id}
          }
        },
        departments: %{
          name: "Department",
          source_table: "departments",
          fields: [:id, :name, :parent_id],
          columns: %{id: %{type: :integer}, name: %{type: :string}, parent_id: %{type: :integer}}
        },
        regions: %{
          name: "Region",
          source_table: "regions",
          fields: [:id, :name, :country],
          columns: %{id: %{type: :integer}, name: %{type: :string}, country: %{type: :string}}
        },
        order_items: %{
          name: "Order Item",
          source_table: "order_items",
          fields: [:id, :quantity, :order_id, :product_id],
          columns: %{
            id: %{type: :integer},
            quantity: %{type: :integer},
            order_id: %{type: :integer},
            product_id: %{type: :integer}
          },
          associations: %{
            product: %{queryable: :products, field: :product, owner_key: :product_id, related_key: :id}
          }
        },
        products: %{
          name: "Product",
          source_table: "products",
          fields: [:id, :name, :category_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            category_id: %{type: :integer}
          },
          associations: %{
            category: %{queryable: :categories, field: :category, owner_key: :category_id, related_key: :id},
            tags: %{queryable: :product_tags, field: :tags, owner_key: :id, related_key: :product_id}
          }
        },
        categories: %{
          name: "Category",
          source_table: "categories",
          fields: [:id, :name, :parent_id],
          columns: %{id: %{type: :integer}, name: %{type: :string}, parent_id: %{type: :integer}}
        },
        product_tags: %{
          name: "Product Tag",
          source_table: "product_tags",
          fields: [:id, :name, :product_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            product_id: %{type: :integer}
          }
        }
      },
      joins: %{
        customer: %{
          type: :star_dimension,
          display_field: :name,
          joins: %{
            organization: %{
              type: :hierarchical,
              hierarchy_type: :adjacency_list,
              depth_limit: 6,
              joins: %{
                manager: %{type: :hierarchical, hierarchy_type: :adjacency_list, depth_limit: 5},
                department: %{type: :hierarchical, hierarchy_type: :adjacency_list, depth_limit: 4}
              }
            },
            region: %{type: :star_dimension, display_field: :name}
          }
        },
        items: %{
          type: :left,
          joins: %{
            product: %{
              type: :star_dimension,
              display_field: :name,
              joins: %{
                category: %{type: :hierarchical, hierarchy_type: :adjacency_list, depth_limit: 5},
                tags: %{type: :tagging, tag_field: :name}
              }
            }
          }
        }
      }
    }
  end

  defp build_recursive_cte_domain do
    %{
      name: "Recursive CTE Test",
      source: %{
        source_table: "items",
        primary_key: :id,
        fields: [:id, :name, :category_id, :employee_id, :active],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          category_id: %{type: :integer},
          employee_id: %{type: :integer},
          active: %{type: :boolean}
        },
        associations: %{
          category: %{queryable: :categories, field: :category, owner_key: :category_id, related_key: :id},
          employee: %{queryable: :employees, field: :employee, owner_key: :employee_id, related_key: :id}
        }
      },
      schemas: %{
        categories: %{
          name: "Category",
          source_table: "categories",
          fields: [:id, :name, :parent_id],
          columns: %{id: %{type: :integer}, name: %{type: :string}, parent_id: %{type: :integer}}
        },
        employees: %{
          name: "Employee",
          source_table: "employees",
          fields: [:id, :name, :manager_id],
          columns: %{id: %{type: :integer}, name: %{type: :string}, manager_id: %{type: :integer}}
        }
      },
      joins: %{
        category: %{
          type: :hierarchical,
          hierarchy_type: :adjacency_list,
          depth_limit: 8,
          alias_prefix: "category_hierarchy"
        },
        employee: %{
          type: :hierarchical,
          hierarchy_type: :adjacency_list,
          depth_limit: 6,
          alias_prefix: "employee_hierarchy"
        }
      }
    }
  end

  defp build_memory_test_domain(schema_count) do
    schemas = for i <- 1..schema_count do
      {:"schema_#{i}", %{
        name: "Schema #{i}",
        source_table: "table_#{i}",
        fields: [:id, :name, :value, :active],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          value: %{type: :decimal},
          active: %{type: :boolean}
        }
      }}
    end |> Enum.into(%{})
    
    associations = for i <- 1..schema_count do
      {:"schema_#{i}", %{queryable: :"schema_#{i}", field: :"schema_#{i}", owner_key: :"schema_#{i}_id", related_key: :id}}
    end |> Enum.into(%{})
    
    joins = for i <- 1..schema_count do
      {:"schema_#{i}", %{type: :left, display_field: :name}}
    end |> Enum.into(%{})
    
    %{
      name: "Memory Test Domain",
      source: %{
        source_table: "main_table",
        primary_key: :id,
        fields: [:id, :data] ++ (for i <- 1..schema_count, do: :"schema_#{i}_id"),
        columns: %{id: %{type: :integer}, data: %{type: :string}},
        associations: associations
      },
      schemas: schemas,
      joins: joins
    }
  end

  defp build_validation_domain do
    %{
      name: "Validation Test",
      source: %{
        source_table: "main",
        primary_key: :id,
        fields: [:id, :name, :value, :category_id, :tag_id],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          value: %{type: :decimal},
          category_id: %{type: :integer},
          tag_id: %{type: :integer}
        },
        associations: %{
          category: %{queryable: :categories, field: :category, owner_key: :category_id, related_key: :id},
          tags: %{queryable: :tags, field: :tags, owner_key: :id, related_key: :main_id}
        }
      },
      schemas: %{
        categories: %{
          name: "Category",
          source_table: "categories",
          fields: [:id, :name, :parent_id],
          columns: %{id: %{type: :integer}, name: %{type: :string}, parent_id: %{type: :integer}}
        },
        tags: %{
          name: "Tag",
          source_table: "tags",
          fields: [:id, :name, :main_id],
          columns: %{id: %{type: :integer}, name: %{type: :string}, main_id: %{type: :integer}}
        }
      },
      joins: %{
        category: %{type: :hierarchical, hierarchy_type: :adjacency_list, depth_limit: 5},
        tags: %{type: :tagging, tag_field: :name}
      }
    }
  end

  defp build_large_select_list(count) do
    base_fields = ["id", "name", "value", {:func, "count", ["*"]}, {:func, "sum", ["value"]}]
    dynamic_fields = for i <- 1..(count-5), do: "field_#{i}"
    base_fields ++ dynamic_fields
  end

  defp build_large_filter_list(count) do
    base_filters = [
      {"active", true},
      {"value", {:gt, 0}},
      {"name", {:not_null}}
    ]
    dynamic_filters = for i <- 1..(count-3), do: {"field_#{i}", {:eq, "test_#{i}"}}
    base_filters ++ dynamic_filters
  end

  defp build_large_group_by_list(count) do
    base_groups = ["name", "category"]
    dynamic_groups = for i <- 1..(count-2), do: "field_#{i}"
    base_groups ++ dynamic_groups
  end

  defp generate_random_select_list(seed) do
    :rand.seed(:exsss, seed)
    count = :rand.uniform(10) + 5
    fields = ["id", "name", "value"]
    functions = [{:func, "count", ["*"]}, {:func, "sum", ["value"]}, {:func, "avg", ["value"]}]
    
    Enum.take_random(fields ++ functions, count)
  end

  defp generate_random_filter_list(seed) do
    :rand.seed(:exsss, seed)
    count = :rand.uniform(8) + 2
    base_filters = [
      {"active", true},
      {"value", {:gt, 10}},
      {"name", {:like, "%test%"}},
      {"category[name]", {:in, ["A", "B", "C"]}},
      {"tags_filter", "important"}
    ]
    
    Enum.take_random(base_filters, count)
  end

  defp generate_random_group_by_list(seed) do
    :rand.seed(:exsss, seed)
    count = :rand.uniform(5) + 1
    fields = ["name", "category[name]", "tags_count", "active"]
    
    Enum.take_random(fields, count)
  end
end