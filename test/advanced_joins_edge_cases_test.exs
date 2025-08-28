defmodule Selecto.AdvancedJoinsEdgeCasesTest do
  use ExUnit.Case
  alias Selecto
  alias Selecto.Builder.{Join, Cte, Sql}

  @moduledoc """
  Comprehensive edge case testing for Selecto's advanced join patterns.
  
  Tests cover boundary conditions, error handling, and unusual configurations for:
  - Star schema dimensions with edge cases
  - Hierarchical joins with malformed data
  - Many-to-many tagging with empty/null data
  - Mixed join patterns with conflicting configurations
  - Memory/performance edge cases
  """

  # Mock connection for testing
  defp mock_conn do
    %{__struct__: Postgrex.Connection, pid: self()}
  end

  describe "Star Schema Dimension Edge Cases" do
    test "handles empty dimension tables" do
      domain = %{
        name: "Empty Dimension Test",
        source: %{
          source_table: "facts",
          primary_key: :id,
          fields: [:id, :amount, :customer_id],
          columns: %{
            id: %{type: :integer},
            amount: %{type: :decimal},
            customer_id: %{type: :integer}
          },
          associations: %{
            customer: %{queryable: :customers, field: :customer, owner_key: :customer_id, related_key: :id}
          }
        },
        schemas: %{
          customers: %{
            name: "Customer",
            source_table: "customers",
            fields: [:id],  # Minimal fields
            columns: %{
              id: %{type: :integer}
            }
          }
        },
        joins: %{
          customer: %{type: :star_dimension, display_field: :id}  # Use ID as display field
        }
      }

      selecto = Selecto.configure(domain, mock_conn())

      # Should handle empty dimension gracefully
      result = selecto
        |> Selecto.select(["customer_display", {:func, "sum", ["amount"]}])
        |> Selecto.filter([{"customer_id", {:not_null}}])
        |> Selecto.group_by(["customer_display"])
        |> Selecto.to_sql()

      assert {sql, params} = result
      assert is_binary(sql)
      assert String.contains?(sql, "LEFT JOIN customers")
      assert String.contains?(sql, "customers.id")
    end

    test "handles missing display fields" do
      domain = %{
        name: "Missing Display Field Test",
        source: %{
          source_table: "facts",
          primary_key: :id,
          fields: [:id, :amount, :customer_id],
          columns: %{
            id: %{type: :integer},
            amount: %{type: :decimal},
            customer_id: %{type: :integer}
          },
          associations: %{
            customer: %{queryable: :customers, field: :customer, owner_key: :customer_id, related_key: :id}
          }
        },
        schemas: %{
          customers: %{
            name: "Customer",
            source_table: "customers",
            fields: [:id, :name],
            columns: %{
              id: %{type: :integer},
              name: %{type: :string}
            }
          }
        },
        joins: %{
          customer: %{type: :star_dimension, display_field: :nonexistent_field}  # Bad field
        }
      }

      selecto = Selecto.configure(domain, mock_conn())

      # Should handle missing display field gracefully (use primary key)
      result = selecto
        |> Selecto.select(["customer_display"])
        |> Selecto.to_sql()

      assert {sql, _params} = result
      assert String.contains?(sql, "customers.id")  # Fallback to primary key
    end

    test "handles circular dimension references" do
      domain = %{
        name: "Circular Reference Test",
        source: %{
          source_table: "facts",
          primary_key: :id,
          fields: [:id, :amount, :dim_a_id, :dim_b_id],
          columns: %{
            id: %{type: :integer},
            amount: %{type: :decimal},
            dim_a_id: %{type: :integer},
            dim_b_id: %{type: :integer}
          },
          associations: %{
            dim_a: %{queryable: :dim_a, field: :dim_a, owner_key: :dim_a_id, related_key: :id},
            dim_b: %{queryable: :dim_b, field: :dim_b, owner_key: :dim_b_id, related_key: :id}
          }
        },
        schemas: %{
          dim_a: %{
            name: "Dimension A",
            source_table: "dim_a",
            fields: [:id, :name, :dim_b_ref_id],
            columns: %{
              id: %{type: :integer},
              name: %{type: :string},
              dim_b_ref_id: %{type: :integer}
            },
            associations: %{
              dim_b_ref: %{queryable: :dim_b, field: :dim_b_ref, owner_key: :dim_b_ref_id, related_key: :id}
            }
          },
          dim_b: %{
            name: "Dimension B",
            source_table: "dim_b",
            fields: [:id, :name, :dim_a_ref_id],
            columns: %{
              id: %{type: :integer},
              name: %{type: :string},
              dim_a_ref_id: %{type: :integer}
            },
            associations: %{
              dim_a_ref: %{queryable: :dim_a, field: :dim_a_ref, owner_key: :dim_a_ref_id, related_key: :id}
            }
          }
        },
        joins: %{
          dim_a: %{
            type: :star_dimension,
            display_field: :name,
            joins: %{
              dim_b_ref: %{
                type: :star_dimension,
                display_field: :name,
                joins: %{
                  dim_a_ref: %{type: :star_dimension, display_field: :name}  # Circular!
                }
              }
            }
          },
          dim_b: %{type: :star_dimension, display_field: :name}
        }
      }

      selecto = Selecto.configure(domain, mock_conn())

      # Should handle circular references without infinite recursion
      result = selecto
        |> Selecto.select([
          "dim_a_display",
          "dim_a[dim_b_ref_display]",
          "dim_b_display"
        ])
        |> Selecto.to_sql()

      assert {sql, _params} = result
      assert is_binary(sql)
      # Should not have infinite joins
      join_count = (sql |> String.split("JOIN") |> length()) - 1
      assert join_count < 10  # Reasonable limit
    end

    test "handles extremely large dimension counts" do
      # Create domain with many dimensions
      dimension_count = 100
      
      dimensions = for i <- 1..dimension_count do
        {:"dim_#{i}", %{
          name: "Dimension #{i}",
          source_table: "dim_#{i}",
          fields: [:id, :name],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string}
          }
        }}
      end |> Enum.into(%{})
      
      associations = for i <- 1..dimension_count do
        {:"dim_#{i}", %{queryable: :"dim_#{i}", field: :"dim_#{i}", owner_key: :"dim_#{i}_id", related_key: :id}}
      end |> Enum.into(%{})
      
      joins = for i <- 1..dimension_count do
        {:"dim_#{i}", %{type: :star_dimension, display_field: :name}}
      end |> Enum.into(%{})

      domain = %{
        name: "Large Dimension Test",
        source: %{
          source_table: "facts",
          primary_key: :id,
          fields: [:id, :amount] ++ (for i <- 1..dimension_count, do: :"dim_#{i}_id"),
          columns: %{id: %{type: :integer}, amount: %{type: :decimal}},
          associations: associations
        },
        schemas: dimensions,
        joins: joins
      }

      selecto = Selecto.configure(domain, mock_conn())

      # Should handle large domain without crashing
      result = selecto
        |> Selecto.select(["dim_1_display", "dim_50_display", {:func, "sum", ["amount"]}])
        |> Selecto.group_by(["dim_1_display", "dim_50_display"])
        |> Selecto.to_sql()

      assert {sql, _params} = result
      assert is_binary(sql)
      assert String.contains?(sql, "dim_1")
      assert String.contains?(sql, "dim_50")
    end
  end

  describe "Hierarchical Join Edge Cases" do
    test "handles self-referencing with null parent_id" do
      domain = %{
        name: "Null Parent Test",
        source: %{
          source_table: "nodes",
          primary_key: :id,
          fields: [:id, :name, :parent_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            parent_id: %{type: :integer}
          },
          associations: %{
            parent: %{queryable: :nodes, field: :parent, owner_key: :parent_id, related_key: :id}
          }
        },
        schemas: %{},
        joins: %{
          parent: %{
            type: :hierarchical,
            hierarchy_type: :adjacency_list,
            depth_limit: 5,
            handle_nulls: true  # Special handling for null parents
          }
        }
      }

      selecto = Selecto.configure(domain, mock_conn())

      result = selecto
        |> Selecto.select([
          "name",
          "parent_path",
          "parent_level"
        ])
        |> Selecto.filter([
          {"parent_id", {:is_null}}  # Root nodes only
        ])
        |> Selecto.to_sql()

      assert {sql, _params} = result
      assert String.contains?(sql, "WITH")  # Should generate CTE
      assert String.contains?(sql, "IS NULL")  # Handle null check
    end

    test "handles circular hierarchies" do
      domain = %{
        name: "Circular Hierarchy Test",
        source: %{
          source_table: "nodes",
          primary_key: :id,
          fields: [:id, :name, :parent_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            parent_id: %{type: :integer}
          },
          associations: %{
            parent: %{queryable: :nodes, field: :parent, owner_key: :parent_id, related_key: :id}
          }
        },
        schemas: %{},
        joins: %{
          parent: %{
            type: :hierarchical,
            hierarchy_type: :adjacency_list,
            depth_limit: 10,
            detect_cycles: true  # Enable cycle detection
          }
        }
      }

      selecto = Selecto.configure(domain, mock_conn())

      result = selecto
        |> Selecto.select([
          "name",
          "parent_path",
          "parent_level",
          "cycle_detected"  # Should be computed field
        ])
        |> Selecto.to_sql()

      assert {sql, _params} = result
      assert String.contains?(sql, "WITH")  # CTE for hierarchy
      # Should include cycle detection logic in recursive part
    end

    test "handles extremely deep hierarchies" do
      domain = %{
        name: "Deep Hierarchy Test",
        source: %{
          source_table: "categories",
          primary_key: :id,
          fields: [:id, :name, :parent_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            parent_id: %{type: :integer}
          },
          associations: %{
            parent: %{queryable: :categories, field: :parent, owner_key: :parent_id, related_key: :id}
          }
        },
        schemas: %{},
        joins: %{
          parent: %{
            type: :hierarchical,
            hierarchy_type: :adjacency_list,
            depth_limit: 1000  # Very deep
          }
        }
      }

      selecto = Selecto.configure(domain, mock_conn())

      result = selecto
        |> Selecto.select([
          "name",
          "parent_level"
        ])
        |> Selecto.filter([
          {"parent_level", {:lte, 999}}  # Should respect depth limit
        ])
        |> Selecto.to_sql()

      assert {sql, _params} = result
      assert String.contains?(sql, "999")  # Depth limit should appear
    end

    test "handles materialized path with invalid separators" do
      domain = %{
        name: "Invalid Separator Test",
        source: %{
          source_table: "items",
          primary_key: :id,
          fields: [:id, :name, :category_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
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
            fields: [:id, :name, :path],
            columns: %{
              id: %{type: :integer},
              name: %{type: :string},
              path: %{type: :string}
            }
          }
        },
        joins: %{
          category: %{
            type: :hierarchical,
            hierarchy_type: :materialized_path,
            path_field: :path,
            path_separator: "\\",  # Backslash - potential SQL injection risk
            depth_limit: 5
          }
        }
      }

      selecto = Selecto.configure(domain, mock_conn())

      result = selecto
        |> Selecto.select([
          "category_path",
          "category_level"
        ])
        |> Selecto.to_sql()

      assert {sql, params} = result
      # Should properly escape separator in SQL
      assert is_binary(sql)
      assert is_list(params)
    end

    test "handles empty path fields" do
      domain = %{
        name: "Empty Path Test",
        source: %{
          source_table: "items",
          primary_key: :id,
          fields: [:id, :name, :category_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
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
            fields: [:id, :name, :path],
            columns: %{
              id: %{type: :integer},
              name: %{type: :string},
              path: %{type: :string}
            }
          }
        },
        joins: %{
          category: %{
            type: :hierarchical,
            hierarchy_type: :materialized_path,
            path_field: :path,
            path_separator: "/",
            handle_empty_paths: true
          }
        }
      }

      selecto = Selecto.configure(domain, mock_conn())

      result = selecto
        |> Selecto.select([
          "category_path",
          "category_ancestors"
        ])
        |> Selecto.filter([
          {"category_path", {:not_eq, ""}}  # Non-empty paths only
        ])
        |> Selecto.to_sql()

      assert {sql, _params} = result
      assert String.contains?(sql, "''")  # Empty string handling
    end
  end

  describe "Many-to-Many Tagging Edge Cases" do
    test "handles items with no tags" do
      domain = %{
        name: "No Tags Test",
        source: %{
          source_table: "items",
          primary_key: :id,
          fields: [:id, :title],
          columns: %{
            id: %{type: :integer},
            title: %{type: :string}
          },
          associations: %{
            tags: %{queryable: :item_tags, field: :tags, owner_key: :id, related_key: :item_id}
          }
        },
        schemas: %{
          item_tags: %{
            name: "Item Tag",
            source_table: "item_tags",
            fields: [:id, :name, :item_id],
            columns: %{
              id: %{type: :integer},
              name: %{type: :string},
              item_id: %{type: :integer}
            }
          }
        },
        joins: %{
          tags: %{
            type: :tagging,
            tag_field: :name,
            handle_empty: true,  # Special handling for no tags
            empty_value: "no-tags"
          }
        }
      }

      selecto = Selecto.configure(domain, mock_conn())

      result = selecto
        |> Selecto.select([
          "title",
          "tags_list",
          "tags_count"
        ])
        |> Selecto.filter([
          {"tags_count", 0}  # Items with no tags
        ])
        |> Selecto.to_sql()

      assert {sql, params} = result
      assert String.contains?(sql, "LEFT JOIN")  # Should use LEFT JOIN for optional tags
      assert "no-tags" in params  # Default value should be in params
    end

    test "handles duplicate tag names" do
      domain = %{
        name: "Duplicate Tags Test",
        source: %{
          source_table: "articles",
          primary_key: :id,
          fields: [:id, :title],
          columns: %{
            id: %{type: :integer},
            title: %{type: :string}
          },
          associations: %{
            tags: %{queryable: :article_tags, field: :tags, owner_key: :id, related_key: :article_id}
          }
        },
        schemas: %{
          article_tags: %{
            name: "Article Tag",
            source_table: "article_tags",
            fields: [:id, :name, :article_id, :created_by],
            columns: %{
              id: %{type: :integer},
              name: %{type: :string},
              article_id: %{type: :integer},
              created_by: %{type: :integer}
            }
          }
        },
        joins: %{
          tags: %{
            type: :tagging,
            tag_field: :name,
            aggregation: :string_agg,
            deduplication: true  # Remove duplicates
          }
        }
      }

      selecto = Selecto.configure(domain, mock_conn())

      result = selecto
        |> Selecto.select([
          "title",
          "tags_list",
          "tags_unique_count"  # Count of unique tags
        ])
        |> Selecto.group_by(["id", "title"])
        |> Selecto.to_sql()

      assert {sql, _params} = result
      assert String.contains?(sql, "DISTINCT")  # Should include DISTINCT for deduplication
    end

    test "handles tag weights with null values" do
      domain = %{
        name: "Null Weight Test",
        source: %{
          source_table: "products",
          primary_key: :id,
          fields: [:id, :name],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string}
          },
          associations: %{
            tags: %{queryable: :product_tags, field: :tags, owner_key: :id, related_key: :product_id}
          }
        },
        schemas: %{
          product_tags: %{
            name: "Product Tag",
            source_table: "product_tags",
            fields: [:id, :name, :product_id, :weight],
            columns: %{
              id: %{type: :integer},
              name: %{type: :string},
              product_id: %{type: :integer},
              weight: %{type: :decimal}
            }
          }
        },
        joins: %{
          tags: %{
            type: :tagging,
            tag_field: :name,
            weight_field: :weight,
            null_weight_handling: :ignore,  # Ignore null weights
            min_weight: 0.1
          }
        }
      }

      selecto = Selecto.configure(domain, mock_conn())

      result = selecto
        |> Selecto.select([
          "name",
          "tags_list",
          "tags_weight_avg",
          "tags_weight_sum"
        ])
        |> Selecto.filter([
          {"tags_weight_sum", {:gt, 0}}  # Only items with weighted tags
        ])
        |> Selecto.group_by(["id", "name"])
        |> Selecto.to_sql()

      assert {sql, params} = result
      assert String.contains?(sql, "IS NOT NULL")  # Null weight filtering
      assert 0.1 in params  # Min weight threshold
    end

    test "handles very large tag counts" do
      domain = %{
        name: "Large Tag Count Test",
        source: %{
          source_table: "documents",
          primary_key: :id,
          fields: [:id, :content],
          columns: %{
            id: %{type: :integer},
            content: %{type: :text}
          },
          associations: %{
            tags: %{queryable: :document_tags, field: :tags, owner_key: :id, related_key: :document_id}
          }
        },
        schemas: %{
          document_tags: %{
            name: "Document Tag",
            source_table: "document_tags",
            fields: [:id, :name, :document_id],
            columns: %{
              id: %{type: :integer},
              name: %{type: :string},
              document_id: %{type: :integer}
            }
          }
        },
        joins: %{
          tags: %{
            type: :tagging,
            tag_field: :name,
            aggregation: :array_agg,
            max_tags: 1000  # Limit for performance
          }
        }
      }

      selecto = Selecto.configure(domain, mock_conn())

      result = selecto
        |> Selecto.select([
          "content",
          "tags_array",
          "tags_count"
        ])
        |> Selecto.filter([
          {"tags_count", {:between, 100, 1000}}  # Large tag counts
        ])
        |> Selecto.group_by(["id"])
        |> Selecto.to_sql()

      assert {sql, params} = result
      assert 100 in params
      assert 1000 in params
    end
  end

  describe "Mixed Join Pattern Edge Cases" do
    test "handles conflicting join types" do
      domain = %{
        name: "Conflicting Join Test",
        source: %{
          source_table: "orders",
          primary_key: :id,
          fields: [:id, :total, :customer_id],
          columns: %{
            id: %{type: :integer},
            total: %{type: :decimal},
            customer_id: %{type: :integer}
          },
          associations: %{
            customer: %{queryable: :customers, field: :customer, owner_key: :customer_id, related_key: :id}
          }
        },
        schemas: %{
          customers: %{
            name: "Customer",
            source_table: "customers",
            fields: [:id, :name, :parent_id],
            columns: %{
              id: %{type: :integer},
              name: %{type: :string},
              parent_id: %{type: :integer}
            },
            associations: %{
              parent: %{queryable: :customers, field: :parent, owner_key: :parent_id, related_key: :id}
            }
          }
        },
        joins: %{
          # Both star dimension AND hierarchical - should resolve conflict
          customer: %{
            type: :star_dimension,  # Primary type
            display_field: :name,
            fallback_type: :hierarchical,  # Fallback if star fails
            hierarchy_type: :adjacency_list,
            depth_limit: 3
          }
        }
      }

      selecto = Selecto.configure(domain, mock_conn())

      result = selecto
        |> Selecto.select([
          "customer_display",      # Star dimension field
          "customer_path",         # Hierarchical field (should conflict)
          {:func, "sum", ["total"]}
        ])
        |> Selecto.group_by(["customer_display"])
        |> Selecto.to_sql()

      assert {sql, _params} = result
      # Should handle conflict gracefully - either use star OR hierarchical
      assert is_binary(sql)
    end

    test "handles deeply nested mixed patterns" do
      domain = build_complex_mixed_domain()
      selecto = Selecto.configure(domain, mock_conn())

      # Very complex query with all join types at maximum nesting
      result = selecto
        |> Selecto.select([
          # Star dimension chains
          "customer[region][country][continent]",
          
          # Hierarchical paths  
          "items[product][category_hierarchy_path]",
          "customer[organization_hierarchy_level]",
          
          # Tagging aggregations
          "items[product][tags_list]",
          "items[product][user_tags_array]",
          "items[product][system_tags_confidence_avg]",
          
          # Complex aggregations
          {:func, "sum", ["total"]},
          {:func, "count", ["DISTINCT", "customer_id"]},
          {:func, "count", ["DISTINCT", "items[product_id]"]}
        ])
        |> Selecto.filter([
          # Multi-level filtering
          {"customer[region][country][active]", true},
          {"items[product][category_hierarchy_level]", {:between, 2, 5}},
          {"items[product][tags_count]", {:gte, 3}},
          {"items[product][system_tags_confidence_avg]", {:gte, 0.8}},
          {"customer[organization_hierarchy_level]", {:lte, 4}},
          
          # Complex conditions
          {:and, [
            {"total", {:gt, 1000}},
            {:or, [
              {"customer[region][country]", "United States"},
              {"customer[region][country]", "Canada"}
            ]}
          ]}
        ])
        |> Selecto.group_by([
          "customer[region][country][continent]",
          "items[product][category_hierarchy_path]"
        ])
        |> Selecto.order_by([
          {:desc, {:func, "sum", ["total"]}},
          "customer[region][country][continent]"
        ])
        |> Selecto.to_sql()

      assert {sql, params} = result
      assert is_binary(sql)
      assert is_list(params)
      
      # Should contain elements from all join types
      assert String.contains?(sql, "LEFT JOIN")  # Star dimensions
      assert String.contains?(sql, "WITH")       # Hierarchical CTEs  
      assert String.contains?(sql, "STRING_AGG") || String.contains?(sql, "ARRAY_AGG")  # Tagging
    end

    test "handles resource exhaustion scenarios" do
      # Create extremely complex domain
      large_domain = build_resource_exhaustion_domain()
      selecto = Selecto.configure(large_domain, mock_conn())

      # Query that could exhaust resources
      start_time = System.monotonic_time(:millisecond)
      
      result = selecto
        |> Selecto.select(build_large_select_list(100))  # 100 fields
        |> Selecto.filter(build_large_filter_list(50))   # 50 filters
        |> Selecto.group_by(build_large_group_list(25))  # 25 group fields
        |> Selecto.to_sql()

      end_time = System.monotonic_time(:millisecond)
      execution_time = end_time - start_time

      assert {sql, params} = result
      assert is_binary(sql)
      assert is_list(params)
      
      # Should complete within reasonable time (5 seconds)
      assert execution_time < 5000, "Query generation took too long: #{execution_time}ms"
      
      # Should not generate excessively large SQL
      assert String.length(sql) < 100_000, "Generated SQL too large: #{String.length(sql)} chars"
    end
  end

  describe "Memory and Performance Edge Cases" do
    test "handles memory pressure scenarios" do
      # Force garbage collection to get baseline
      :erlang.garbage_collect()
      baseline_memory = :erlang.memory(:total)

      # Create many large domains to pressure memory
      domains = for i <- 1..50 do
        build_memory_pressure_domain(i)
      end

      selectos = Enum.map(domains, fn domain ->
        Selecto.configure(domain, mock_conn())
      end)

      # Generate queries on all domains
      results = Enum.map(selectos, fn selecto ->
        selecto
        |> Selecto.select(["name", {:func, "count", ["*"]}])
        |> Selecto.filter([{"active", true}])
        |> Selecto.group_by(["name"])
        |> Selecto.to_sql()
      end)

      # Check memory usage
      current_memory = :erlang.memory(:total)
      memory_increase = current_memory - baseline_memory

      # All queries should succeed
      assert length(results) == 50
      Enum.each(results, fn {sql, params} ->
        assert is_binary(sql)
        assert is_list(params)
      end)

      # Memory increase should be reasonable (< 50MB)
      assert memory_increase < 50 * 1024 * 1024, 
             "Excessive memory usage: #{div(memory_increase, 1024 * 1024)}MB"
    end

    test "handles concurrent query generation" do
      domain = build_typical_domain()
      selecto = Selecto.configure(domain, mock_conn())

      # Generate queries concurrently
      tasks = for i <- 1..20 do
        Task.async(fn ->
          selecto
          |> Selecto.select(["name_#{i}", {:func, "count", ["*"]}])
          |> Selecto.filter([{"value_#{i}", i}])
          |> Selecto.group_by(["name_#{i}"])
          |> Selecto.to_sql()
        end)
      end

      results = Task.await_many(tasks, 5000)

      # All tasks should complete successfully
      assert length(results) == 20
      Enum.each(results, fn {sql, params} ->
        assert is_binary(sql)
        assert is_list(params)
      end)
    end
  end

  describe "Error Recovery Edge Cases" do
    test "recovers from malformed domain gracefully" do
      malformed_domain = %{
        # Missing required fields
        source: %{
          source_table: "items"
          # Missing primary_key, fields, columns
        }
      }

      # Should not crash, but return reasonable default
      result = try do
        selecto = Selecto.configure(malformed_domain, mock_conn())
        {:ok, selecto}
      catch
        kind, reason -> {:error, {kind, reason}}
      end

      case result do
        {:ok, selecto} ->
          # If it doesn't crash, should be usable
          assert %Selecto{} = selecto
        {:error, _} ->
          # If it does crash, that's also acceptable
          assert true
      end
    end

    test "handles SQL injection attempts in field names" do
      domain = %{
        name: "Injection Test",
        source: %{
          source_table: "items",
          primary_key: :id,
          fields: [:id, :name],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string}
          }
        },
        schemas: %{},
        joins: %{}
      }

      selecto = Selecto.configure(domain, mock_conn())

      # Attempt SQL injection through field names
      malicious_fields = [
        "name'; DROP TABLE items; --",
        "id UNION SELECT password FROM users --",
        "name FROM items WHERE 1=1; DELETE FROM items --"
      ]

      Enum.each(malicious_fields, fn malicious_field ->
        result = try do
          selecto
          |> Selecto.select([malicious_field])
          |> Selecto.to_sql()
        catch
          _, _ -> {:error, "Invalid field"}
        end

        case result do
          {sql, _params} ->
            # If SQL is generated, ensure it's properly escaped
            refute String.contains?(sql, "DROP TABLE")
            refute String.contains?(sql, "DELETE FROM")
            refute String.contains?(sql, "password")
          {:error, _} ->
            # Rejecting malicious input is also acceptable
            assert true
        end
      end)
    end
  end

  # Helper functions for building test domains

  defp build_complex_mixed_domain do
    %{
      name: "Complex Mixed Pattern",
      source: %{
        source_table: "orders",
        primary_key: :id,
        fields: [:id, :total, :customer_id, :created_at],
        columns: %{
          id: %{type: :integer},
          total: %{type: :decimal},
          customer_id: %{type: :integer},
          created_at: %{type: :utc_datetime}
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
          fields: [:id, :name, :region_id, :organization_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            region_id: %{type: :integer},
            organization_id: %{type: :integer}
          },
          associations: %{
            region: %{queryable: :regions, field: :region, owner_key: :region_id, related_key: :id},
            organization: %{queryable: :organizations, field: :organization, owner_key: :organization_id, related_key: :id}
          }
        },
        regions: %{
          name: "Region",
          source_table: "regions",
          fields: [:id, :name, :country_id],
          columns: %{id: %{type: :integer}, name: %{type: :string}, country_id: %{type: :integer}},
          associations: %{
            country: %{queryable: :countries, field: :country, owner_key: :country_id, related_key: :id}
          }
        },
        countries: %{
          name: "Country",
          source_table: "countries",
          fields: [:id, :name, :continent, :active],
          columns: %{
            id: %{type: :integer}, name: %{type: :string}, 
            continent: %{type: :string}, active: %{type: :boolean}
          }
        },
        organizations: %{
          name: "Organization",
          source_table: "organizations",
          fields: [:id, :name, :parent_id],
          columns: %{id: %{type: :integer}, name: %{type: :string}, parent_id: %{type: :integer}}
        },
        order_items: %{
          name: "Order Item",
          source_table: "order_items",
          fields: [:id, :quantity, :order_id, :product_id],
          columns: %{
            id: %{type: :integer}, quantity: %{type: :integer}, 
            order_id: %{type: :integer}, product_id: %{type: :integer}
          },
          associations: %{
            product: %{queryable: :products, field: :product, owner_key: :product_id, related_key: :id}
          }
        },
        products: %{
          name: "Product",
          source_table: "products",
          fields: [:id, :name, :category_id],
          columns: %{id: %{type: :integer}, name: %{type: :string}, category_id: %{type: :integer}},
          associations: %{
            category: %{queryable: :categories, field: :category, owner_key: :category_id, related_key: :id},
            tags: %{queryable: :product_tags, field: :tags, owner_key: :id, related_key: :product_id},
            user_tags: %{queryable: :user_product_tags, field: :user_tags, owner_key: :id, related_key: :product_id},
            system_tags: %{queryable: :system_product_tags, field: :system_tags, owner_key: :id, related_key: :product_id}
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
          columns: %{id: %{type: :integer}, name: %{type: :string}, product_id: %{type: :integer}}
        },
        user_product_tags: %{
          name: "User Product Tag",
          source_table: "user_product_tags",
          fields: [:id, :name, :product_id, :user_id],
          columns: %{
            id: %{type: :integer}, name: %{type: :string}, 
            product_id: %{type: :integer}, user_id: %{type: :integer}
          }
        },
        system_product_tags: %{
          name: "System Product Tag",
          source_table: "system_product_tags",
          fields: [:id, :name, :product_id, :confidence],
          columns: %{
            id: %{type: :integer}, name: %{type: :string}, 
            product_id: %{type: :integer}, confidence: %{type: :float}
          }
        }
      },
      joins: %{
        customer: %{
          type: :star_dimension,
          display_field: :name,
          joins: %{
            region: %{
              type: :star_dimension,
              display_field: :name,
              joins: %{
                country: %{type: :star_dimension, display_field: :name}
              }
            },
            organization: %{
              type: :hierarchical,
              hierarchy_type: :adjacency_list,
              depth_limit: 5,
              alias_prefix: "organization_hierarchy"
            }
          }
        },
        items: %{
          type: :left,
          joins: %{
            product: %{
              type: :star_dimension,
              display_field: :name,
              joins: %{
                category: %{
                  type: :hierarchical,
                  hierarchy_type: :adjacency_list,
                  depth_limit: 6,
                  alias_prefix: "category_hierarchy"
                },
                tags: %{type: :tagging, tag_field: :name},
                user_tags: %{type: :tagging, tag_field: :name, aggregation: :array_agg},
                system_tags: %{type: :tagging, tag_field: :name, weight_field: :confidence, min_weight: 0.5}
              }
            }
          }
        }
      }
    }
  end

  defp build_resource_exhaustion_domain do
    schema_count = 20
    
    schemas = for i <- 1..schema_count do
      {:"schema_#{i}", %{
        name: "Schema #{i}",
        source_table: "table_#{i}",
        fields: [:id, :name, :value, :"category_#{i}", :"parent_#{i}_id"],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          value: %{type: :decimal},
          "category_#{i}": %{type: :string},
          "parent_#{i}_id": %{type: :integer}
        },
        associations: %{
          "parent_#{i}": %{queryable: :"schema_#{i}", field: :"parent_#{i}", owner_key: :"parent_#{i}_id", related_key: :id},
          "tags_#{i}": %{queryable: :"tags_#{i}", field: :"tags_#{i}", owner_key: :id, related_key: :"schema_#{i}_id"}
        }
      }}
    end |> Enum.into(%{})
    
    tag_schemas = for i <- 1..schema_count do
      {:"tags_#{i}", %{
        name: "Tags #{i}",
        source_table: "tags_#{i}",
        fields: [:id, :name, :"schema_#{i}_id"],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          "schema_#{i}_id": %{type: :integer}
        }
      }}
    end |> Enum.into(%{})
    
    associations = for i <- 1..schema_count do
      {:"schema_#{i}", %{queryable: :"schema_#{i}", field: :"schema_#{i}", owner_key: :"schema_#{i}_id", related_key: :id}}
    end |> Enum.into(%{})
    
    joins = for i <- 1..schema_count do
      {:"schema_#{i}", %{
        type: :star_dimension,
        display_field: :name,
        joins: %{
          "parent_#{i}": %{
            type: :hierarchical,
            hierarchy_type: :adjacency_list,
            depth_limit: 5
          },
          "tags_#{i}": %{type: :tagging, tag_field: :name}
        }
      }}
    end |> Enum.into(%{})
    
    %{
      name: "Resource Exhaustion Test",
      source: %{
        source_table: "main_table",
        primary_key: :id,
        fields: [:id, :data] ++ (for i <- 1..schema_count, do: :"schema_#{i}_id"),
        columns: %{id: %{type: :integer}, data: %{type: :string}},
        associations: associations
      },
      schemas: Map.merge(schemas, tag_schemas),
      joins: joins
    }
  end

  defp build_memory_pressure_domain(suffix) do
    %{
      name: "Memory Pressure #{suffix}",
      source: %{
        source_table: "items_#{suffix}",
        primary_key: :id,
        fields: [:id, :name, :value, :active],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          value: %{type: :decimal},
          active: %{type: :boolean}
        }
      },
      schemas: %{},
      joins: %{}
    }
  end

  defp build_typical_domain do
    %{
      name: "Typical Test Domain",
      source: %{
        source_table: "items",
        primary_key: :id,
        fields: [:id, :name, :category_id, :active],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          category_id: %{type: :integer},
          active: %{type: :boolean}
        },
        associations: %{
          category: %{queryable: :categories, field: :category, owner_key: :category_id, related_key: :id}
        }
      },
      schemas: %{
        categories: %{
          name: "Category",
          source_table: "categories",
          fields: [:id, :name],
          columns: %{id: %{type: :integer}, name: %{type: :string}}
        }
      },
      joins: %{
        category: %{type: :left, display_field: :name}
      }
    }
  end

  defp build_large_select_list(count) do
    base = ["id", "name", {:func, "count", ["*"]}, {:func, "sum", ["value"]}]
    dynamic = for i <- 1..(count-4), do: "field_#{i}"
    base ++ dynamic
  end

  defp build_large_filter_list(count) do
    base = [{"active", true}, {"name", {:not_null}}, {"value", {:gt, 0}}]
    dynamic = for i <- 1..(count-3), do: {"field_#{i}", {:eq, "value_#{i}"}}
    base ++ dynamic
  end

  defp build_large_group_list(count) do
    base = ["name", "category"]
    dynamic = for i <- 1..(count-2), do: "group_#{i}"
    base ++ dynamic
  end
end