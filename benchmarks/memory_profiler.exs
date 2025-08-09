# Selecto Memory Profiler
#
# Run with: mix run benchmarks/memory_profiler.exs
#
# This script analyzes memory usage patterns for different join types
# and helps identify potential memory leaks or inefficiencies.

defmodule SelectoMemoryProfiler do
  alias Selecto

  def run_memory_analysis do
    IO.puts("🧠 Selecto Memory Usage Analysis")
    IO.puts("=" |> String.duplicate(50))
    
    # Get baseline memory
    :erlang.garbage_collect()
    baseline_memory = get_memory_info()
    
    IO.puts("Baseline Memory Usage:")
    print_memory_info(baseline_memory)
    IO.puts("")

    # Test different scenarios
    scenarios = [
      {"Simple Star Schema", &test_star_schema_memory/0},
      {"Complex Snowflake Schema", &test_snowflake_memory/0},
      {"Deep Hierarchy (10 levels)", &test_deep_hierarchy_memory/0},
      {"Many-to-Many Tagging", &test_tagging_memory/0},
      {"Large Mixed Domain", &test_large_domain_memory/0},
      {"Repeated Query Generation", &test_repeated_queries/0}
    ]

    results = Enum.map(scenarios, fn {name, test_func} ->
      IO.puts("Testing: #{name}")
      
      # Clean up before test
      :erlang.garbage_collect()
      start_memory = get_memory_info()
      
      # Run test
      test_func.()
      
      # Measure after test
      end_memory = get_memory_info()
      
      # Clean up after test
      :erlang.garbage_collect()
      cleanup_memory = get_memory_info()
      
      memory_diff = calculate_memory_diff(start_memory, end_memory)
      cleanup_diff = calculate_memory_diff(end_memory, cleanup_memory)
      
      IO.puts("  Memory increase: #{format_bytes(memory_diff[:total])} total")
      IO.puts("  Process memory: #{format_bytes(memory_diff[:processes])}")
      IO.puts("  Binary memory: #{format_bytes(memory_diff[:binary])}")
      IO.puts("  Cleanup recovered: #{format_bytes(-cleanup_diff[:total])}")
      IO.puts("")
      
      {name, memory_diff, cleanup_diff}
    end)

    # Summary
    IO.puts("Memory Analysis Summary:")
    IO.puts("-" |> String.duplicate(30))
    
    Enum.each(results, fn {name, memory_diff, cleanup_diff} ->
      net_usage = memory_diff[:total] + cleanup_diff[:total]
      IO.puts("#{String.pad_trailing(name, 25)} | Total: #{format_bytes(memory_diff[:total])} | Net: #{format_bytes(net_usage)}")
    end)
    
    # Memory efficiency recommendations
    print_recommendations(results)
  end

  defp test_star_schema_memory do
    conn = mock_conn()
    domain = build_star_schema_domain(20)  # 20 dimensions
    selecto = Selecto.configure(domain, conn)
    
    # Generate multiple queries
    for _i <- 1..100 do
      selecto
      |> Selecto.select(["customer_display", "product_display", {:func, "sum", ["amount"]}])
      |> Selecto.filter([{"customer[segment]", "Premium"}, {"active", true}])
      |> Selecto.group_by(["customer[segment]"])
      |> Selecto.to_sql()
    end
  end

  defp test_snowflake_memory do
    conn = mock_conn()
    domain = build_complex_snowflake_domain()
    selecto = Selecto.configure(domain, conn)
    
    # Generate complex nested queries
    for _i <- 1..50 do
      selecto
      |> Selecto.select([
        "customer[region][country][continent]",
        "product[category][parent][grandparent][name]",
        {:func, "sum", ["amount"]}
      ])
      |> Selecto.filter([
        {"customer[region][country][active]", true},
        {"product[category][level]", {:lte, 5}}
      ])
      |> Selecto.group_by(["customer[region][country][continent]"])
      |> Selecto.to_sql()
    end
  end

  defp test_deep_hierarchy_memory do
    conn = mock_conn()
    domain = build_deep_hierarchy_domain(10)  # 10 levels deep
    selecto = Selecto.configure(domain, conn)
    
    # Generate hierarchical CTE queries
    for _i <- 1..25 do
      selecto
      |> Selecto.select([
        "name",
        "hierarchy_path", 
        "hierarchy_level",
        "hierarchy_ancestors",
        {:func, "count", ["*"]}
      ])
      |> Selecto.filter([
        {"hierarchy_level", {:between, 2, 8}},
        {"active", true}
      ])
      |> Selecto.group_by(["hierarchy_level"])
      |> Selecto.to_sql()
    end
  end

  defp test_tagging_memory do
    conn = mock_conn()
    domain = build_multi_tagging_domain()
    selecto = Selecto.configure(domain, conn)
    
    # Generate complex tagging queries
    for _i <- 1..75 do
      selecto
      |> Selecto.select([
        "title",
        "tags_list",
        "tags_count",
        "user_tags_array", 
        "system_tags_list",
        "category_tags_list",
        {:func, "count", ["*"]}
      ])
      |> Selecto.filter([
        {"tags_filter", "programming"},
        {"user_tags_any", ["web", "mobile", "api"]},
        {"system_tags_confidence", {:gte, 0.8}},
        {"category_tags_count", {:gte, 2}}
      ])
      |> Selecto.group_by(["tags_count"])
      |> Selecto.to_sql()
    end
  end

  defp test_large_domain_memory do
    conn = mock_conn()
    domain = build_very_large_domain(100)  # 100 schemas
    selecto = Selecto.configure(domain, conn)
    
    # Generate queries with many joins
    for _i <- 1..25 do
      selecto
      |> Selecto.select(build_large_select_list(50))
      |> Selecto.filter(build_large_filter_list(30))
      |> Selecto.group_by(build_large_group_list(20))
      |> Selecto.to_sql()
    end
  end

  defp test_repeated_queries do
    conn = mock_conn()
    domain = build_typical_domain()
    selecto = Selecto.configure(domain, conn)
    
    # Test for memory leaks in repeated query generation
    for _i <- 1..1000 do
      selecto
      |> Selecto.select(["name", "category[name]", {:func, "count", ["*"]}])
      |> Selecto.filter([{"active", true}, {"category[level]", {:lte, 3}}])
      |> Selecto.group_by(["category[name]"])
      |> Selecto.to_sql()
    end
  end

  # Helper functions
  
  defp mock_conn do
    %{__struct__: Postgrex.Connection, pid: self()}
  end

  defp get_memory_info do
    %{
      total: :erlang.memory(:total),
      processes: :erlang.memory(:processes),
      processes_used: :erlang.memory(:processes_used),
      system: :erlang.memory(:system),
      atom: :erlang.memory(:atom),
      atom_used: :erlang.memory(:atom_used),
      binary: :erlang.memory(:binary),
      code: :erlang.memory(:code),
      ets: :erlang.memory(:ets)
    }
  end

  defp print_memory_info(memory_info) do
    IO.puts("  Total:        #{format_bytes(memory_info[:total])}")
    IO.puts("  Processes:    #{format_bytes(memory_info[:processes])}")
    IO.puts("  Binary:       #{format_bytes(memory_info[:binary])}")
    IO.puts("  ETS:          #{format_bytes(memory_info[:ets])}")
    IO.puts("  Code:         #{format_bytes(memory_info[:code])}")
  end

  defp calculate_memory_diff(start_memory, end_memory) do
    Map.new(start_memory, fn {key, start_val} ->
      end_val = Map.get(end_memory, key, 0)
      {key, end_val - start_val}
    end)
  end

  defp format_bytes(bytes) when bytes < 1024, do: "#{bytes}B"
  defp format_bytes(bytes) when bytes < 1024 * 1024 do
    "#{Float.round(bytes / 1024, 1)}KB"
  end
  defp format_bytes(bytes) when bytes < 1024 * 1024 * 1024 do
    "#{Float.round(bytes / (1024 * 1024), 1)}MB"
  end
  defp format_bytes(bytes) do
    "#{Float.round(bytes / (1024 * 1024 * 1024), 1)}GB"
  end

  defp print_recommendations(results) do
    IO.puts("\n💡 Memory Optimization Recommendations:")
    IO.puts("-" |> String.duplicate(45))
    
    # Find highest memory usage
    {highest_name, highest_usage, _} = Enum.max_by(results, fn {_, diff, _} -> diff[:total] end)
    IO.puts("• #{highest_name} uses the most memory (#{format_bytes(highest_usage[:total])})")
    
    # Find patterns with poor cleanup
    poor_cleanup = Enum.filter(results, fn {_, memory_diff, cleanup_diff} ->
      net_usage = memory_diff[:total] + cleanup_diff[:total]
      net_usage > memory_diff[:total] * 0.1  # More than 10% not cleaned up
    end)
    
    if poor_cleanup != [] do
      IO.puts("• Potential memory leaks detected in:")
      Enum.each(poor_cleanup, fn {name, _, _} -> IO.puts("  - #{name}") end)
    else
      IO.puts("• ✅ Good memory cleanup across all patterns")
    end
    
    # General recommendations
    IO.puts("• For large domains, consider pagination or result limiting")
    IO.puts("• Deep hierarchies benefit from depth limits")
    IO.puts("• Complex tagging queries should use selective filters")
    IO.puts("• Mixed patterns work best with indexed dimension filters")
  end

  # Domain builders for testing

  defp build_star_schema_domain(dimension_count) do
    dimensions = for i <- 1..dimension_count do
      {:"dim_#{i}", %{
        name: "Dimension #{i}",
        source_table: "dim_#{i}",
        fields: [:id, :name, :category],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          category: %{type: :string}
        }
      }}
    end |> Enum.into(%{})
    
    associations = for i <- 1..dimension_count do
      {:"dim_#{i}", %{queryable: :"dim_#{i}", field: :"dim_#{i}", owner_key: :"dim_#{i}_id", related_key: :id}}
    end |> Enum.into(%{})
    
    joins = for i <- 1..dimension_count do
      {:"dim_#{i}", %{type: :star_dimension, display_field: :name}}
    end |> Enum.into(%{})
    
    %{
      name: "Large Star Schema",
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
  end

  defp build_complex_snowflake_domain do
    %{
      name: "Complex Snowflake",
      source: %{
        source_table: "facts",
        primary_key: :id,
        fields: [:id, :amount, :customer_id, :product_id],
        columns: %{
          id: %{type: :integer},
          amount: %{type: :decimal},
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
          fields: [:id, :name, :region_id],
          columns: %{id: %{type: :integer}, name: %{type: :string}, region_id: %{type: :integer}},
          associations: %{
            region: %{queryable: :regions, field: :region, owner_key: :region_id, related_key: :id}
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
        products: %{
          name: "Product",
          source_table: "products",
          fields: [:id, :name, :category_id],
          columns: %{id: %{type: :integer}, name: %{type: :string}, category_id: %{type: :integer}},
          associations: %{
            category: %{queryable: :categories, field: :category, owner_key: :category_id, related_key: :id}
          }
        },
        categories: %{
          name: "Category",
          source_table: "categories",
          fields: [:id, :name, :parent_id, :level],
          columns: %{
            id: %{type: :integer}, name: %{type: :string}, 
            parent_id: %{type: :integer}, level: %{type: :integer}
          },
          associations: %{
            parent: %{queryable: :categories, field: :parent, owner_key: :parent_id, related_key: :id},
            grandparent: %{queryable: :categories, field: :grandparent, owner_key: :grandparent_id, related_key: :id}
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
              depth_limit: 6,
              joins: %{
                parent: %{type: :left},
                grandparent: %{type: :left}
              }
            }
          }
        }
      }
    }
  end

  defp build_deep_hierarchy_domain(depth) do
    %{
      name: "Deep Hierarchy",
      source: %{
        source_table: "nodes",
        primary_key: :id,
        fields: [:id, :name, :parent_id, :active],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          parent_id: %{type: :integer},
          active: %{type: :boolean}
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
          depth_limit: depth,
          alias_prefix: "hierarchy"
        }
      }
    }
  end

  defp build_multi_tagging_domain do
    %{
      name: "Multi Tagging",
      source: %{
        source_table: "items",
        primary_key: :id,
        fields: [:id, :title],
        columns: %{id: %{type: :integer}, title: %{type: :string}},
        associations: %{
          tags: %{queryable: :item_tags, field: :tags, owner_key: :id, related_key: :item_id},
          user_tags: %{queryable: :user_tags, field: :user_tags, owner_key: :id, related_key: :item_id},
          system_tags: %{queryable: :system_tags, field: :system_tags, owner_key: :id, related_key: :item_id},
          category_tags: %{queryable: :category_tags, field: :category_tags, owner_key: :id, related_key: :item_id}
        }
      },
      schemas: %{
        item_tags: %{
          name: "Item Tag",
          source_table: "item_tags",
          fields: [:id, :name, :item_id],
          columns: %{id: %{type: :integer}, name: %{type: :string}, item_id: %{type: :integer}}
        },
        user_tags: %{
          name: "User Tag",
          source_table: "user_tags",
          fields: [:id, :name, :item_id],
          columns: %{id: %{type: :integer}, name: %{type: :string}, item_id: %{type: :integer}}
        },
        system_tags: %{
          name: "System Tag",
          source_table: "system_tags",
          fields: [:id, :name, :item_id, :confidence],
          columns: %{
            id: %{type: :integer}, name: %{type: :string},
            item_id: %{type: :integer}, confidence: %{type: :float}
          }
        },
        category_tags: %{
          name: "Category Tag",
          source_table: "category_tags",
          fields: [:id, :name, :item_id],
          columns: %{id: %{type: :integer}, name: %{type: :string}, item_id: %{type: :integer}}
        }
      },
      joins: %{
        tags: %{type: :tagging, tag_field: :name},
        user_tags: %{type: :tagging, tag_field: :name, aggregation: :array_agg},
        system_tags: %{type: :tagging, tag_field: :name, weight_field: :confidence},
        category_tags: %{type: :tagging, tag_field: :name}
      }
    }
  end

  defp build_very_large_domain(schema_count) do
    # Build a domain with many schemas to test memory usage
    schemas = for i <- 1..schema_count do
      {:"schema_#{i}", %{
        name: "Schema #{i}",
        source_table: "table_#{i}",
        fields: [:id, :name, :value, :category],
        columns: %{
          id: %{type: :integer}, name: %{type: :string},
          value: %{type: :decimal}, category: %{type: :string}
        }
      }}
    end |> Enum.into(%{})
    
    associations = for i <- 1..min(schema_count, 50) do  # Limit associations for memory test
      {:"schema_#{i}", %{queryable: :"schema_#{i}", field: :"schema_#{i}", owner_key: :"schema_#{i}_id", related_key: :id}}
    end |> Enum.into(%{})
    
    joins = for i <- 1..min(schema_count, 50) do
      {:"schema_#{i}", %{type: :left, display_field: :name}}
    end |> Enum.into(%{})
    
    %{
      name: "Very Large Domain",
      source: %{
        source_table: "main",
        primary_key: :id,
        fields: [:id, :data],
        columns: %{id: %{type: :integer}, data: %{type: :string}},
        associations: associations
      },
      schemas: schemas,
      joins: joins
    }
  end

  defp build_typical_domain do
    %{
      name: "Typical Domain",
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
          fields: [:id, :name, :parent_id, :level],
          columns: %{
            id: %{type: :integer}, name: %{type: :string},
            parent_id: %{type: :integer}, level: %{type: :integer}
          }
        }
      },
      joins: %{
        category: %{type: :hierarchical, hierarchy_type: :adjacency_list, depth_limit: 5}
      }
    }
  end

  defp build_large_select_list(count) do
    base = ["id", "name", {:func, "count", ["*"]}]
    dynamic = for i <- 1..(count-3), do: "field_#{i}"
    base ++ dynamic
  end

  defp build_large_filter_list(count) do
    base = [{"active", true}, {"name", {:not_null}}]
    dynamic = for i <- 1..(count-2), do: {"field_#{i}", {:eq, "value_#{i}"}}
    base ++ dynamic
  end

  defp build_large_group_list(count) do
    base = ["name"]
    dynamic = for i <- 1..(count-1), do: "group_#{i}"
    base ++ dynamic
  end
end

# Run the memory analysis
SelectoMemoryProfiler.run_memory_analysis()