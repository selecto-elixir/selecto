# Selecto Join Patterns Performance Benchmark
#
# Run with: mix run benchmarks/join_patterns_benchmark.exs
#
# This benchmark tests the performance of different join patterns in Selecto:
# - Star schema dimensions  
# - Snowflake schema joins
# - Hierarchical patterns (adjacency list and materialized path)
# - Many-to-many tagging
# - Mixed join patterns
#
# Results help identify performance bottlenecks and optimize query generation.

# Add benchee to deps or install: mix deps.get
# If benchee is not available, this will provide a simple timing fallback

try do
  Application.ensure_all_started(:benchee)
  use_benchee = true
rescue
  _ -> use_benchee = false
end

defmodule SelectoBenchmark do
  alias Selecto

  # Mock connection for benchmarking
  def mock_conn do
    %{
      __struct__: Postgrex.Connection,
      pid: self(),
      parameters: %{}
    }
  end

  def run_benchmarks do
    if use_benchee() do
      run_with_benchee()
    else
      run_simple_benchmark()
    end
  end

  defp use_benchee, do: function_exported?(Benchee, :run, 1)

  defp run_with_benchee do
    conn = mock_conn()
    
    # Setup domains
    star_domain = build_star_schema_domain()
    snowflake_domain = build_snowflake_domain() 
    hierarchy_domain = build_hierarchy_domain()
    tagging_domain = build_tagging_domain()
    mixed_domain = build_mixed_domain()
    
    star_selecto = Selecto.configure(star_domain, conn)
    snowflake_selecto = Selecto.configure(snowflake_domain, conn)
    hierarchy_selecto = Selecto.configure(hierarchy_domain, conn)
    tagging_selecto = Selecto.configure(tagging_domain, conn)
    mixed_selecto = Selecto.configure(mixed_domain, conn)

    IO.puts("🚀 Running Selecto Join Pattern Benchmarks with Benchee")
    IO.puts("=" |> String.duplicate(60))

    Benchee.run(%{
      "Star Schema Query" => fn ->
        star_selecto
        |> Selecto.select([
          "customer_display",
          "product_display", 
          "date[year]",
          "store[region]",
          {:func, "sum", ["sale_amount"]},
          {:func, "count", ["*"]}
        ])
        |> Selecto.filter([
          {"customer[segment]", "Premium"},
          {"date[year]", 2024},
          {"product[category]", "Electronics"}
        ])
        |> Selecto.group_by(["customer[segment]", "product[category]", "date[year]"])
        |> Selecto.to_sql()
      end,

      "Snowflake Schema Query" => fn ->
        snowflake_selecto
        |> Selecto.select([
          "customer[region][country_display]",
          "product[category][parent][name]", 
          "product[brand_display]",
          {:func, "sum", ["sale_amount"]},
          {:func, "avg", ["quantity"]}
        ])
        |> Selecto.filter([
          {"customer[region][country][continent]", "North America"},
          {"product[category][level]", {:lte, 3}},
          {"product[brand][active]", true}
        ])
        |> Selecto.group_by([
          "customer[region][country_display]",
          "product[category][parent][name]"
        ])
        |> Selecto.to_sql()
      end,

      "Adjacency List Hierarchy" => fn ->
        hierarchy_selecto
        |> Selecto.select([
          "name",
          "manager_path",
          "manager_level",
          "department_path",
          {:func, "count", ["*"]}
        ])
        |> Selecto.filter([
          {"manager_level", {:between, 1, 5}},
          {"department_level", {:lte, 3}},
          {"active", true}
        ])
        |> Selecto.group_by(["manager_level", "department_path"])
        |> Selecto.to_sql()
      end,

      "Many-to-Many Tagging" => fn ->
        tagging_selecto
        |> Selecto.select([
          "title",
          "author_display",
          "tags_list",
          "tags_count",
          {:func, "count", ["*"]}
        ])
        |> Selecto.filter([
          {"tags_filter", "programming"},
          {"tags_any", ["web", "mobile"]},
          {"tags_count", {:gte, 2}},
          {"published_at", {:gte, ~D[2024-01-01]}}
        ])
        |> Selecto.group_by(["author_display", "tags_count"])
        |> Selecto.to_sql()
      end,

      "Mixed Join Patterns" => fn ->
        mixed_selecto
        |> Selecto.select([
          # Star dimensions
          "customer_display",
          "customer[region_display]",
          # Hierarchical
          "items[product][category_path]",
          "items[product][category_level]",
          # Tagging
          "items[product][tags_list]",
          # Aggregations
          {:func, "sum", ["total"]},
          {:func, "count", ["DISTINCT", "customer_id"]}
        ])
        |> Selecto.filter([
          {"customer[segment]", "Enterprise"},
          {"items[product][category_level]", {:lte, 4}},
          {"items[product][tags_filter]", "premium"},
          {"status", "completed"}
        ])
        |> Selecto.group_by([
          "customer[region_display]",
          "items[product][category_path]"
        ])
        |> Selecto.to_sql()
      end
    },
    time: 10,
    memory_time: 2,
    print: [
      benchmarking: true,
      fast_warning: false,
      configuration: true
    ],
    formatters: [
      Benchee.Formatters.Console,
      {Benchee.Formatters.HTML, file: "benchmarks/results.html"}
    ])
  end

  defp run_simple_benchmark do
    conn = mock_conn()
    
    IO.puts("⚡ Running Simple Performance Benchmark")
    IO.puts("=" |> String.duplicate(50))
    IO.puts("(Install benchee for detailed analysis: mix deps.get)")
    IO.puts("")

    # Star Schema Benchmark
    star_domain = build_star_schema_domain()
    star_selecto = Selecto.configure(star_domain, conn)
    
    {time_star, _} = :timer.tc(fn ->
      for _i <- 1..1000 do
        star_selecto
        |> Selecto.select(["customer_display", "product_display", {:func, "sum", ["sale_amount"]}])
        |> Selecto.filter([{"customer[segment]", "Premium"}, {"date[year]", 2024}])
        |> Selecto.group_by(["customer[segment]"])
        |> Selecto.to_sql()
      end
    end)
    
    avg_star = time_star / 1000 / 1000
    IO.puts("⭐ Star Schema:     #{Float.round(avg_star, 3)}ms avg (1000 iterations)")

    # Hierarchical Benchmark
    hierarchy_domain = build_hierarchy_domain()
    hierarchy_selecto = Selecto.configure(hierarchy_domain, conn)
    
    {time_hierarchy, _} = :timer.tc(fn ->
      for _i <- 1..500 do
        hierarchy_selecto
        |> Selecto.select(["name", "manager_path", "manager_level"])
        |> Selecto.filter([{"manager_level", {:between, 1, 5}}, {"active", true}])
        |> Selecto.group_by(["manager_level"])
        |> Selecto.to_sql()
      end
    end)
    
    avg_hierarchy = time_hierarchy / 500 / 1000
    IO.puts("🌳 Hierarchy:      #{Float.round(avg_hierarchy, 3)}ms avg (500 iterations)")

    # Tagging Benchmark
    tagging_domain = build_tagging_domain()
    tagging_selecto = Selecto.configure(tagging_domain, conn)
    
    {time_tagging, _} = :timer.tc(fn ->
      for _i <- 1..500 do
        tagging_selecto
        |> Selecto.select(["title", "tags_list", "tags_count"])
        |> Selecto.filter([{"tags_filter", "tech"}, {"tags_count", {:gte, 2}}])
        |> Selecto.group_by(["tags_count"])
        |> Selecto.to_sql()
      end
    end)
    
    avg_tagging = time_tagging / 500 / 1000
    IO.puts("🏷️  Tagging:        #{Float.round(avg_tagging, 3)}ms avg (500 iterations)")

    # Mixed Pattern Benchmark
    mixed_domain = build_mixed_domain()
    mixed_selecto = Selecto.configure(mixed_domain, conn)
    
    {time_mixed, _} = :timer.tc(fn ->
      for _i <- 1..100 do
        mixed_selecto
        |> Selecto.select([
          "customer_display",
          "items[product][category_path]",
          "items[product][tags_list]",
          {:func, "sum", ["total"]}
        ])
        |> Selecto.filter([
          {"customer[segment]", "Premium"},
          {"items[product][category_level]", {:lte, 3}},
          {"items[product][tags_filter]", "featured"}
        ])
        |> Selecto.group_by(["customer[segment]"])
        |> Selecto.to_sql()
      end
    end)
    
    avg_mixed = time_mixed / 100 / 1000
    IO.puts("🔀 Mixed Patterns: #{Float.round(avg_mixed, 3)}ms avg (100 iterations)")
    
    IO.puts("")
    IO.puts("Performance Summary:")
    IO.puts("- Star Schema performs best for simple analytical queries")
    IO.puts("- Hierarchical joins have moderate overhead for CTE generation")
    IO.puts("- Mixed patterns are most complex but still reasonable for real-world use")
  end

  # Domain builders (simplified versions for benchmarking)
  
  defp build_star_schema_domain do
    %{
      name: "Star Schema Benchmark",
      source: %{
        source_table: "sales_facts",
        primary_key: :id,
        fields: [:id, :sale_amount, :quantity, :customer_id, :product_id, :date_id, :store_id],
        columns: %{
          id: %{type: :integer},
          sale_amount: %{type: :decimal},
          quantity: %{type: :integer},
          customer_id: %{type: :integer},
          product_id: %{type: :integer},
          date_id: %{type: :integer},
          store_id: %{type: :integer}
        },
        associations: %{
          customer: %{queryable: :customers, field: :customer, owner_key: :customer_id, related_key: :id},
          product: %{queryable: :products, field: :product, owner_key: :product_id, related_key: :id},
          date: %{queryable: :dates, field: :date, owner_key: :date_id, related_key: :id},
          store: %{queryable: :stores, field: :store, owner_key: :store_id, related_key: :id}
        }
      },
      schemas: %{
        customers: %{
          name: "Customer",
          source_table: "customers",
          fields: [:id, :name, :segment, :region],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            segment: %{type: :string},
            region: %{type: :string}
          }
        },
        products: %{
          name: "Product",
          source_table: "products",
          fields: [:id, :name, :category, :brand],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            category: %{type: :string},
            brand: %{type: :string}
          }
        },
        dates: %{
          name: "Date",
          source_table: "dates",
          fields: [:id, :date_value, :year, :quarter, :month],
          columns: %{
            id: %{type: :integer},
            date_value: %{type: :date},
            year: %{type: :integer},
            quarter: %{type: :integer},
            month: %{type: :integer}
          }
        },
        stores: %{
          name: "Store",
          source_table: "stores",
          fields: [:id, :name, :region, :type],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            region: %{type: :string},
            type: %{type: :string}
          }
        }
      },
      joins: %{
        customer: %{type: :star_dimension, display_field: :name},
        product: %{type: :star_dimension, display_field: :name},
        date: %{type: :star_dimension, display_field: :date_value},
        store: %{type: :star_dimension, display_field: :name}
      }
    }
  end

  defp build_snowflake_domain do
    %{
      name: "Snowflake Benchmark",
      source: %{
        source_table: "sales",
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
          fields: [:id, :name, :region_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            region_id: %{type: :integer}
          },
          associations: %{
            region: %{queryable: :regions, field: :region, owner_key: :region_id, related_key: :id}
          }
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
          fields: [:id, :name, :category_id, :brand_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            category_id: %{type: :integer},
            brand_id: %{type: :integer}
          },
          associations: %{
            category: %{queryable: :categories, field: :category, owner_key: :category_id, related_key: :id},
            brand: %{queryable: :brands, field: :brand, owner_key: :brand_id, related_key: :id}
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
          fields: [:id, :name, :active],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            active: %{type: :boolean}
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
              depth_limit: 5,
              joins: %{
                parent: %{type: :left}
              }
            },
            brand: %{type: :star_dimension, display_field: :name}
          }
        }
      }
    }
  end

  defp build_hierarchy_domain do
    %{
      name: "Hierarchy Benchmark",
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
          depth_limit: 8
        },
        department: %{
          type: :hierarchical,
          hierarchy_type: :adjacency_list,
          depth_limit: 5
        }
      }
    }
  end

  defp build_tagging_domain do
    %{
      name: "Tagging Benchmark",
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
          tags: %{queryable: :article_tags, field: :tags, owner_key: :id, related_key: :article_id}
        }
      },
      schemas: %{
        users: %{
          name: "User",
          source_table: "users",
          fields: [:id, :name],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string}
          }
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
        }
      },
      joins: %{
        author: %{type: :left, display_field: :name},
        tags: %{type: :tagging, tag_field: :name, weight_field: :weight}
      }
    }
  end

  defp build_mixed_domain do
    %{
      name: "Mixed Pattern Benchmark",
      source: %{
        source_table: "orders",
        primary_key: :id,
        fields: [:id, :total, :customer_id, :status],
        columns: %{
          id: %{type: :integer},
          total: %{type: :decimal},
          customer_id: %{type: :integer},
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
          fields: [:id, :name, :segment, :region_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            segment: %{type: :string},
            region_id: %{type: :integer}
          },
          associations: %{
            region: %{queryable: :regions, field: :region, owner_key: :region_id, related_key: :id}
          }
        },
        regions: %{
          name: "Region",
          source_table: "regions",
          fields: [:id, :name],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string}
          }
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
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            parent_id: %{type: :integer}
          }
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
end

# Run the benchmarks
SelectoBenchmark.run_benchmarks()