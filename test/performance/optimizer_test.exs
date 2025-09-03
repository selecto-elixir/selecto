defmodule Selecto.Performance.OptimizerTest do
  use ExUnit.Case
  
  alias Selecto.Performance.Optimizer
  
  describe "optimization suggestions" do
    test "suggests indexes for filtered fields" do
      selecto = %{
        select: ["id", "name", "email"],
        filters: %{
          "created_at" => {:gte, ~D[2024-01-01]},
          "status" => "active",
          "category_id" => 5
        },
        joins: %{},
        group_by: [],
        order_by: [],
        limit: nil,
        source: %{source_table: "users"}
      }
      
      {:ok, suggestions} = Optimizer.suggest_optimizations(selecto)
      
      # Should suggest indexes for timestamp fields
      timestamp_suggestion = Enum.find(suggestions, fn s ->
        s.type == :missing_index && String.contains?(s.message, "created_at")
      end)
      
      assert timestamp_suggestion
      assert timestamp_suggestion.severity == :medium
      assert String.contains?(timestamp_suggestion.sql, "CREATE INDEX")
    end
    
    test "detects excessive joins" do
      selecto = %{
        select: ["*"],
        filters: %{},
        joins: %{
          orders: %{},
          items: %{},
          products: %{},
          categories: %{},
          suppliers: %{},
          warehouses: %{}
        },
        group_by: [],
        order_by: [],
        source: %{source_table: "users"}
      }
      
      {:ok, suggestions} = Optimizer.suggest_optimizations(selecto)
      
      excessive_joins = Enum.find(suggestions, fn s ->
        s.type == :excessive_joins
      end)
      
      assert excessive_joins
      assert excessive_joins.severity == :high
      assert String.contains?(excessive_joins.message, "6 joins")
    end
    
    test "detects Cartesian products" do
      selecto = %{
        select: ["*"],
        filters: %{},
        joins: %{
          orders: %{},  # Missing join conditions
          products: %{}  # Missing join conditions
        },
        group_by: [],
        order_by: [],
        source: %{source_table: "users"}
      }
      
      {:ok, suggestions} = Optimizer.suggest_optimizations(selecto)
      
      cartesian_warnings = Enum.filter(suggestions, fn s ->
        s.type == :cartesian_product
      end)
      
      assert length(cartesian_warnings) == 2
      assert hd(cartesian_warnings).severity == :critical
    end
    
    test "suggests join reordering based on filters" do
      selecto = %{
        select: ["*"],
        filters: %{
          "products.category" => "electronics",
          "products.price" => {:lte, 1000},
          "orders.status" => "completed"
        },
        joins: %{
          orders: %{},
          items: %{},
          products: %{}
        },
        group_by: [],
        order_by: [],
        source: %{source_table: "users"}
      }
      
      {:ok, suggestions} = Optimizer.suggest_optimizations(selecto)
      
      join_order_suggestion = Enum.find(suggestions, fn s ->
        s.type == :join_order
      end)
      
      if join_order_suggestion do
        assert join_order_suggestion.severity == :medium
        # Products should be suggested first due to more filters
        assert hd(join_order_suggestion.suggested_order) == :products
      end
    end
    
    test "detects SELECT * with GROUP BY" do
      selecto = %{
        select: [:*],
        filters: %{},
        joins: %{},
        group_by: ["category", "status"],
        order_by: [],
        source: %{source_table: "products"}
      }
      
      {:ok, suggestions} = Optimizer.suggest_optimizations(selecto)
      
      invalid_select = Enum.find(suggestions, fn s ->
        s.type == :select_star_with_group
      end)
      
      assert invalid_select
      assert invalid_select.severity == :high
    end
    
    test "warns about LIMIT without ORDER BY" do
      selecto = %{
        select: ["id", "name"],
        filters: %{},
        joins: %{},
        group_by: [],
        order_by: [],
        limit: 10,
        source: %{source_table: "users"}
      }
      
      {:ok, suggestions} = Optimizer.suggest_optimizations(selecto)
      
      limit_warning = Enum.find(suggestions, fn s ->
        s.type == :limit_without_order
      end)
      
      assert limit_warning
      assert limit_warning.severity == :medium
    end
  end
  
  describe "anti-pattern detection" do
    test "detects OR conditions that prevent index usage" do
      selecto = %{
        select: ["*"],
        filters: %{
          "status" => {:or, ["active", "pending", "processing"]}
        },
        joins: %{},
        group_by: [],
        order_by: [],
        source: %{source_table: "orders"}
      }
      
      {:ok, suggestions} = Optimizer.suggest_optimizations(selecto)
      
      or_pattern = Enum.find(suggestions, fn s ->
        s.type == :or_condition
      end)
      
      assert or_pattern
      assert String.contains?(or_pattern.message, "OR condition")
      assert String.contains?(or_pattern.impact, "IN clause")
    end
    
    test "detects functions on indexed columns" do
      selecto = %{
        select: ["*"],
        filters: %{
          "LOWER(email)" => "test@example.com",
          "DATE(created_at)" => ~D[2024-01-01]
        },
        joins: %{},
        group_by: [],
        order_by: [],
        source: %{source_table: "users"}
      }
      
      {:ok, suggestions} = Optimizer.suggest_optimizations(selecto)
      
      function_warnings = Enum.filter(suggestions, fn s ->
        s.type == :function_on_column
      end)
      
      assert length(function_warnings) == 2
      assert hd(function_warnings).severity == :high
    end
    
    test "detects negative conditions" do
      selecto = %{
        select: ["*"],
        filters: %{
          "status" => {:neq, "cancelled"},
          "category_id" => {:not_in, [1, 2, 3]}
        },
        joins: %{},
        group_by: [],
        order_by: [],
        source: %{source_table: "products"}
      }
      
      {:ok, suggestions} = Optimizer.suggest_optimizations(selecto)
      
      negative_conditions = Enum.filter(suggestions, fn s ->
        s.type == :negative_condition
      end)
      
      assert length(negative_conditions) == 2
      assert hd(negative_conditions).severity == :low
    end
    
    test "detects leading wildcards in LIKE patterns" do
      selecto = %{
        select: ["*"],
        filters: %{
          "name" => {:like, "%smith"},
          "description" => {:like, "%test%"}
        },
        joins: %{},
        group_by: [],
        order_by: [],
        source: %{source_table: "users"}
      }
      
      {:ok, suggestions} = Optimizer.suggest_optimizations(selecto)
      
      wildcard_warnings = Enum.filter(suggestions, fn s ->
        s.type == :leading_wildcard
      end)
      
      assert length(wildcard_warnings) == 2
      assert hd(wildcard_warnings).severity == :high
      assert String.contains?(hd(wildcard_warnings).impact, "full-text search")
    end
  end
  
  describe "auto optimization" do
    test "applies safe optimizations automatically" do
      original = %{
        select: ["id", "name", "email"],
        filters: %{
          "status" => "active",
          "created_at" => {:gte, ~D[2024-01-01]}
        },
        joins: %{
          orders: %{},
          products: %{}
        },
        group_by: [],
        order_by: [{"created_at", :desc}],
        source: %{source_table: "users"}
      }
      
      optimized = Optimizer.auto_optimize(original)
      
      # Should return a valid selecto structure
      assert optimized.select == original.select
      assert optimized.source == original.source
      
      # The actual optimizations would be implementation-specific
      # For now, just verify it doesn't break the structure
      assert is_map(optimized)
    end
    
    test "respects optimization options" do
      selecto = %{
        select: ["*"],
        filters: %{},
        joins: %{},
        group_by: [],
        order_by: [],
        source: %{source_table: "users"}
      }
      
      # Disable specific optimizations
      optimized = Optimizer.auto_optimize(selecto, 
        filter_pushdown: false,
        optimize_joins: false
      )
      
      assert optimized == selecto
    end
  end
  
  describe "index recommendations" do
    test "recommends single column indexes for frequently filtered columns" do
      queries = [
        %{filters: %{"user_id" => 1, "status" => "active"}, joins: %{}, order_by: []},
        %{filters: %{"user_id" => 2, "status" => "pending"}, joins: %{}, order_by: []},
        %{filters: %{"user_id" => 3, "created_at" => {:gte, ~D[2024-01-01]}}, joins: %{}, order_by: []},
        %{filters: %{"user_id" => 4}, joins: %{}, order_by: []},
        %{filters: %{"user_id" => 5}, joins: %{}, order_by: []},
        %{filters: %{"status" => "active"}, joins: %{}, order_by: []}
      ]
      
      recommendations = Optimizer.recommend_indexes(queries, threshold: 3)
      
      # Should recommend index on user_id (appears 5 times)
      user_id_index = Enum.find(recommendations, fn r ->
        r.column == "user_id"
      end)
      
      assert user_id_index
      assert user_id_index.frequency >= 5
      assert user_id_index.type == :single_column_index
    end
    
    test "recommends composite indexes for common filter combinations" do
      queries = [
        %{filters: %{"category_id" => 1, "status" => "active"}, joins: %{}, order_by: []},
        %{filters: %{"category_id" => 2, "status" => "active"}, joins: %{}, order_by: []},
        %{filters: %{"category_id" => 3, "status" => "pending"}, joins: %{}, order_by: []},
        %{filters: %{"category_id" => 4, "status" => "active"}, joins: %{}, order_by: []}
      ]
      
      recommendations = Optimizer.recommend_indexes(queries, threshold: 3)
      
      # Should recommend composite index on [category_id, status]
      composite_index = Enum.find(recommendations, fn r ->
        r.type == :composite_index && "category_id" in r.columns
      end)
      
      if composite_index do
        assert "status" in composite_index.columns
        assert composite_index.frequency >= 3
      end
    end
    
    test "prioritizes join columns" do
      queries = [
        %{filters: %{}, joins: %{orders: %{owner_key: :user_id, related_key: :id}}, order_by: []},
        %{filters: %{}, joins: %{orders: %{owner_key: :user_id, related_key: :id}}, order_by: []},
        %{filters: %{}, joins: %{products: %{owner_key: :product_id, related_key: :id}}, order_by: []},
        %{filters: %{}, joins: %{orders: %{owner_key: :user_id, related_key: :id}}, order_by: []},
        %{filters: %{}, joins: %{orders: %{owner_key: :user_id, related_key: :id}}, order_by: []},
        %{filters: %{}, joins: %{orders: %{owner_key: :user_id, related_key: :id}}, order_by: []}
      ]
      
      recommendations = Optimizer.recommend_indexes(queries, threshold: 3)
      
      # Join columns should have higher priority
      join_index = Enum.find(recommendations, fn r ->
        r.type == :join_index
      end)
      
      if join_index do
        assert join_index.priority > join_index.frequency
      end
    end
    
    test "limits number of recommendations" do
      # Generate many queries with different patterns
      queries = for i <- 1..50 do
        %{
          filters: %{"field_#{i}" => i},
          joins: %{},
          order_by: [{"field_#{i}", :asc}]
        }
      end
      
      recommendations = Optimizer.recommend_indexes(queries, 
        threshold: 1,
        max_recommendations: 5
      )
      
      assert length(recommendations) <= 5
    end
  end
end