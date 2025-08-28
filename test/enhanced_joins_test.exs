defmodule Selecto.EnhancedJoinsTest do
  use ExUnit.Case, async: true
  
  alias Selecto.EnhancedJoins
  
  describe "enhanced join configuration" do
    test "configures self-join correctly" do
      association = %{
        field: :manager,
        owner_key: :manager_id,
        related_key: :id
      }
      
      config = %{
        type: :self_join,
        self_key: :manager_id,
        target_key: :id,
        alias: "mgr",
        condition_type: :left
      }
      
      queryable = %{
        source_table: "employees",
        fields: [:id, :name, :manager_id],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          manager_id: %{type: :integer}
        }
      }
      
      result = EnhancedJoins.configure_enhanced_join(
        :manager, association, config, :selecto_root, %{}, queryable
      )
      
      assert result.join_type == :self_join
      assert result.self_key == :manager_id
      assert result.target_key == :id
      assert result.join_alias == "mgr"
      assert result.condition_type == :left
      assert result.source_table == "employees"
    end
    
    test "configures lateral join correctly" do
      association = %{
        field: :recent_orders,
        owner_key: :id,
        related_key: :customer_id
      }
      
      config = %{
        type: :lateral_join,
        lateral_query: "SELECT * FROM orders o WHERE o.customer_id = customers.id ORDER BY o.created_at DESC LIMIT 5",
        alias: "recent"
      }
      
      queryable = %{
        source_table: "orders",
        fields: [:id, :customer_id, :total, :created_at],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          customer_id: %{type: :integer},
          total: %{type: :decimal},
          created_at: %{type: :utc_datetime}
        }
      }
      
      result = EnhancedJoins.configure_enhanced_join(
        :recent_orders, association, config, :selecto_root, %{}, queryable
      )
      
      assert result.join_type == :lateral_join
      assert result.lateral_query == config.lateral_query
      assert result.join_alias == "recent"
    end
    
    test "configures cross join correctly" do
      association = %{
        field: :variants,
        owner_key: :id,
        related_key: :product_id
      }
      
      config = %{
        type: :cross_join,
        alias: "variants"
      }
      
      queryable = %{
        source_table: "product_options",
        fields: [:id, :name, :value],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          value: %{type: :string}
        }
      }
      
      result = EnhancedJoins.configure_enhanced_join(
        :variants, association, config, :selecto_root, %{}, queryable
      )
      
      assert result.join_type == :cross_join
      assert result.join_alias == "variants"
      assert Map.has_key?(result.config.custom_columns, "variants_combination")
    end
    
    test "configures full outer join correctly" do
      association = %{
        field: :transactions,
        owner_key: :account_id,
        related_key: :account_id
      }
      
      config = %{
        type: :full_outer_join,
        left_key: :account_id,
        right_key: :account_id,
        alias: "trans"
      }
      
      queryable = %{
        source_table: "transactions",
        fields: [:id, :account_id, :amount, :type],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          account_id: %{type: :integer},
          amount: %{type: :decimal},
          type: %{type: :string}
        }
      }
      
      result = EnhancedJoins.configure_enhanced_join(
        :transactions, association, config, :selecto_root, %{}, queryable
      )
      
      assert result.join_type == :full_outer_join
      assert result.left_key == :account_id
      assert result.right_key == :account_id
      assert result.join_alias == "trans"
      assert Map.has_key?(result.config.custom_columns, "transactions_coalesced")
    end
    
    test "configures conditional join correctly" do
      association = %{
        field: :discounts,
        owner_key: :id,
        related_key: :applicable_to
      }
      
      config = %{
        type: :conditional_join,
        conditions: [
          {:field_comparison, "orders.total", :gte, "discounts.minimum_amount"},
          {:date_range, "orders.created_at", "discounts.valid_from", "discounts.valid_to"}
        ],
        condition_type: :left
      }
      
      queryable = %{
        source_table: "discounts",
        fields: [:id, :name, :minimum_amount, :valid_from, :valid_to],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          minimum_amount: %{type: :decimal},
          valid_from: %{type: :date},
          valid_to: %{type: :date}
        }
      }
      
      result = EnhancedJoins.configure_enhanced_join(
        :discounts, association, config, :selecto_root, %{}, queryable
      )
      
      assert result.join_type == :conditional_join
      assert length(result.conditions) == 2
      assert result.condition_type == :left
    end
    
    test "raises error for lateral join without lateral_query" do
      association = %{field: :test}
      config = %{type: :lateral_join}
      queryable = %{source_table: "test", fields: [], redact_fields: [], columns: %{}}
      
      assert_raise ArgumentError, "Lateral join requires :lateral_query configuration", fn ->
        EnhancedJoins.configure_enhanced_join(
          :test, association, config, :selecto_root, %{}, queryable
        )
      end
    end
    
    test "raises error for conditional join without conditions" do
      association = %{field: :test}
      config = %{type: :conditional_join}
      queryable = %{source_table: "test", fields: [], redact_fields: [], columns: %{}}
      
      assert_raise ArgumentError, "Conditional join requires :conditions configuration", fn ->
        EnhancedJoins.configure_enhanced_join(
          :test, association, config, :selecto_root, %{}, queryable
        )
      end
    end
  end
  
  describe "enhanced join SQL generation" do
    test "generates self-join SQL correctly" do
      join_config = %{
        join_type: :self_join,
        source: "employees",
        join_alias: "mgr",
        self_key: :manager_id,
        target_key: :id,
        condition_type: :left
      }
      
      result = EnhancedJoins.build_enhanced_join_sql(join_config, %{})
      sql = IO.iodata_to_binary(result)
      
      assert sql =~ "LEFT JOIN employees mgr"
      assert sql =~ "selecto_root.manager_id = mgr.id"
    end
    
    test "generates lateral join SQL correctly" do
      join_config = %{
        join_type: :lateral_join,
        lateral_query: "SELECT * FROM orders WHERE customer_id = customers.id LIMIT 5",
        join_alias: "recent"
      }
      
      result = EnhancedJoins.build_enhanced_join_sql(join_config, %{})
      sql = IO.iodata_to_binary(result)
      
      assert sql =~ "LEFT JOIN LATERAL"
      assert sql =~ "SELECT * FROM orders WHERE customer_id = customers.id LIMIT 5"
      assert sql =~ "recent ON true"
    end
    
    test "generates cross join SQL correctly" do
      join_config = %{
        join_type: :cross_join,
        source: "product_options",
        join_alias: "variants"
      }
      
      result = EnhancedJoins.build_enhanced_join_sql(join_config, %{})
      sql = IO.iodata_to_binary(result)
      
      assert sql =~ "CROSS JOIN product_options variants"
    end
    
    test "generates full outer join SQL correctly" do
      join_config = %{
        join_type: :full_outer_join,
        source: "transactions",
        join_alias: "trans",
        left_key: :account_id,
        right_key: :account_id
      }
      
      result = EnhancedJoins.build_enhanced_join_sql(join_config, %{})
      sql = IO.iodata_to_binary(result)
      
      assert sql =~ "FULL OUTER JOIN transactions trans"
      assert sql =~ "selecto_root.account_id = trans.account_id"
    end
    
    test "generates conditional join SQL correctly" do
      join_config = %{
        join_type: :conditional_join,
        source: "discounts",
        join_alias: "disc",
        conditions: [
          {:field_comparison, "orders.total", :gte, "discounts.minimum_amount"},
          {:date_range, "orders.created_at", "discounts.valid_from", "discounts.valid_to"}
        ],
        condition_type: :left
      }
      
      result = EnhancedJoins.build_enhanced_join_sql(join_config, %{})
      sql = IO.iodata_to_binary(result)
      
      assert sql =~ "LEFT JOIN discounts disc"
      assert sql =~ "orders.total >= discounts.minimum_amount"
      assert sql =~ "orders.created_at BETWEEN discounts.valid_from AND discounts.valid_to"
    end
    
    test "returns nil for unsupported join types" do
      join_config = %{join_type: :unsupported}
      
      result = EnhancedJoins.build_enhanced_join_sql(join_config, %{})
      
      assert result == nil
    end
  end
  
  describe "condition SQL generation" do
    test "generates field comparison conditions" do
      conditions = [
        {:field_comparison, "table1.field1", :eq, "table2.field2"},
        {:field_comparison, "table1.amount", :gte, "table2.minimum"}
      ]
      
      # Using the private function through the public interface
      join_config = %{
        join_type: :conditional_join,
        source: "test",
        join_alias: "t",
        conditions: conditions,
        condition_type: :inner
      }
      
      result = EnhancedJoins.build_enhanced_join_sql(join_config, %{})
      sql = IO.iodata_to_binary(result)
      
      assert sql =~ "table1.field1 = table2.field2"
      assert sql =~ "table1.amount >= table2.minimum"
      assert sql =~ " AND "
    end
    
    test "generates date range conditions" do
      conditions = [
        {:date_range, "orders.created_at", "promotions.start_date", "promotions.end_date"}
      ]
      
      join_config = %{
        join_type: :conditional_join,
        source: "test",
        join_alias: "t", 
        conditions: conditions,
        condition_type: :inner
      }
      
      result = EnhancedJoins.build_enhanced_join_sql(join_config, %{})
      sql = IO.iodata_to_binary(result)
      
      assert sql =~ "orders.created_at BETWEEN promotions.start_date AND promotions.end_date"
    end
    
    test "generates custom SQL conditions" do
      conditions = [
        {:custom_sql, "table1.status = 'active' AND table2.enabled = true"}
      ]
      
      join_config = %{
        join_type: :conditional_join,
        source: "test",
        join_alias: "t",
        conditions: conditions,
        condition_type: :inner
      }
      
      result = EnhancedJoins.build_enhanced_join_sql(join_config, %{})
      sql = IO.iodata_to_binary(result)
      
      assert sql =~ "table1.status = 'active' AND table2.enabled = true"
    end
  end
end