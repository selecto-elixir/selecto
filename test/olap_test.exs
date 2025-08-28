defmodule Selecto.OlapTest do
  use ExUnit.Case
  alias Selecto.Builder.Sql.Olap

  @moduledoc """
  Tests for Phase 4 OLAP dimension optimization implementation.
  
  Validates star schema dimensions, snowflake schema normalization chains,
  and analytical query optimizations for data warehouse workloads.
  """

  describe "Star Schema Dimensions" do
    test "build_star_dimension_join generates optimized fact-to-dimension JOIN" do
      selecto = %{domain: %{source: %{source_table: "sales_facts"}}}
      
      config = %{
        source: "customers",
        display_field: "full_name",
        dimension_key: "customer_id",
        requires_join: :selecto_root,
        owner_key: "customer_id",
        my_key: "id"
      }
      
      result = Olap.build_star_dimension_join(selecto, :customers, config, [], [], [])
      
      assert is_tuple(result)
      assert tuple_size(result) == 3
      
      {from_clause, params, ctes} = result
      join_sql = IO.iodata_to_binary(from_clause)
      
      # Should generate direct fact-to-dimension JOIN
      assert String.contains?(join_sql, "LEFT JOIN customers")
      assert String.contains?(join_sql, "ON sales_facts.customer_id =")
      
      # Star schema doesn't need extra parameters
      assert params == []
      assert ctes == []
    end
    
    test "star dimension join uses fact table reference correctly" do
      selecto = %{domain: %{source: %{source_table: "order_facts"}}}
      
      config = %{
        source: "products",
        dimension_key: "product_id",
        requires_join: :selecto_root,
        owner_key: "product_id",
        my_key: "id"
      }
      
      {from_clause, _, _} = Olap.build_star_dimension_join(selecto, :products, config, [], [], [])
      join_sql = IO.iodata_to_binary(from_clause)
      
      # Should reference the correct fact table
      assert String.contains?(join_sql, "order_facts.product_id")
      assert String.contains?(join_sql, "LEFT JOIN products")
    end
    
    test "star dimension join handles custom dimension keys" do
      selecto = %{domain: %{source: %{source_table: "transactions"}}}
      
      config = %{
        source: "stores",
        dimension_key: "store_code",  # Custom key instead of default
        requires_join: :selecto_root,
        owner_key: "store_code",
        my_key: "code"
      }
      
      {from_clause, _, _} = Olap.build_star_dimension_join(selecto, :stores, config, [], [], [])
      join_sql = IO.iodata_to_binary(from_clause)
      
      # Should use the custom dimension key
      assert String.contains?(join_sql, "transactions.store_code")
    end
  end
  
  describe "Snowflake Schema Dimensions" do
    test "build_snowflake_dimension_join generates normalization chain" do
      selecto = %{domain: %{source: %{source_table: "sales"}}}
      
      config = %{
        source: "products",
        display_field: "name",
        dimension_key: "product_id",
        normalization_joins: [
          %{table: "categories", key: "id", foreign_key: "category_id"},
          %{table: "brands", key: "id", foreign_key: "brand_id"}
        ],
        requires_join: :selecto_root,
        owner_key: "product_id",
        my_key: "id"
      }
      
      result = Olap.build_snowflake_dimension_join(selecto, :products, config, [], [], [])
      
      assert is_tuple(result)
      {from_clause, params, ctes} = result
      join_sql = IO.iodata_to_binary(from_clause)
      
      # Should have primary dimension JOIN
      assert String.contains?(join_sql, "LEFT JOIN products")
      assert String.contains?(join_sql, "ON sales.product_id =")
      
      # Should have normalization chain JOINs
      assert String.contains?(join_sql, "LEFT JOIN categories products_categories")
      assert String.contains?(join_sql, "LEFT JOIN brands products_brands")
      
      # Should chain the normalization JOINs properly (account for quoted aliases)
      assert String.contains?(join_sql, "category_id = products_categories.id")
      assert String.contains?(join_sql, "brand_id = products_brands.id")
      
      # Snowflake doesn't add parameters by default
      assert params == []
      assert ctes == []
    end
    
    test "snowflake dimension handles empty normalization chain" do
      selecto = %{domain: %{source: %{source_table: "facts"}}}
      
      config = %{
        source: "simple_dim",
        normalization_joins: [],  # Empty chain - should act like star schema
        requires_join: :selecto_root,
        owner_key: "dim_id",
        my_key: "id"
      }
      
      {from_clause, _, _} = Olap.build_snowflake_dimension_join(selecto, :simple_dim, config, [], [], [])
      join_sql = IO.iodata_to_binary(from_clause)
      
      # Should just have the primary JOIN
      assert String.contains?(join_sql, "LEFT JOIN simple_dim")
      assert String.contains?(join_sql, "ON facts.simple_dim_id =")
      
      # No normalization JOINs
      refute String.contains?(join_sql, "LEFT JOIN simple_dim_")
    end
    
    test "snowflake dimension handles complex normalization chain" do
      selecto = %{domain: %{source: %{source_table: "orders"}}}
      
      config = %{
        source: "items",
        normalization_joins: [
          %{table: "subcategories", key: "id", foreign_key: "subcategory_id"},
          %{table: "categories", key: "id", foreign_key: "category_id"},
          %{table: "departments", key: "id", foreign_key: "department_id"}
        ],
        requires_join: :selecto_root,
        owner_key: "item_id",
        my_key: "id"
      }
      
      {from_clause, _, _} = Olap.build_snowflake_dimension_join(selecto, :items, config, [], [], [])
      join_sql = IO.iodata_to_binary(from_clause)
      
      # Should have all levels of the hierarchy
      assert String.contains?(join_sql, "LEFT JOIN items")
      assert String.contains?(join_sql, "LEFT JOIN subcategories items_subcategories")
      assert String.contains?(join_sql, "LEFT JOIN categories items_categories")
      assert String.contains?(join_sql, "LEFT JOIN departments items_departments")
      
      # Should properly chain the references (with quoted aliases)
      assert String.contains?(join_sql, "\"items\".subcategory_id = items_subcategories.id")
      assert String.contains?(join_sql, "items_subcategories.category_id = items_categories.id")
      assert String.contains?(join_sql, "items_categories.department_id = items_departments.id")
    end
  end
  
  describe "OLAP Join Pattern Detection" do
    test "build_olap_join_with_optimization detects star schema" do
      selecto = %{domain: %{source: %{source_table: "fact_sales"}}}
      
      config = %{
        source: "dim_customer",
        requires_join: :selecto_root,
        owner_key: "customer_id",
        my_key: "id"
      }
      
      # Test star schema pattern
      result = Olap.build_olap_join_with_optimization(selecto, :customer, config, :star, [], [], [])
      
      assert is_tuple(result)
      {from_clause, _, _} = result
      join_sql = IO.iodata_to_binary(from_clause)
      
      # Should generate star schema JOIN
      assert String.contains?(join_sql, "LEFT JOIN dim_customer")
      assert String.contains?(join_sql, "fact_sales.customer_id")
    end
    
    test "build_olap_join_with_optimization detects snowflake schema" do
      selecto = %{domain: %{source: %{source_table: "sales_facts"}}}
      
      config = %{
        source: "products",
        normalization_joins: [%{table: "categories", key: "id"}],
        requires_join: :selecto_root,
        owner_key: "product_id",
        my_key: "id"
      }
      
      # Test snowflake schema pattern
      result = Olap.build_olap_join_with_optimization(selecto, :products, config, :snowflake, [], [], [])
      
      assert is_tuple(result)
      {from_clause, _, _} = result
      join_sql = IO.iodata_to_binary(from_clause)
      
      # Should generate snowflake schema JOINs
      assert String.contains?(join_sql, "LEFT JOIN products")
      assert String.contains?(join_sql, "LEFT JOIN categories products_categories")
    end
    
    test "build_olap_join_with_optimization handles unknown patterns" do
      selecto = %{domain: %{source: %{source_table: "data"}}}
      
      config = %{
        source: "lookup",
        requires_join: :selecto_root,
        owner_key: "lookup_id",
        my_key: "id"
      }
      
      # Test unknown pattern - should fallback to basic JOIN
      result = Olap.build_olap_join_with_optimization(selecto, :lookup, config, :unknown, [], [], [])
      
      assert is_tuple(result)
      {from_clause, _, _} = result
      join_sql = IO.iodata_to_binary(from_clause)
      
      # Should generate basic LEFT JOIN
      assert String.contains?(join_sql, "LEFT JOIN lookup")
    end
  end
  
  describe "OLAP Query Optimization" do
    test "build_fact_table_optimization generates hints for large tables" do
      fact_config = %{
        table_name: "huge_sales_facts",
        estimated_rows: 50_000_000,  # Large table
        large_fact_table: true
      }
      
      join_configs = %{
        optimize_join_order: true
      }
      
      {hints, params} = Olap.build_fact_table_optimization(:selecto, fact_config, join_configs)
      
      # Should generate optimization hints
      assert is_list(hints)
      hints_sql = IO.iodata_to_binary(hints)
      
      # Should include large fact table hints
      assert String.contains?(hints_sql, "LARGE_FACT_TABLE: huge_sales_facts")
      assert String.contains?(hints_sql, "seq_page_cost")
      
      # Should include JOIN order hints
      assert String.contains?(hints_sql, "dimension selectivity")
      
      # Optimization hints don't add parameters
      assert params == []
    end
    
    test "build_fact_table_optimization handles small tables efficiently" do
      small_fact_config = %{
        table_name: "small_facts",
        estimated_rows: 1000  # Small table
      }
      
      {hints, params} = Olap.build_fact_table_optimization(:selecto, small_fact_config, %{})
      
      # Small tables shouldn't get large table optimizations
      hints_sql = IO.iodata_to_binary(hints)
      refute String.contains?(hints_sql, "seq_page_cost")
      
      assert params == []
    end
  end
  
  describe "Dimension Filter Optimization" do
    test "build_dimension_filter_optimization handles mixed filter types" do
      filter_config = %{
        dimension_filters: [
          {"customers.region", "=", "North America"},
          {"products.category", "IN", ["Electronics", "Books"]}
        ],
        fact_filters: [
          {"sales.amount", ">", 1000},
          {"sales.date", "BETWEEN", ["2023-01-01", "2023-12-31"]}
        ]
      }
      
      {where_iodata, params} = Olap.build_dimension_filter_optimization(filter_config)
      
      # Should generate combined WHERE clause
      assert is_list(where_iodata)
      where_sql = IO.iodata_to_binary(where_iodata)
      
      # Should include all filter types
      assert String.contains?(where_sql, "customers.region")
      assert String.contains?(where_sql, "products.category")
      assert String.contains?(where_sql, "sales.amount")
      assert String.contains?(where_sql, "sales.date")
      
      # Should extract all parameters (arrays kept as single parameters)
      assert is_list(params)
      assert "North America" in params
      assert ["Electronics", "Books"] in params
      assert 1000 in params
      assert ["2023-01-01", "2023-12-31"] in params
    end
    
    test "build_dimension_filter_optimization handles empty filters" do
      empty_config = %{
        dimension_filters: [],
        fact_filters: []
      }
      
      {where_iodata, params} = Olap.build_dimension_filter_optimization(empty_config)
      
      # Should handle empty gracefully
      assert where_iodata == []
      assert params == []
    end
    
    test "build_dimension_filter_optimization handles single filter types" do
      # Only dimension filters
      dim_only_config = %{
        dimension_filters: [{"regions.name", "=", "Europe"}],
        fact_filters: []
      }
      
      {where_iodata, params} = Olap.build_dimension_filter_optimization(dim_only_config)
      
      where_sql = IO.iodata_to_binary(where_iodata)
      assert String.contains?(where_sql, "regions.name")
      assert params == ["Europe"]
    end
  end
end