defmodule Selecto.Builder.LateralJoinTest do
  use ExUnit.Case, async: true
  
  alias Selecto.Builder.LateralJoin
  alias Selecto.Advanced.LateralJoin.Spec
  
  describe "LATERAL join SQL generation" do
    test "generates SQL for table function LATERAL join" do
      spec = %Spec{
        id: "lateral_features_123",
        join_type: :inner,
        subquery_builder: nil,
        table_function: {:unnest, "film.special_features"},
        alias: "features",
        correlation_refs: ["film.special_features"],
        validated: true
      }
      
      {sql, params} = LateralJoin.build_lateral_join(spec)
      
      assert sql == "INNER JOIN LATERAL UNNEST(film.special_features) AS features ON true"
      assert params == []
    end
    
    test "generates SQL for generate_series function" do
      spec = %Spec{
        id: "lateral_numbers_456",
        join_type: :left,
        subquery_builder: nil,
        table_function: {:function, :generate_series, [1, 10]},
        alias: "numbers",
        correlation_refs: [],
        validated: true
      }
      
      {sql, params} = LateralJoin.build_lateral_join(spec)
      
      assert sql == "LEFT JOIN LATERAL GENERATE_SERIES(?, ?) AS numbers ON true"
      assert params == [1, 10]
    end
    
    test "generates SQL for function with correlation references" do
      spec = %Spec{
        id: "lateral_stats_789", 
        join_type: :inner,
        subquery_builder: nil,
        table_function: {:function, :get_customer_stats, ["customer.customer_id", 2023]},
        alias: "customer_stats",
        correlation_refs: ["customer.customer_id"],
        validated: true
      }
      
      {sql, params} = LateralJoin.build_lateral_join(spec)
      
      assert sql == "INNER JOIN LATERAL GET_CUSTOMER_STATS(customer.customer_id, ?) AS customer_stats ON true"
      assert params == [2023]
    end
    
    test "generates SQL for correlated subquery LATERAL join" do
      subquery_builder = fn _base ->
        %Selecto{
          domain: %{
            source: %{source_table: "rental"}
          },
          postgrex_opts: [],
          set: %{
            selected: [{"count", :count}],
            filtered: [{"customer_id", {:ref, "customer.customer_id"}}]
          }
        }
      end
      
      spec = %Spec{
        id: "lateral_rentals_999",
        join_type: :left,
        subquery_builder: subquery_builder,
        table_function: nil,
        alias: "recent_rentals",
        correlation_refs: ["customer.customer_id"],
        validated: true
      }
      
      {sql, params} = LateralJoin.build_lateral_join(spec)
      
      assert String.contains?(sql, "LEFT JOIN LATERAL (")
      assert String.contains?(sql, ") AS recent_rentals ON true")
      assert String.contains?(sql, "SELECT")
      assert String.contains?(sql, "FROM rental")
    end
    
    test "handles multiple LATERAL joins" do
      specs = [
        %Spec{
          id: "lateral_1",
          join_type: :inner,
          subquery_builder: nil,
          table_function: {:unnest, "film.special_features"},
          alias: "features",
          correlation_refs: [],
          validated: true
        },
        %Spec{
          id: "lateral_2", 
          join_type: :left,
          subquery_builder: nil,
          table_function: {:function, :generate_series, [1, 5]},
          alias: "numbers",
          correlation_refs: [],
          validated: true
        }
      ]
      
      {sql_parts, params} = LateralJoin.build_lateral_joins(specs)
      
      assert length(sql_parts) == 2
      assert Enum.at(sql_parts, 0) == "INNER JOIN LATERAL UNNEST(film.special_features) AS features ON true"
      assert Enum.at(sql_parts, 1) == "LEFT JOIN LATERAL GENERATE_SERIES(?, ?) AS numbers ON true" 
      assert params == [1, 5]
    end
    
    test "handles empty LATERAL join list" do
      {sql_parts, params} = LateralJoin.build_lateral_joins([])
      
      assert sql_parts == []
      assert params == []
    end
  end
  
  describe "join type SQL generation" do
    test "generates correct SQL for all join types" do
      join_type_tests = [
        {:left, "LEFT"},
        {:inner, "INNER"},
        {:right, "RIGHT"},
        {:full, "FULL"}
      ]
      
      for {join_type, expected_sql} <- join_type_tests do
        spec = %Spec{
          id: "test_#{join_type}",
          join_type: join_type,
          subquery_builder: nil,
          table_function: {:unnest, "array_column"},
          alias: "elements",
          correlation_refs: [],
          validated: true
        }
        
        {sql, _params} = LateralJoin.build_lateral_join(spec)
        assert String.starts_with?(sql, expected_sql)
      end
    end
    
    test "raises error for unknown join type" do
      spec = %Spec{
        id: "test_invalid",
        join_type: :invalid_type,
        subquery_builder: nil,
        table_function: {:unnest, "array_column"},
        alias: "elements",
        correlation_refs: [],
        validated: true
      }
      
      assert_raise ArgumentError, ~r/Unknown LATERAL join type/, fn ->
        LateralJoin.build_lateral_join(spec)
      end
    end
  end
  
  describe "table function SQL generation" do
    test "handles UNNEST with simple column reference" do
      spec = %Spec{
        id: "test_unnest",
        join_type: :inner,
        subquery_builder: nil,
        table_function: {:unnest, "simple_array"},
        alias: "elements",
        correlation_refs: [],
        validated: true
      }
      
      {sql, params} = LateralJoin.build_lateral_join(spec)
      
      assert String.contains?(sql, "UNNEST(simple_array)")
      assert params == []
    end
    
    test "handles functions with mixed argument types" do
      spec = %Spec{
        id: "test_mixed_args",
        join_type: :inner,
        subquery_builder: nil,
        table_function: {:function, :test_func, [
          {:ref, "table.field"},
          "literal_string",
          42,
          true,
          {:literal, "explicit_literal"}
        ]},
        alias: "results",
        correlation_refs: [],
        validated: true
      }
      
      {sql, params} = LateralJoin.build_lateral_join(spec)
      
      assert String.contains?(sql, "TEST_FUNC(table.field, ?, ?, ?, ?)")
      assert params == ["literal_string", 42, true, "explicit_literal"]
    end
    
    test "raises error for unknown table function type" do
      spec = %Spec{
        id: "test_unknown",
        join_type: :inner,
        subquery_builder: nil,
        table_function: {:unknown_type, "some_data"},
        alias: "results",
        correlation_refs: [],
        validated: true
      }
      
      assert_raise ArgumentError, ~r/Unknown table function specification/, fn ->
        LateralJoin.build_lateral_join(spec)
      end
    end
  end
  
  describe "SQL integration" do
    test "integrates LATERAL joins into base SQL" do
      base_sql_parts = [
        "SELECT customer.name, customer.email",
        " FROM customer"
      ]
      
      lateral_specs = [
        %Spec{
          id: "lateral_test",
          join_type: :left,
          subquery_builder: nil,
          table_function: {:unnest, "customer.tags"},
          alias: "customer_tags",
          correlation_refs: [],
          validated: true
        }
      ]
      
      {updated_sql, lateral_params} = LateralJoin.integrate_lateral_joins_sql(base_sql_parts, lateral_specs)
      
      assert length(updated_sql) > length(base_sql_parts)
      assert lateral_params == []
      
      # Check that LATERAL join was added
      combined_sql = IO.iodata_to_binary(updated_sql)
      assert String.contains?(combined_sql, "LEFT JOIN LATERAL")
      assert String.contains?(combined_sql, "customer_tags")
    end
    
    test "handles empty LATERAL joins integration" do
      base_sql_parts = ["SELECT * FROM table"]
      
      {updated_sql, lateral_params} = LateralJoin.integrate_lateral_joins_sql(base_sql_parts, [])
      
      assert updated_sql == base_sql_parts
      assert lateral_params == []
    end
    
    test "preserves parameter order in complex queries" do
      lateral_specs = [
        %Spec{
          id: "lateral_1",
          join_type: :inner,
          subquery_builder: nil,
          table_function: {:function, :func1, [1, "param1"]},
          alias: "result1",
          correlation_refs: [],
          validated: true
        },
        %Spec{
          id: "lateral_2",
          join_type: :left,
          subquery_builder: nil,
          table_function: {:function, :func2, ["param2", 3.14]},
          alias: "result2", 
          correlation_refs: [],
          validated: true
        }
      ]
      
      {_sql_parts, params} = LateralJoin.build_lateral_joins(lateral_specs)
      
      assert params == [1, "param1", "param2", 3.14]
    end
  end
end