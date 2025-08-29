defmodule Selecto.Advanced.LateralJoinTest do
  use ExUnit.Case, async: true
  
  alias Selecto.Advanced.LateralJoin
  alias Selecto.Advanced.LateralJoin.{Spec, CorrelationError}
  
  describe "LATERAL join specification creation" do
    test "creates correlated subquery LATERAL join spec" do
      subquery_builder = fn _base ->
        %Selecto{
          domain: %{},
          postgrex_opts: [],
          set: %{
            selected: ["rental_count"],
            filtered: [{"customer_id", {:ref, "customer.customer_id"}}]
          }
        }
      end
      
      spec = LateralJoin.create_lateral_join(:left, subquery_builder, "recent_rentals")
      
      assert %Spec{
        join_type: :left,
        subquery_builder: ^subquery_builder,
        table_function: nil,
        alias: "recent_rentals",
        validated: false
      } = spec
      
      assert is_binary(spec.id)
      assert String.starts_with?(spec.id, "lateral_recent_rentals_")
    end
    
    test "creates table function LATERAL join spec" do
      spec = LateralJoin.create_lateral_join(:inner, {:unnest, "film.special_features"}, "features")
      
      assert %Spec{
        join_type: :inner,
        subquery_builder: nil,
        table_function: {:unnest, "film.special_features"},
        alias: "features",
        validated: false
      } = spec
    end
    
    test "creates function LATERAL join spec" do
      spec = LateralJoin.create_lateral_join(:inner, {:function, :generate_series, [1, 10]}, "numbers")
      
      assert %Spec{
        join_type: :inner,
        table_function: {:function, :generate_series, [1, 10]},
        alias: "numbers"
      } = spec
    end
  end
  
  describe "correlation validation" do
    setup do
      # Create a mock base selecto with domain information
      base_selecto = %Selecto{
        domain: %{
          source: %{
            source_table: "customer",
            fields: [:customer_id, :first_name, :last_name, :email],
            columns: %{
              customer_id: %{type: :integer},
              first_name: %{type: :string},
              last_name: %{type: :string},
              email: %{type: :string}
            }
          }
        },
        postgrex_opts: [],
        set: %{
          joins: %{
            rental: %{
              target_table: "rental",
              fields: ["rental_id", "rental_date", "customer_id"]
            }
          }
        }
      }
      
      {:ok, base_selecto: base_selecto}
    end
    
    test "validates valid correlations in subquery", %{base_selecto: base_selecto} do
      subquery_builder = fn _base ->
        %Selecto{
          domain: %{},
          postgrex_opts: [],
          set: %{
            selected: [{"rental_count", :count}],
            filtered: [{"customer_id", {:ref, "customer.customer_id"}}]
          }
        }
      end
      
      spec = LateralJoin.create_lateral_join(:left, subquery_builder, "recent_rentals")
      
      assert {:ok, validated_spec} = LateralJoin.validate_correlations(spec, base_selecto)
      assert validated_spec.validated == true
      assert "customer.customer_id" in validated_spec.correlation_refs
    end
    
    test "rejects invalid correlations in subquery", %{base_selecto: base_selecto} do
      subquery_builder = fn _base ->
        %Selecto{
          domain: %{},
          postgrex_opts: [],
          set: %{
            selected: [{"rental_count", :count}],
            filtered: [{"customer_id", {:ref, "customer.invalid_field"}}]
          }
        }
      end
      
      spec = LateralJoin.create_lateral_join(:left, subquery_builder, "recent_rentals")
      
      assert {:error, %CorrelationError{
        type: :invalid_correlation,
        referenced_field: "customer.invalid_field"
      }} = LateralJoin.validate_correlations(spec, base_selecto)
    end
    
    test "validates table function correlations", %{base_selecto: base_selecto} do
      # Create updated domain with tags field for this test
      updated_domain = %{
        source: %{
          source_table: "customer",
          fields: [:customer_id, :first_name, :last_name, :email, :tags],
          columns: %{
            customer_id: %{type: :integer},
            first_name: %{type: :string},
            last_name: %{type: :string},
            email: %{type: :string},
            tags: %{type: :array}
          }
        }
      }
      
      updated_base_selecto = %{base_selecto | domain: updated_domain}
      
      spec = LateralJoin.create_lateral_join(:inner, {:unnest, "customer.tags"}, "tag_elements")
      
      assert {:ok, validated_spec} = LateralJoin.validate_correlations(spec, updated_base_selecto)
      assert validated_spec.validated == true
    end
    
    test "validates function arguments with correlations", %{base_selecto: base_selecto} do
      spec = LateralJoin.create_lateral_join(
        :inner, 
        {:function, :some_function, [1, {:ref, "customer.customer_id"}]}, 
        "func_result"
      )
      
      assert {:ok, validated_spec} = LateralJoin.validate_correlations(spec, base_selecto)
      assert validated_spec.validated == true
    end
  end
  
  describe "correlation reference extraction" do
    test "extracts refs from UNNEST table functions" do
      spec = LateralJoin.create_lateral_join(:inner, {:unnest, "table.column"}, "elements")
      assert "table.column" in spec.correlation_refs
    end
    
    test "extracts refs from function arguments" do
      spec = LateralJoin.create_lateral_join(
        :inner,
        {:function, :test_func, [1, {:ref, "table.field"}, "literal"]},
        "result"
      )
      assert {:ref, "table.field"} in spec.correlation_refs or "table.field" in spec.correlation_refs
    end
    
    test "handles functions without correlations" do
      spec = LateralJoin.create_lateral_join(
        :inner,
        {:function, :generate_series, [1, 100]},
        "numbers"
      )
      assert spec.correlation_refs == []
    end
  end
  
  describe "error handling" do
    test "handles validation errors gracefully" do
      base_selecto = %Selecto{domain: %{}, postgrex_opts: [], set: %{}}
      
      subquery_builder = fn _base ->
        raise "Test error"
      end
      
      spec = LateralJoin.create_lateral_join(:left, subquery_builder, "failing_lateral")
      
      assert {:error, %CorrelationError{
        type: :validation_error,
        message: message
      }} = LateralJoin.validate_correlations(spec, base_selecto)
      
      assert String.contains?(message, "Error validating LATERAL subquery")
    end
    
    test "provides helpful error messages for missing fields" do
      base_selecto = %Selecto{
        domain: %{
          source: %{
            source_table: "customer", 
            fields: [:id, :name],
            columns: %{id: %{type: :integer}, name: %{type: :string}}
          }
        },
        postgrex_opts: [],
        set: %{}
      }
      
      spec = LateralJoin.create_lateral_join(:inner, {:unnest, "customer.nonexistent"}, "bad_ref")
      
      assert {:error, %CorrelationError{
        type: :invalid_correlation,
        message: message,
        available_fields: available_fields,
        referenced_field: "customer.nonexistent"
      }} = LateralJoin.validate_correlations(spec, base_selecto)
      
      assert String.contains?(message, "Cannot reference field")
      assert "customer.id" in available_fields
      assert "customer.name" in available_fields
    end
  end
  
  describe "join type validation" do
    test "supports all standard join types" do
      join_types = [:left, :inner, :right, :full]
      
      for join_type <- join_types do
        spec = LateralJoin.create_lateral_join(
          join_type, 
          {:unnest, "table.array_field"}, 
          "elements"
        )
        assert spec.join_type == join_type
      end
    end
  end
  
  describe "complex scenarios" do
    test "handles multiple correlation references" do
      subquery_builder = fn _base ->
        %Selecto{
          domain: %{},
          postgrex_opts: [],
          set: %{
            selected: ["count"],
            filtered: [
              {"customer_id", {:ref, "customer.customer_id"}},
              {"store_id", {:ref, "customer.store_id"}},
              {"rental_date", {:>, {:ref, "customer.last_update"}}}
            ]
          }
        }
      end
      
      base_selecto = %Selecto{
        domain: %{
          source: %{
            source_table: "customer",
            fields: [:customer_id, :store_id, :last_update],
            columns: %{
              customer_id: %{type: :integer},
              store_id: %{type: :integer},
              last_update: %{type: :utc_datetime}
            }
          }
        },
        postgrex_opts: [],
        set: %{}
      }
      
      spec = LateralJoin.create_lateral_join(:left, subquery_builder, "complex_lateral")
      
      assert {:ok, validated_spec} = LateralJoin.validate_correlations(spec, base_selecto)
      
      expected_refs = [
        "customer.customer_id",
        "customer.store_id", 
        "customer.last_update"
      ]
      
      for ref <- expected_refs do
        assert ref in validated_spec.correlation_refs
      end
    end
    
    test "validates with existing joins in base query" do
      base_selecto = %Selecto{
        domain: %{
          source: %{
            source_table: "customer",
            fields: [:customer_id, :first_name],
            columns: %{
              customer_id: %{type: :integer},
              first_name: %{type: :string}
            }
          }
        },
        postgrex_opts: [],
        set: %{
          joins: %{
            store: %{
              target_table: "store",
              fields: ["store_id", "manager_staff_id"]
            }
          }
        }
      }
      
      subquery_builder = fn _base ->
        %Selecto{
          domain: %{},
          postgrex_opts: [],
          set: %{
            selected: ["rental_count"],
            filtered: [
              {"customer_id", {:ref, "customer.customer_id"}},
              {"store_id", {:ref, "store.store_id"}}
            ]
          }
        }
      end
      
      spec = LateralJoin.create_lateral_join(:left, subquery_builder, "store_rentals")
      
      assert {:ok, validated_spec} = LateralJoin.validate_correlations(spec, base_selecto)
      assert "customer.customer_id" in validated_spec.correlation_refs
      assert "store.store_id" in validated_spec.correlation_refs
    end
  end
end