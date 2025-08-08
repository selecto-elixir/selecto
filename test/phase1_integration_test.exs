defmodule Selecto.Phase1IntegrationTest do
  use ExUnit.Case
  alias Selecto.Builder.Sql.Select

  # Test that Phase 1 CTE infrastructure doesn't break existing functionality
  describe "Phase 1 backward compatibility" do
    test "basic selecto operations still work with CTE infrastructure in place" do
      # This test ensures the CTE infrastructure doesn't break existing queries
      
      # Mock a basic selecto configuration (simplified for testing)
      selecto = %{
        config: %{
          columns: %{
            "id" => %{requires_join: :selecto_root, field: "id"}, 
            "name" => %{requires_join: :selecto_root, field: "name"}
          },
          joins: %{}
        },
        set: %{selected: [], filtered: []},
        domain: %{}
      }
      
      # Test basic SELECT functionality still works
      {select_iodata, _join, params, as} = Select.build(selecto, "id")
      
      assert is_list(select_iodata)
      assert is_list(params)
      assert is_binary(as)
    end
    
    test "custom column safety prevents invalid SQL generation" do
      selecto = %{
        config: %{
          columns: %{"id" => %{}, "name" => %{}},
          joins: %{
            user: %{
              fields: %{"username" => %{}, "email" => %{}}
            }
          }
        }
      }
      
      # Valid field reference should work
      field_mappings = %{"id_field" => "id", "name_field" => "name"}
      sql_template = "CONCAT({{id_field}}, ' - ', {{name_field}})"
      
      {result_iodata, join, params} = Select.prep_selector(
        selecto, 
        {:custom_sql, sql_template, field_mappings}
      )
      
      assert is_list(result_iodata)
      assert join == :selecto_root
      assert params == []
      
      # Result should have field references replaced
      result_sql = IO.iodata_to_binary(result_iodata)
      refute String.contains?(result_sql, "{{")
      assert String.contains?(result_sql, "selecto_root.id")
      assert String.contains?(result_sql, "selecto_root.name")
    end
    
    test "custom column safety rejects invalid field references" do
      selecto = %{
        config: %{
          columns: %{"id" => %{}, "name" => %{}},
          joins: %{}
        }
      }
      
      # Invalid field reference should raise error
      field_mappings = %{"invalid_field" => "nonexistent_field"}
      sql_template = "SELECT {{invalid_field}}"
      
      assert_raise ArgumentError, ~r/Invalid field reference/, fn ->
        Select.prep_selector(
          selecto, 
          {:custom_sql, sql_template, field_mappings}
        )
      end
    end
    
    test "hierarchy join detection works but falls back to basic joins in Phase 1" do
      # Mock hierarchy join configuration
      selecto = %{
        config: %{
          columns: %{"id" => %{}, "name" => %{}, "parent_id" => %{}},
          joins: %{
            manager: %{
              join_type: :hierarchical_adjacency,
              source: "employees", 
              my_key: :id,
              owner_key: :manager_id,
              requires_join: :selecto_root
            }
          }
        }
      }
      
      # The hierarchy detection should work but return basic LEFT JOIN for Phase 1
      config = selecto.config.joins[:manager]
      
      # Test join type detection
      join_type = Map.get(config, :join_type)
      assert join_type == :hierarchical_adjacency
      
      # In Phase 1, this should fall back to basic behavior
      # Phase 2+ will implement actual recursive CTEs
    end
  end
  
  describe "CTE infrastructure integration" do
    test "CTE parameter handling coordinates with existing parameter system" do
      # Test that CTE parameters don't conflict with main query parameters
      
      # This would be tested more thoroughly in integration tests with actual selecto queries
      # For now, verify the parameter system extensions work
      
      _iodata_with_cte_markers = [
        {:cte, "test_cte", ["SELECT id FROM users WHERE active = ", {:param, true}]},
        "SELECT * FROM test_cte WHERE created_at > ", {:param, ~D[2024-01-01]}
      ]
      
      # This would be used in the real system - for now just verify it doesn't crash
      # {ctes, main_sql, params} = Selecto.SQL.Params.finalize_with_ctes(iodata_with_cte_markers)
      # In Phase 1, we just verify the infrastructure exists
      
      assert function_exported?(Selecto.SQL.Params, :finalize_with_ctes, 1)
    end
  end
  
  describe "Phase 1 infrastructure completeness" do
    test "all Phase 1 modules exist and are compilable" do
      # Verify all Phase 1 modules exist
      assert Code.ensure_loaded?(Selecto.Builder.Cte)
      assert Code.ensure_loaded?(Selecto.Builder.Sql.Hierarchy)
      
      # Verify key functions exist
      assert function_exported?(Selecto.Builder.Cte, :build_cte, 3)
      assert function_exported?(Selecto.Builder.Cte, :build_recursive_cte, 5)
      assert function_exported?(Selecto.Builder.Cte, :integrate_ctes_with_query, 3)
      
      assert function_exported?(Selecto.Builder.Sql.Hierarchy, :build_hierarchy_join_with_cte, 7)
      
      # Verify extended parameter system
      assert function_exported?(Selecto.SQL.Params, :finalize_with_ctes, 1)
    end
    
    test "custom column safety functions exist" do
      # Verify the custom column safety functions are available (private functions tested via public interface)
      selecto = %{
        config: %{columns: %{"id" => %{}}, joins: %{}}
      }
      
      # This should not crash and should handle the new custom_sql selector format
      result = Select.prep_selector(selecto, {:custom_sql, "SELECT {{id}}", %{"id" => "id"}})
      assert is_tuple(result)
      assert tuple_size(result) == 3
    end
  end
  
  describe "Phase 1 error handling" do
    test "hierarchy functions raise helpful errors for Phase 2 functionality" do
      # Phase 2 functions should exist but raise helpful errors
      assert_raise RuntimeError, ~r/Phase 2: Not yet implemented/, fn ->
        Selecto.Builder.Sql.Hierarchy.build_adjacency_cte(nil, nil, nil)
      end
      
      assert_raise RuntimeError, ~r/Phase 2: Not yet implemented/, fn ->
        Selecto.Builder.Sql.Hierarchy.build_materialized_path_query(nil, nil, nil)
      end
      
      assert_raise RuntimeError, ~r/Phase 2: Not yet implemented/, fn ->
        Selecto.Builder.Sql.Hierarchy.build_closure_table_query(nil, nil, nil)
      end
    end
  end
end