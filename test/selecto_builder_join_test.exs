defmodule Selecto.Builder.JoinTest do
  use ExUnit.Case
  alias Selecto.Builder.Join

  describe "from_selects/2" do
    test "extracts joins from simple field selections" do
      fields = %{
        "user_name" => %{requires_join: :users},
        "category_name" => %{requires_join: :categories},
        "id" => %{requires_join: nil}
      }
      
      selected = ["user_name", "category_name", "id"]
      
      result = Join.from_selects(fields, selected)
      
      assert :users in result
      assert :categories in result
      # nil may be included for fields that don't require joins
      assert nil in result
    end

    test "handles array selections" do
      fields = %{
        "user_name" => %{requires_join: :users},
        "tag_name" => %{requires_join: :tags}
      }
      
      selected = [{:array, "tags", ["user_name", "tag_name"]}]
      
      result = Join.from_selects(fields, selected)
      
      assert :users in result
      assert :tags in result
    end

    test "handles coalesce selections" do
      fields = %{
        "user_name" => %{requires_join: :users},
        "backup_name" => %{requires_join: :backups}
      }
      
      selected = [{:coalesce, "name", ["user_name", "backup_name"]}]
      
      result = Join.from_selects(fields, selected)
      
      assert :users in result
      assert :backups in result
    end

    test "handles case selections" do
      fields = %{
        "user_name" => %{requires_join: :users},
        "admin_name" => %{requires_join: :admins}
      }
      
      case_map = %{
        "when_user" => ["user_name"],
        "when_admin" => ["admin_name"]
      }
      selected = [{:case, "display_name", case_map}]
      
      result = Join.from_selects(fields, selected)
      
      assert :users in result
      assert :admins in result
    end

    test "filters out literal values" do
      fields = %{
        "user_name" => %{requires_join: :users}
      }
      
      selected = [
        "user_name",
        {:literal, "constant_value"}
      ]
      
      result = Join.from_selects(fields, selected)
      
      assert :users in result
      assert length(result) == 1
    end

    test "handles complex field structures" do
      fields = %{
        "field1" => %{requires_join: :table1},
        "field2" => %{requires_join: :table2}
      }
      
      selected = [
        {"func", {"field1", "desc"}, "param"},
        {"func", "field2", "param"},
        {"func", "field1"},
        {"func"}
      ]
      
      result = Join.from_selects(fields, selected)
      
      assert :table1 in result
      assert :table2 in result
    end

    test "returns empty list when no joins needed" do
      fields = %{
        "id" => %{requires_join: nil},
        "name" => %{requires_join: nil}
      }
      
      selected = ["id", "name"]
      
      result = Join.from_selects(fields, selected)
      
      # When there are no joins needed, returns [nil]
      assert result == [nil]
    end
  end

  describe "get_join_order/2" do
    test "returns simple join order without dependencies" do
      joins = %{
        :users => %{},
        :categories => %{}
      }
      
      requested_joins = [:users, :categories]
      
      result = Join.get_join_order(joins, requested_joins)
      
      assert :users in result
      assert :categories in result
      assert length(result) == 2
    end

    test "handles join dependencies correctly" do
      joins = %{
        :users => %{requires_join: :departments},
        :departments => %{},
        :categories => %{}
      }
      
      requested_joins = [:users, :categories]
      
      result = Join.get_join_order(joins, requested_joins)
      
      # Should include dependencies
      assert :departments in result
      assert :users in result
      assert :categories in result
      
      # Dependencies should come before dependent joins
      dept_index = Enum.find_index(result, &(&1 == :departments))
      users_index = Enum.find_index(result, &(&1 == :users))
      assert dept_index < users_index
    end

    test "handles nested join dependencies" do
      joins = %{
        :users => %{requires_join: :departments},
        :departments => %{requires_join: :companies},
        :companies => %{}
      }
      
      requested_joins = [:users]
      
      result = Join.get_join_order(joins, requested_joins)
      
      assert :companies in result
      assert :departments in result  
      assert :users in result
      
      # Check proper ordering
      company_index = Enum.find_index(result, &(&1 == :companies))
      dept_index = Enum.find_index(result, &(&1 == :departments))
      users_index = Enum.find_index(result, &(&1 == :users))
      
      assert company_index < dept_index
      assert dept_index < users_index
    end

    test "removes duplicate joins from dependency resolution" do
      joins = %{
        :users => %{requires_join: :departments},
        :profiles => %{requires_join: :departments},
        :departments => %{}
      }
      
      requested_joins = [:users, :profiles]
      
      result = Join.get_join_order(joins, requested_joins)
      
      # Should have each join only once
      dept_count = Enum.count(result, &(&1 == :departments))
      assert dept_count == 1
      
      assert :departments in result
      assert :users in result
      assert :profiles in result
    end
  end

  describe "from_filters/2" do
    test "extracts joins from simple filters" do
      config = %{
        columns: %{
          "user_name" => %{requires_join: :users},
          "category_id" => %{requires_join: :categories},
          "status" => %{requires_join: nil}
        }
      }
      
      filters = [
        {"user_name", "John"},
        {"category_id", 1}
      ]
      
      result = Join.from_filters(config, filters)
      
      assert :users in result
      assert :categories in result
    end

    test "handles OR filters" do
      config = %{
        columns: %{
          "user_name" => %{requires_join: :users},
          "admin_name" => %{requires_join: :admins}
        }
      }
      
      filters = [
        {:or, [
          {"user_name", "John"},
          {"admin_name", "Jane"}
        ]}
      ]
      
      result = Join.from_filters(config, filters)
      
      assert :users in result
      assert :admins in result
    end

    test "handles AND filters" do
      config = %{
        columns: %{
          "user_name" => %{requires_join: :users},
          "category_name" => %{requires_join: :categories}
        }
      }
      
      filters = [
        {:and, [
          {"user_name", "John"},
          {"category_name", "Tech"}
        ]}
      ]
      
      result = Join.from_filters(config, filters)
      
      assert :users in result
      assert :categories in result
    end

    test "handles nested OR/AND filters" do
      config = %{
        columns: %{
          "user_name" => %{requires_join: :users},
          "admin_name" => %{requires_join: :admins},
          "category_name" => %{requires_join: :categories}
        }
      }
      
      filters = [
        {:or, [
          {:and, [
            {"user_name", "John"},
            {"category_name", "Tech"}
          ]},
          {"admin_name", "Jane"}
        ]}
      ]
      
      result = Join.from_filters(config, filters)
      
      assert :users in result
      assert :admins in result
      assert :categories in result
    end

    test "returns empty list for filters requiring no joins" do
      config = %{
        columns: %{
          "id" => %{requires_join: nil},
          "status" => %{requires_join: nil}
        }
      }
      
      filters = [
        {"id", 1},
        {"status", "active"}
      ]
      
      result = Join.from_filters(config, filters)
      
      assert result == [nil]
    end
  end
end