defmodule Selecto.Builder.Sql.WhereTest do
  use ExUnit.Case

  alias Selecto.Builder.Sql.Where
  alias Selecto.SQL.Params

  # Import removed - not used in current tests

  # Mock selecto struct for testing
  defp mock_selecto do
    %Selecto{
      config: %{
        columns: %{
          "name" => %{
            requires_join: :selecto_root,
            field: :name,
            type: :string
          },
          "id" => %{
            requires_join: :selecto_root,
            field: :id,
            type: :integer
          },
          "active" => %{
            requires_join: :selecto_root,
            field: :active,
            type: :boolean
          }
        }
      }
    }
  end

  describe "build/2 with iodata" do
    test "simple equality generates iodata with param marker" do
      selecto = mock_selecto()
      {joins, iodata, _params} = Where.build(selecto, {"name", "John"})

      assert joins == [:selecto_root]

      {sql, final_params} = Params.finalize(iodata)
      assert sql == " \"selecto_root\".\"name\" = $1 "
      assert final_params == ["John"]
    end

    test "between clause generates iodata with two param markers" do
      selecto = mock_selecto()
      {joins, iodata, _params} = Where.build(selecto, {"id", {:between, 1, 10}})

      # Between returns single join (not list)
      assert joins == :selecto_root

      {sql, final_params} = Params.finalize(iodata)
      assert sql == " \"selecto_root\".\"id\" between $1 and $2 "
      assert final_params == [1, 10]
    end

    test "AND conjunction combines clauses correctly" do
      selecto = mock_selecto()
      filters = [{"name", "John"}, {"active", true}]
      {_joins, iodata, _params} = Where.build(selecto, {:and, filters})

      {sql, final_params} = Params.finalize(iodata)
      expected_sql = "(( \"selecto_root\".\"name\" = $1 ) and ( \"selecto_root\".\"active\" = $2 ))"
      assert sql == expected_sql
      assert final_params == ["John", true]
    end

    test "list membership (ANY) works with iodata" do
      selecto = mock_selecto()
      {_joins, iodata, _params} = Where.build(selecto, {"id", [1, 2, 3]})

      {sql, final_params} = Params.finalize(iodata)
      assert sql == " \"selecto_root\".\"id\" = ANY($1) "
      assert final_params == [[1, 2, 3]]
    end
  end

  describe "text search" do
    test "text_search generates tsvector query" do
      selecto = mock_selecto()
      {joins, iodata, _params} = Where.build(selecto, {"name", {:text_search, "search term"}})

      assert joins == :selecto_root

      {sql, params} = Params.finalize(iodata)
      assert String.contains?(sql, "@@ websearch_to_tsquery(")
      assert params == ["search term"]
    end
  end

  describe "subquery operations" do
    test "subquery IN generates correct SQL" do
      selecto = mock_selecto()
      subquery = "SELECT id FROM users WHERE active = true"
      {joins, iodata, params} = Where.build(selecto, {"id", {:subquery, :in, subquery, []}})

      assert :selecto_root in joins

      {sql, _params} = Params.finalize(iodata)
      assert String.contains?(sql, " in ")
      assert String.contains?(sql, subquery)
    end

    test "subquery with ANY/ALL comparison" do
      selecto = mock_selecto()
      subquery = "SELECT score FROM tests"
      
      {_joins, iodata_any, _params} = Where.build(selecto, {"id", ">", {:subquery, :any, subquery, []}})
      {sql_any, _} = Params.finalize(iodata_any)
      assert String.contains?(sql_any, " > any (")

      {_joins, iodata_all, _params} = Where.build(selecto, {"id", "<", {:subquery, :all, subquery, []}})
      {sql_all, _} = Params.finalize(iodata_all)
      assert String.contains?(sql_all, " < all (")
    end

    test "EXISTS subquery" do
      selecto = mock_selecto()
      subquery = "SELECT 1 FROM orders WHERE user_id = users.id"
      {joins, iodata, params} = Where.build(selecto, {:exists, subquery, []})

      assert joins == []
      
      {sql, _params} = Params.finalize(iodata)
      assert String.contains?(sql, " exists (")
      assert params == []
    end
  end

  describe "logical operations" do
    test "NOT negates conditions" do
      selecto = mock_selecto()
      {_joins, iodata, _params} = Where.build(selecto, {:not, {"active", true}})

      {sql, params} = Params.finalize(iodata)
      assert String.contains?(sql, "not ( ")
      assert String.contains?(sql, " )")
      assert params == [true]
    end

    test "OR conjunction combines clauses" do
      selecto = mock_selecto()
      filters = [{"name", "John"}, {"name", "Jane"}]
      {_joins, iodata, _params} = Where.build(selecto, {:or, filters})

      {sql, params} = Params.finalize(iodata)
      assert String.contains?(sql, " or ")
      assert String.contains?(sql, "(")
      assert String.contains?(sql, ")")
      assert params == ["John", "Jane"]
    end
  end

  describe "comparison operations" do
    test "like and ilike patterns" do
      selecto = mock_selecto()
      
      {_joins, like_iodata, _params} = Where.build(selecto, {"name", {:like, "%John%"}})
      {like_sql, like_params} = Params.finalize(like_iodata)
      assert String.contains?(like_sql, " like ")
      assert like_params == ["%John%"]

      {_joins, ilike_iodata, _params} = Where.build(selecto, {"name", {:ilike, "%john%"}})
      {ilike_sql, ilike_params} = Params.finalize(ilike_iodata)
      assert String.contains?(ilike_sql, " ilike ")
      assert ilike_params == ["%john%"]
    end

    test "various comparison operators" do
      selecto = mock_selecto()
      
      operators = ["=", "!=", "<", ">", "<=", ">="]
      
      for op <- operators do
        {_joins, iodata, _params} = Where.build(selecto, {"id", {op, 100}})
        {sql, params} = Params.finalize(iodata)
        assert String.contains?(sql, " #{op} ")
        assert params == [100]
      end
    end
  end

  describe "null operations" do
    test "is null checks" do
      selecto = mock_selecto()
      {_joins, iodata, params} = Where.build(selecto, {"name", nil})

      {sql, _params} = Params.finalize(iodata)
      assert String.contains?(sql, " is null ")
      assert params == []
    end

    test "is not null checks" do
      selecto = mock_selecto()
      {_joins, iodata, params} = Where.build(selecto, {"name", :not_null})

      {sql, _params} = Params.finalize(iodata)
      assert String.contains?(sql, " is not null ")
      assert params == []
    end
  end

  describe "type conversion" do
    test "to_type handles integer conversion" do
      selecto = %Selecto{
        config: %{
          columns: %{
            "count" => %{
              requires_join: :selecto_root,
              field: :count,
              type: :integer
            }
          }
        }
      }
      
      # String to integer conversion
      {_joins, iodata, _params} = Where.build(selecto, {"count", {"=", "123"}})
      {sql, params} = Params.finalize(iodata)
      assert params == [123]  # Should be converted to integer
    end

    test "to_type handles id conversion" do
      selecto = %Selecto{
        config: %{
          columns: %{
            "user_id" => %{
              requires_join: :selecto_root,
              field: :user_id,
              type: :id
            }
          }
        }
      }
      
      # String to integer conversion for ID type
      {_joins, iodata, _params} = Where.build(selecto, {"user_id", {"=", "456"}})
      {sql, params} = Params.finalize(iodata)
      assert params == [456]  # Should be converted to integer
    end

    test "to_type preserves other types" do
      selecto = mock_selecto()
      
      # Should preserve original value for non-numeric types
      {_joins, iodata, _params} = Where.build(selecto, {"name", {"=", "test"}})
      {sql, params} = Params.finalize(iodata)
      assert params == ["test"]  # Should remain string
    end
  end

  describe "complex nested conditions" do
    test "deeply nested AND/OR conditions" do
      selecto = mock_selecto()
      
      complex_filter = {:and, [
        {"active", true},
        {:or, [
          {"name", "John"},
          {"name", "Jane"}
        ]},
        {"id", {">", 100}}
      ]}
      
      {_joins, iodata, _params} = Where.build(selecto, complex_filter)
      {sql, params} = Params.finalize(iodata)
      
      assert String.contains?(sql, " and ")
      assert String.contains?(sql, " or ")
      assert String.contains?(sql, " > ")
      assert params == [true, "John", "Jane", 100]
    end

    test "NOT with complex conditions" do
      selecto = mock_selecto()
      
      not_filter = {:not, {:and, [
        {"active", true},
        {"name", "John"}
      ]}}
      
      {_joins, iodata, _params} = Where.build(selecto, not_filter)
      {sql, params} = Params.finalize(iodata)
      
      assert String.contains?(sql, "not ( ")
      assert String.contains?(sql, " and ")
      assert params == [true, "John"]
    end
  end

  describe "edge cases and error handling" do
    test "empty AND/OR lists" do
      selecto = mock_selecto()
      
      {_joins, and_iodata, _params} = Where.build(selecto, {:and, []})
      {and_sql, and_params} = Params.finalize(and_iodata)
      assert and_sql == "()"
      assert and_params == []

      {_joins, or_iodata, _params} = Where.build(selecto, {:or, []})
      {or_sql, or_params} = Params.finalize(or_iodata)
      assert or_sql == "()"
      assert or_params == []
    end

    test "single item AND/OR lists" do
      selecto = mock_selecto()
      
      {_joins, and_iodata, _params} = Where.build(selecto, {:and, [{"active", true}]})
      {and_sql, and_params} = Params.finalize(and_iodata)
      assert String.contains?(and_sql, "active")
      assert and_params == [true]

      {_joins, or_iodata, _params} = Where.build(selecto, {:or, [{"name", "John"}]})
      {or_sql, or_params} = Params.finalize(or_iodata)
      assert String.contains?(or_sql, "name")
      assert or_params == ["John"]
    end
  end

  describe "list operations with type conversion" do
    test "list with integer type conversion" do
      selecto = %Selecto{
        config: %{
          columns: %{
            "scores" => %{
              requires_join: :selecto_root,
              field: :scores,
              type: :integer
            }
          }
        }
      }
      
      {_joins, iodata, _params} = Where.build(selecto, {"scores", ["1", "2", "3"]})
      {sql, params} = Params.finalize(iodata)
      assert String.contains?(sql, " = ANY(")
      assert params == [[1, 2, 3]]  # Should be converted to integers
    end

    test "list with mixed type values" do
      selecto = mock_selecto()
      
      {_joins, iodata, _params} = Where.build(selecto, {"name", ["Alice", "Bob", "Charlie"]})
      {sql, params} = Params.finalize(iodata)
      assert String.contains?(sql, " = ANY(")
      assert params == [["Alice", "Bob", "Charlie"]]
    end
  end
end
