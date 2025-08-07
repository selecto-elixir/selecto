defmodule Selecto.Builder.Sql.WhereTest do
  use ExUnit.Case

  alias Selecto.Builder.Sql.Where
  alias Selecto.SQL.Params

  # Import the Selecto module to access field/2
  import Selecto, only: [field: 2]

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
end
