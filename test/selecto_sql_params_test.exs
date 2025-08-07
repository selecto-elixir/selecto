defmodule Selecto.SQL.ParamsTest do
  use ExUnit.Case

  alias Selecto.SQL.Params

  describe "finalize/1" do
    test "handles empty list" do
      assert Params.finalize([]) == {"", []}
    end

    test "handles simple strings" do
      assert Params.finalize(["SELECT ", "name"]) == {"SELECT name", []}
    end

    test "handles single param" do
      result = Params.finalize(["SELECT name WHERE id = ", {:param, 42}])
      assert result == {"SELECT name WHERE id = $1", [42]}
    end

    test "handles multiple params" do
      result = Params.finalize([
        "SELECT name WHERE id = ", {:param, 42},
        " AND active = ", {:param, true}
      ])
      assert result == {"SELECT name WHERE id = $1 AND active = $2", [42, true]}
    end

    test "handles nested lists" do
      result = Params.finalize([
        "SELECT ",
        ["name", " WHERE id = ", {:param, 42}],
        " AND active = ", {:param, true}
      ])
      assert result == {"SELECT name WHERE id = $1 AND active = $2", [42, true]}
    end

    test "handles mixed types" do
      result = Params.finalize([
        "SELECT count FROM users WHERE active = ", {:param, true},
        " AND created_at > ", {:param, ~D[2024-01-01]}
      ])
      expected_sql = "SELECT count FROM users WHERE active = $1 AND created_at > $2"
      expected_params = [true, ~D[2024-01-01]]
      assert result == {expected_sql, expected_params}
    end
  end
end
