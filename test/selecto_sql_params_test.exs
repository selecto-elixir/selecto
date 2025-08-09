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

  describe "finalize_with_ctes/1" do
    test "handles fragments without CTEs like normal finalize" do
      fragments = ["SELECT * FROM users WHERE id = ", {:param, 123}]
      
      {ctes, sql, params} = Params.finalize_with_ctes(fragments)
      
      assert ctes == []
      assert sql == "SELECT * FROM users WHERE id = "
      assert params == [123]
    end

    test "extracts and processes single CTE" do
      fragments = [
        {:cte, "user_stats", [
          "SELECT user_id, COUNT(*) as post_count FROM posts WHERE user_id = ",
          {:param, 123},
          " GROUP BY user_id"
        ]},
        "SELECT * FROM user_stats WHERE post_count > ",
        {:param, 5}
      ]
      
      {ctes, sql, params} = Params.finalize_with_ctes(fragments)
      
      assert length(ctes) == 1
      {cte_name, cte_sql} = hd(ctes)
      assert cte_name == "user_stats"
      assert cte_sql == "SELECT user_id, COUNT(*) as post_count FROM posts WHERE user_id = $1 GROUP BY user_id"
      assert sql == "SELECT * FROM user_stats WHERE post_count > "
      assert params == [123, 5]
    end

    test "handles multiple CTEs with proper parameter coordination" do
      fragments = [
        {:cte, "active_users", [
          "SELECT id FROM users WHERE active = ", {:param, true}
        ]},
        {:cte, "recent_posts", [
          "SELECT user_id FROM posts WHERE created_at > ", {:param, ~D[2023-01-01]}
        ]},
        "SELECT au.id FROM active_users au JOIN recent_posts rp ON au.id = rp.user_id WHERE au.id = ",
        {:param, 456}
      ]
      
      {ctes, sql, params} = Params.finalize_with_ctes(fragments)
      
      assert length(ctes) == 2
      
      [{cte1_name, cte1_sql}, {cte2_name, cte2_sql}] = ctes
      assert cte1_name == "active_users"
      assert cte1_sql == "SELECT id FROM users WHERE active = $1"
      assert cte2_name == "recent_posts"
      assert cte2_sql == "SELECT user_id FROM posts WHERE created_at > $1"
      
      assert sql == "SELECT au.id FROM active_users au JOIN recent_posts rp ON au.id = rp.user_id WHERE au.id = "
      assert params == [true, ~D[2023-01-01], 456]
    end

    test "handles nested iodata within CTEs" do
      fragments = [
        {:cte, "complex_cte", [
          "SELECT ",
          ["id, name"],
          " FROM users WHERE ",
          ["age > ", {:param, 18}, " AND status = ", {:param, "active"}]
        ]},
        "SELECT * FROM complex_cte"
      ]
      
      {ctes, sql, params} = Params.finalize_with_ctes(fragments)
      
      assert length(ctes) == 1
      {cte_name, cte_sql} = hd(ctes)
      assert cte_name == "complex_cte"
      assert cte_sql == "SELECT id, name FROM users WHERE age > $1 AND status = $2"
      assert sql == "SELECT * FROM complex_cte"
      assert params == [18, "active"]
    end

    test "handles CTEs with no parameters" do
      fragments = [
        {:cte, "constants", ["SELECT 1 as one, 'hello' as greeting"]},
        "SELECT * FROM constants"
      ]
      
      {ctes, sql, params} = Params.finalize_with_ctes(fragments)
      
      assert length(ctes) == 1
      {cte_name, cte_sql} = hd(ctes)
      assert cte_name == "constants"
      assert cte_sql == "SELECT 1 as one, 'hello' as greeting"
      assert sql == "SELECT * FROM constants"
      assert params == []
    end

    test "handles empty fragments list" do
      fragments = []
      
      {ctes, sql, params} = Params.finalize_with_ctes(fragments)
      
      assert ctes == []
      assert sql == ""
      assert params == []
    end

    test "handles mixed CTE and parameter ordering" do
      fragments = [
        {:cte, "cte1", ["SELECT id FROM users WHERE age > ", {:param, 21}]},
        "SELECT * FROM cte1 WHERE id = ", {:param, 100},
        {:cte, "cte2", ["SELECT name FROM profiles WHERE user_id = ", {:param, 200}]}
      ]
      
      {ctes, sql, params} = Params.finalize_with_ctes(fragments)
      
      # Should have 2 CTEs
      assert length(ctes) == 2
      
      # Parameters should be in processing order: CTEs first, then main query params, then extracted params
      assert params == [21, 200, 100]
      
      # Check CTE SQL has proper parameter numbers (each CTE gets its own parameter numbering)
      [{_, cte1_sql}, {_, cte2_sql}] = ctes
      assert cte1_sql == "SELECT id FROM users WHERE age > $1"
      assert cte2_sql == "SELECT name FROM profiles WHERE user_id = $1"
      assert sql == "SELECT * FROM cte1 WHERE id = "
    end
  end

  describe "parameter extraction edge cases" do
    test "handles deeply nested iodata" do
      fragments = [
        "SELECT * FROM (",
        [
          "SELECT id FROM users WHERE ",
          [
            "age IN (",
            [
              {:param, 18}, ", ",
              [
                {:param, 21}, ", ",
                {:param, 25}
              ]
            ],
            ")"
          ]
        ],
        ") AS subquery"
      ]
      
      {sql, params} = Params.finalize(fragments)
      
      assert sql == "SELECT * FROM (SELECT id FROM users WHERE age IN ($1, $2, $3)) AS subquery"
      assert params == [18, 21, 25]
    end

    test "handles consecutive parameters without separators" do
      fragments = [
        "VALUES (",
        {:param, 1}, {:param, 2}, {:param, 3},
        ")"
      ]
      
      {sql, params} = Params.finalize(fragments)
      
      assert sql == "VALUES ($1$2$3)"
      assert params == [1, 2, 3]
    end

    test "converts non-string values to strings" do
      fragments = [
        "SELECT ", 42, " FROM users WHERE active = ", true, " AND pi = ", 3.14
      ]
      
      {sql, params} = Params.finalize(fragments)
      
      assert sql == "SELECT 42 FROM users WHERE active = true AND pi = 3.14"
      assert params == []
    end

    test "handles atoms as values" do
      fragments = [
        "SELECT * FROM users WHERE status = ", :active, " OR role = ", :admin
      ]
      
      {sql, params} = Params.finalize(fragments)
      
      assert sql == "SELECT * FROM users WHERE status = active OR role = admin"
      assert params == []
    end

    test "handles mixed parameter and literal values" do
      fragments = [
        "SELECT ", 1, " as literal, ", {:param, "parameterized"}, " as param"
      ]
      
      {sql, params} = Params.finalize(fragments)
      
      assert sql == "SELECT 1 as literal, $1 as param"
      assert params == ["parameterized"]
    end
  end

  describe "CTE extraction edge cases" do
    test "handles deeply nested CTEs in iodata" do
      fragments = [
        [
          {:cte, "nested_cte", [
            "SELECT id FROM users WHERE active = ", {:param, true}
          ]},
          [
            "SELECT * FROM nested_cte WHERE id = ", {:param, 123}
          ]
        ]
      ]
      
      {ctes, sql, params} = Params.finalize_with_ctes(fragments)
      
      assert length(ctes) == 1
      {cte_name, cte_sql} = hd(ctes)
      assert cte_name == "nested_cte"
      assert cte_sql == "SELECT id FROM users WHERE active = $1"
      assert sql == "SELECT * FROM nested_cte WHERE id = "
      assert params == [true, 123]
    end

    test "handles empty CTE iodata" do
      fragments = [
        {:cte, "empty_cte", []},
        "SELECT * FROM empty_cte"
      ]
      
      {ctes, sql, params} = Params.finalize_with_ctes(fragments)
      
      assert length(ctes) == 1
      {cte_name, cte_sql} = hd(ctes)
      assert cte_name == "empty_cte"
      assert cte_sql == ""
      assert sql == "SELECT * FROM empty_cte"
      assert params == []
    end

    test "handles CTE with only parameters" do
      fragments = [
        {:cte, "param_only", [{:param, "value1"}, {:param, "value2"}]},
        "SELECT * FROM param_only"
      ]
      
      {ctes, sql, params} = Params.finalize_with_ctes(fragments)
      
      assert length(ctes) == 1
      {cte_name, cte_sql} = hd(ctes)
      assert cte_name == "param_only"
      assert cte_sql == "$1$2"
      assert sql == "SELECT * FROM param_only"
      assert params == ["value1", "value2"]
    end
  end
end
