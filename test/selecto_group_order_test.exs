defmodule Selecto.GroupOrderTest do
  use ExUnit.Case

  test "GROUP BY and ORDER BY with new iodata parameterization (phase 2)" do
    # Domain configuration
    domain = %{
      source: %{
        source_table: "users",
        primary_key: :id,
        fields: [:id, :name, :email, :age],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          email: %{type: :string},
          age: %{type: :integer}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      name: "User"
    }

    selecto = Selecto.configure(domain, :mock_connection)

    # Test with GROUP BY and ORDER BY
    selecto = 
      selecto
      |> Selecto.select([{:count}])
      |> Selecto.group_by(["age"])
      |> Selecto.order_by([{"age", :desc}])

    {sql, aliases, params} = Selecto.gen_sql(selecto, [])

    # Verify SQL structure
    assert String.contains?(sql, "select")
    assert String.contains?(sql, "count(*)")
    assert String.contains?(sql, "group by")
    assert String.contains?(sql, "order by")
    assert String.contains?(sql, "\"selecto_root\".\"age\"")
    assert String.contains?(sql, "desc")
    
    # Verify no legacy sentinel remains
    refute String.contains?(sql, "^SelectoParam^")
    
    # Verify params structure (should be empty for this query)
    assert is_list(params)
    
    # Verify aliases structure  
    assert is_list(aliases)
    assert length(aliases) == 1  # count(*)
  end

  test "ROLLUP functionality preserves special handling" do
    domain = %{
      source: %{
        source_table: "sales",
        primary_key: :id, 
        fields: [:id, :region, :amount],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          region: %{type: :string},
          amount: %{type: :decimal}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      name: "Sales"
    }

    selecto = Selecto.configure(domain, :mock_connection)

    # Test with ROLLUP - this should trigger the special case handling
    selecto = 
      selecto
      |> Selecto.select([{:sum, "amount"}])
      |> Selecto.group_by([rollup: ["region"]])
      |> Selecto.order_by([{"region", :asc}])

    {sql, _aliases, _params} = Selecto.gen_sql(selecto, [])

    # Verify ROLLUP special case handling
    assert String.contains?(sql, "rollup")
    assert String.contains?(sql, "select * from (")
    assert String.contains?(sql, ") as rollupfix")
    
    # Verify no legacy sentinel remains
    refute String.contains?(sql, "^SelectoParam^")
  end
end
