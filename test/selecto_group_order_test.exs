defmodule Selecto.GroupOrderTest do
  use ExUnit.Case

  defmodule NoRollupAdapter do
    def connect(connection), do: {:ok, connection}
    def supports?(:rollup), do: false
    def supports?(_feature), do: false
    def placeholder(_index), do: "?"
    def quote_identifier(identifier), do: to_string(identifier)
  end

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
    assert String.contains?(sql, "selecto_root.age")
    assert String.contains?(sql, "desc")

    # Verify no legacy sentinel remains
    refute String.contains?(sql, "^SelectoParam^")

    # Verify params structure (should be empty for this query)
    assert is_list(params)

    # Verify aliases structure  
    assert is_list(aliases)
    # count(*)
    assert length(aliases) == 1
  end

  test "ROLLUP falls back to plain grouping for adapters without rollup support" do
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

    selecto =
      Selecto.configure(domain, :mock_connection, adapter: NoRollupAdapter, validate: false)

    selecto =
      selecto
      |> Selecto.select([{:sum, "amount"}])
      |> Selecto.group_by(rollup: ["region"])
      |> Selecto.order_by([{"region", :asc}])

    {sql, _aliases, _params} = Selecto.gen_sql(selecto, [])

    assert String.contains?(String.downcase(sql), "group by")
    refute String.contains?(String.downcase(sql), "rollup")
    refute String.contains?(sql, ") as rollupfix")
  end
end
