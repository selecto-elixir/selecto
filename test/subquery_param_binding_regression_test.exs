defmodule Selecto.SubqueryParamBindingRegressionTest do
  use ExUnit.Case

  @domain %{
    name: "Subquery Param Binding",
    source: %{
      source_table: "orders",
      primary_key: :id,
      fields: [:id, :order_number, :customer_id, :status, :total],
      redact_fields: [],
      columns: %{
        id: %{type: :integer},
        order_number: %{type: :string},
        customer_id: %{type: :integer},
        status: %{type: :string},
        total: %{type: :decimal}
      },
      associations: %{}
    },
    schemas: %{},
    joins: %{}
  }

  test "parameterized IN subquery keeps its params and placeholder ordering" do
    query =
      Selecto.configure(@domain, :mock_connection, validate: false)
      |> Selecto.select(["order_number", "customer_id", "status"])
      |> Selecto.filter(
        {"customer_id", {:subquery, :in, "SELECT id FROM customers WHERE tier = $1", ["silver"]}}
      )
      |> Selecto.filter({"status", "delivered"})

    {sql, params} = Selecto.to_sql(query)

    assert sql =~ ~r/in\s*\(SELECT id FROM customers WHERE tier = \$1\)/i
    assert sql =~ ~r/status\s*=\s*\$2/i
    assert params == ["silver", "delivered"]
  end

  test "parameterized EXISTS subquery keeps its params and placeholder ordering" do
    query =
      Selecto.configure(@domain, :mock_connection, validate: false)
      |> Selecto.select(["order_number", "customer_id", "status"])
      |> Selecto.filter({
        :exists,
        "SELECT 1 FROM customers c WHERE c.id = selecto_root.customer_id AND c.tier = $1",
        ["gold"]
      })
      |> Selecto.filter({"status", "delivered"})

    {sql, params} = Selecto.to_sql(query)

    assert sql =~
             ~r/exists\s*\(SELECT 1 FROM customers c WHERE c\.id = selecto_root\.customer_id AND c\.tier = \$1\)/i

    assert sql =~ ~r/status\s*=\s*\$2/i
    assert params == ["gold", "delivered"]
  end
end
