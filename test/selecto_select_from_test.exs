defmodule Selecto.SelectFromTest do
  use ExUnit.Case

  defp domain(source_table, fields, columns) do
    %{
      source: %{
        source_table: source_table,
        primary_key: :id,
        fields: fields,
        redact_fields: [],
        columns: columns,
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      name: "Test"
    }
  end

  test "basic select/from SQL renders" do
    d =
      domain("users", [:id, :name, :email], %{
        id: %{type: :integer},
        name: %{type: :string},
        email: %{type: :string}
      })

    selecto = Selecto.configure(d, :mock_connection) |> Selecto.select(["name", "email"])
    {sql, _aliases, params} = Selecto.gen_sql(selecto, [])

    assert sql =~ ~r/select/i
    assert sql =~ ~r/from\s+users/i
    assert params == []
  end

  test "function selectors compile" do
    d =
      domain("orders", [:id, :amount, :status], %{
        id: %{type: :integer},
        amount: %{type: :decimal},
        status: %{type: :string}
      })

    selecto =
      Selecto.configure(d, :mock_connection)
      |> Selecto.select([{:count}, {:sum, "amount"}])

    {sql, _aliases, _params} = Selecto.gen_sql(selecto, [])
    assert sql =~ ~r/count\(\*\)/i
    assert sql =~ ~r/sum\(/i
  end
end
