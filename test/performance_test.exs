defmodule Selecto.PerformanceTest do
  use ExUnit.Case

  test "configure and SQL generation are fast enough for simple domain" do
    domain = %{
      name: "perf_domain",
      source: %{
        source_table: "users",
        primary_key: :id,
        fields: [:id, :name, :email],
        redact_fields: [],
        columns: %{id: %{type: :integer}, name: %{type: :string}, email: %{type: :string}},
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }

    {micros, {sql, _params}} =
      :timer.tc(fn ->
        selecto =
          Selecto.configure(domain, :mock_connection)
          |> Selecto.select(["name", "email"])
          |> Selecto.filter([{"name", {:like, "%a%"}}])

        Selecto.to_sql(selecto)
      end)

    assert is_binary(sql)
    assert micros < 1_000_000
  end
end
