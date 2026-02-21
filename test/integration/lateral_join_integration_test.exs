defmodule Selecto.Integration.LateralJoinTest do
  use ExUnit.Case, async: true

  setup do
    domain = %{
      name: "film_domain",
      source: %{
        source_table: "film",
        primary_key: :film_id,
        fields: [:film_id, :title, :rating, :special_features],
        redact_fields: [],
        columns: %{film_id: %{type: :integer}, title: %{type: :string}, rating: %{type: :string}, special_features: %{type: :array}},
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }

    {:ok, domain: domain}
  end

  test "adds lateral join spec to selecto set", %{domain: domain} do
    selecto =
      Selecto.configure(domain, [], validate: false)
      |> Selecto.select(["title"])
      |> Selecto.lateral_join(:inner, {:unnest, "film.special_features"}, "features")

    lateral_joins = Map.get(selecto.set, :lateral_joins, [])
    assert length(lateral_joins) == 1
  end

  test "to_sql includes lateral SQL fragment", %{domain: domain} do
    selecto =
      Selecto.configure(domain, [], validate: false)
      |> Selecto.select(["title"])
      |> Selecto.lateral_join(:left, {:function, :generate_series, [1, 10]}, "numbers")

    {sql, params} = Selecto.to_sql(selecto)
    assert sql =~ "JOIN LATERAL"
    assert sql =~ "GENERATE_SERIES"
    assert is_list(params)
  end

  test "invalid reference raises correlation error", %{domain: domain} do
    assert_raise Selecto.Advanced.LateralJoin.CorrelationError, fn ->
      Selecto.configure(domain, [], validate: false)
      |> Selecto.lateral_join(:inner, {:unnest, "film.nonexistent"}, "bad")
    end
  end
end
