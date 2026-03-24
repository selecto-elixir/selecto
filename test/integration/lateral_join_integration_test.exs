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
        columns: %{
          film_id: %{type: :integer},
          title: %{type: :string},
          rating: %{type: :string},
          special_features: %{type: :array}
        },
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

  test "mssql compiles left lateral joins to outer apply", %{domain: domain} do
    subquery =
      Selecto.configure(domain, :mock_connection,
        adapter: SelectoDBMSSQL.Adapter,
        validate: false
      )
      |> Selecto.select([{:count, "*"}])
      |> Selecto.filter({"rating", "PG"})

    selecto =
      Selecto.configure(domain, :mock_connection,
        adapter: SelectoDBMSSQL.Adapter,
        validate: false
      )
      |> Selecto.select(["title"])
      |> Selecto.lateral_join(:left, fn _ -> subquery end, "matching_films")

    {sql, params} = Selecto.to_sql(selecto)

    assert sql =~ "OUTER APPLY"
    assert sql =~ "@p1"
    refute sql =~ "JOIN LATERAL"
    assert params == ["PG"]
  end

  test "mssql rejects unsupported lateral join types", %{domain: domain} do
    selecto =
      Selecto.configure(domain, :mock_connection,
        adapter: SelectoDBMSSQL.Adapter,
        validate: false
      )
      |> Selecto.select(["title"])
      |> Selecto.lateral_join(:right, {:function, :generate_series, [1, 10]}, "numbers")

    assert_raise RuntimeError, ~r/only supports :inner and :left lateral joins/, fn ->
      Selecto.to_sql(selecto)
    end
  end

  test "mssql supports correlated top-n apply with pagination" do
    customer_domain = customer_domain()
    order_domain = order_domain()

    recent_order_query =
      Selecto.configure(order_domain, :mock_connection,
        adapter: SelectoDBMSSQL.Adapter,
        validate: false
      )
      |> Selecto.select(["id", "inserted_at"])
      |> Selecto.filter({"customer_id", {:ref, "selecto_root.customer_id"}})
      |> Selecto.order_by([{"inserted_at", :desc}])
      |> Selecto.limit(1)

    query =
      Selecto.configure(customer_domain, :mock_connection,
        adapter: SelectoDBMSSQL.Adapter,
        validate: false
      )
      |> Selecto.select(["name"])
      |> Selecto.lateral_join(:left, fn _ -> recent_order_query end, "recent_order")

    {sql, params} = Selecto.to_sql(query)

    assert params == []
    assert sql =~ "OUTER APPLY"
    assert sql =~ "from orders subq_root_orders"
    assert sql =~ "subq_root_orders.customer_id = selecto_root.customer_id"
    assert sql =~ "order by subq_root_orders.inserted_at desc"
    assert String.downcase(sql) =~ "offset 0 rows fetch next 1 rows only"
  end

  test "mssql supports correlated top-n cross apply with pagination" do
    customer_domain = customer_domain()
    order_domain = order_domain()

    latest_order_query =
      Selecto.configure(order_domain, :mock_connection,
        adapter: SelectoDBMSSQL.Adapter,
        validate: false
      )
      |> Selecto.select(["id", "inserted_at"])
      |> Selecto.filter({"customer_id", {:ref, "selecto_root.customer_id"}})
      |> Selecto.order_by([{"inserted_at", :desc}])
      |> Selecto.limit(1)

    query =
      Selecto.configure(customer_domain, :mock_connection,
        adapter: SelectoDBMSSQL.Adapter,
        validate: false
      )
      |> Selecto.select(["name"])
      |> Selecto.lateral_join(:inner, fn _ -> latest_order_query end, "latest_order")

    {sql, params} = Selecto.to_sql(query)

    assert params == []
    assert sql =~ "CROSS APPLY"
    refute sql =~ "OUTER APPLY"
    assert sql =~ "from orders subq_root_orders"
    assert sql =~ "subq_root_orders.customer_id = selecto_root.customer_id"
    assert sql =~ "order by subq_root_orders.inserted_at desc"
    assert String.downcase(sql) =~ "offset 0 rows fetch next 1 rows only"
  end

  test "invalid reference raises correlation error", %{domain: domain} do
    assert_raise Selecto.Advanced.LateralJoin.CorrelationError, fn ->
      Selecto.configure(domain, [], validate: false)
      |> Selecto.lateral_join(:inner, {:unnest, "film.nonexistent"}, "bad")
    end
  end

  defp customer_domain do
    %{
      name: "customer_domain",
      source: %{
        source_table: "customers",
        primary_key: :customer_id,
        fields: [:customer_id, :name],
        redact_fields: [],
        columns: %{
          customer_id: %{type: :integer},
          name: %{type: :string}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end

  defp order_domain do
    %{
      name: "order_domain",
      source: %{
        source_table: "orders",
        primary_key: :id,
        fields: [:id, :customer_id, :inserted_at],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          customer_id: %{type: :integer},
          inserted_at: %{type: :naive_datetime}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end
end
