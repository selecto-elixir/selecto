defmodule Selecto.QueryMembersTest do
  use ExUnit.Case, async: true

  defp normalize_sql(sql) do
    sql
    |> then(&Regex.replace(~r/\s+/, &1, " "))
    |> String.trim()
  end

  defp order_domain do
    %{
      name: "Orders",
      source: %{
        source_table: "orders",
        primary_key: :id,
        fields: [:id, :order_number, :status, :total, :tags, :customer_id],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          order_number: %{type: :string},
          status: %{type: :string},
          total: %{type: :decimal},
          tags: %{type: {:array, :string}},
          customer_id: %{type: :integer}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end

  defp customer_domain do
    %{
      name: "Customers",
      source: %{
        source_table: "customers",
        primary_key: :id,
        fields: [:id, :name, :tier],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          tier: %{type: :string}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end

  defp order_domain_with_query_members do
    domain = order_domain()

    query_members = %{
      ctes: %{
        order_totals: %{
          query: fn _selecto ->
            Selecto.configure(order_domain(), :mock_connection, validate: false)
            |> Selecto.select(["id", "total"])
            |> Selecto.filter({"status", "delivered"})
          end,
          columns: ["id", "total"],
          join: [owner_key: :id, related_key: :id, fields: :infer]
        }
      },
      values: %{
        status_labels: %{
          rows: [
            ["processing", "In Progress"],
            ["shipped", "In Transit"],
            ["delivered", "Completed"]
          ],
          columns: ["status", "status_label"],
          as: "status_labels",
          join: [owner_key: :status, related_key: :status]
        }
      },
      subqueries: %{},
      laterals: %{
        tag_expansion: %{
          source: {:function, :generate_series, [1, 3]},
          as: "series_rows",
          join_type: :inner
        }
      },
      unnests: %{
        product_tags: %{
          array_field: "tags",
          as: "product_tag",
          ordinality: "product_tag_position"
        }
      }
    }

    Map.put(domain, :query_members, query_members)
  end

  defp customer_domain_with_query_members do
    domain = customer_domain()

    query_members = %{
      ctes: %{},
      values: %{},
      subqueries: %{
        high_value_delivered: %{
          query: fn _selecto ->
            Selecto.configure(order_domain(), :mock_connection, validate: false)
            |> Selecto.select(["customer_id", "order_number", "total"])
            |> Selecto.filter(
              {:and,
               [
                 {"status", "delivered"},
                 {"total", {:>, 1000}}
               ]}
            )
          end,
          type: :inner,
          on: [%{left: "id", right: "customer_id"}]
        }
      }
    }

    Map.put(domain, :query_members, query_members)
  end

  test "with_cte/2 resolves named CTE member and applies configured join" do
    query =
      Selecto.configure(order_domain_with_query_members(), :mock_connection, validate: false)
      |> Selecto.with_cte(:order_totals)
      |> Selecto.select(["order_number", "order_totals.total"])

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == ["delivered"]
    assert sql =~ "WITH order_totals (id, total) AS ("
    assert sql =~ "left join order_totals order_totals on order_totals.id = selecto_root.id"
  end

  test "with_cte/2 is idempotent by CTE name" do
    query =
      Selecto.configure(order_domain_with_query_members(), :mock_connection, validate: false)
      |> Selecto.with_cte(:order_totals)
      |> Selecto.with_cte(:order_totals)
      |> Selecto.select(["order_number", "order_totals.total"])

    {sql, _params} = Selecto.to_sql(query)

    assert Regex.scan(~r/order_totals \(id, total\) AS \(/i, sql) |> length() == 1
  end

  test "with_values/2 resolves named VALUES member and applies configured join" do
    query =
      Selecto.configure(order_domain_with_query_members(), :mock_connection, validate: false)
      |> Selecto.with_values(:status_labels)
      |> Selecto.select(["order_number", "status_labels.status_label"])

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == []

    assert sql =~
             "WITH status_labels (\"status\", \"status_label\") AS (VALUES ('processing', 'In Progress'), ('shipped', 'In Transit'), ('delivered', 'Completed'))"

    assert sql =~
             "left join status_labels status_labels on status_labels.status = selecto_root.status"
  end

  test "with_subquery/2 resolves named subquery member and preserves bind params" do
    query =
      Selecto.configure(customer_domain_with_query_members(), :mock_connection, validate: false)
      |> Selecto.with_subquery(:high_value_delivered)
      |> Selecto.select([
        "name",
        "high_value_delivered.order_number",
        "high_value_delivered.total"
      ])

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == ["delivered", 1000]
    assert sql =~ "from customers selecto_root inner join ("
    assert sql =~ "where (((( selecto_root.status = $1 ) and ( selecto_root.total > $2 ))))"
    assert sql =~ ") high_value_delivered on selecto_root.id = high_value_delivered.customer_id"
  end

  test "with_lateral/2 resolves named lateral member" do
    query =
      Selecto.configure(order_domain_with_query_members(), :mock_connection, validate: false)
      |> Selecto.with_lateral(:tag_expansion)
      |> Selecto.select(["order_number"])

    {sql, params} = Selecto.to_sql(query)

    assert params == [1, 3]
    assert sql =~ ~r/inner\s+join\s+lateral/i
    assert sql =~ ~r/series_rows/i
  end

  test "with_unnest/2 resolves named unnest member" do
    query =
      Selecto.configure(order_domain_with_query_members(), :mock_connection, validate: false)
      |> Selecto.with_unnest(:product_tags)
      |> Selecto.select(["order_number"])

    {sql, params} = Selecto.to_sql(query)

    assert params == []
    assert sql =~ ~r/unnest\("selecto_root"\."tags"\)/i
    assert sql =~ ~r/AS\s+product_tag\(value,\s*product_tag_position\)/i
  end

  test "with_unnest/2 is idempotent by alias" do
    query =
      Selecto.configure(order_domain_with_query_members(), :mock_connection, validate: false)
      |> Selecto.with_unnest(:product_tags)
      |> Selecto.with_unnest(:product_tags)
      |> Selecto.select(["order_number"])

    {sql, _params} = Selecto.to_sql(query)

    assert Regex.scan(~r/unnest\("selecto_root"\."tags"\)/i, sql) |> length() == 1
  end

  test "named helpers raise useful errors for unknown members" do
    selecto =
      Selecto.configure(order_domain_with_query_members(), :mock_connection, validate: false)

    assert_raise ArgumentError,
                 ~r/Named CTE 'missing_member' was not found in domain query_members/,
                 fn ->
                   Selecto.with_cte(selecto, :missing_member)
                 end
  end
end
