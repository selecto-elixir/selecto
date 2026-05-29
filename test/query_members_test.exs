defmodule Selecto.QueryMembersTest do
  use ExUnit.Case, async: true

  alias Selecto.Expr, as: X

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
        fields: [:id, :order_number, :status, :total, :tags, :metadata, :customer_id],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          order_number: %{type: :string},
          status: %{type: :string},
          total: %{type: :decimal},
          tags: %{type: {:array, :string}},
          metadata: %{type: :json},
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
        },
        line_item_rows: %{
          source: {:json_each, "selecto_root.metadata", "$[*]"},
          as: "item_rows",
          join_type: :inner
        },
        line_item_tree: %{
          source: {:json_tree, "selecto_root.metadata", nil},
          as: "item_tree",
          join_type: :left
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

  defp order_domain_with_stacked_ctes do
    domain = order_domain()

    query_members = %{
      ctes: %{
        active_orders: %{
          query: fn selecto ->
            selecto
            |> Selecto.select(["id", "customer_id", "status"])
            |> Selecto.filter({"status", "delivered"})
          end,
          columns: ["id", "customer_id", "status"],
          join: [owner_key: :id, related_key: :id, fields: :infer]
        },
        customer_order_rollups: %{
          query: fn selecto ->
            selecto
            |> Selecto.select([
              "customer_id",
              X.as(X.count("*"), "order_count")
            ])
            |> Selecto.group_by(["customer_id"])
          end,
          columns: ["customer_id", "order_count"],
          join: [owner_key: :customer_id, related_key: :customer_id, fields: :infer]
        }
      },
      values: %{},
      subqueries: %{},
      laterals: %{},
      unnests: %{}
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

    assert sql =~
             "order_totals (id, total) AS ( select cte_order_totals.id, cte_order_totals.total from orders cte_order_totals"

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

  test "stacked named CTEs do not nest prior user CTE WITH clauses inside later CTE bodies" do
    query =
      Selecto.configure(order_domain_with_stacked_ctes(), :mock_connection, validate: false)
      |> Selecto.with_cte(:active_orders)
      |> Selecto.with_cte(:customer_order_rollups)
      |> Selecto.select(["order_number", "customer_order_rollups.order_count"])

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == ["delivered"]
    assert Regex.scan(~r/WITH active_orders/i, sql) |> length() == 1
    refute sql =~ "customer_order_rollups (customer_id, order_count) AS ( WITH active_orders"
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

    assert sql =~
             "where (((( subq_root_orders_high_value_delivered.status = $1 ) and ( subq_root_orders_high_value_delivered.total > $2 ))))"

    assert sql =~
             "select subq_root_orders_high_value_delivered.customer_id, subq_root_orders_high_value_delivered.order_number, subq_root_orders_high_value_delivered.total"

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

  test "with_lateral/3 allows overriding alias and join type" do
    query =
      Selecto.configure(order_domain_with_query_members(), :mock_connection, validate: false)
      |> Selecto.with_lateral(:tag_expansion, as: "series_override", join_type: :left)
      |> Selecto.select(["order_number"])

    {sql, params} = Selecto.to_sql(query)

    assert params == [1, 3]
    assert sql =~ ~r/left\s+join\s+lateral/i
    assert sql =~ ~r/series_override/i
  end

  test "with_lateral/2 registers columns for named sqlite json_each laterals" do
    query =
      Selecto.configure(order_domain_with_query_members(), :mock_connection, validate: false)
      |> Map.put(:adapter, SelectoDBSQLite.Adapter)
      |> Selecto.with_lateral(:line_item_rows)
      |> Selecto.select(["order_number", "item_rows.key", "item_rows.value"])
      |> Selecto.filter({"item_rows.value", "sku-123"})

    {sql, params} = Selecto.to_sql(query)

    assert params == ["sku-123"]
    assert sql =~ "JSON_EACH(selecto_root.metadata, '$[*]')"
    assert sql =~ "item_rows.\"key\""
    assert sql =~ "item_rows.value"
    assert sql =~ ~r/where.*item_rows\.value\s*=\s*\?/i
    refute sql =~ ~r/join\s+lateral/i
  end

  test "with_lateral/3 supports alias override for named sqlite json_tree laterals" do
    query =
      Selecto.configure(order_domain_with_query_members(), :mock_connection, validate: false)
      |> Map.put(:adapter, SelectoDBSQLite.Adapter)
      |> Selecto.with_lateral(:line_item_tree, as: "tree_override", join_type: :inner)
      |> Selecto.select(["order_number", "tree_override.fullkey", "tree_override.parent"])
      |> Selecto.filter({"tree_override.fullkey", "$.items[0].sku"})

    {sql, params} = Selecto.to_sql(query)

    assert params == ["$.items[0].sku"]
    assert sql =~ "INNER JOIN JSON_TREE(selecto_root.metadata) AS tree_override ON true"
    assert sql =~ "tree_override.fullkey"
    assert sql =~ "tree_override.parent"
    assert sql =~ ~r/where.*tree_override\.fullkey\s*=\s*\?/i
    refute sql =~ ~r/join\s+lateral/i
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

  test "with_unnest/3 allows overriding alias and ordinality" do
    query =
      Selecto.configure(order_domain_with_query_members(), :mock_connection, validate: false)
      |> Selecto.with_unnest(:product_tags, as: "tag_override", ordinality: "tag_idx")
      |> Selecto.select(["order_number", "tag_override"])

    {sql, params} = Selecto.to_sql(query)

    assert params == []
    assert sql =~ ~r/unnest\("selecto_root"\."tags"\)/i
    assert sql =~ ~r/AS\s+tag_override\(value,\s*tag_idx\)/i
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
