defmodule Selecto.WindowJsonRegressionTest do
  use ExUnit.Case, async: true

  defmodule EctoAdapterTag do
    use Ecto.Schema

    schema "tags" do
      field :name, :string
    end
  end

  defmodule EctoAdapterProduct do
    use Ecto.Schema

    schema "products" do
      field :name, :string
      many_to_many :tags, EctoAdapterTag, join_through: "product_tags"
    end
  end

  defp normalize_sql(sql) do
    sql
    |> then(&Regex.replace(~r/\s+/, &1, " "))
    |> String.trim()
  end

  defp employee_domain do
    %{
      name: "Employees",
      source: %{
        source_table: "employees",
        primary_key: :id,
        fields: [:id, :first_name, :department, :salary, :active],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          first_name: %{type: :string},
          department: %{type: :string},
          salary: %{type: :decimal},
          active: %{type: :boolean}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end

  defp product_domain do
    %{
      name: "Products",
      source: %{
        source_table: "products",
        primary_key: :id,
        fields: [:id, :name, :sku, :price, :active, :tags, :metadata],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          sku: %{type: :string},
          price: %{type: :decimal},
          active: %{type: :boolean},
          tags: %{type: {:array, :string}},
          metadata: %{type: :jsonb}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end

  defp order_domain do
    %{
      name: "Orders",
      source: %{
        source_table: "orders",
        primary_key: :id,
        fields: [:id, :order_number, :status, :total],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          order_number: %{type: :string},
          status: %{type: :string},
          total: %{type: :decimal}
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

  defp order_domain_with_customer_join do
    %{
      name: "Orders",
      source: %{
        source_table: "orders",
        primary_key: :id,
        fields: [:id, :order_number, :status, :total, :customer_id],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          order_number: %{type: :string},
          status: %{type: :string},
          total: %{type: :decimal},
          customer_id: %{type: :integer}
        },
        associations: %{
          customer: %{field: :customer, queryable: :customers, owner_key: :customer_id, related_key: :id}
        }
      },
      schemas: %{
        customers: %{
          source_table: "customers",
          primary_key: :id,
          fields: [:id, :name, :tier],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            tier: %{type: :string}
          }
        }
      },
      joins: %{
        customer: %{
          name: "Customer",
          type: :left,
          source: "customers",
          on: [%{left: "customer_id", right: "id"}],
          fields: %{
            name: %{type: :string},
            tier: %{type: :string}
          }
        }
      }
    }
  end

  test "window SQL uses selecto_root alias for unqualified fields" do
    query =
      Selecto.configure(employee_domain(), :mock_connection, validate: false)
      |> Selecto.select(["first_name", "department", "salary"])
      |> Selecto.window_function(:row_number, [],
        over: [partition_by: ["department"], order_by: [{"salary", :desc}]],
        as: "department_salary_rank"
      )
      |> Selecto.window_function(:avg, ["salary"],
        over: [partition_by: ["department"]],
        as: "department_avg_salary"
      )

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == []

    assert sql =~
             "ROW_NUMBER() OVER (PARTITION BY selecto_root.department ORDER BY selecto_root.salary DESC) AS department_salary_rank"

    assert sql =~
             "AVG(selecto_root.salary) OVER (PARTITION BY selecto_root.department) AS department_avg_salary"

    refute sql =~ "PARTITION BY employees.department"
    refute sql =~ "ORDER BY employees.salary"
  end

  test "json_select + json_filter build valid SQL in SELECT and WHERE" do
    query =
      Selecto.configure(product_domain(), :mock_connection, validate: false)
      |> Selecto.select(["name", "sku", "price"])
      |> Selecto.json_select([
        {:json_extract_text, "metadata", "$.price_band", as: "price_band"},
        {:json_extract_text, "metadata", "$.warehouse.zone", as: "warehouse_zone"}
      ])
      |> Selecto.json_filter({:json_contains, "metadata", %{"price_band" => "premium"}})
      |> Selecto.filter({"active", true})
      |> Selecto.order_by({"price", :desc})
      |> Selecto.json_order_by({:json_extract_text, "metadata", "$.warehouse.zone", :asc})

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == [true]
    assert sql =~ "metadata ->> 'price_band' AS \"price_band\""
    assert sql =~ "metadata -> 'warehouse' ->> 'zone' AS \"warehouse_zone\""
    assert sql =~ "metadata @> '{\"price_band\":\"premium\"}'"
    assert sql =~ "where (( selecto_root.active = $1 ) and ( metadata @> '{\"price_band\":\"premium\"}' ))"
    assert sql =~ "order by selecto_root.price desc, metadata -> 'warehouse' ->> 'zone' asc"
  end

  test "unnest emits CROSS JOIN LATERAL clause in FROM" do
    query =
      Selecto.configure(product_domain(), :mock_connection, validate: false)
      |> Selecto.select([
        "name",
        {:field, {:raw_sql, "product_tag"}, "product_tag"}
      ])
      |> Selecto.unnest("tags", as: "product_tag")
      |> Selecto.filter({"active", true})

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == [true]
    assert sql =~ "from products selecto_root CROSS JOIN LATERAL UNNEST(\"selecto_root\".\"tags\") AS product_tag"
    assert sql =~ "where (( selecto_root.active = $1 ))"
  end

  test "with_values integrates as WITH clause and can be joined" do
    query =
      Selecto.configure(order_domain(), :mock_connection, validate: false)
      |> Selecto.with_values(
        [
          ["processing", "In Progress"],
          ["shipped", "In Transit"],
          ["delivered", "Completed"]
        ],
        columns: ["status", "status_label"],
        as: "status_labels"
      )
      |> Selecto.join(:status_labels,
        source: "status_labels",
        type: :left,
        owner_key: :status,
        related_key: :status,
        fields: %{
          status: %{type: :string},
          status_label: %{type: :string}
        }
      )
      |> Selecto.select(["order_number", "status", "status_labels.status_label"])
      |> Selecto.limit(5)

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == []
    assert sql =~ "WITH status_labels (\"status\", \"status_label\") AS (VALUES ('processing', 'In Progress'), ('shipped', 'In Transit'), ('delivered', 'Completed'))"
    assert sql =~ "from orders selecto_root left join status_labels status_labels on status_labels.status = selecto_root.status"
  end

  test "join_subquery injects parameterized subquery and preserves params" do
    high_value_delivered_orders =
      Selecto.configure(order_domain_with_customer_join(), :mock_connection, validate: false)
      |> Selecto.select(["customer_id", "order_number", "total"])
      |> Selecto.filter({:and, [
        {"status", "delivered"},
        {"total", {:>, 1000}}
      ]})

    query =
      Selecto.configure(customer_domain(), :mock_connection, validate: false)
      |> Selecto.join_subquery(:high_value_delivered, high_value_delivered_orders,
        type: :inner,
        on: [%{left: "id", right: "customer_id"}]
      )
      |> Selecto.select(["name", "tier", "high_value_delivered.order_number", "high_value_delivered.total"])
      |> Selecto.limit(5)

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == ["delivered", 1000]
    assert sql =~ "from customers selecto_root inner join ("
    assert sql =~ "select selecto_root.customer_id, selecto_root.order_number, selecto_root.total"
    assert sql =~ "where (((( selecto_root.status = $1 ) and ( selecto_root.total > $2 ))))"
    assert sql =~ ") high_value_delivered on selecto_root.id = high_value_delivered.customer_id"
  end

  test "join_parameterize exposes dot notation fields for generated aliases" do
    query =
      Selecto.configure(order_domain_with_customer_join(), :mock_connection, validate: false)
      |> Selecto.join_parameterize(:customer, "alias_a")
      |> Selecto.join_parameterize(:customer, "alias_b")
      |> Selecto.select(["order_number", "customer:alias_a.name", "customer:alias_b.tier"])
      |> Selecto.limit(3)

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == []
    assert sql =~ "select selecto_root.order_number, \"customer:alias_a\".name, \"customer:alias_b\".tier"
    assert sql =~ "left join customers \"customer:alias_a\" on \"customer:alias_a\".id = selecto_root.customer_id"
    assert sql =~ "left join customers \"customer:alias_b\" on \"customer:alias_b\".id = selecto_root.customer_id"
  end

  test "dynamic custom join can join non-association table with dot-notation fields" do
    query =
      Selecto.configure(product_domain(), :mock_connection, validate: false)
      |> Selecto.join(:reviews,
        source: "reviews",
        type: :left,
        owner_key: :id,
        related_key: :product_id,
        fields: %{
          rating: %{type: :integer},
          title: %{type: :string},
          helpful_count: %{type: :integer}
        }
      )
      |> Selecto.select(["name", "reviews.rating", "reviews.title", "reviews.helpful_count"])
      |> Selecto.filter({:and, [
        {"active", true},
        {"reviews.rating", {:>=, 4}}
      ]})
      |> Selecto.order_by({"reviews.rating", :desc})
      |> Selecto.limit(10)

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == [true, 4]
    assert sql =~ "from products selecto_root left join reviews reviews on reviews.product_id = selecto_root.id"
    assert sql =~ "where (((( selecto_root.active = $1 ) and ( reviews.rating >= $2 ))))"
    assert sql =~ "order by reviews.rating desc"
  end

  test "output format transformers handle aliases list from Selecto query metadata" do
    rows = [["Wireless Headphones", Decimal.new("79.99")]]
    columns = ["name", "price"]
    aliases = ["6ee949dc-86f5-4ed2-bac8-078786d26fd2", "4e2455d1-28c3-4aa2-8189-6804f489f46e"]

    assert {:ok, [map_row]} = Selecto.Output.Formats.transform({rows, columns, aliases}, :maps, [])
    assert map_row["name"] == "Wireless Headphones"
    assert map_row["price"] == Decimal.new("79.99")

    assert {:ok, json} = Selecto.Output.Formats.transform({rows, columns, aliases}, :json, [])
    assert json =~ "\"name\":\"Wireless Headphones\""
    assert json =~ "\"price\":\"79.99\""

    assert {:ok, csv} = Selecto.Output.Formats.transform({rows, columns, aliases}, :csv, [])
    assert csv =~ "name,price"
    assert csv =~ "Wireless Headphones,79.99"
  end

  test "stream output transformer handles aliases list from Selecto query metadata" do
    rows = [["Wireless Headphones", Decimal.new("79.99")], ["Smart Watch", Decimal.new("199.99")]]
    columns = ["name", "price"]
    aliases = ["6ee949dc-86f5-4ed2-bac8-078786d26fd2", "4e2455d1-28c3-4aa2-8189-6804f489f46e"]

    assert {:ok, stream} = Selecto.Output.Formats.transform({rows, columns, aliases}, {:stream, :maps}, [])

    maps = Enum.take(stream, 2)
    assert maps == [
             %{"name" => "Wireless Headphones", "price" => Decimal.new("79.99")},
             %{"name" => "Smart Watch", "price" => Decimal.new("199.99")}
           ]
  end

  test "join/3 honors explicit on conditions for custom joins" do
    query =
      Selecto.configure(order_domain_with_customer_join(), :mock_connection, validate: false)
      |> Selecto.join(:customer_lookup,
        source: "customers",
        type: :inner,
        on: [%{left: "customer_id", right: "id"}],
        fields: %{
          name: %{type: :string},
          tier: %{type: :string}
        }
      )
      |> Selecto.select(["order_number", "customer_lookup.name", "customer_lookup.tier", "total"])
      |> Selecto.limit(5)

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == []
    assert sql =~ "from orders selecto_root inner join customers customer_lookup on selecto_root.customer_id = customer_lookup.id"
  end

  test "star_dimension join uses selecto_root alias in ON clause" do
    star_domain =
      order_domain_with_customer_join()
      |> put_in([:joins, :customer, :type], :star_dimension)

    query =
      Selecto.configure(star_domain, :mock_connection, validate: false)
      |> Selecto.select(["customer.name", {:count, "*"}])
      |> Selecto.group_by(["customer.name"])
      |> Selecto.limit(5)

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == []
    assert sql =~ "LEFT JOIN customers customer ON selecto_root.customer_id = customer.id"
    refute sql =~ "ON orders.customer_id = customer.id"
  end

  test "lateral subquery join includes subquery params and keeps global placeholder order" do
    subquery_query =
      Selecto.configure(order_domain(), :mock_connection, validate: false)
      |> Selecto.select([{:count, "*"}])
      |> Selecto.filter({"status", "delivered"})

    query =
      Selecto.configure(product_domain(), :mock_connection, validate: false)
      |> Selecto.select([
        "name",
        {:field, {:raw_sql, "delivered_stats.count"}, "delivered_order_count"}
      ])
      |> Selecto.lateral_join(:left, fn _ -> subquery_query end, "delivered_stats")
      |> Selecto.filter({"active", true})
      |> Selecto.limit(5)

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == ["delivered", true]
    assert sql =~ "LEFT JOIN LATERAL ( select count(*) from orders selecto_root where (( selecto_root.status = $1 )) ) AS delivered_stats ON true"
    assert sql =~ "where (( selecto_root.active = $2 ))"
  end

  test "from_ecto schema introspection supports many_to_many associations without crashing" do
    domain = Selecto.EctoAdapter.schema_to_domain(EctoAdapterProduct, joins: [:tags])

    assert domain.source.source_table == "products"
    assert Map.has_key?(domain.source.associations, :tags)
    assert Map.has_key?(domain.schemas, :ecto_adapter_tag)
  end
end
