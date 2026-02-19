defmodule Selecto.WindowJsonRegressionTest do
  use ExUnit.Case, async: true

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
end
