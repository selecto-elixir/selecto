defmodule Selecto.WindowJsonRegressionTest do
  use ExUnit.Case, async: true

  defmodule EctoAdapterTag do
    use Ecto.Schema

    schema "tags" do
      field(:name, :string)
    end
  end

  defmodule EctoAdapterProduct do
    use Ecto.Schema

    schema "products" do
      field(:name, :string)
      many_to_many(:tags, EctoAdapterTag, join_through: "product_tags")
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
          customer: %{
            field: :customer,
            queryable: :customers,
            owner_key: :customer_id,
            related_key: :id
          }
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

  defp order_domain_with_customer_join_filter do
    order_domain_with_customer_join()
    |> put_in([:joins, :customer, :filters], %{"tier" => %{type: "string"}})
  end

  defp order_domain_with_status_dimension_join do
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
        associations: %{
          ref_load_status: %{
            field: :ref_load_status,
            queryable: :ref_load_statuses,
            owner_key: :status,
            related_key: :id
          }
        }
      },
      schemas: %{
        ref_load_statuses: %{
          source_table: "ref_load_statuses",
          primary_key: :id,
          fields: [:id, :name],
          redact_fields: [],
          columns: %{
            id: %{type: :string},
            name: %{type: :string}
          }
        }
      },
      joins: %{
        ref_load_status: %{
          name: "Load Status",
          type: :star_dimension,
          source: "ref_load_statuses",
          owner_key: :status,
          my_key: :id,
          fields: %{
            id: %{type: :string},
            name: %{type: :string}
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
             "ROW_NUMBER() OVER (PARTITION BY selecto_root.department ORDER BY selecto_root.salary DESC) AS \"department_salary_rank\""

    assert sql =~
             "AVG(selecto_root.salary) OVER (PARTITION BY selecto_root.department) AS \"department_avg_salary\""

    refute sql =~ "PARTITION BY employees.department"
    refute sql =~ "ORDER BY employees.salary"
  end

  test "mssql window aliases use adapter quoting" do
    query =
      Selecto.configure(employee_domain(), :mock_connection,
        adapter: SelectoDBMSSQL.Adapter,
        validate: false
      )
      |> Selecto.select(["first_name", "department", "salary"])
      |> Selecto.window_function(:row_number, [],
        over: [partition_by: ["department"], order_by: [{"salary", :desc}]],
        as: "department salary rank"
      )

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == []

    assert sql =~
             "ROW_NUMBER() OVER (PARTITION BY selecto_root.department ORDER BY selecto_root.salary DESC) AS [department salary rank]"

    refute sql =~ "AS department salary rank"
  end

  test "mssql rejects interval window frames explicitly" do
    query =
      Selecto.configure(employee_domain(), :mock_connection,
        adapter: SelectoDBMSSQL.Adapter,
        validate: false
      )
      |> Selecto.select(["first_name", "salary"])
      |> Selecto.window_function(:avg, ["salary"],
        over: [
          order_by: ["salary"],
          frame: {:range, {:interval, "1 day"}, :current_row}
        ],
        as: "rolling_avg"
      )

    assert_raise RuntimeError, ~r/MSSQL window frames do not support interval boundaries/, fn ->
      Selecto.to_sql(query)
    end
  end

  test "mssql rejects nth_value explicitly" do
    query =
      Selecto.configure(employee_domain(), :mock_connection,
        adapter: SelectoDBMSSQL.Adapter,
        validate: false
      )
      |> Selecto.select(["first_name", "salary"])
      |> Selecto.window_function(:nth_value, ["salary", 2],
        over: [order_by: ["salary"]],
        as: "second_salary"
      )

    assert_raise RuntimeError, ~r/MSSQL window functions do not support nth_value yet/, fn ->
      Selecto.to_sql(query)
    end
  end

  test "mssql lag and lead compile with offset params and quoted aliases" do
    query =
      Selecto.configure(employee_domain(), :mock_connection,
        adapter: SelectoDBMSSQL.Adapter,
        validate: false
      )
      |> Selecto.select(["first_name", "department", "salary"])
      |> Selecto.window_function(:lag, ["salary", 2],
        over: [partition_by: ["department"], order_by: [{"salary", :desc}]],
        as: "prev salary"
      )
      |> Selecto.window_function(:lead, ["salary", 3],
        over: [partition_by: ["department"], order_by: [{"salary", :desc}]],
        as: "next salary"
      )

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == [2, 3]

    assert sql =~
             "LAG(selecto_root.salary, @p1) OVER (PARTITION BY selecto_root.department ORDER BY selecto_root.salary DESC) AS [prev salary]"

    assert sql =~
             "LEAD(selecto_root.salary, @p2) OVER (PARTITION BY selecto_root.department ORDER BY selecto_root.salary DESC) AS [next salary]"
  end

  test "mssql window aggregate names use sql server variants" do
    query =
      Selecto.configure(employee_domain(), :mock_connection,
        adapter: SelectoDBMSSQL.Adapter,
        validate: false
      )
      |> Selecto.select(["first_name", "department", "salary"])
      |> Selecto.window_function(:stddev, ["salary"],
        over: [partition_by: ["department"]],
        as: "salary stdev"
      )
      |> Selecto.window_function(:variance, ["salary"],
        over: [partition_by: ["department"]],
        as: "salary variance"
      )

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == []

    assert sql =~
             "STDEV(selecto_root.salary) OVER (PARTITION BY selecto_root.department) AS [salary stdev]"

    assert sql =~
             "VAR(selecto_root.salary) OVER (PARTITION BY selecto_root.department) AS [salary variance]"

    refute sql =~ "STDDEV(selecto_root.salary)"
    refute sql =~ "VARIANCE(selecto_root.salary)"
  end

  test "mssql non-window aggregate names use sql server variants" do
    query =
      Selecto.configure(employee_domain(), :mock_connection,
        adapter: SelectoDBMSSQL.Adapter,
        validate: false
      )
      |> Selecto.select([
        {:func, :stddev, ["salary"], [as: "salary stdev"]},
        {:func, :variance, ["salary"], [as: "salary variance"]}
      ])

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == []
    assert sql =~ "select STDEV(selecto_root.salary), VAR(selecto_root.salary)"
    refute sql =~ "stddev(selecto_root.salary)"
    refute sql =~ "variance(selecto_root.salary)"
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
    assert sql =~ "\"metadata\" @> '{\"price_band\":\"premium\"}'::jsonb"

    assert sql =~
             "where (( selecto_root.active = $1 ) and ( \"metadata\" @> '{\"price_band\":\"premium\"}'::jsonb ))"

    assert sql =~ "order by selecto_root.price desc, metadata -> 'warehouse' ->> 'zone' asc"
  end

  test "mssql json_select + json_filter + json_order_by use json_value" do
    query =
      Selecto.configure(product_domain(), :mock_connection,
        adapter: SelectoDBMSSQL.Adapter,
        validate: false
      )
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
    downcased = String.downcase(sql)

    assert params == [true]
    assert sql =~ "JSON_VALUE(selecto_root.metadata, '$.price_band') AS [price_band]"

    assert sql =~
             "JSON_VALUE(selecto_root.metadata, '$.warehouse.zone') AS [warehouse_zone]"

    assert downcased =~
             "where (( selecto_root.active = @p1 ) and ( json_value(selecto_root.metadata, '$.price_band') = 'premium' ))"

    assert downcased =~
             "order by selecto_root.price desc, json_value(selecto_root.metadata, '$.warehouse.zone') asc"
  end

  test "mssql dot-path json selectors and filters use json_value casts" do
    query =
      Selecto.configure(product_domain(), :mock_connection,
        adapter: SelectoDBMSSQL.Adapter,
        validate: false
      )
      |> Selecto.select(["name", "metadata.price_band"])
      |> Selecto.filter({"metadata.price_band", "premium"})

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == ["premium"]
    assert sql =~ "JSON_VALUE(selecto_root.metadata, '$.price_band')"
    assert sql =~ "where (( JSON_VALUE(selecto_root.metadata, '$.price_band') = @p1 ))"
  end

  test "mssql json path exists uses json_value/json_query helpers" do
    query =
      Selecto.configure(product_domain(), :mock_connection,
        adapter: SelectoDBMSSQL.Adapter,
        validate: false
      )
      |> Selecto.select(["name"])
      |> Selecto.json_filter({:json_path_exists, "metadata", "$.warehouse.zone"})

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == []

    assert sql =~
             "(JSON_QUERY(selecto_root.metadata, '$.warehouse.zone') IS NOT NULL OR JSON_VALUE(selecto_root.metadata, '$.warehouse.zone') IS NOT NULL)"
  end

  test "mssql json array filters use openjson" do
    query =
      Selecto.configure(product_domain(), :mock_connection,
        adapter: SelectoDBMSSQL.Adapter,
        validate: false
      )
      |> Selecto.select(["name"])
      |> Selecto.filter({"metadata.tags", {:contains, "featured"}})
      |> Selecto.filter({"metadata.tags", {:contains_all, ["featured", "new"]}})

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == []
    assert sql =~ "OPENJSON(selecto_root.metadata, '$.tags')"
    assert sql =~ "WHERE value = 'featured'"
    assert sql =~ "WHERE value = 'new'"
  end

  test "mysql json_select + json_filter + json_order_by use json_extract" do
    query =
      Selecto.configure(product_domain(), [], validate: false)
      |> Map.put(:adapter, SelectoDBMySQL.Adapter)
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
    downcased = String.downcase(sql)

    assert params == [true]

    assert sql =~
             "JSON_UNQUOTE(JSON_EXTRACT(`selecto_root`.`metadata`, '$.price_band')) AS `price_band`"

    assert sql =~
             "JSON_UNQUOTE(JSON_EXTRACT(`selecto_root`.`metadata`, '$.warehouse.zone')) AS `warehouse_zone`"

    assert downcased =~
             "where (( selecto_root.active = ? ) and ( json_contains(`selecto_root`.`metadata`, '{\"price_band\":\"premium\"}') ))"

    assert downcased =~
             "order by selecto_root.price desc, json_unquote(json_extract(`selecto_root`.`metadata`, '$.warehouse.zone')) asc"
  end

  test "mysql dot-path json selectors and filters use json_extract casts" do
    query =
      Selecto.configure(product_domain(), [], validate: false)
      |> Map.put(:adapter, SelectoDBMySQL.Adapter)
      |> Selecto.select(["name", "metadata.price_band"])
      |> Selecto.filter({"metadata.price_band", "premium"})

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == ["premium"]
    assert sql =~ "JSON_UNQUOTE(JSON_EXTRACT(`selecto_root`.`metadata`, '$.price_band'))"

    assert sql =~
             "where (( JSON_UNQUOTE(JSON_EXTRACT(`selecto_root`.`metadata`, '$.price_band')) = ? ))"
  end

  test "mysql json path exists and array filters use mysql json functions" do
    query =
      Selecto.configure(product_domain(), [], validate: false)
      |> Map.put(:adapter, SelectoDBMySQL.Adapter)
      |> Selecto.select(["name"])
      |> Selecto.json_filter({:json_path_exists, "metadata", "$.warehouse.zone"})
      |> Selecto.filter({"metadata.tags", {:contains, "featured"}})
      |> Selecto.filter({"metadata.tags", {:contains_all, ["featured", "new"]}})

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == []
    assert sql =~ "JSON_CONTAINS_PATH(`selecto_root`.`metadata`, 'one', '$.warehouse.zone')"
    assert sql =~ "JSON_CONTAINS(`selecto_root`.`metadata`, '\"featured\"', '$.tags')"
    assert sql =~ "JSON_CONTAINS(`selecto_root`.`metadata`, '\"new\"', '$.tags')"
  end

  test "sqlite json_select + json_filter + json_order_by use json_extract" do
    query =
      Selecto.configure(product_domain(), [], validate: false)
      |> Map.put(:adapter, SelectoDBSQLite.Adapter)
      |> Selecto.select(["name", "sku", "price"])
      |> Selecto.json_select([
        {:json_extract_text, "metadata", "$.price_band", as: "price_band"},
        {:json_extract_text, "metadata", "$.warehouse.zone", as: "warehouse_zone"}
      ])
      |> Selecto.json_filter({:json_path_exists, "metadata", "$.warehouse.zone"})
      |> Selecto.filter({"metadata.price_band", "premium"})
      |> Selecto.order_by({"price", :desc})
      |> Selecto.json_order_by({:json_extract_text, "metadata", "$.warehouse.zone", :asc})

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)
    downcased = String.downcase(sql)

    assert params == ["premium"]
    assert sql =~ ~s|json_extract("selecto_root"."metadata", '$.price_band') AS "price_band"|

    assert sql =~
             ~s|json_extract("selecto_root"."metadata", '$.warehouse.zone') AS "warehouse_zone"|

    assert downcased =~
             ~s|where (( json_extract("selecto_root"."metadata", '$.price_band') = ? ) and ( json_type("selecto_root"."metadata", '$.warehouse.zone') is not null ))|

    assert downcased =~
             ~s|order by selecto_root.price desc, json_extract("selecto_root"."metadata", '$.warehouse.zone') asc|
  end

  test "sqlite json_filter rejects unsupported containment helper" do
    query =
      Selecto.configure(product_domain(), [], validate: false)
      |> Map.put(:adapter, SelectoDBSQLite.Adapter)
      |> Selecto.select(["name"])
      |> Selecto.json_filter({:json_contains, "metadata", %{"price_band" => "premium"}})

    assert_raise RuntimeError, ~r/does not support this JSON operation/, fn ->
      Selecto.to_sql(query)
    end
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

    assert sql =~
             "from products selecto_root CROSS JOIN LATERAL UNNEST(\"selecto_root\".\"tags\") AS product_tag"

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

    assert sql =~
             "WITH status_labels (\"status\", \"status_label\") AS (VALUES ('processing', 'In Progress'), ('shipped', 'In Transit'), ('delivered', 'Completed'))"

    assert sql =~
             "from orders selecto_root left join status_labels status_labels on status_labels.status = selecto_root.status"
  end

  test "with_values can auto-join via join options" do
    query =
      Selecto.configure(order_domain(), :mock_connection, validate: false)
      |> Selecto.with_values(
        [
          ["processing", "In Progress"],
          ["shipped", "In Transit"],
          ["delivered", "Completed"]
        ],
        columns: ["status", "status_label"],
        as: "status_labels",
        join: [owner_key: :status, related_key: :status]
      )
      |> Selecto.select(["order_number", "status", "status_labels.status_label"])
      |> Selecto.limit(5)

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == []

    assert sql =~
             "WITH status_labels (\"status\", \"status_label\") AS (VALUES ('processing', 'In Progress'), ('shipped', 'In Transit'), ('delivered', 'Completed'))"

    assert sql =~
             "from orders selecto_root left join status_labels status_labels on status_labels.status = selecto_root.status"
  end

  test "with_values join: true infers join keys from first VALUES column" do
    query =
      Selecto.configure(order_domain(), :mock_connection, validate: false)
      |> Selecto.with_values(
        [
          ["processing", "In Progress"],
          ["shipped", "In Transit"]
        ],
        columns: ["status", "status_label"],
        as: "status_labels",
        join: true
      )
      |> Selecto.select(["order_number", "status_labels.status_label"])

    {sql, _params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert sql =~
             "left join status_labels status_labels on status_labels.status = selecto_root.status"
  end

  test "with_values uses SELECT UNION ALL CTEs for mysql-like adapters" do
    query =
      Selecto.configure(order_domain(), :mock_connection, validate: false)
      |> Map.put(:adapter, SelectoDBMySQL.Adapter)
      |> Selecto.with_values(
        [
          ["processing", "In Progress"],
          ["shipped", "In Transit"],
          ["delivered", "Completed"]
        ],
        columns: ["status", "status_label"],
        as: "status_labels",
        join: [owner_key: :status, related_key: :status]
      )
      |> Selecto.select(["order_number", "status_labels.status_label"])

    {sql, _params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert sql =~
             "WITH status_labels AS (SELECT 'processing' AS \"status\", 'In Progress' AS \"status_label\" UNION ALL SELECT 'shipped' AS \"status\", 'In Transit' AS \"status_label\" UNION ALL SELECT 'delivered' AS \"status\", 'Completed' AS \"status_label\")"

    assert sql =~
             "left join status_labels status_labels on status_labels.status = selecto_root.status"
  end

  test "with_cte can auto-join with inferred CTE fields" do
    query =
      Selecto.configure(order_domain_with_customer_join(), :mock_connection, validate: false)
      |> Selecto.with_cte(
        "order_totals",
        fn ->
          Selecto.configure(order_domain_with_customer_join(), :mock_connection, validate: false)
          |> Selecto.select(["id", "total"])
          |> Selecto.filter({"status", "delivered"})
        end,
        columns: ["id", "total"],
        join: [owner_key: :id, related_key: :id, fields: :infer]
      )
      |> Selecto.select(["order_number", "order_totals.total"])
      |> Selecto.limit(5)

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == ["delivered"]
    assert sql =~ "WITH order_totals (id, total) AS ("

    assert sql =~
             "left join order_totals order_totals on order_totals.id = selecto_root.id"
  end

  test "with_recursive_cte can auto-join recursive CTE" do
    query =
      Selecto.configure(order_domain(), :mock_connection, validate: false)
      |> Selecto.with_recursive_cte("order_chain",
        base_query: fn ->
          Selecto.configure(order_domain(), :mock_connection, validate: false)
          |> Selecto.select(["id", "status"])
          |> Selecto.filter({"status", "processing"})
        end,
        recursive_query: fn _cte_ref ->
          Selecto.configure(order_domain(), :mock_connection, validate: false)
          |> Selecto.select(["id", "status"])
        end,
        columns: ["id", "status"],
        join: [owner_key: :id, related_key: :id, fields: :infer]
      )
      |> Selecto.select(["order_number", "order_chain.status"])
      |> Selecto.limit(5)

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == ["processing"]
    assert sql =~ "WITH RECURSIVE order_chain (id, status) AS ("
    assert sql =~ "left join order_chain order_chain on order_chain.id = selecto_root.id"
  end

  test "with_recursive_cte omits RECURSIVE keyword for mssql adapter" do
    query =
      Selecto.configure(order_domain(), :mock_connection, validate: false)
      |> Map.put(:adapter, SelectoDBMSSQL.Adapter)
      |> Selecto.with_recursive_cte("order_chain",
        base_query: fn ->
          Selecto.configure(order_domain(), :mock_connection, validate: false)
          |> Map.put(:adapter, SelectoDBMSSQL.Adapter)
          |> Selecto.select(["id", "status"])
          |> Selecto.filter({"status", "processing"})
        end,
        recursive_query: fn _cte_ref ->
          Selecto.configure(order_domain(), :mock_connection, validate: false)
          |> Map.put(:adapter, SelectoDBMSSQL.Adapter)
          |> Selecto.select(["id", "status"])
        end,
        columns: ["id", "status"],
        join: [owner_key: :id, related_key: :id, fields: :infer]
      )
      |> Selecto.select(["order_number", "order_chain.status"])

    {sql, _params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    refute sql =~ "WITH RECURSIVE"
    assert sql =~ "WITH order_chain (id, status) AS ("
  end

  test "list filters expand placeholders for non-array-any adapters" do
    query =
      Selecto.configure(order_domain(), :mock_connection, validate: false)
      |> Selecto.select(["order_number", "status"])
      |> Selecto.filter({"status", {:in, ["processing", "shipped", "delivered"]}})
      |> Selecto.filter({"status", {:not_in, ["cancelled", "returned"]}})

    {mysql_sql, _params} = Selecto.to_sql(%{query | adapter: SelectoDBMySQL.Adapter})
    {sqlite_sql, _params} = Selecto.to_sql(%{query | adapter: SelectoDBSQLite.Adapter})
    {mssql_sql, _params} = Selecto.to_sql(%{query | adapter: SelectoDBMSSQL.Adapter})

    mysql_sql = normalize_sql(mysql_sql)
    sqlite_sql = normalize_sql(sqlite_sql)
    mssql_sql = normalize_sql(mssql_sql)

    assert mysql_sql =~ "status IN (?, ?, ?)"
    assert mysql_sql =~ "status NOT IN (?, ?)"

    assert sqlite_sql =~ "status IN (?, ?, ?)"
    assert sqlite_sql =~ "status NOT IN (?, ?)"

    assert mssql_sql =~ "status IN (@p1, @p2, @p3)"
    assert mssql_sql =~ "status NOT IN (@p4, @p5)"
  end

  test "with_ctes supports joins: [...] batch auto-join" do
    order_totals_cte =
      Selecto.Advanced.CTE.create_cte(
        "order_totals",
        fn ->
          Selecto.configure(order_domain_with_customer_join(), :mock_connection, validate: false)
          |> Selecto.select(["id", "total"])
        end,
        columns: ["id", "total"]
      )

    customer_spend_cte =
      Selecto.Advanced.CTE.create_cte(
        "customer_spend",
        fn ->
          Selecto.configure(order_domain_with_customer_join(), :mock_connection, validate: false)
          |> Selecto.select(["customer_id", "total"])
        end,
        columns: ["customer_id", "total"]
      )

    query =
      Selecto.configure(order_domain_with_customer_join(), :mock_connection, validate: false)
      |> Selecto.with_ctes([order_totals_cte, customer_spend_cte],
        joins: [
          [name: "order_totals", owner_key: :id, related_key: :id, fields: :infer],
          [
            name: "customer_spend",
            owner_key: :customer_id,
            related_key: :customer_id,
            fields: :infer
          ]
        ]
      )
      |> Selecto.select(["order_number", "order_totals.total", "customer_spend.total"])
      |> Selecto.limit(5)

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == []
    assert sql =~ "order_totals (id, total) AS ("
    assert sql =~ "customer_spend (customer_id, total) AS ("
    assert sql =~ "left join order_totals order_totals on order_totals.id = selecto_root.id"

    assert sql =~
             "left join customer_spend customer_spend on customer_spend.customer_id = selecto_root.customer_id"
  end

  test "CTE auto-join fields: :infer requires declared CTE columns" do
    assert_raise ArgumentError,
                 "Cannot infer fields for CTE 'order_totals' because it has no declared columns. Provide fields explicitly or declare CTE columns.",
                 fn ->
                   Selecto.configure(order_domain(), :mock_connection, validate: false)
                   |> Selecto.with_cte(
                     "order_totals",
                     fn ->
                       Selecto.configure(order_domain(), :mock_connection, validate: false)
                       |> Selecto.select(["id", "total"])
                     end,
                     join: [owner_key: :id, related_key: :id, fields: :infer]
                   )
                 end
  end

  test "join_subquery injects parameterized subquery and preserves params" do
    high_value_delivered_orders =
      Selecto.configure(order_domain_with_customer_join(), :mock_connection, validate: false)
      |> Selecto.select(["customer_id", "order_number", "total"])
      |> Selecto.filter(
        {:and,
         [
           {"status", "delivered"},
           {"total", {:>, 1000}}
         ]}
      )

    query =
      Selecto.configure(customer_domain(), :mock_connection, validate: false)
      |> Selecto.join_subquery(:high_value_delivered, high_value_delivered_orders,
        type: :inner,
        on: [%{left: "id", right: "customer_id"}]
      )
      |> Selecto.select([
        "name",
        "tier",
        "high_value_delivered.order_number",
        "high_value_delivered.total"
      ])
      |> Selecto.limit(5)

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == ["delivered", 1000]
    assert sql =~ "from customers selecto_root inner join ("

    assert sql =~
             "select subq_root_orders_high_value_delivered.customer_id, subq_root_orders_high_value_delivered.order_number, subq_root_orders_high_value_delivered.total"

    assert sql =~
             "where (((( subq_root_orders_high_value_delivered.status = $1 ) and ( subq_root_orders_high_value_delivered.total > $2 ))))"

    assert sql =~ ") high_value_delivered on selecto_root.id = high_value_delivered.customer_id"
  end

  test "join_subquery uses unique root aliases across multiple subqueries" do
    delivered_orders =
      Selecto.configure(order_domain_with_customer_join(), :mock_connection, validate: false)
      |> Selecto.select(["customer_id", "order_number", "total"])
      |> Selecto.filter(
        {:and,
         [
           {"status", "delivered"},
           {"total", {:>, 1000}}
         ]}
      )

    shipped_orders =
      Selecto.configure(order_domain_with_customer_join(), :mock_connection, validate: false)
      |> Selecto.select(["customer_id", "order_number", "total"])
      |> Selecto.filter(
        {:and,
         [
           {"status", "shipped"},
           {"total", {:>, 500}}
         ]}
      )

    query =
      Selecto.configure(customer_domain(), :mock_connection, validate: false)
      |> Selecto.join_subquery(:high_value_delivered, delivered_orders,
        type: :left,
        on: [%{left: "id", right: "customer_id"}]
      )
      |> Selecto.join_subquery(:high_value_shipped, shipped_orders,
        type: :left,
        on: [%{left: "id", right: "customer_id"}]
      )
      |> Selecto.select([
        "name",
        "high_value_delivered.order_number",
        "high_value_shipped.order_number"
      ])

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == ["delivered", 1000, "shipped", 500]
    assert sql =~ "from orders subq_root_orders_high_value_delivered"
    assert sql =~ "from orders subq_root_orders_high_value_shipped"

    [first_alias, second_alias] =
      Regex.scan(~r/from orders (subq_root_orders_[a-z0-9_]+)/, sql, capture: :all_but_first)
      |> List.flatten()

    assert first_alias != second_alias
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

    assert sql =~
             "select selecto_root.order_number, \"customer:alias_a\".name, \"customer:alias_b\".tier"

    assert sql =~
             "left join customers \"customer:alias_a\" on \"customer:alias_a\".id = selecto_root.customer_id"

    assert sql =~
             "left join customers \"customer:alias_b\" on \"customer:alias_b\".id = selecto_root.customer_id"
  end

  test "join_parameterize applies parameter filters in join ON clause" do
    query =
      Selecto.configure(order_domain_with_customer_join_filter(), :mock_connection,
        validate: false
      )
      |> Selecto.join_parameterize(:customer, "tier_premium", tier: "premium")
      |> Selecto.join_parameterize(:customer, "tier_standard", tier: "standard")
      |> Selecto.select([
        "order_number",
        "customer:tier_premium.name",
        "customer:tier_standard.name"
      ])
      |> Selecto.limit(3)

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == ["premium", "standard"]

    assert sql =~
             "left join customers \"customer:tier_premium\" on \"customer:tier_premium\".id = selecto_root.customer_id and \"customer:tier_premium\".tier = $1"

    assert sql =~
             "left join customers \"customer:tier_standard\" on \"customer:tier_standard\".id = selecto_root.customer_id and \"customer:tier_standard\".tier = $2"
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
      |> Selecto.filter(
        {:and,
         [
           {"active", true},
           {"reviews.rating", {:>=, 4}}
         ]}
      )
      |> Selecto.order_by({"reviews.rating", :desc})
      |> Selecto.limit(10)

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == [true, 4]

    assert sql =~
             "from products selecto_root left join reviews reviews on reviews.product_id = selecto_root.id"

    assert sql =~ "where (((( selecto_root.active = $1 ) and ( reviews.rating >= $2 ))))"
    assert sql =~ "order by reviews.rating desc"
  end

  test "subquery IN filter wraps subquery SQL in parentheses" do
    query =
      Selecto.configure(order_domain_with_customer_join(), :mock_connection, validate: false)
      |> Selecto.select(["order_number", "customer_id", "status", "total"])
      |> Selecto.filter(
        {"customer_id", {:subquery, :in, "SELECT id FROM customers WHERE tier = 'gold'", []}}
      )
      |> Selecto.order_by({"total", :desc})
      |> Selecto.limit(10)

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == []

    assert sql =~
             "where (( selecto_root.customer_id in (SELECT id FROM customers WHERE tier = 'gold') ))"
  end

  test "output format transformers handle aliases list from Selecto query metadata" do
    rows = [["Wireless Headphones", Decimal.new("79.99")]]
    columns = ["name", "price"]
    aliases = ["6ee949dc-86f5-4ed2-bac8-078786d26fd2", "4e2455d1-28c3-4aa2-8189-6804f489f46e"]

    assert {:ok, [map_row]} =
             Selecto.Output.Formats.transform({rows, columns, aliases}, :maps, [])

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

    assert {:ok, stream} =
             Selecto.Output.Formats.transform({rows, columns, aliases}, {:stream, :maps}, [])

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

    assert sql =~
             "from orders selecto_root inner join customers customer_lookup on selecto_root.customer_id = customer_lookup.id"
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

  test "star_dimension join honors owner_key and my_key" do
    query =
      Selecto.configure(order_domain_with_status_dimension_join(), :mock_connection,
        validate: false
      )
      |> Selecto.select(["ref_load_status.name", {:count, "*"}])
      |> Selecto.group_by(["ref_load_status.name"])
      |> Selecto.limit(5)

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == []

    assert sql =~
             "LEFT JOIN ref_load_statuses ref_load_status ON selecto_root.status = ref_load_status.id"

    refute sql =~ "selecto_root.ref_load_status_id = ref_load_status.id"
  end

  test "snowflake_dimension join honors owner_key and my_key" do
    snowflake_domain =
      order_domain_with_status_dimension_join()
      |> put_in([:joins, :ref_load_status, :type], :snowflake_dimension)
      |> put_in([:joins, :ref_load_status, :normalization_joins], [])

    query =
      Selecto.configure(snowflake_domain, :mock_connection, validate: false)
      |> Selecto.select(["ref_load_status.name", {:count, "*"}])
      |> Selecto.group_by(["ref_load_status.name"])
      |> Selecto.limit(5)

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == []

    assert sql =~
             "LEFT JOIN ref_load_statuses ref_load_status ON selecto_root.status = ref_load_status.id"

    refute sql =~ "selecto_root.ref_load_status_id = ref_load_status.id"
  end

  test "snowflake_dimension with normalization chain keeps custom root keys" do
    snowflake_domain =
      order_domain_with_status_dimension_join()
      |> put_in([:joins, :ref_load_status, :type], :snowflake_dimension)
      |> put_in([:joins, :ref_load_status, :normalization_joins], [
        %{table: "status_groups", key: "id", foreign_key: "status_group_id"}
      ])

    query =
      Selecto.configure(snowflake_domain, :mock_connection, validate: false)
      |> Selecto.select(["ref_load_status.name", {:count, "*"}])
      |> Selecto.group_by(["ref_load_status.name"])
      |> Selecto.limit(5)

    {sql, params} = Selecto.to_sql(query)
    sql = normalize_sql(sql)

    assert params == []

    assert sql =~
             "LEFT JOIN ref_load_statuses ref_load_status ON selecto_root.status = ref_load_status.id"

    assert sql =~
             "LEFT JOIN status_groups ref_load_status_status_groups ON ref_load_status.status_group_id = ref_load_status_status_groups.id"

    refute sql =~ "selecto_root.ref_load_status_id = ref_load_status.id"
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

    assert sql =~
             "LEFT JOIN LATERAL ( select count(*) from orders subq_root_orders where (( subq_root_orders.status = $1 )) ) AS delivered_stats ON true"

    assert sql =~ "where (( selecto_root.active = $2 ))"
  end

  test "from_ecto schema introspection supports many_to_many associations without crashing" do
    domain = Selecto.EctoAdapter.schema_to_domain(EctoAdapterProduct, joins: [:tags])

    assert domain.source.source_table == "products"
    assert Map.has_key?(domain.source.associations, :tags)
    assert Map.has_key?(domain.schemas, :ecto_adapter_tag)
  end
end
