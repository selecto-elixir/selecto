defmodule Selecto.SubselectIntegrationTest do
  use ExUnit.Case, async: true
  doctest Selecto.Builder.Subselect

  alias Selecto.Builder.Subselect
  alias Selecto.SQL.Params

  def test_domain do
    %{
      source: %{
        source_table: "attendees",
        primary_key: :attendee_id,
        fields: [:attendee_id, :event_id, :name, :email],
        redact_fields: [],
        columns: %{
          attendee_id: %{type: :integer},
          event_id: %{type: :integer},
          name: %{type: :string},
          email: %{type: :string}
        },
        associations: %{
          orders: %{
            queryable: :orders,
            field: :orders,
            owner_key: :attendee_id,
            related_key: :attendee_id
          }
        }
      },
      schemas: %{
        orders: %{
          source_table: "orders",
          primary_key: :order_id,
          fields: [:order_id, :attendee_id, :product_name, :quantity, :price, :metadata],
          redact_fields: [],
          columns: %{
            order_id: %{type: :integer},
            attendee_id: %{type: :integer},
            product_name: %{type: :string},
            quantity: %{type: :integer},
            price: %{type: :decimal},
            metadata: %{
              type: :jsonb,
              schema: %{
                "priority" => %{type: :string},
                "warehouse" => %{
                  type: :object,
                  schema: %{"zone" => %{type: :string}}
                }
              }
            }
          },
          associations: %{}
        }
      },
      name: "Attendee",
      joins: %{
        orders: %{type: :left, name: "orders"}
      }
    }
  end

  def create_test_selecto do
    domain = test_domain()
    postgrex_opts = [hostname: "localhost", username: "test"]
    Selecto.configure(domain, postgrex_opts, validate: false)
  end

  def create_mssql_test_selecto do
    domain = test_domain()
    Selecto.configure(domain, :mock_connection, adapter: SelectoDBMSSQL.Adapter, validate: false)
  end

  describe "build_subselect_clauses/1" do
    test "builds JSON aggregation subselect" do
      selecto =
        create_test_selecto()
        |> Selecto.subselect([
          %{
            fields: ["product_name", "quantity"],
            target_schema: :orders,
            format: :json_agg,
            alias: "order_items"
          }
        ])

      {clauses, params} = Subselect.build_subselect_clauses(selecto)

      {clause_sql, finalized_params} = Params.finalize(clauses)

      assert clause_sql =~ ~r/json_agg/i
      assert clause_sql =~ ~r/json_build_object/i
      assert clause_sql =~ ~r/as\s+"order_items"/i
      assert clause_sql =~ ~r/from\s+orders/i
      assert clause_sql =~ ~r/where/i
      assert params == finalized_params
    end

    test "builds MSSQL JSON aggregation subselect without postgres JSON functions" do
      selecto =
        create_mssql_test_selecto()
        |> Selecto.subselect([
          %{
            fields: ["product_name", "quantity"],
            target_schema: :orders,
            format: :json_agg,
            alias: "order_items"
          }
        ])

      {clauses, params} = Subselect.build_subselect_clauses(selecto)
      {clause_sql, finalized_params} = Params.finalize(clauses)

      assert clause_sql =~ ~r/for json path/i
      refute clause_sql =~ ~r/json_build_object/i
      refute clause_sql =~ ~r/json_agg/i
      assert clause_sql =~ ~r/as\s+\[order_items\]/i
      assert params == finalized_params
    end

    test "builds MSSQL JSON aggregation subselect with nested json path fields" do
      selecto =
        create_mssql_test_selecto()
        |> Selecto.subselect([
          %{
            fields: ["product_name", "metadata.priority", "metadata.warehouse.zone"],
            target_schema: :orders,
            format: :json_agg,
            alias: "order_items"
          }
        ])

      {clauses, params} = Subselect.build_subselect_clauses(selecto)
      {clause_sql, finalized_params} = Params.finalize(clauses)

      assert clause_sql =~ ~r/for json path/i
      assert clause_sql =~ "JSON_VALUE(sub_orders.metadata, '$.priority') AS [priority]"

      assert clause_sql =~
               "JSON_VALUE(sub_orders.metadata, '$.warehouse.zone') AS [zone]"

      assert params == finalized_params
    end

    test "builds array aggregation subselect" do
      selecto =
        create_test_selecto()
        |> Selecto.subselect([
          %{
            fields: ["product_name"],
            target_schema: :orders,
            format: :array_agg,
            alias: "product_names"
          }
        ])

      {clauses, _params} = Subselect.build_subselect_clauses(selecto)
      {clause_sql, _finalized_params} = Params.finalize(clauses)

      assert clause_sql =~ ~r/array_agg/i
      assert clause_sql =~ ~r/as\s+"product_names"/i
    end

    test "builds string aggregation subselect" do
      selecto =
        create_test_selecto()
        |> Selecto.subselect([
          %{
            fields: ["product_name"],
            target_schema: :orders,
            format: :string_agg,
            alias: "product_list",
            separator: "; "
          }
        ])

      {clauses, params} = Subselect.build_subselect_clauses(selecto)
      {clause_sql, _finalized_params} = Params.finalize(clauses)

      assert clause_sql =~ ~r/string_agg/i
      assert clause_sql =~ ~r/as\s+"product_list"/i
      assert "; " in params
    end

    test "builds count subselect" do
      selecto =
        create_test_selecto()
        |> Selecto.subselect([
          %{
            # Field doesn't matter for count
            fields: ["product_name"],
            target_schema: :orders,
            format: :count,
            alias: "order_count"
          }
        ])

      {clauses, _params} = Subselect.build_subselect_clauses(selecto)
      {clause_sql, _finalized_params} = Params.finalize(clauses)

      assert clause_sql =~ ~r/count/i
      assert clause_sql =~ ~r/as\s+"order_count"/i
    end

    test "builds multiple subselects" do
      selecto =
        create_test_selecto()
        |> Selecto.subselect([
          %{
            fields: ["product_name"],
            target_schema: :orders,
            format: :json_agg,
            alias: "products"
          },
          %{
            fields: ["quantity"],
            target_schema: :orders,
            format: :array_agg,
            alias: "quantities"
          }
        ])

      {clauses, _params} = Subselect.build_subselect_clauses(selecto)
      {clauses_sql, _finalized_params} = Params.finalize(clauses)

      assert clauses_sql =~ ~r/json_agg/i
      assert clauses_sql =~ ~r/array_agg/i
      assert clauses_sql =~ ~r/as\s+"products"/i
      assert clauses_sql =~ ~r/as\s+"quantities"/i
    end
  end

  describe "build_single_subselect/2" do
    test "creates proper correlation condition" do
      selecto = create_test_selecto()

      config = %{
        fields: ["product_name"],
        target_schema: :orders,
        format: :json_agg,
        alias: "products",
        order_by: [],
        filters: []
      }

      {subselect, _params} = Subselect.build_single_subselect(selecto, config)

      subselect_sql = IO.iodata_to_binary(subselect)

      # Should have correlation condition
      assert subselect_sql =~ "WHERE"
      assert subselect_sql =~ "sub_orders"
      assert subselect_sql =~ "= selecto_root."
    end

    test "includes ORDER BY when specified" do
      selecto = create_test_selecto()

      config = %{
        fields: ["product_name"],
        target_schema: :orders,
        format: :json_agg,
        alias: "products",
        order_by: [{:desc, :product_name}],
        filters: []
      }

      {subselect, _params} = Subselect.build_single_subselect(selecto, config)

      subselect_sql = IO.iodata_to_binary(subselect)

      refute subselect_sql =~ ~r/order by/i
    end

    test "includes additional filters when specified" do
      selecto = create_test_selecto()

      config = %{
        fields: ["product_name"],
        target_schema: :orders,
        format: :json_agg,
        alias: "products",
        order_by: [],
        filters: [{"quantity", {:gt, 1}}]
      }

      {subselect, params} = Subselect.build_single_subselect(selecto, config)
      {subselect_sql, _finalized_params} = Params.finalize(subselect)

      # Additional filter joined with correlation
      assert subselect_sql =~ "AND"
      assert {:gt, 1} in params
    end
  end

  describe "resolve_join_condition/2" do
    test "resolves simple join condition" do
      selecto = create_test_selecto()

      {:ok, {source_field, target_field}} = Subselect.resolve_join_condition(selecto, :orders)

      assert is_binary(source_field)
      assert is_binary(target_field)
    end
  end

  describe "full SQL generation integration" do
    test "generates complete query with subselects" do
      selecto =
        create_test_selecto()
        |> Selecto.select(["name", "email"])
        |> Selecto.subselect(["orders.product_name, quantity"])
        |> Selecto.filter([{"event_id", 123}])

      {sql, _aliases, params} = Selecto.gen_sql(selecto, [])

      # Should have main SELECT fields and subselects
      assert sql =~ ~r/select/i
      assert sql =~ "name"
      assert sql =~ "email"
      assert sql =~ "json_agg"
      assert sql =~ "json_build_object"

      # Should have main FROM clause
      assert sql =~ ~r/from\s+attendees/i

      # Should have main WHERE clause for filters
      assert sql =~ ~r/where/i

      # Should have correlated subquery
      assert sql =~ ~r/from\s+orders/i

      # Parameters should include filter values
      assert 123 in params
    end

    test "handles subselects with string field syntax" do
      selecto =
        create_test_selecto()
        |> Selecto.select(["name"])
        |> Selecto.subselect(["orders.product_name"])

      {sql, _aliases, _params} = Selecto.gen_sql(selecto, [])

      assert sql =~ ~r/select/i
      assert sql =~ "name"
      assert sql =~ "json_agg"
      assert sql =~ ~r/from\s+attendees/i
    end

    test "handles multiple subselects with different formats" do
      selecto =
        create_test_selecto()
        |> Selecto.select(["name"])
        |> Selecto.subselect([
          %{
            fields: ["product_name"],
            target_schema: :orders,
            format: :json_agg,
            alias: "products"
          },
          %{
            fields: ["quantity"],
            target_schema: :orders,
            format: :count,
            alias: "order_count"
          }
        ])

      {sql, _aliases, _params} = Selecto.gen_sql(selecto, [])

      assert sql =~ "json_agg"
      assert sql =~ "count"
      assert sql =~ ~r/as\s+"products"/i
      assert sql =~ ~r/as\s+"order_count"/i
    end

    test "combines with filtering and ordering" do
      selecto =
        create_test_selecto()
        |> Selecto.select(["name"])
        |> Selecto.subselect(["orders.product_name"])
        |> Selecto.filter([{"event_id", 123}])
        |> Selecto.order_by(["name"])

      {sql, _aliases, params} = Selecto.gen_sql(selecto, [])

      assert sql =~ ~r/select/i
      assert sql =~ "json_agg"
      assert sql =~ ~r/where/i
      assert sql =~ ~r/order\s+by/i
      assert 123 in params
    end

    test "works without regular SELECT fields" do
      selecto =
        create_test_selecto()
        |> Selecto.subselect(["orders.product_name"])

      {sql, _aliases, _params} = Selecto.gen_sql(selecto, [])

      # Should still generate valid SQL with just subselects
      assert sql =~ ~r/select/i
      assert sql =~ "json_agg"
    end
  end

  describe "error handling in SQL generation" do
    test "handles empty subselect configurations gracefully" do
      selecto =
        create_test_selecto()
        |> Selecto.select(["name"])

      # Should not have any subselects
      {clauses, params} = Subselect.build_subselect_clauses(selecto)

      assert clauses == []
      assert params == []
    end
  end
end
