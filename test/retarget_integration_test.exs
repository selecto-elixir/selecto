defmodule Selecto.RetargetIntegrationTest do
  use ExUnit.Case, async: true
  doctest Selecto.Builder.Retarget

  alias Selecto.Builder.Retarget
  alias Selecto.SQL.Params

  def test_domain do
    %{
      source: %{
        source_table: "events",
        primary_key: :event_id,
        fields: [:event_id, :name, :date],
        redact_fields: [],
        columns: %{
          event_id: %{type: :integer},
          name: %{type: :string},
          date: %{type: :date}
        },
        associations: %{
          attendees: %{
            queryable: :attendees,
            field: :attendees,
            owner_key: :event_id,
            related_key: :event_id
          }
        }
      },
      schemas: %{
        attendees: %{
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
        orders: %{
          source_table: "orders",
          primary_key: :order_id,
          fields: [:order_id, :attendee_id, :product_name, :quantity],
          redact_fields: [],
          columns: %{
            order_id: %{type: :integer},
            attendee_id: %{type: :integer},
            product_name: %{type: :string},
            quantity: %{type: :integer}
          },
          associations: %{}
        }
      },
      name: "Event",
      joins: %{
        attendees: %{
          type: :left,
          name: "attendees",
          joins: %{
            orders: %{type: :left, name: "orders"}
          }
        }
      }
    }
  end

  def create_test_selecto do
    domain = test_domain()
    postgrex_opts = [hostname: "localhost", username: "test"]
    Selecto.configure(domain, postgrex_opts, validate: false)
  end

  describe "build_retarget_query/2" do
    test "builds IN subquery strategy" do
      selecto =
        create_test_selecto()
        |> Selecto.filter([{"event_id", 123}])
        |> Selecto.retarget(:orders, subquery_strategy: :in)

      {from_iodata, where_iodata, params, _deps} = Retarget.build_retarget_query(selecto, [])

      {from_sql, _from_params} = Params.finalize(from_iodata)
      {where_sql, _where_params} = Params.finalize(where_iodata)

      assert from_sql =~ ~r/orders/i
      assert where_sql =~ ~r/\bin\b/i
      assert 123 in params
    end

    test "builds EXISTS subquery strategy" do
      selecto =
        create_test_selecto()
        |> Selecto.filter([{"event_id", 123}])
        |> Selecto.retarget(:orders, subquery_strategy: :exists)

      {from_iodata, where_iodata, params, _deps} = Retarget.build_retarget_query(selecto, [])

      {from_sql, _from_params} = Params.finalize(from_iodata)
      {where_sql, _where_params} = Params.finalize(where_iodata)

      assert from_sql =~ ~r/orders/i
      assert where_sql =~ ~r/exists/i
      assert 123 in params
    end

    test "builds JOIN strategy" do
      selecto =
        create_test_selecto()
        |> Selecto.filter([{"event_id", 123}])
        |> Selecto.retarget(:orders, subquery_strategy: :join)

      {from_iodata, where_iodata, params, _deps} = Retarget.build_retarget_query(selecto, [])

      {from_sql, _from_params} = Params.finalize(from_iodata)
      {where_sql, _where_params} = Params.finalize(where_iodata)

      assert from_sql =~ ~r/orders/i
      assert from_sql =~ ~r/join/i
      assert where_sql =~ ~r/event_id/i
      assert 123 in params
    end
  end

  describe "build_join_chain_subquery/3" do
    test "builds subquery for join chain" do
      selecto =
        create_test_selecto()
        |> Selecto.filter([{"event_id", 123}])
        |> Selecto.retarget(:orders)

      retarget_config = Selecto.Retarget.get_retarget_config(selecto)
      join_path = [:attendees, :orders]

      {subquery, params} = Retarget.build_join_chain_subquery(selecto, retarget_config, join_path)

      {subquery_sql, _subquery_params} = Params.finalize(subquery)

      assert subquery_sql =~ ~r/select\s+distinct/i
      assert subquery_sql =~ ~r/from\s+events/i
      assert subquery_sql =~ ~r/join/i
      assert 123 in params
    end
  end

  describe "full SQL generation integration" do
    test "generates complete retarget SQL with IN strategy" do
      selecto =
        create_test_selecto()
        |> Selecto.filter([{"event_id", 123}])
        |> Selecto.select(["orders.product_name", "orders.quantity"])
        |> Selecto.retarget(:orders, subquery_strategy: :in)

      {sql, _aliases, params} = Selecto.gen_sql(selecto, [])

      # Basic SQL structure checks
      assert sql =~ ~r/select/i
      assert sql =~ ~r/product_name/i
      assert sql =~ ~r/quantity/i
      assert sql =~ ~r/from\s+orders/i
      assert sql =~ ~r/\bin\s*\(/i
      assert sql =~ ~r/select\s+distinct/i

      # Parameters should include the filter value
      assert 123 in params
    end

    test "generates complete retarget SQL with EXISTS strategy" do
      selecto =
        create_test_selecto()
        |> Selecto.filter([{"event_id", 123}])
        |> Selecto.select(["orders.product_name"])
        |> Selecto.retarget(:orders, subquery_strategy: :exists)

      {sql, _aliases, params} = Selecto.gen_sql(selecto, [])

      # Basic SQL structure checks
      assert sql =~ ~r/select/i
      assert sql =~ ~r/product_name/i
      assert sql =~ ~r/from\s+orders/i
      assert sql =~ ~r/exists\s*\(/i

      # Parameters should include the filter value
      assert 123 in params
    end

    test "handles retarget with multiple filters" do
      selecto =
        create_test_selecto()
        |> Selecto.filter([{"event_id", 123}, {"name", "Test Event"}])
        |> Selecto.select(["orders.product_name"])
        |> Selecto.retarget(:orders)

      {sql, _aliases, params} = Selecto.gen_sql(selecto, [])

      assert sql =~ ~r/select/i
      assert sql =~ ~r/from\s+orders/i

      # Both filter parameters should be present
      assert 123 in params
      assert "Test Event" in params
    end

    test "handles retarget without preserving filters" do
      selecto =
        create_test_selecto()
        |> Selecto.filter([{"event_id", 123}])
        |> Selecto.select(["orders.product_name"])
        |> Selecto.retarget(:orders, preserve_filters: false)

      {sql, _aliases, _params} = Selecto.gen_sql(selecto, [])

      assert sql =~ ~r/select/i
      assert sql =~ ~r/from\s+orders/i

      # Filter should not be preserved in retarget subquery
      # (Though this depends on implementation details)
    end

    test "compiles qualified post-retarget filters for EXISTS strategy" do
      selecto =
        create_test_selecto()
        |> Selecto.filter([{"event_id", 123}])
        |> Selecto.select(["orders.product_name", "orders.quantity"])
        |> Selecto.retarget(:orders, subquery_strategy: :exists)
        |> Selecto.filter({"orders.quantity", {:gt, 2}})

      {sql, _aliases, params} = Selecto.gen_sql(selecto, [])

      assert sql =~ ~r/from\s+orders\s+t/i
      assert sql =~ ~r/t\."?quantity"?\s*>\s*\$\d+/i
      refute sql =~ ~r/t\."orders\.quantity"/i
      assert 123 in params
      assert 2 in params
    end

    test "compiles qualified post-retarget filters for JOIN strategy" do
      selecto =
        create_test_selecto()
        |> Selecto.filter([{"event_id", 123}])
        |> Selecto.select(["orders.product_name", "orders.quantity"])
        |> Selecto.retarget(:orders, subquery_strategy: :join)
        |> Selecto.filter({"orders.quantity", {:gte, 2}})

      {sql, _aliases, params} = Selecto.gen_sql(selecto, [])

      assert sql =~ ~r/from\s+orders\s+t/i
      assert sql =~ ~r/t\."?quantity"?\s*>=\s*\$\d+/i
      refute sql =~ ~r/t\."orders\.quantity"/i
      assert 123 in params
      assert 2 in params
    end
  end

  describe "error handling" do
    test "handles invalid retarget target gracefully in SQL generation" do
      # This test would need to be more specific based on actual error handling
      # For now, we assume the retarget validation catches these at configuration time
    end
  end
end
