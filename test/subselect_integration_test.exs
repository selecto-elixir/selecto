defmodule Selecto.SubselectIntegrationTest do
  use ExUnit.Case, async: true
  doctest Selecto.Builder.Subselect

  alias Selecto.Builder.Subselect

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
          fields: [:order_id, :attendee_id, :product_name, :quantity, :price],
          redact_fields: [],
          columns: %{
            order_id: %{type: :integer},
            attendee_id: %{type: :integer},
            product_name: %{type: :string},
            quantity: %{type: :integer},
            price: %{type: :decimal}
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

  describe "build_subselect_clauses/1" do
    test "builds JSON aggregation subselect" do
      selecto = create_test_selecto()
      |> Selecto.subselect([
           %{
             fields: ["product_name", "quantity"],
             target_schema: :orders,
             format: :json_agg,
             alias: "order_items"
           }
         ])
      
      {clauses, params} = Subselect.build_subselect_clauses(selecto)
      
      assert length(clauses) == 1
      [clause] = clauses
      
      # Convert to string for easier testing
      clause_sql = IO.iodata_to_binary(clause)
      
      assert clause_sql =~ "json_agg"
      assert clause_sql =~ "json_build_object"
      assert clause_sql =~ "AS \"order_items\""
      assert clause_sql =~ "FROM orders"
      assert clause_sql =~ "WHERE"
    end

    test "builds array aggregation subselect" do
      selecto = create_test_selecto()
      |> Selecto.subselect([
           %{
             fields: ["product_name"],
             target_schema: :orders,
             format: :array_agg,
             alias: "product_names"
           }
         ])
      
      {clauses, _params} = Subselect.build_subselect_clauses(selecto)
      [clause] = clauses
      
      clause_sql = IO.iodata_to_binary(clause)
      
      assert clause_sql =~ "array_agg"
      assert clause_sql =~ "AS \"product_names\""
    end

    test "builds string aggregation subselect" do
      selecto = create_test_selecto()
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
      [clause] = clauses
      
      clause_sql = IO.iodata_to_binary(clause)
      
      assert clause_sql =~ "string_agg"
      assert clause_sql =~ "AS \"product_list\""
      assert "; " in params
    end

    test "builds count subselect" do
      selecto = create_test_selecto()
      |> Selecto.subselect([
           %{
             fields: ["product_name"],  # Field doesn't matter for count
             target_schema: :orders,
             format: :count,
             alias: "order_count"
           }
         ])
      
      {clauses, _params} = Subselect.build_subselect_clauses(selecto)
      [clause] = clauses
      
      clause_sql = IO.iodata_to_binary(clause)
      
      assert clause_sql =~ "count"
      assert clause_sql =~ "AS \"order_count\""
    end

    test "builds multiple subselects" do
      selecto = create_test_selecto()
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
      
      assert length(clauses) == 2
      
      clauses_sql = Enum.map(clauses, &IO.iodata_to_binary/1)
      
      assert Enum.any?(clauses_sql, &(&1 =~ "json_agg"))
      assert Enum.any?(clauses_sql, &(&1 =~ "array_agg"))
      assert Enum.any?(clauses_sql, &(&1 =~ "AS \"products\""))
      assert Enum.any?(clauses_sql, &(&1 =~ "AS \"quantities\""))
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
      assert subselect_sql =~ "= s."  # Main query alias
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
      
      assert subselect_sql =~ "ORDER BY"
      assert subselect_sql =~ "DESC"
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
      
      subselect_sql = IO.iodata_to_binary(subselect)
      
      assert subselect_sql =~ "AND"  # Additional filter joined with correlation
      assert 1 in params  # Filter parameter
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
      selecto = create_test_selecto()
      |> Selecto.select(["name", "email"])
      |> Selecto.subselect(["orders[product_name, quantity]"])
      |> Selecto.filter([{"event_id", 123}])
      
      {sql, _aliases, params} = Selecto.gen_sql(selecto, [])
      
      # Should have main SELECT fields and subselects
      assert sql =~ "SELECT"
      assert sql =~ "name"
      assert sql =~ "email"
      assert sql =~ "json_agg"
      assert sql =~ "json_build_object"
      
      # Should have main FROM clause
      assert sql =~ "FROM attendees"
      
      # Should have main WHERE clause for filters
      assert sql =~ "WHERE"
      
      # Should have correlated subquery
      assert sql =~ "FROM orders"
      
      # Parameters should include filter values
      assert 123 in params
    end

    test "handles subselects with string field syntax" do
      selecto = create_test_selecto()
      |> Selecto.select(["name"])
      |> Selecto.subselect(["orders[product_name]"])
      
      {sql, _aliases, _params} = Selecto.gen_sql(selecto, [])
      
      assert sql =~ "SELECT"
      assert sql =~ "name"
      assert sql =~ "json_agg"
      assert sql =~ "FROM attendees"
    end

    test "handles multiple subselects with different formats" do
      selecto = create_test_selecto()
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
      assert sql =~ "AS products"
      assert sql =~ "AS order_count"
    end

    test "combines with filtering and ordering" do
      selecto = create_test_selecto()
      |> Selecto.select(["name"])
      |> Selecto.subselect(["orders[product_name]"])
      |> Selecto.filter([{"event_id", 123}])
      |> Selecto.order_by(["name"])
      
      {sql, _aliases, params} = Selecto.gen_sql(selecto, [])
      
      assert sql =~ "SELECT"
      assert sql =~ "json_agg"
      assert sql =~ "WHERE"
      assert sql =~ "ORDER BY"
      assert 123 in params
    end

    test "works without regular SELECT fields" do
      selecto = create_test_selecto()
      |> Selecto.subselect(["orders[product_name]"])
      
      {sql, _aliases, _params} = Selecto.gen_sql(selecto, [])
      
      # Should still generate valid SQL with just subselects
      assert sql =~ "SELECT"
      assert sql =~ "json_agg"
      assert sql =~ "FROM attendees"
    end
  end

  describe "error handling in SQL generation" do
    test "handles empty subselect configurations gracefully" do
      selecto = create_test_selecto()
      |> Selecto.select(["name"])
      
      # Should not have any subselects
      {clauses, params} = Subselect.build_subselect_clauses(selecto)
      
      assert clauses == []
      assert params == []
    end
  end
end