defmodule Selecto.SubselectTest do
  use ExUnit.Case, async: true
  doctest Selecto.Subselect

  alias Selecto.Subselect

  # Reuse the same test domain from PivotTest
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
      name: "Event",
      joins: %{
        attendees: %{type: :left, name: "attendees"},
        orders: %{type: :left, name: "orders"}
      }
    }
  end

  def create_test_selecto do
    domain = test_domain()
    postgrex_opts = [hostname: "localhost", username: "test"]
    Selecto.configure(domain, postgrex_opts, validate: false)
  end

  describe "subselect/3" do
    test "adds subselect configuration with string field specs" do
      selecto = create_test_selecto()
      
      subselected = Subselect.subselect(selecto, ["orders[product_name]"])
      
      assert Subselect.has_subselects?(subselected)
      configs = Subselect.get_subselect_configs(subselected)
      assert length(configs) == 1
      
      [config] = configs
      assert config.fields == ["product_name"]
      assert config.target_schema == :orders
      assert config.format == :json_agg
    end

    test "parses multiple fields in string format" do
      selecto = create_test_selecto()
      
      subselected = Subselect.subselect(selecto, ["orders[product_name, quantity, price]"])
      
      configs = Subselect.get_subselect_configs(subselected)
      [config] = configs
      assert config.fields == ["product_name", "quantity", "price"]
    end

    test "supports multiple field specifications" do
      selecto = create_test_selecto()
      
      subselected = Subselect.subselect(selecto, [
        "orders[product_name]",
        "attendees[name]"
      ])
      
      configs = Subselect.get_subselect_configs(subselected)
      assert length(configs) == 2
      
      order_config = Enum.find(configs, &(&1.target_schema == :orders))
      attendee_config = Enum.find(configs, &(&1.target_schema == :attendees))
      
      assert order_config.fields == ["product_name"]
      assert attendee_config.fields == ["name"]
    end

    test "supports map-based configuration" do
      selecto = create_test_selecto()
      
      subselected = Subselect.subselect(selecto, [
        %{
          fields: ["product_name", "quantity"],
          target_schema: :orders,
          format: :array_agg,
          alias: "order_items"
        }
      ])
      
      configs = Subselect.get_subselect_configs(subselected)
      [config] = configs
      
      assert config.fields == ["product_name", "quantity"]
      assert config.target_schema == :orders
      assert config.format == :array_agg
      assert config.alias == "order_items"
    end

    test "applies default options" do
      selecto = create_test_selecto()
      
      subselected = Subselect.subselect(selecto, ["orders[product_name]"], 
        format: :string_agg,
        alias_prefix: "agg"
      )
      
      configs = Subselect.get_subselect_configs(subselected)
      [config] = configs
      
      assert config.format == :string_agg
      assert config.alias == "agg_orders"
    end

    test "validates target schema exists" do
      selecto = create_test_selecto()
      
      assert_raise ArgumentError, ~r/Target schema invalid_schema not found/, fn ->
        Subselect.subselect(selecto, ["invalid_schema[field]"])
      end
    end

    test "validates fields exist in target schema" do
      selecto = create_test_selecto()
      
      assert_raise ArgumentError, ~r/Fields.*not found in schema/, fn ->
        Subselect.subselect(selecto, ["orders[invalid_field]"])
      end
    end

    test "fails with invalid field format" do
      selecto = create_test_selecto()
      
      assert_raise ArgumentError, ~r/Invalid field format/, fn ->
        Subselect.subselect(selecto, ["invalid_format"])
      end
    end
  end

  describe "group_subselects_by_table/1" do
    test "groups subselects by target schema" do
      selecto = create_test_selecto()
      |> Subselect.subselect([
           "orders[product_name]",
           "orders[quantity]",
           "attendees[name]"
         ])
      
      grouped = Subselect.group_subselects_by_table(selecto)
      
      assert Map.has_key?(grouped, :orders)
      assert Map.has_key?(grouped, :attendees)
      assert length(grouped[:orders]) == 2
      assert length(grouped[:attendees]) == 1
    end
  end

  describe "validate_subselect_config/2" do
    test "validates valid configuration" do
      selecto = create_test_selecto()
      
      config = %{
        fields: ["product_name"],
        target_schema: :orders,
        format: :json_agg,
        alias: "orders"
      }
      
      assert :ok = Subselect.validate_subselect_config(selecto, config)
    end

    test "fails for non-existent schema" do
      selecto = create_test_selecto()
      
      config = %{
        fields: ["field"],
        target_schema: :invalid,
        format: :json_agg,
        alias: "invalid"
      }
      
      assert_raise ArgumentError, ~r/Target schema invalid not found/, fn ->
        Subselect.validate_subselect_config(selecto, config)
      end
    end
  end

  describe "resolve_join_path/2" do
    test "resolves direct relationship path" do
      selecto = create_test_selecto()
      
      {:ok, path} = Subselect.resolve_join_path(selecto, :attendees)
      
      assert path == [:attendees]
    end

    test "resolves nested relationship path" do
      selecto = create_test_selecto()
      
      {:ok, path} = Subselect.resolve_join_path(selecto, :orders)
      
      assert path == [:attendees, :orders]
    end

    test "fails for unreachable schema" do
      selecto = create_test_selecto()
      
      {:error, reason} = Subselect.resolve_join_path(selecto, :invalid)
      
      assert reason =~ "No join path found"
    end
  end

  describe "clear_subselects/1" do
    test "removes all subselect configurations" do
      selecto = create_test_selecto()
      |> Subselect.subselect(["orders[product_name]"])
      
      assert Subselect.has_subselects?(selecto)
      
      cleared = Subselect.clear_subselects(selecto)
      
      refute Subselect.has_subselects?(cleared)
      assert Subselect.get_subselect_configs(cleared) == []
    end
  end

  describe "has_subselects?/1" do
    test "returns false for queries without subselects" do
      selecto = create_test_selecto()
      
      refute Subselect.has_subselects?(selecto)
    end

    test "returns true for queries with subselects" do
      selecto = create_test_selecto()
      |> Subselect.subselect(["orders[product_name]"])
      
      assert Subselect.has_subselects?(selecto)
    end
  end

  describe "integration with regular selects" do
    test "subselects work alongside regular field selections" do
      selecto = create_test_selecto()
      |> Selecto.select(["name", "date"])
      |> Subselect.subselect(["orders[product_name]"])
      
      # Both regular and subselect fields should be configured
      assert length(selecto.set.selected) == 2
      assert Subselect.has_subselects?(selecto)
      
      configs = Subselect.get_subselect_configs(selecto)
      assert length(configs) == 1
    end
  end
end