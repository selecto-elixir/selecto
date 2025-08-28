defmodule Selecto.PivotTest do
  use ExUnit.Case, async: true
  doctest Selecto.Pivot

  alias Selecto.Pivot

  # Test domain configuration for event/attendee/order scenario
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

  describe "pivot/3" do
    test "adds pivot configuration to selecto struct" do
      selecto = create_test_selecto()
      
      pivoted = Pivot.pivot(selecto, :orders)
      
      assert Pivot.has_pivot?(pivoted)
      pivot_config = Pivot.get_pivot_config(pivoted)
      assert pivot_config.target_schema == :orders
      assert pivot_config.preserve_filters == true
      assert pivot_config.subquery_strategy == :in
    end

    test "calculates join path correctly for direct relationship" do
      selecto = create_test_selecto()
      
      {:ok, path} = Pivot.calculate_join_path(selecto, :attendees)
      
      assert path == [:attendees]
    end

    test "calculates join path correctly for nested relationship" do
      selecto = create_test_selecto()
      
      {:ok, path} = Pivot.calculate_join_path(selecto, :orders)
      
      assert path == [:attendees, :orders]
    end

    test "fails for non-existent target schema" do
      selecto = create_test_selecto()
      
      assert_raise ArgumentError, ~r/Invalid pivot configuration/, fn ->
        Pivot.pivot(selecto, :non_existent)
      end
    end

    test "supports custom options" do
      selecto = create_test_selecto()
      
      pivoted = Pivot.pivot(selecto, :orders, 
        preserve_filters: false, 
        subquery_strategy: :exists
      )
      
      pivot_config = Pivot.get_pivot_config(pivoted)
      assert pivot_config.preserve_filters == false
      assert pivot_config.subquery_strategy == :exists
    end
  end

  describe "validate_pivot_path/2" do
    test "validates existing join path" do
      selecto = create_test_selecto()
      join_path = [:attendees, :orders]
      
      assert :ok = Pivot.validate_pivot_path(selecto, join_path)
    end

    test "fails for invalid join path" do
      selecto = create_test_selecto()
      join_path = [:invalid_join]
      
      assert {:error, _reason} = Pivot.validate_pivot_path(selecto, join_path)
    end
  end

  describe "reset_pivot/1" do
    test "removes pivot configuration" do
      selecto = create_test_selecto()
      
      pivoted = Pivot.pivot(selecto, :orders)
      assert Pivot.has_pivot?(pivoted)
      
      reset = Pivot.reset_pivot(pivoted)
      refute Pivot.has_pivot?(reset)
      assert Pivot.get_pivot_config(reset) == nil
    end
  end

  describe "has_pivot?/1" do
    test "returns false for non-pivoted query" do
      selecto = create_test_selecto()
      
      refute Pivot.has_pivot?(selecto)
    end

    test "returns true for pivoted query" do
      selecto = create_test_selecto()
      pivoted = Pivot.pivot(selecto, :orders)
      
      assert Pivot.has_pivot?(pivoted)
    end
  end

  describe "integration with filtering" do
    test "pivot preserves existing filters in configuration" do
      selecto = create_test_selecto()
      |> Selecto.filter([{"event_id", 123}])
      |> Pivot.pivot(:orders)
      
      pivot_config = Pivot.get_pivot_config(selecto)
      assert pivot_config.preserve_filters == true
      
      # Original filters should still be in the selecto struct
      assert selecto.set.filtered == [{"event_id", 123}]
    end
  end
end