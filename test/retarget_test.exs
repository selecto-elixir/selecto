defmodule Selecto.RetargetTest do
  use ExUnit.Case, async: true
  doctest Selecto.Retarget

  alias Selecto.Retarget

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

  describe "retarget/3" do
    test "adds retarget configuration to selecto struct" do
      selecto = create_test_selecto()

      retargeted = Retarget.retarget(selecto, :orders)

      assert Retarget.has_retarget?(retargeted)
      retarget_config = Retarget.get_retarget_config(retargeted)
      assert retarget_config.target_schema == :orders
      assert retarget_config.preserve_filters == true
      assert retarget_config.subquery_strategy == :in
    end

    test "calculates join path correctly for direct relationship" do
      selecto = create_test_selecto()

      {:ok, path} = Retarget.calculate_join_path(selecto, :attendees)

      assert path == [:attendees]
    end

    test "calculates join path correctly for nested relationship" do
      selecto = create_test_selecto()

      {:ok, path} = Retarget.calculate_join_path(selecto, :orders)

      assert path == [:attendees, :orders]
    end

    test "fails for non-existent target schema" do
      selecto = create_test_selecto()

      assert_raise ArgumentError, ~r/Invalid retarget configuration/, fn ->
        Retarget.retarget(selecto, :non_existent)
      end
    end

    test "supports custom options" do
      selecto = create_test_selecto()

      retargeted =
        Retarget.retarget(selecto, :orders,
          preserve_filters: false,
          subquery_strategy: :exists
        )

      retarget_config = Retarget.get_retarget_config(retargeted)
      assert retarget_config.preserve_filters == false
      assert retarget_config.subquery_strategy == :exists
    end
  end

  describe "validate_retarget_path/2" do
    test "validates existing join path" do
      selecto = create_test_selecto()
      join_path = [:attendees, :orders]

      assert :ok = Retarget.validate_retarget_path(selecto, join_path)
    end

    test "fails for invalid join path" do
      selecto = create_test_selecto()
      join_path = [:invalid_join]

      assert {:error, _reason} = Retarget.validate_retarget_path(selecto, join_path)
    end
  end

  describe "reset_retarget/1" do
    test "removes retarget configuration" do
      selecto = create_test_selecto()

      retargeted = Retarget.retarget(selecto, :orders)
      assert Retarget.has_retarget?(retargeted)

      reset = Retarget.reset_retarget(retargeted)
      refute Retarget.has_retarget?(reset)
      assert Retarget.get_retarget_config(reset) == nil
    end
  end

  describe "has_retarget?/1" do
    test "returns false for non-retargeted query" do
      selecto = create_test_selecto()

      refute Retarget.has_retarget?(selecto)
    end

    test "returns true for retargeted query" do
      selecto = create_test_selecto()
      retargeted = Retarget.retarget(selecto, :orders)

      assert Retarget.has_retarget?(retargeted)
    end
  end

  describe "integration with filtering" do
    test "retarget preserves existing filters in configuration" do
      selecto =
        create_test_selecto()
        |> Selecto.filter([{"event_id", 123}])
        |> Retarget.retarget(:orders)

      retarget_config = Retarget.get_retarget_config(selecto)
      assert retarget_config.preserve_filters == true

      # Original filters should still be in the selecto struct
      assert selecto.set.filtered == [{"event_id", 123}]
    end
  end
end
