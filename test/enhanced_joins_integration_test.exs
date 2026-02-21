defmodule Selecto.EnhancedJoinsIntegrationTest do
  use ExUnit.Case, async: true

  alias Selecto.Builder.Sql

  test "enhanced join configs do not break base SQL generation" do
    domain = %{
      name: "Customers",
      source: %{
        source_table: "customers",
        primary_key: :id,
        fields: [:id, :name],
        redact_fields: [],
        columns: %{id: %{type: :integer}, name: %{type: :string}},
        associations: %{
          recent_orders: %{queryable: :orders, field: :recent_orders, owner_key: :id, related_key: :customer_id}
        }
      },
      schemas: %{
        orders: %{
          source_table: "orders",
          primary_key: :id,
          fields: [:id, :customer_id, :total],
          redact_fields: [],
          columns: %{id: %{type: :integer}, customer_id: %{type: :integer}, total: %{type: :decimal}},
          associations: %{}
        }
      },
      joins: %{
        recent_orders: %{type: :lateral_join, lateral_query: "SELECT * FROM orders o WHERE o.customer_id = customers.id", alias: "recent"}
      }
    }

    selecto =
      Selecto.configure(domain, :mock_connection, validate: false)
      |> Selecto.select(["name"])

    {sql, _aliases, _params} = Sql.build(selecto, [])
    assert sql =~ ~r/from\s+customers/i
    assert sql =~ ~r/select/i
  end

  test "field helpers remain callable" do
    domain = %{
      name: "Users",
      source: %{
        source_table: "users",
        primary_key: :id,
        fields: [:id, :name],
        redact_fields: [],
        columns: %{id: %{type: :integer}, name: %{type: :string}},
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }

    selecto = Selecto.configure(domain, :mock_connection, validate: false)
    assert {:ok, _} = Selecto.resolve_field(selecto, "name")
    assert is_list(Selecto.field_suggestions(selecto, "na"))
    assert is_map(Selecto.available_fields(selecto))
  end
end
