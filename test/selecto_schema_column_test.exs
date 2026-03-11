defmodule Selecto.Schema.ColumnTest do
  use ExUnit.Case, async: true

  alias Selecto.Schema.Column

  test "joined column label uses join config :name" do
    source = %{
      columns: %{
        co_name: %{type: :string}
      }
    }

    domain = %{
      name: "Shipment",
      joins: %{
        payload: %{name: "Payload"}
      },
      columns: %{}
    }

    columns = Column.configure_columns(:payload, [:co_name], source, domain)

    assert columns["payload.co_name"].name == "Payload: Co name"
  end

  test "joined column label uses join config name across atom/string join ids" do
    source = %{
      columns: %{
        co_name: %{type: :string}
      }
    }

    domain = %{
      name: "Shipment",
      joins: %{
        payload: %{"name" => "Payload"}
      },
      columns: %{}
    }

    columns = Column.configure_columns("payload", ["co_name"], source, domain)

    assert columns["payload.co_name"].name == "Payload: Co name"
  end

  test "joined column label prefers assigned name atoms over humanize fallback" do
    source = %{
      columns: %{
        co_name: %{type: :string}
      }
    }

    domain = %{
      name: "Shipment",
      joins: %{
        payload: %{name: :payload_label}
      },
      columns: %{}
    }

    columns = Column.configure_columns(:payload, [:co_name], source, domain)

    assert columns["payload.co_name"].name == "payload_label: Co name"
  end
end
