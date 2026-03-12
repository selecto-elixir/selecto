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

  test "joined column label uses join config name when passed as domain context" do
    source = %{
      columns: %{
        co_name: %{type: :string}
      }
    }

    # This mirrors the join recursion path where configure_columns/4 receives
    # the join config itself, not the top-level domain map.
    join_config_domain = %{
      name: "Payload",
      columns: %{}
    }

    columns = Column.configure_columns(:load_det, [:co_name], source, join_config_domain)

    assert columns["load_det.co_name"].name == "Payload: Co name"
  end
end
