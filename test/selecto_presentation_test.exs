defmodule Selecto.PresentationTest do
  use ExUnit.Case, async: true

  alias Selecto.Presentation

  test "normalizes explicit measurement presentation metadata" do
    column = %{
      type: :decimal,
      presentation: %{
        "semantic_type" => "measurement",
        "quantity" => "temperature",
        "canonical_unit" => "c",
        "available_units" => ["f", "kelvin"],
        "format" => %{"maximum_fraction_digits" => 1}
      }
    }

    assert %{presentation: presentation} = Presentation.normalize_column(column)
    assert presentation.semantic_type == :measurement
    assert presentation.quantity == :temperature
    assert presentation.canonical_unit == :celsius
    assert presentation.default_unit == :celsius
    assert presentation.available_units == [:fahrenheit, :kelvin, :celsius]
    assert presentation.format == %{"maximum_fraction_digits" => 1}
  end

  test "derives temporal presentation from epoch-backed datetime columns" do
    column = %{
      type: :integer,
      presentation_type: :utc_datetime,
      datetime_storage: :unix_ms
    }

    assert Presentation.temporal?(column)
    assert Presentation.temporal_kind(column) == :instant

    assert Presentation.presentation(column) == %{
             semantic_type: :temporal,
             temporal_kind: :instant,
             storage_timezone: "Etc/UTC",
             display_timezone: :viewer
           }
  end
end
