defmodule Selecto.Output.FormatsTest do
  use ExUnit.Case, async: true

  alias Selecto.Output.Formats

  describe "transform/3" do
    setup do
      # Sample test data
      rows = [
        ["John Doe", "john@example.com", 25, true],
        ["Jane Smith", "jane@example.com", 30, false]
      ]
      columns = ["name", "email", "age", "active"]
      aliases = %{
        "name" => "full_name",
        "email" => "email_address",
        "age" => "user_age",
        "active" => "is_active"
      }

      {:ok, rows: rows, columns: columns, aliases: aliases}
    end

    test "transforms to raw format (no transformation)", %{rows: rows, columns: columns, aliases: aliases} do
      {:ok, result} = Formats.transform({rows, columns, aliases}, :raw)
      assert result == {rows, columns, aliases}
    end

    test "transforms to maps with string keys", %{rows: rows, columns: columns, aliases: aliases} do
      {:ok, maps} = Formats.transform({rows, columns, aliases}, :maps)

      assert length(maps) == 2
      assert List.first(maps) == %{
        "full_name" => "John Doe",
        "email_address" => "john@example.com",
        "user_age" => 25,
        "is_active" => true
      }
    end

    test "transforms to maps with atom keys", %{rows: rows, columns: columns, aliases: aliases} do
      {:ok, maps} = Formats.transform({rows, columns, aliases}, {:maps, keys: :atoms})

      assert length(maps) == 2
      assert List.first(maps) == %{
        full_name: "John Doe",
        email_address: "john@example.com",
        user_age: 25,
        is_active: true
      }
    end

    test "returns error for unknown format", %{rows: rows, columns: columns, aliases: aliases} do
      {:error, {:unknown_format, :invalid_format}} =
        Formats.transform({rows, columns, aliases}, :invalid_format)
    end
  end

  describe "validate_format/2" do
    test "validates raw format" do
      assert :ok == Formats.validate_format(:raw)
    end

    test "validates maps format with options" do
      assert :ok == Formats.validate_format({:maps, keys: :atoms})
      assert :ok == Formats.validate_format({:maps, transform: :camelCase})
    end

    test "returns error for invalid format" do
      assert {:error, {:invalid_format, :bad_format}} ==
        Formats.validate_format(:bad_format)
    end

    test "returns error for invalid map options" do
      assert {:error, _} = Formats.validate_format({:maps, keys: :invalid_key_type})
    end
  end

  describe "available_formats/0" do
    test "returns list of available formats" do
      formats = Formats.available_formats()

      assert is_list(formats)
      assert length(formats) > 0

      # Check that basic formats are included
      format_types = Enum.map(formats, & &1.format)
      assert :raw in format_types
      assert :maps in format_types
      assert :json in format_types
      assert :csv in format_types
    end
  end

  describe "performance_info/1" do
    test "returns performance info for different formats" do
      raw_info = Formats.performance_info(:raw)
      maps_info = Formats.performance_info(:maps)

      assert raw_info.memory_overhead == 0
      assert raw_info.streaming_capable == false

      assert maps_info.memory_overhead > 0
      assert maps_info.streaming_capable == true
      assert is_integer(maps_info.recommended_max_rows)
    end
  end
end
