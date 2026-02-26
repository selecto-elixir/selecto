defmodule Selecto.Output.TypeCoercionTest do
  use ExUnit.Case
  alias Selecto.Output.TypeCoercion

  describe "coerce_value/4 - nil handling" do
    test "returns nil for nil values" do
      assert TypeCoercion.coerce_value(nil, "integer") == nil
      assert TypeCoercion.coerce_value(nil, "text") == nil
      assert TypeCoercion.coerce_value(nil) == nil
    end
  end

  describe "coerce_value/4 - :ignore strategy" do
    test "returns value unchanged with :ignore strategy" do
      assert TypeCoercion.coerce_value("123", "integer", :ignore) == "123"
      assert TypeCoercion.coerce_value("hello", nil, :ignore) == "hello"
    end
  end

  describe "coerce_value/4 - integer coercion" do
    test "keeps integers as-is" do
      assert TypeCoercion.coerce_value(42, "integer") == 42
      assert TypeCoercion.coerce_value(-100, "bigint") == -100
    end

    test "parses string integers" do
      assert TypeCoercion.coerce_value("42", "integer") == 42
      assert TypeCoercion.coerce_value("-100", "int4") == -100
    end

    test "converts whole floats to integers" do
      assert TypeCoercion.coerce_value(42.0, "integer") == 42
    end

    test "returns original value for invalid integer strings with :safe" do
      assert TypeCoercion.coerce_value("not_a_number", "integer", :safe) == "not_a_number"
    end

    test "raises for invalid integer with :strict" do
      assert_raise ArgumentError, fn ->
        TypeCoercion.coerce_value("not_a_number", "integer", :strict)
      end
    end
  end

  describe "coerce_value/4 - float coercion" do
    test "keeps floats as-is" do
      assert TypeCoercion.coerce_value(3.14, "real") == 3.14
    end

    test "converts integers to floats" do
      assert TypeCoercion.coerce_value(42, "float8") == 42.0
    end

    test "parses string floats" do
      assert TypeCoercion.coerce_value("3.14", "double precision") == 3.14
    end

    test "returns original value for invalid float with :safe" do
      assert TypeCoercion.coerce_value("not_a_float", "real", :safe) == "not_a_float"
    end
  end

  describe "coerce_value/4 - decimal coercion" do
    test "keeps Decimal as-is" do
      dec = Decimal.new("123.45")
      assert TypeCoercion.coerce_value(dec, "decimal") == dec
    end

    test "converts strings to Decimal" do
      result = TypeCoercion.coerce_value("123.45", "numeric")
      assert %Decimal{} = result
      assert Decimal.equal?(result, Decimal.new("123.45"))
    end

    test "converts integers to Decimal" do
      result = TypeCoercion.coerce_value(100, "decimal")
      assert %Decimal{} = result
    end

    test "converts floats to Decimal" do
      result = TypeCoercion.coerce_value(3.14, "money")
      assert %Decimal{} = result
    end
  end

  describe "coerce_value/4 - boolean coercion" do
    test "keeps booleans as-is" do
      assert TypeCoercion.coerce_value(true, "boolean") == true
      assert TypeCoercion.coerce_value(false, "bool") == false
    end

    test "parses truthy strings" do
      for val <- ["true", "t", "yes", "y", "1", "on", "TRUE", "YES"] do
        assert TypeCoercion.coerce_value(val, "boolean") == true, "Expected #{val} to be true"
      end
    end

    test "parses falsy strings" do
      for val <- ["false", "f", "no", "n", "0", "off", "FALSE", "NO"] do
        assert TypeCoercion.coerce_value(val, "boolean") == false, "Expected #{val} to be false"
      end
    end

    test "converts 1 and 0 to boolean" do
      assert TypeCoercion.coerce_value(1, "boolean") == true
      assert TypeCoercion.coerce_value(0, "boolean") == false
    end
  end

  describe "coerce_value/4 - string coercion" do
    test "converts values to string" do
      assert TypeCoercion.coerce_value(123, "varchar") == "123"
      assert TypeCoercion.coerce_value(:atom, "text") == "atom"
      assert TypeCoercion.coerce_value("hello", "char") == "hello"
    end
  end

  describe "coerce_value/4 - date coercion" do
    test "keeps Date as-is" do
      date = ~D[2024-01-15]
      assert TypeCoercion.coerce_value(date, "date") == date
    end

    test "parses ISO8601 date strings" do
      assert TypeCoercion.coerce_value("2024-01-15", "date") == ~D[2024-01-15]
    end

    test "returns original value for invalid date with :safe" do
      assert TypeCoercion.coerce_value("not-a-date", "date", :safe) == "not-a-date"
    end
  end

  describe "coerce_value/4 - time coercion" do
    test "keeps Time as-is" do
      time = ~T[10:30:00]
      assert TypeCoercion.coerce_value(time, "time") == time
    end

    test "parses ISO8601 time strings" do
      assert TypeCoercion.coerce_value("10:30:00", "time without time zone") == ~T[10:30:00]
    end
  end

  describe "coerce_value/4 - naive_datetime coercion" do
    test "keeps NaiveDateTime as-is" do
      ndt = ~N[2024-01-15 10:30:00]
      assert TypeCoercion.coerce_value(ndt, "timestamp") == ndt
    end

    test "parses ISO8601 naive datetime strings" do
      result = TypeCoercion.coerce_value("2024-01-15T10:30:00", "timestamp without time zone")
      assert result == ~N[2024-01-15 10:30:00]
    end
  end

  describe "coerce_value/4 - utc_datetime coercion" do
    test "keeps DateTime as-is" do
      dt = ~U[2024-01-15 10:30:00Z]
      assert TypeCoercion.coerce_value(dt, "timestamptz") == dt
    end

    test "parses ISO8601 UTC datetime strings" do
      result = TypeCoercion.coerce_value("2024-01-15T10:30:00Z", "timestamp with time zone")
      assert %DateTime{} = result
    end
  end

  describe "coerce_value/4 - map/json coercion" do
    test "keeps maps as-is" do
      map = %{a: 1, b: 2}
      assert TypeCoercion.coerce_value(map, "jsonb") == map
    end

    test "parses JSON strings" do
      result = TypeCoercion.coerce_value(~s({"a": 1, "b": 2}), "json")
      assert result == %{"a" => 1, "b" => 2}
    end

    test "returns original value for invalid JSON with :safe" do
      assert TypeCoercion.coerce_value("not json", "jsonb", :safe) == "not json"
    end
  end

  describe "coerce_value/4 - list/array coercion" do
    test "keeps lists as-is" do
      list = [1, 2, 3]
      assert TypeCoercion.coerce_value(list, "array") == list
    end

    test "parses JSON array strings" do
      result = TypeCoercion.coerce_value("[1, 2, 3]", "_int4")
      assert result == [1, 2, 3]
    end
  end

  describe "coerce_value/4 - uuid coercion" do
    test "validates and keeps valid UUIDs" do
      uuid = "550e8400-e29b-41d4-a716-446655440000"
      assert TypeCoercion.coerce_value(uuid, "uuid") == uuid
    end

    test "validates uppercase UUIDs" do
      uuid = "550E8400-E29B-41D4-A716-446655440000"
      assert TypeCoercion.coerce_value(uuid, "uuid") == uuid
    end

    test "returns original value for invalid UUID with :safe" do
      assert TypeCoercion.coerce_value("not-a-uuid", "uuid", :safe) == "not-a-uuid"
    end
  end

  describe "coerce_value/4 - binary coercion" do
    test "keeps binary as-is" do
      binary = <<1, 2, 3>>
      assert TypeCoercion.coerce_value(binary, "bytea") == binary
    end
  end

  describe "coerce_value/4 - spatial coercion" do
    test "parses geometry geojson strings to map" do
      geojson = ~s({"type":"Point","coordinates":[1.0,2.0]})

      assert TypeCoercion.coerce_value(geojson, "geometry") == %{
               "type" => "Point",
               "coordinates" => [1.0, 2.0]
             }

      assert TypeCoercion.coerce_value(geojson, "geometry(Point,4326)") == %{
               "type" => "Point",
               "coordinates" => [1.0, 2.0]
             }
    end

    test "parses geography geojson strings to map" do
      geojson = ~s({"type":"Point","coordinates":[-87.62,41.88]})

      assert TypeCoercion.coerce_value(geojson, "geography") == %{
               "type" => "Point",
               "coordinates" => [-87.62, 41.88]
             }
    end
  end

  describe "coerce_value/4 - custom coercion" do
    test "applies custom coercion function with arity 1" do
      custom = %{"mytype" => fn val -> String.upcase(val) end}
      result = TypeCoercion.coerce_value("hello", "mytype", :custom, custom)
      assert result == "HELLO"
    end

    test "applies custom coercion function with arity 2" do
      custom = %{"mytype" => fn val, _type -> String.reverse(val) end}
      result = TypeCoercion.coerce_value("hello", "mytype", :custom, custom)
      assert result == "olleh"
    end

    test "falls back to safe coercion if custom function not found" do
      custom = %{}
      result = TypeCoercion.coerce_value("42", "integer", :custom, custom)
      assert result == 42
    end

    test "handles errors in custom function safely" do
      custom = %{"mytype" => fn _val -> raise "boom" end}
      result = TypeCoercion.coerce_value("test", "mytype", :custom, custom)
      assert result == "test"
    end
  end

  describe "coerce_value/4 - auto-detection without column type" do
    test "auto-detects integers" do
      assert TypeCoercion.coerce_value("42", nil) == 42
      assert TypeCoercion.coerce_value("-100", nil) == -100
    end

    test "auto-detects floats" do
      assert TypeCoercion.coerce_value("3.14", nil) == 3.14
    end

    test "auto-detects booleans" do
      assert TypeCoercion.coerce_value("true", nil) == true
      assert TypeCoercion.coerce_value("false", nil) == false
    end

    test "auto-detects dates" do
      assert TypeCoercion.coerce_value("2024-01-15", nil) == ~D[2024-01-15]
    end

    test "auto-detects UUIDs but keeps as string" do
      uuid = "550e8400-e29b-41d4-a716-446655440000"
      assert TypeCoercion.coerce_value(uuid, nil) == uuid
    end

    test "auto-detects JSON objects" do
      result = TypeCoercion.coerce_value(~s({"a": 1}), nil)
      assert result == %{"a" => 1}
    end

    test "auto-detects JSON arrays" do
      result = TypeCoercion.coerce_value("[1, 2, 3]", nil)
      assert result == [1, 2, 3]
    end

    test "keeps plain strings as-is" do
      assert TypeCoercion.coerce_value("hello world", nil) == "hello world"
    end

    test "keeps non-string values unchanged" do
      assert TypeCoercion.coerce_value([1, 2, 3], nil) == [1, 2, 3]
      assert TypeCoercion.coerce_value(%{a: 1}, nil) == %{a: 1}
    end
  end

  describe "get_elixir_type/1" do
    test "returns correct type for known PostgreSQL types" do
      assert TypeCoercion.get_elixir_type("integer") == :integer
      assert TypeCoercion.get_elixir_type("bigint") == :integer
      assert TypeCoercion.get_elixir_type("text") == :string
      assert TypeCoercion.get_elixir_type("boolean") == :boolean
      assert TypeCoercion.get_elixir_type("jsonb") == :map
      assert TypeCoercion.get_elixir_type("timestamptz") == :utc_datetime
      assert TypeCoercion.get_elixir_type("geometry") == :geometry
      assert TypeCoercion.get_elixir_type("geometry(Point,4326)") == :geometry
      assert TypeCoercion.get_elixir_type("geography(Point,4326)") == :geography
    end

    test "returns :unknown for unknown types" do
      assert TypeCoercion.get_elixir_type("custom_type") == :unknown
    end
  end

  describe "supported_types/0" do
    test "returns map of all supported type mappings" do
      types = TypeCoercion.supported_types()
      assert is_map(types)
      assert Map.has_key?(types, "integer")
      assert Map.has_key?(types, "text")
      assert Map.has_key?(types, "jsonb")
    end
  end

  describe "batch_coerce/4" do
    test "coerces multiple values with their types" do
      values = ["42", "hello", "true"]
      types = ["integer", "text", "boolean"]
      result = TypeCoercion.batch_coerce(values, types)
      assert result == [42, "hello", true]
    end

    test "handles nil values in batch" do
      values = [nil, "42", nil]
      types = ["text", "integer", "boolean"]
      result = TypeCoercion.batch_coerce(values, types)
      assert result == [nil, 42, nil]
    end
  end
end
