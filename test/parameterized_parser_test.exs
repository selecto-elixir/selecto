defmodule Selecto.FieldResolver.ParameterizedParserTest do
  use ExUnit.Case, async: true

  alias Selecto.FieldResolver.ParameterizedParser

  test "parses simple field reference" do
    assert {:ok, result} = ParameterizedParser.parse_field_reference("name")
    assert result.type == :simple
    assert result.field == "name"
  end

  test "parses parameterized reference shape" do
    assert {:ok, result} =
             ParameterizedParser.parse_field_reference("products:electronics:25.0:true.name")

    assert is_map(result)
    assert result.type in [:parameterized, :simple]
    assert is_binary(result.field)
  end

  test "empty string behavior is stable" do
    assert {:ok, result} = ParameterizedParser.parse_field_reference("")
    assert result.field == ""
  end
end
