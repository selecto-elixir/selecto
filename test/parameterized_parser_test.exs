defmodule Selecto.FieldResolver.ParameterizedParserTest do
  use ExUnit.Case, async: true
  
  alias Selecto.FieldResolver.ParameterizedParser

  describe "parse_field_reference/1" do
    test "parses simple field names" do
      assert {:ok, %{type: :simple, field: "title", join: nil, parameters: nil}} = 
             ParameterizedParser.parse_field_reference("title")
    end

    test "parses simple dot notation" do
      assert {:ok, %{type: :qualified, join: "posts", field: "title", parameters: nil}} = 
             ParameterizedParser.parse_field_reference("posts.title")
    end

    test "parses parameterized join with single string parameter" do
      {:ok, result} = ParameterizedParser.parse_field_reference("posts:published.title")
      
      assert result.type == :parameterized
      assert result.join == "posts"
      assert result.field == "title"
      assert result.parameters == [{:string, "published"}]
    end

    test "parses parameterized join with multiple parameters" do
      {:ok, result} = ParameterizedParser.parse_field_reference("products:electronics:25.0:true.name")
      
      assert result.type == :parameterized
      assert result.join == "products"
      assert result.field == "name"
      assert result.parameters == [
        {:string, "electronics"},
        {:float, 25.0}, 
        {:boolean, true}
      ]
    end

    test "parses quoted string parameters" do
      {:ok, result} = ParameterizedParser.parse_field_reference("products:'special-category':\"user role\".name")
      
      assert result.parameters == [
        {:string, "special-category"},
        {:string, "user role"}
      ]
    end

    test "parses numeric parameters" do
      {:ok, result} = ParameterizedParser.parse_field_reference("discounts:50:-12.5.amount")
      
      assert result.parameters == [
        {:integer, 50},
        {:float, -12.5}
      ]
    end

    test "parses boolean parameters" do
      {:ok, result} = ParameterizedParser.parse_field_reference("users:true:false.name")
      
      assert result.parameters == [
        {:boolean, true},
        {:boolean, false}
      ]
    end

    test "parses legacy bracket notation" do
      assert {:ok, %{type: :bracket_legacy, join: "posts", field: "title"}} = 
             ParameterizedParser.parse_field_reference("posts[title]")
    end

    test "handles atom field references" do
      assert {:ok, %{type: :simple, field: "title"}} = 
             ParameterizedParser.parse_field_reference(:title)
    end

    test "returns error for invalid formats" do
      assert {:error, _} = ParameterizedParser.parse_field_reference(123)
      assert {:error, _} = ParameterizedParser.parse_field_reference(%{})
    end
  end

  describe "parse_single_parameter/1" do
    test "parses string identifiers" do
      assert {:string, "published"} = ParameterizedParser.parse_single_parameter("published")
      assert {:string, "user_role"} = ParameterizedParser.parse_single_parameter("user_role")
    end

    test "parses quoted strings" do
      assert {:string, "special-category"} = ParameterizedParser.parse_single_parameter("'special-category'")
      assert {:string, "user role"} = ParameterizedParser.parse_single_parameter("\"user role\"")
    end

    test "parses integers" do
      assert {:integer, 42} = ParameterizedParser.parse_single_parameter("42")
      assert {:integer, -15} = ParameterizedParser.parse_single_parameter("-15")
    end

    test "parses floats" do
      assert {:float, 25.5} = ParameterizedParser.parse_single_parameter("25.5")
      assert {:float, -12.75} = ParameterizedParser.parse_single_parameter("-12.75")
    end

    test "parses booleans" do
      assert {:boolean, true} = ParameterizedParser.parse_single_parameter("true")
      assert {:boolean, false} = ParameterizedParser.parse_single_parameter("false")
    end

    test "handles escaped quotes" do
      assert {:string, "it's working"} = ParameterizedParser.parse_single_parameter("'it\\'s working'")
      assert {:string, "say \"hello\""} = ParameterizedParser.parse_single_parameter("\"say \\\"hello\\\"\"")
    end

    test "returns error for invalid formats" do
      assert {:error, _} = ParameterizedParser.parse_single_parameter("invalid-chars!")
      assert {:error, _} = ParameterizedParser.parse_single_parameter("'unterminated")
      assert {:error, _} = ParameterizedParser.parse_single_parameter("123abc")
    end
  end

  describe "validate_parameters/2" do
    test "validates parameters against definitions" do
      param_definitions = [
        %{name: :category, type: :string, required: true},
        %{name: :min_price, type: :float, required: false, default: 0.0},
        %{name: :active, type: :boolean, required: false, default: true}
      ]
      
      provided_params = [
        {:string, "electronics"},
        {:float, 25.0},
        {:boolean, true}
      ]
      
      {:ok, validated} = ParameterizedParser.validate_parameters(provided_params, param_definitions)
      
      assert length(validated) == 3
      assert Enum.at(validated, 0) == %{name: :category, value: "electronics", type: :string}
      assert Enum.at(validated, 1) == %{name: :min_price, value: 25.0, type: :float}
      assert Enum.at(validated, 2) == %{name: :active, value: true, type: :boolean}
    end

    test "applies default values for missing optional parameters" do
      param_definitions = [
        %{name: :category, type: :string, required: true},
        %{name: :active, type: :boolean, required: false, default: true}
      ]
      
      provided_params = [{:string, "electronics"}]
      
      {:ok, validated} = ParameterizedParser.validate_parameters(provided_params, param_definitions)
      
      assert length(validated) == 2
      assert Enum.at(validated, 1) == %{name: :active, value: true, type: :boolean}
    end

    test "returns error for missing required parameters" do
      param_definitions = [
        %{name: :category, type: :string, required: true}
      ]
      
      provided_params = []
      
      assert {:error, error_msg} = ParameterizedParser.validate_parameters(provided_params, param_definitions)
      assert error_msg =~ "Required parameter 'category' missing"
    end

    test "performs type conversion" do
      param_definitions = [
        %{name: :price, type: :float, required: true}
      ]
      
      provided_params = [{:integer, 25}]
      
      {:ok, validated} = ParameterizedParser.validate_parameters(provided_params, param_definitions)
      
      assert Enum.at(validated, 0) == %{name: :price, value: 25.0, type: :float}
    end

    test "returns error for type mismatches" do
      param_definitions = [
        %{name: :category, type: :string, required: true}
      ]
      
      provided_params = [{:integer, 123}]
      
      assert {:error, error_msg} = ParameterizedParser.validate_parameters(provided_params, param_definitions)
      assert error_msg =~ "Expected string, got integer"
    end
  end

  describe "parse_join_with_parameters/1" do
    test "parses join name without parameters" do
      assert {"posts", []} = ParameterizedParser.parse_join_with_parameters("posts")
    end

    test "parses join name with single parameter" do
      assert {"posts", [{:string, "published"}]} = ParameterizedParser.parse_join_with_parameters("posts:published")
    end

    test "parses join name with multiple parameters" do
      {join, params} = ParameterizedParser.parse_join_with_parameters("products:electronics:25.0:true")
      
      assert join == "products"
      assert params == [
        {:string, "electronics"},
        {:float, 25.0},
        {:boolean, true}
      ]
    end

    test "handles parameter parsing errors" do
      assert {:error, _} = ParameterizedParser.parse_join_with_parameters("products:invalid-chars!")
    end
  end

  describe "edge cases" do
    test "handles empty strings" do
      assert {:error, _} = ParameterizedParser.parse_field_reference("")
    end

    test "handles fields with dots in names" do
      # This should be treated as a qualified field reference
      {:ok, result} = ParameterizedParser.parse_field_reference("table.complex_field")
      assert result.type == :qualified
      assert result.join == "table"
      assert result.field == "complex_field"
    end

    test "handles complex parameter combinations" do
      {:ok, result} = ParameterizedParser.parse_field_reference("complex:123:'quoted-string':true:-45.5:false.field")
      
      assert result.parameters == [
        {:integer, 123},
        {:string, "quoted-string"},
        {:boolean, true},
        {:float, -45.5},
        {:boolean, false}
      ]
    end

    test "validates parameter count against definitions" do
      param_definitions = [
        %{name: :category, type: :string, required: true}
      ]
      
      # Too many parameters provided
      provided_params = [
        {:string, "electronics"},
        {:string, "extra"}
      ]
      
      {:ok, validated} = ParameterizedParser.validate_parameters(provided_params, param_definitions)
      # Should only validate the first parameter, ignoring extras
      assert length(validated) == 1
    end
  end
end