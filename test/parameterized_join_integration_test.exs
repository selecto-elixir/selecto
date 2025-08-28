defmodule Selecto.ParameterizedJoinIntegrationTest do
  use ExUnit.Case
  
  alias Selecto.Schema.ParameterizedJoin
  
  describe "process_parameterized_join/6" do
    setup do
      join_config = %{
        name: "Products",
        type: :left,
        parameters: [
          %{name: :category, type: :string, required: true, description: "Product category"},
          %{name: :min_price, type: :float, required: false, default: 0.0, description: "Minimum price"},
          %{name: :active, type: :boolean, required: false, default: true, description: "Active products only"},
          %{name: :featured, type: :boolean, required: false, default: false, description: "Featured products"}
        ],
        join_condition: """
        {join_alias}.category = $param_category AND
        {join_alias}.price >= $param_min_price AND
        {join_alias}.active = $param_active AND
        {join_alias}.featured = $param_featured
        """
      }
      
      queryable = %{
        source_table: "products",
        fields: [:id, :name, :price, :category, :active, :featured],
        primary_key: :id
      }
      
      {:ok, join_config: join_config, queryable: queryable}
    end
    
    test "processes parameterized join with all parameters", %{join_config: join_config, queryable: queryable} do
      parameters = [
        {:string, "electronics"},
        {:float, 25.0},
        {:boolean, true},
        {:boolean, true}
      ]
      
      result = ParameterizedJoin.process_parameterized_join(
        :products, join_config, parameters, :selecto_root, nil, queryable
      )
      
      assert result.parameter_signature == "electronics:25.0:true:true"
      assert length(result.parameters) == 4
      
      [param1, param2, param3, param4] = result.parameters
      assert param1 == %{name: :category, value: "electronics", type: :string}
      assert param2 == %{name: :min_price, value: 25.0, type: :float}
      assert param3 == %{name: :active, value: true, type: :boolean}
      assert param4 == %{name: :featured, value: true, type: :boolean}
      
      assert result.parameter_context == %{
        category: "electronics",
        min_price: 25.0,
        active: true,
        featured: true
      }
      
      expected_condition = """
      {join_alias}.category = 'electronics' AND
      {join_alias}.price >= 25.0 AND
      {join_alias}.active = true AND
      {join_alias}.featured = true
      """
      
      # Normalize whitespace for comparison
      normalized_result = String.replace(result.join_condition, ~r/\s+/, " ")
      normalized_expected = String.replace(expected_condition, ~r/\s+/, " ")
      
      assert String.trim(normalized_result) == String.trim(normalized_expected)
    end
    
    test "applies default values for missing optional parameters", %{join_config: join_config, queryable: queryable} do
      parameters = [
        {:string, "books"}  # Only category provided
      ]
      
      result = ParameterizedJoin.process_parameterized_join(
        :products, join_config, parameters, :selecto_root, nil, queryable
      )
      
      assert result.parameter_signature == "books"
      assert length(result.parameters) == 4
      
      [param1, param2, param3, param4] = result.parameters
      assert param1.value == "books"
      assert param2.value == 0.0    # default
      assert param3.value == true   # default
      assert param4.value == false  # default
    end
    
    test "raises error for missing required parameters", %{join_config: join_config, queryable: queryable} do
      parameters = []  # No parameters provided, but category is required
      
      assert_raise RuntimeError, ~r/Required parameter 'category' missing/, fn ->
        ParameterizedJoin.process_parameterized_join(
          :products, join_config, parameters, :selecto_root, nil, queryable
        )
      end
    end
    
    test "raises error for parameter type mismatch", %{join_config: join_config, queryable: queryable} do
      parameters = [
        {:integer, 123}  # Should be string for category
      ]
      
      assert_raise RuntimeError, ~r/Expected string, got integer/, fn ->
        ParameterizedJoin.process_parameterized_join(
          :products, join_config, parameters, :selecto_root, nil, queryable
        )
      end
    end
  end
  
  describe "validate_parameters/2" do
    test "validates all parameter types correctly" do
      param_definitions = [
        %{name: :category, type: :string, required: true},
        %{name: :count, type: :integer, required: true},
        %{name: :price, type: :float, required: true},
        %{name: :active, type: :boolean, required: true},
        %{name: :tag, type: :atom, required: true}
      ]
      
      provided_params = [
        {:string, "electronics"},
        {:integer, 10},
        {:float, 99.99},
        {:boolean, true},
        {:string, "featured"}  # Will be converted to atom
      ]
      
      validated = ParameterizedJoin.validate_parameters(param_definitions, provided_params)
      
      assert length(validated) == 5
      assert Enum.at(validated, 0).value == "electronics"
      assert Enum.at(validated, 1).value == 10
      assert Enum.at(validated, 2).value == 99.99
      assert Enum.at(validated, 3).value == true
      assert Enum.at(validated, 4).value == :featured  # converted to atom
    end
    
    test "performs type conversions" do
      param_definitions = [
        %{name: :price, type: :float, required: true},
        %{name: :count, type: :integer, required: true},
        %{name: :active, type: :boolean, required: true}
      ]
      
      provided_params = [
        {:integer, 25},      # integer -> float
        {:string, "10"},     # string -> integer
        {:string, "true"}    # string -> boolean
      ]
      
      validated = ParameterizedJoin.validate_parameters(param_definitions, provided_params)
      
      assert Enum.at(validated, 0).value == 25.0
      assert Enum.at(validated, 1).value == 10
      assert Enum.at(validated, 2).value == true
    end
    
    test "handles edge cases in type conversion" do
      param_definitions = [
        %{name: :active1, type: :boolean, required: true},
        %{name: :active2, type: :boolean, required: true},
        %{name: :active3, type: :boolean, required: true},
        %{name: :active4, type: :boolean, required: true}
      ]
      
      provided_params = [
        {:string, "1"},      # "1" -> true
        {:string, "0"},      # "0" -> false
        {:string, "TRUE"},   # case insensitive
        {:string, "False"}   # case insensitive
      ]
      
      validated = ParameterizedJoin.validate_parameters(param_definitions, provided_params)
      
      assert Enum.at(validated, 0).value == true
      assert Enum.at(validated, 1).value == false
      assert Enum.at(validated, 2).value == true
      assert Enum.at(validated, 3).value == false
    end
  end
  
  describe "build_parameter_signature/1" do
    test "builds signature from parameters" do
      parameters = [
        {:string, "electronics"},
        {:float, 25.0},
        {:boolean, true}
      ]
      
      signature = ParameterizedJoin.build_parameter_signature(parameters)
      assert signature == "electronics:25.0:true"
    end
    
    test "handles empty parameter list" do
      assert ParameterizedJoin.build_parameter_signature([]) == ""
      assert ParameterizedJoin.build_parameter_signature(nil) == ""
    end
    
    test "handles special characters in string parameters" do
      parameters = [
        {:string, "special-category"},
        {:string, "user name with spaces"},
        {:integer, 123}
      ]
      
      signature = ParameterizedJoin.build_parameter_signature(parameters)
      assert signature == "special-category:user name with spaces:123"
    end
  end
  
  describe "build_parameter_context/1" do
    test "builds context map from validated parameters" do
      validated_params = [
        %{name: :category, value: "electronics", type: :string},
        %{name: :min_price, value: 25.0, type: :float},
        %{name: :active, value: true, type: :boolean}
      ]
      
      context = ParameterizedJoin.build_parameter_context(validated_params)
      
      assert context == %{
        category: "electronics",
        min_price: 25.0,
        active: true
      }
    end
    
    test "handles empty parameter list" do
      context = ParameterizedJoin.build_parameter_context([])
      assert context == %{}
    end
  end
  
  describe "resolve_parameterized_condition/2" do
    test "replaces parameter placeholders in join condition" do
      join_config = %{
        join_condition: "{join_alias}.category = $param_category AND {join_alias}.price >= $param_min_price"
      }
      
      validated_params = [
        %{name: :category, value: "electronics", type: :string},
        %{name: :min_price, value: 25.0, type: :float}
      ]
      
      condition = ParameterizedJoin.resolve_parameterized_condition(join_config, validated_params)
      
      expected = "{join_alias}.category = 'electronics' AND {join_alias}.price >= 25.0"
      assert condition == expected
    end
    
    test "handles different parameter types" do
      join_config = %{
        join_condition: "field1 = $param_str AND field2 = $param_int AND field3 = $param_bool AND field4 = $param_float"
      }
      
      validated_params = [
        %{name: :str, value: "test", type: :string},
        %{name: :int, value: 42, type: :integer},
        %{name: :bool, value: true, type: :boolean},
        %{name: :float, value: 3.14, type: :float}
      ]
      
      condition = ParameterizedJoin.resolve_parameterized_condition(join_config, validated_params)
      
      expected = "field1 = 'test' AND field2 = 42 AND field3 = true AND field4 = 3.14"
      assert condition == expected
    end
    
    test "escapes single quotes in string values" do
      join_config = %{
        join_condition: "name = $param_name"
      }
      
      validated_params = [
        %{name: :name, value: "O'Reilly", type: :string}
      ]
      
      condition = ParameterizedJoin.resolve_parameterized_condition(join_config, validated_params)
      
      expected = "name = 'O''Reilly'"
      assert condition == expected
    end
    
    test "returns nil when no join_condition specified" do
      join_config = %{}
      validated_params = []
      
      condition = ParameterizedJoin.resolve_parameterized_condition(join_config, validated_params)
      assert condition == nil
    end
  end
  
  describe "enhance_join_with_parameters/2" do
    test "enhances base join with parameterized configuration" do
      base_join = %{
        id: :products,
        name: "Products",
        type: :left
      }
      
      parameterized_config = %{
        parameters: [%{name: :category, value: "electronics", type: :string}],
        parameter_context: %{category: "electronics"},
        join_condition: "category = 'electronics'",
        parameter_signature: "electronics"
      }
      
      enhanced = ParameterizedJoin.enhance_join_with_parameters(base_join, parameterized_config)
      
      assert enhanced.is_parameterized == true
      assert enhanced.parameters == parameterized_config.parameters
      assert enhanced.parameter_context == parameterized_config.parameter_context
      assert enhanced.join_condition == parameterized_config.join_condition
      assert enhanced.parameter_signature == parameterized_config.parameter_signature
      
      # Original fields should be preserved
      assert enhanced.id == :products
      assert enhanced.name == "Products"
      assert enhanced.type == :left
    end
  end
end