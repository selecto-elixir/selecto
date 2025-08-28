defmodule Selecto.FieldResolverParameterizedTest do
  use ExUnit.Case, async: true
  
  alias Selecto.FieldResolver

  setup do
    # Mock selecto configuration with parameterized joins
    selecto_config = %{
      source: %{
        fields: [:id, :name, :email],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          email: %{type: :string}
        }
      },
      joins: %{
        products: %{
          id: :products,
          fields: %{
            "name" => %{type: :string, field: "name"},
            "price" => %{type: :float, field: "price"},
            "category_id" => %{type: :integer, field: "category_id"}
          },
          parameters: [
            %{name: :category, type: :string, required: true},
            %{name: :min_price, type: :float, required: false, default: 0.0},
            %{name: :active, type: :boolean, required: false, default: true}
          ]
        },
        categories: %{
          id: :categories,
          fields: %{
            "name" => %{type: :string, field: "name"},
            "description" => %{type: :string, field: "description"}
          }
        }
      }
    }
    
    selecto = %{config: selecto_config}
    {:ok, selecto: selecto}
  end

  describe "resolve_field/2 with parameterized joins" do
    test "resolves simple fields", %{selecto: selecto} do
      {:ok, field_info} = FieldResolver.resolve_field(selecto, "name")
      
      assert field_info.name == "name"
      assert field_info.qualified_name == "name"
      assert field_info.source_join == :selecto_root
      assert field_info.type == :string
      assert field_info.parameters == nil
    end

    test "resolves qualified fields without parameters", %{selecto: selecto} do
      {:ok, field_info} = FieldResolver.resolve_field(selecto, "categories.name")
      
      assert field_info.name == "name"
      assert field_info.qualified_name == "categories.name"
      assert field_info.source_join == :categories
      assert field_info.type == :string
      assert field_info.parameters == nil
    end

    test "resolves parameterized join fields", %{selecto: selecto} do
      {:ok, field_info} = FieldResolver.resolve_field(selecto, "products:electronics:25.0:true.name")
      
      assert field_info.name == "name"
      assert field_info.qualified_name == "products:electronics:25.0:true.name"
      assert field_info.source_join == :products
      assert field_info.type == :string
      assert field_info.parameter_signature == "electronics:25.0:true"
      assert length(field_info.parameters) == 3
      
      [param1, param2, param3] = field_info.parameters
      assert param1 == %{name: :category, value: "electronics", type: :string}
      assert param2 == %{name: :min_price, value: 25.0, type: :float}
      assert param3 == %{name: :active, value: true, type: :boolean}
    end

    test "resolves parameterized join with default values", %{selecto: selecto} do
      {:ok, field_info} = FieldResolver.resolve_field(selecto, "products:electronics.name")
      
      assert field_info.parameter_signature == "electronics"
      assert length(field_info.parameters) == 3
      
      [param1, param2, param3] = field_info.parameters
      assert param1 == %{name: :category, value: "electronics", type: :string}
      assert param2 == %{name: :min_price, value: 0.0, type: :float}  # default
      assert param3 == %{name: :active, value: true, type: :boolean}  # default
    end

    test "handles legacy bracket notation with deprecation warning", %{selecto: selecto} do
      import ExUnit.CaptureLog
      
      log = capture_log(fn ->
        {:ok, field_info} = FieldResolver.resolve_field(selecto, "categories[name]")
        
        assert field_info.name == "name"
        assert field_info.qualified_name == "categories.name"
        assert field_info.source_join == :categories
      end)
      
      assert log =~ "Deprecated bracket notation"
      assert log =~ "Consider using dot notation 'categories.name'"
    end

    test "returns error for non-existent parameterized join", %{selecto: selecto} do
      {:error, error} = FieldResolver.resolve_field(selecto, "unknown:param.field")
      
      assert error.message =~ "Parameterized join 'unknown' not found"
    end

    test "returns error for parameter validation failure", %{selecto: selecto} do
      # Missing required parameter
      {:error, error} = FieldResolver.resolve_field(selecto, "products.name")
      
      assert error.message =~ "Parameter validation failed"
      assert error.message =~ "Required parameter 'category' missing"
    end

    test "returns error for non-existent field in parameterized join", %{selecto: selecto} do
      {:error, error} = FieldResolver.resolve_field(selecto, "products:electronics.unknown_field")
      
      assert error.message =~ "Field 'unknown_field' not found in join"
    end

    test "returns error for type mismatch in parameters", %{selecto: selecto} do
      # Try to pass integer where string is required
      {:error, error} = FieldResolver.resolve_field(selecto, "products:123.name")
      
      assert error.message =~ "Parameter validation failed"
      assert error.message =~ "Expected string, got integer"
    end
  end

  describe "get_available_fields/1 with parameterized joins" do
    test "returns all available fields including joins", %{selecto: selecto} do
      fields = FieldResolver.get_available_fields(selecto)
      
      # Source fields
      assert Map.has_key?(fields, "name")
      assert Map.has_key?(fields, "email")
      assert Map.has_key?(fields, "id")
      
      # Join fields (non-parameterized)
      assert Map.has_key?(fields, "categories.name")
      assert Map.has_key?(fields, "categories.description")
      
      # Parameterized join fields are not included in available fields
      # because they require parameter resolution
      refute Map.has_key?(fields, "products.name")
    end
  end

  describe "suggest_fields/2" do
    test "suggests fields based on partial matches", %{selecto: selecto} do
      suggestions = FieldResolver.suggest_fields(selecto, "nam")
      
      assert "name" in suggestions
      assert "categories.name" in suggestions
    end

    test "ranks suggestions by similarity", %{selecto: selecto} do
      suggestions = FieldResolver.suggest_fields(selecto, "name")
      
      # Exact match should be first
      assert List.first(suggestions) == "name"
    end
  end

  describe "is_ambiguous_field?/2" do
    test "detects ambiguous field names", %{selecto: selecto} do
      # "name" exists in both source and categories
      assert FieldResolver.is_ambiguous_field?(selecto, "name")
    end

    test "returns false for unambiguous fields", %{selecto: selecto} do
      # "email" only exists in source
      refute FieldResolver.is_ambiguous_field?(selecto, "email")
      
      # "description" only exists in categories
      refute FieldResolver.is_ambiguous_field?(selecto, "description")
    end
  end

  describe "get_disambiguation_options/2" do
    test "returns all options for ambiguous field", %{selecto: selecto} do
      options = FieldResolver.get_disambiguation_options(selecto, "name")
      
      assert length(options) == 2
      
      qualified_names = Enum.map(options, & &1.qualified_name)
      assert "name" in qualified_names
      assert "categories.name" in qualified_names
    end
  end

  describe "validate_field_references/2" do
    test "validates list of field references", %{selecto: selecto} do
      field_refs = ["name", "categories.name", "email"]
      
      assert :ok == FieldResolver.validate_field_references(selecto, field_refs)
    end

    test "returns errors for invalid field references", %{selecto: selecto} do
      field_refs = ["name", "invalid.field", "categories.unknown"]
      
      {:error, errors} = FieldResolver.validate_field_references(selecto, field_refs)
      
      assert length(errors) == 2
    end

    test "validates parameterized join references", %{selecto: selecto} do
      field_refs = ["products:electronics:25.0:true.name", "products:books.price"]
      
      assert :ok == FieldResolver.validate_field_references(selecto, field_refs)
    end

    test "returns errors for invalid parameterized references", %{selecto: selecto} do
      field_refs = [
        "products:electronics.unknown_field",  # invalid field
        "products.name",                       # missing required parameter
        "unknown:param.field"                  # invalid join
      ]
      
      {:error, errors} = FieldResolver.validate_field_references(selecto, field_refs)
      
      assert length(errors) == 3
    end
  end

  describe "complex parameterized scenarios" do
    test "handles multiple parameter types correctly", %{selecto: selecto} do
      {:ok, field_info} = FieldResolver.resolve_field(selecto, "products:'special-category':100.5:false.price")
      
      [param1, param2, param3] = field_info.parameters
      assert param1.value == "special-category"
      assert param2.value == 100.5
      assert param3.value == false
    end

    test "performs parameter type conversions", %{selecto: selecto} do
      # Integer should be converted to float for min_price parameter
      {:ok, field_info} = FieldResolver.resolve_field(selecto, "products:electronics:50:true.name")
      
      [_param1, param2, _param3] = field_info.parameters
      assert param2.value == 50.0  # converted from integer to float
      assert param2.type == :float
    end
  end
end