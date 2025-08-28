defmodule Selecto.OptionProviderTest do
  use ExUnit.Case, async: true
  
  alias Selecto.OptionProvider

  describe "static option provider" do
    test "loads static values correctly" do
      provider = %{
        type: :static,
        values: ["active", "inactive", "pending"]
      }
      
      assert {:ok, options} = OptionProvider.load_options(provider)
      
      expected = [
        {"active", "active"},
        {"inactive", "inactive"},
        {"pending", "pending"}
      ]
      
      assert options == expected
    end

    test "handles empty values list" do
      provider = %{type: :static, values: []}
      
      assert {:ok, []} = OptionProvider.load_options(provider)
    end

    test "handles mixed data types in values" do
      provider = %{
        type: :static,
        values: [1, "active", :pending, true]
      }
      
      assert {:ok, options} = OptionProvider.load_options(provider)
      
      expected = [
        {1, "1"},
        {"active", "active"},
        {:pending, "pending"},
        {true, "true"}
      ]
      
      assert options == expected
    end
  end

  describe "enum option provider" do
    # We'll need to create a test schema for this
    defmodule TestSchema do
      use Ecto.Schema
      
      schema "test_table" do
        field :status, Ecto.Enum, values: [active: "active", inactive: "inactive", pending: "pending"]
        field :priority, Ecto.Enum, values: [low: "low", medium: "medium", high: "high"]
      end
    end

    test "loads enum values correctly" do
      provider = %{
        type: :enum,
        schema: TestSchema,
        field: :status
      }
      
      assert {:ok, options} = OptionProvider.load_options(provider)
      
      # Options should be {value, display_key}
      expected = [
        {"active", "active"},
        {"inactive", "inactive"},
        {"pending", "pending"}
      ]
      
      assert Enum.sort(options) == Enum.sort(expected)
    end

    test "handles non-enum field" do
      provider = %{
        type: :enum,
        schema: TestSchema,
        field: :name  # This field doesn't exist as enum
      }
      
      assert {:error, _reason} = OptionProvider.load_options(provider)
    end

    test "handles invalid schema" do
      provider = %{
        type: :enum,
        schema: NonExistentSchema,
        field: :status
      }
      
      assert {:error, _reason} = OptionProvider.load_options(provider)
    end
  end

  describe "validation" do
    test "validates static provider correctly" do
      valid_provider = %{type: :static, values: ["a", "b"]}
      assert :ok = OptionProvider.validate_provider(valid_provider)

      invalid_provider = %{type: :static}
      assert {:error, _reason} = OptionProvider.validate_provider(invalid_provider)
    end

    test "validates domain provider correctly" do
      valid_provider = %{
        type: :domain,
        domain: :test_domain,
        value_field: :id,
        display_field: :name
      }
      assert :ok = OptionProvider.validate_provider(valid_provider)

      invalid_provider = %{type: :domain, domain: :test}
      assert {:error, {:missing_required_fields, _fields}} = 
        OptionProvider.validate_provider(invalid_provider)
    end

    test "validates enum provider correctly" do
      valid_provider = %{
        type: :enum,
        schema: TestSchema,
        field: :status
      }
      assert :ok = OptionProvider.validate_provider(valid_provider)

      invalid_provider = %{type: :enum, schema: "not_atom"}
      assert {:error, _reason} = OptionProvider.validate_provider(invalid_provider)
    end

    test "validates query provider correctly" do
      valid_provider = %{
        type: :query,
        query: "SELECT id, name FROM table",
        params: []
      }
      assert :ok = OptionProvider.validate_provider(valid_provider)

      invalid_provider = %{type: :query, query: 123}
      assert {:error, _reason} = OptionProvider.validate_provider(invalid_provider)
    end

    test "rejects unknown provider types" do
      invalid_provider = %{type: :unknown}
      assert {:error, :unknown_provider_type} = 
        OptionProvider.validate_provider(invalid_provider)
    end
  end

  describe "error handling" do
    test "handles invalid provider configuration gracefully" do
      invalid_provider = %{type: :static}
      
      assert {:error, _reason} = OptionProvider.load_options(invalid_provider)
    end

    test "handles missing selecto for domain provider" do
      provider = %{
        type: :domain,
        domain: :test_domain,
        value_field: :id,
        display_field: :name
      }
      
      assert {:error, :invalid_provider_configuration} = 
        OptionProvider.load_options(provider, nil)
    end
  end
end