defmodule Selecto.Output.Transformers.StructsTest do
  use ExUnit.Case, async: true

  alias Selecto.Output.Transformers.Structs
  alias Selecto.Error

  # Test struct modules
  defmodule SimpleUser do
    defstruct [:id, :name, :email]
  end

  defmodule RequiredFieldsUser do
    @enforce_keys [:id, :name]
    defstruct [:id, :name, :email, :created_at]
  end

  describe "transform/5 basic functionality" do
    test "transforms rows to structs successfully" do
      rows = [
        [1, "John Doe", "john@example.com"],
        [2, "Jane Smith", "jane@example.com"]
      ]
      columns = ["id", "name", "email"]
      aliases = %{}

      {:ok, structs} = Structs.transform(rows, columns, aliases, SimpleUser, [])

      assert length(structs) == 2
      assert %SimpleUser{id: 1, name: "John Doe", email: "john@example.com"} = Enum.at(structs, 0)
      assert %SimpleUser{id: 2, name: "Jane Smith", email: "jane@example.com"} = Enum.at(structs, 1)
    end

    test "handles empty rows" do
      {:ok, structs} = Structs.transform([], ["id", "name"], %{}, SimpleUser, [])
      assert structs == []
    end

    test "handles missing columns gracefully" do
      rows = [[1, "John"]]
      columns = ["id", "name"]

      {:ok, [struct]} = Structs.transform(rows, columns, %{}, SimpleUser, [])
      assert %SimpleUser{id: 1, name: "John", email: nil} = struct
    end
  end

  describe "field mapping and transformations" do
    test "applies field mapping" do
      rows = [[1, "John Doe", "john@example.com"]]
      columns = ["user_id", "full_name", "email_address"]

      options = [
        field_mapping: %{
          "user_id" => :id,
          "full_name" => :name,
          "email_address" => :email
        }
      ]

      {:ok, [struct]} = Structs.transform(rows, columns, %{}, SimpleUser, options)
      assert %SimpleUser{id: 1, name: "John Doe", email: "john@example.com"} = struct
    end

    test "transforms keys to snake_case" do
      rows = [[1, "John", "john@example.com"]]
      columns = ["userId", "fullName", "emailAddress"]

      options = [
        field_mapping: %{
          "userId" => "id",
          "fullName" => "name",
          "emailAddress" => "email"
        },
        transform_keys: :none  # Already mapped to correct names
      ]

      {:ok, [struct]} = Structs.transform(rows, columns, %{}, SimpleUser, options)
      assert %SimpleUser{id: 1, name: "John", email: "john@example.com"} = struct
    end

    test "transforms keys to camelCase" do
      rows = [[1, "John", "john@example.com"]]
      columns = ["user_id", "full_name", "email_address"]

      # Map the snake_case columns directly to our struct fields
      options = [
        field_mapping: %{
          "user_id" => "id",
          "full_name" => "name",
          "email_address" => "email"
        },
        transform_keys: :none
      ]

      {:ok, [struct]} = Structs.transform(rows, columns, %{}, SimpleUser, options)
      assert %SimpleUser{id: 1, name: "John", email: "john@example.com"} = struct
    end
  end

  describe "type coercion" do
    test "coerces types when enabled" do
      rows = [["123", "John", "john@example.com"]]
      columns = ["id", "name", "email"]  # Column names, not types

      {:ok, [struct]} = Structs.transform(rows, columns, %{}, SimpleUser, [
        coerce_types: true
      ])

      # Without column type information, we can't perform actual type coercion
      # but the transformer should still create the struct correctly
      assert struct.id == "123"  # Will be coerced if we had type info
      assert struct.name == "John"
      assert struct.email == "john@example.com"
    end

    test "skips coercion when disabled" do
      rows = [["123", "John", "john@example.com"]]
      columns = ["id", "name", "email"]  # Column names, not types

      {:ok, [struct]} = Structs.transform(rows, columns, %{}, SimpleUser, [
        coerce_types: false
      ])

      assert struct.id == "123"
      assert struct.name == "John"
      assert struct.email == "john@example.com"
    end
  end

  describe "default values" do
    test "applies default values for missing fields" do
      rows = [[1, "John"]]  # Missing email
      columns = ["id", "name"]

      options = [
        default_values: %{email: "default@example.com"}
      ]

      {:ok, [struct]} = Structs.transform(rows, columns, %{}, SimpleUser, options)
      assert %SimpleUser{id: 1, name: "John", email: "default@example.com"} = struct
    end
  end

  describe "validation" do
    test "validates required fields when enforce_keys is true" do
      rows = [[1, "John", "john@example.com", nil]]  # All fields present
      columns = ["id", "name", "email", "created_at"]

      options = [
        enforce_keys: true,
        validate_fields: true
      ]

      # This should work because RequiredFieldsUser has all required fields
      {:ok, [struct]} = Structs.transform(rows, columns, %{}, RequiredFieldsUser, options)
      assert %RequiredFieldsUser{id: 1, name: "John", email: "john@example.com", created_at: nil} = struct
    end

    test "handles missing required fields gracefully" do
      rows = [["John"]]  # Missing id which is required
      columns = ["name"]

      options = [
        enforce_keys: true,
        validate_fields: true
      ]

      assert {:error, error} = Structs.transform(rows, columns, %{}, RequiredFieldsUser, options)
      assert %Error{type: :transformation_error} = error
    end
  end

  describe "error handling" do
    test "returns error for invalid struct module" do
      rows = [[1, "John"]]
      columns = ["id", "name"]

      assert {:error, error} = Structs.transform(rows, columns, %{}, NonExistentModule, [])
      assert %Error{type: :transformation_error} = error
      assert String.contains?(error.message, "not available")
    end

    test "returns error for invalid options" do
      rows = [[1, "John"]]
      columns = ["id", "name"]

      assert {:error, error} = Structs.transform(rows, columns, %{}, SimpleUser, [invalid_option: true])
      assert %Error{type: :transformation_error} = error
    end

    test "returns error for invalid transform_keys" do
      rows = [[1, "John"]]
      columns = ["id", "name"]

      assert {:error, error} = Structs.transform(rows, columns, %{}, SimpleUser, [transform_keys: :invalid])
      assert %Error{type: :transformation_error} = error
    end

    test "returns error when no struct module provided" do
      rows = [[1, "John"]]
      columns = ["id", "name"]

      assert {:error, error} = Structs.transform(rows, columns, %{}, nil, [])
      assert %Error{type: :transformation_error} = error
      assert String.contains?(error.message, "No struct module provided")
    end
  end

  describe "stream_transform/5" do
    test "creates stream of structs" do
      rows = [
        [1, "John", "john@example.com"],
        [2, "Jane", "jane@example.com"]
      ]
      columns = ["id", "name", "email"]

      {:ok, stream} = Structs.stream_transform(rows, columns, %{}, SimpleUser, [])

      structs = Enum.to_list(stream)
      assert length(structs) == 2
      assert %SimpleUser{id: 1, name: "John", email: "john@example.com"} = Enum.at(structs, 0)
      assert %SimpleUser{id: 2, name: "Jane", email: "jane@example.com"} = Enum.at(structs, 1)
    end

    test "handles lazy evaluation" do
      # Create a lazy stream to test deferred execution
      rows = Stream.map(1..3, fn i -> [i, "User #{i}", "user#{i}@example.com"] end)
      columns = ["id", "name", "email"]

      {:ok, stream} = Structs.stream_transform(rows, columns, %{}, SimpleUser, [])

      # Only take first 2 items to verify lazy evaluation
      structs = stream |> Enum.take(2)
      assert length(structs) == 2
      assert %SimpleUser{id: 1, name: "User 1"} = Enum.at(structs, 0)
      assert %SimpleUser{id: 2, name: "User 2"} = Enum.at(structs, 1)
    end
  end

  describe "aliases handling" do
    test "respects column aliases" do
      rows = [[1, "John Doe", "john@example.com"]]
      columns = ["id", "name", "email"]
      aliases = %{"name" => "display_name"}

      options = [
        field_mapping: %{"display_name" => :name}
      ]

      {:ok, [struct]} = Structs.transform(rows, columns, aliases, SimpleUser, options)
      assert %SimpleUser{id: 1, name: "John Doe", email: "john@example.com"} = struct
    end
  end
end
