defmodule Selecto.FieldResolverParameterizedTest do
  use ExUnit.Case, async: true

  alias Selecto.FieldResolver

  setup do
    domain = %{
      name: "resolver_domain",
      source: %{
        source_table: "users",
        primary_key: :id,
        fields: [:id, :name, :email],
        redact_fields: [],
        columns: %{id: %{type: :integer}, name: %{type: :string}, email: %{type: :string}},
        associations: %{
          products: %{
            queryable: :products,
            field: :products,
            owner_key: :id,
            related_key: :category_id
          }
        }
      },
      schemas: %{
        products: %{
          source_table: "products",
          primary_key: :id,
          fields: [:id, :name, :price, :category_id],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            price: %{type: :float},
            category_id: %{type: :integer}
          },
          associations: %{}
        }
      },
      joins: %{products: %{}}
    }

    {:ok, selecto: Selecto.configure(domain, :mock_connection, validate: false)}
  end

  test "resolve_field supports regular joined fields", %{selecto: selecto} do
    assert {:ok, field} = FieldResolver.resolve_field(selecto, "products.name")
    assert field.qualified_name == "products.name"
  end

  test "available fields include join fields", %{selecto: selecto} do
    fields = FieldResolver.get_available_fields(selecto)
    assert Map.has_key?(fields, "products.name")
  end

  test "validate_field_references returns errors for bad fields", %{selecto: selecto} do
    assert {:error, errors} = FieldResolver.validate_field_references(selecto, ["unknown.field"])
    assert is_list(errors)
  end
end
