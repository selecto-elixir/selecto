defmodule Selecto.EctoAdapterThroughAssociationsTest do
  use ExUnit.Case, async: true

  defmodule Customer do
    use Ecto.Schema

    @primary_key {:code, :string, autogenerate: false}
    @foreign_key_type :string

    schema "customers" do
      has_many(:orders, Selecto.EctoAdapterThroughAssociationsTest.Order,
        foreign_key: :buyer_code,
        references: :code
      )

      has_many(:products, through: [:orders, :product])
    end
  end

  defmodule Order do
    use Ecto.Schema

    @primary_key {:order_ref, :string, autogenerate: false}
    @foreign_key_type :string

    schema "orders" do
      field(:buyer_code, :string)
      field(:product_sku, :string)

      belongs_to(:customer, Selecto.EctoAdapterThroughAssociationsTest.Customer,
        define_field: false,
        foreign_key: :buyer_code,
        references: :code,
        type: :string
      )

      belongs_to(:product, Selecto.EctoAdapterThroughAssociationsTest.Product,
        define_field: false,
        foreign_key: :product_sku,
        references: :sku,
        type: :string
      )
    end
  end

  defmodule Product do
    use Ecto.Schema

    @primary_key {:sku, :string, autogenerate: false}

    schema "products" do
      field(:name, :string)
    end
  end

  test "schema_to_domain infers through association keys from association path" do
    domain = Selecto.EctoAdapter.schema_to_domain(Customer, joins: [:products])

    assert %{products: assoc} = domain.source.associations
    assert assoc.type == :has_many_through
    assert assoc.owner_key == :code
    assert assoc.related_key == :sku
  end
end
