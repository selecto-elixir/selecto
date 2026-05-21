defmodule Selecto.EctoAdapterUuidTest do
  use ExUnit.Case, async: true

  defmodule Account do
    use Ecto.Schema

    @primary_key {:id, :binary_id, autogenerate: true}
    @foreign_key_type :binary_id

    schema "accounts" do
      field(:name, :string)
      has_many(:events, Selecto.EctoAdapterUuidTest.Event)
    end
  end

  defmodule Event do
    use Ecto.Schema

    @primary_key {:id, :binary_id, autogenerate: true}
    @foreign_key_type :binary_id

    schema "events" do
      field(:external_id, Ecto.UUID)
      field(:name, :string)

      belongs_to(:account, Selecto.EctoAdapterUuidTest.Account)
    end
  end

  test "schema_to_domain maps binary_id primary and foreign keys to uuid columns" do
    domain = Selecto.EctoAdapter.schema_to_domain(Event, joins: [:account])

    assert domain.source.primary_key == :id
    assert domain.source.columns.id.type == :uuid
    assert domain.source.columns.account_id.type == :uuid
    assert domain.source.columns.external_id.type == :uuid
    assert domain.source.columns.name.type == :string
  end

  test "join schema columns preserve uuid types" do
    domain = Selecto.EctoAdapter.schema_to_domain(Event, joins: [:account])

    assert domain.schemas.account.columns.id.type == :uuid
    assert domain.schemas.account.columns.name.type == :string
  end
end
