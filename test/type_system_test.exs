defmodule Selecto.TypeSystemTest do
  use ExUnit.Case

  alias Selecto.TypeSystem

  defp domain do
    %{
      name: "Type test",
      source: %{
        source_table: "users",
        primary_key: :id,
        fields: [:id, :age, :name, :active],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          age: %{type: :integer},
          name: %{type: :string},
          active: %{type: :boolean}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end

  test "infers primitive literals" do
    assert {:ok, :integer} = TypeSystem.infer_type(nil, 1)
    assert {:ok, :decimal} = TypeSystem.infer_type(nil, 1.2)
    assert {:ok, :boolean} = TypeSystem.infer_type(nil, true)
    assert {:ok, :string} = TypeSystem.infer_type(nil, "abc")
    assert {:ok, :uuid} = TypeSystem.infer_type(nil, "550e8400-e29b-41d4-a716-446655440000")
    assert {:ok, :unknown} = TypeSystem.infer_type(nil, nil)
  end

  test "infers literal expression wrappers" do
    assert {:ok, :date} = TypeSystem.infer_type(nil, {:literal, ~D[2024-01-01]})
    assert {:ok, :time} = TypeSystem.infer_type(nil, {:literal, ~T[10:11:12]})

    assert {:ok, :utc_datetime} =
             TypeSystem.infer_type(
               nil,
               {:literal, DateTime.from_naive!(~N[2024-01-01 10:11:12], "Etc/UTC")}
             )

    assert {:ok, :naive_datetime} =
             TypeSystem.infer_type(nil, {:literal, ~N[2024-01-01 10:11:12]})

    assert {:ok, {:array, :unknown}} = TypeSystem.infer_type(nil, {:literal, [1, 2, 3]})
    assert {:ok, :jsonb} = TypeSystem.infer_type(nil, {:literal, %{a: 1}})
    assert {:ok, :string} = TypeSystem.infer_type(nil, {:literal_string, "x"})
  end

  test "infers aggregate and scalar expressions" do
    selecto = Selecto.configure(domain(), nil)

    assert {:ok, :bigint} = TypeSystem.infer_type(selecto, {:count})
    assert {:ok, :bigint} = TypeSystem.infer_type(selecto, {:count, "*"})
    assert {:ok, :decimal} = TypeSystem.infer_type(selecto, {:sum, "age"})
    assert {:ok, :string} = TypeSystem.infer_type(selecto, {:min, "age"})
    assert {:ok, :string} = TypeSystem.infer_type(selecto, {:concat, ["name", "name"]})
    assert {:ok, :string} = TypeSystem.infer_type(selecto, {:abs, "age"})
    assert {:ok, :string} = TypeSystem.infer_type(selecto, "age")
    assert {:ok, :string} = TypeSystem.infer_type(selecto, "missing_field")
  end

  test "type categories and compatibility" do
    assert :numeric == TypeSystem.type_category(:integer)
    assert :string == TypeSystem.type_category(:varchar)
    assert :datetime == TypeSystem.type_category(:timestamp)
    assert :array == TypeSystem.type_category({:array, :integer})
    assert :unknown == TypeSystem.type_category(:something_else)

    assert TypeSystem.compatible?(:integer, :decimal)
    refute TypeSystem.compatible?(:integer, :string)
    assert TypeSystem.compatible?(:unknown, :string)
  end

  test "coercion rules for operations" do
    assert {:ok, :decimal} = TypeSystem.coerce_types(:integer, :decimal, :arithmetic)
    assert {:ok, :boolean} = TypeSystem.coerce_types(:integer, :string, :comparison)
    assert {:ok, :text} = TypeSystem.coerce_types(:varchar, :string, :concatenation)
    assert {:ok, :bigint} = TypeSystem.coerce_types(:integer, :bigint, :union)
    assert {:error, _} = TypeSystem.coerce_types(:integer, :string, :union)
  end

  test "normalize and parse SQL types" do
    assert :integer == TypeSystem.normalize_type(:id)
    assert :uuid == TypeSystem.normalize_type(:binary_id)
    assert {:array, :integer} == TypeSystem.normalize_type({:array, :id})

    assert :integer == TypeSystem.parse_sql_type("INT4")
    assert :varchar == TypeSystem.parse_sql_type("varchar(255)")
    assert :char == TypeSystem.parse_sql_type("character(10)")
    assert :time == TypeSystem.parse_sql_type("time with time zone")
    assert :time == TypeSystem.parse_sql_type("timestamp without time zone")
    assert :unknown == TypeSystem.parse_sql_type("mystery")
  end
end
