defmodule Selecto.AdvancedJoinsEdgeCasesTest do
  use ExUnit.Case

  alias Selecto
  alias Selecto.DomainValidator.ValidationError

  defp mock_conn do
    %{__struct__: Postgrex.Connection, pid: self()}
  end

  describe "domain validation edge cases" do
    test "rejects dimensions without primary_key" do
      invalid_domain = %{
        name: "Invalid dimensions",
        source: %{
          source_table: "facts",
          primary_key: :id,
          fields: [:id, :dim_id],
          redact_fields: [],
          columns: %{id: %{type: :integer}, dim_id: %{type: :integer}},
          associations: %{
            dim: %{queryable: :dims, field: :dim, owner_key: :dim_id, related_key: :id}
          }
        },
        schemas: %{
          dims: %{
            source_table: "dims",
            fields: [:id, :name],
            redact_fields: [],
            columns: %{id: %{type: :integer}, name: %{type: :string}},
            associations: %{}
          }
        },
        joins: %{dim: %{type: :star_dimension, display_field: :name}}
      }

      assert_raise ValidationError, fn ->
        Selecto.configure(invalid_domain, mock_conn())
      end
    end

    test "rejects self hierarchy with invalid queryable reference" do
      invalid_domain = %{
        name: "Invalid hierarchy",
        source: %{
          source_table: "nodes",
          primary_key: :id,
          fields: [:id, :parent_id],
          redact_fields: [],
          columns: %{id: %{type: :integer}, parent_id: %{type: :integer}},
          associations: %{
            parent: %{queryable: :nodes, field: :parent, owner_key: :parent_id, related_key: :id}
          }
        },
        schemas: %{},
        joins: %{parent: %{type: :hierarchical, hierarchy_type: :adjacency_list}}
      }

      assert_raise ValidationError, fn ->
        Selecto.configure(invalid_domain, mock_conn())
      end
    end
  end

  describe "query generation edge cases" do
    test "supports star dimensions with explicit display fields" do
      domain = %{
        name: "Star dimension",
        source: %{
          source_table: "facts",
          primary_key: :id,
          fields: [:id, :amount, :customer_id],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            amount: %{type: :decimal},
            customer_id: %{type: :integer}
          },
          associations: %{
            customer: %{
              queryable: :customers,
              field: :customer,
              owner_key: :customer_id,
              related_key: :id
            }
          }
        },
        schemas: %{
          customers: %{
            source_table: "customers",
            primary_key: :id,
            fields: [:id, :name],
            redact_fields: [],
            columns: %{id: %{type: :integer}, name: %{type: :string}},
            associations: %{}
          }
        },
        joins: %{customer: %{type: :star_dimension, display_field: :name}}
      }

      {sql, _params} =
        domain
        |> Selecto.configure(mock_conn())
        |> Selecto.select(["customer_display", "amount"])
        |> Selecto.group_by(["customer_display"])
        |> Selecto.to_sql()

      sql = String.downcase(sql)
      assert String.contains?(sql, "left join customers")
      assert String.contains?(sql, "customer.name")
    end

    test "rejects malicious selector names" do
      domain = %{
        name: "Selector validation",
        source: %{
          source_table: "users",
          primary_key: :id,
          fields: [:id, :name],
          redact_fields: [],
          columns: %{id: %{type: :integer}, name: %{type: :string}},
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }

      selecto = Selecto.configure(domain, mock_conn())

      assert_raise RuntimeError, fn ->
        selecto
        |> Selecto.select(["name; DROP TABLE users; --"])
        |> Selecto.to_sql()
      end
    end

    test "handles wide star schema configuration" do
      dimension_count = 25

      dimensions =
        for i <- 1..dimension_count, into: %{} do
          {"dim_#{i}" |> String.to_atom(),
           %{
             source_table: "dim_#{i}",
             primary_key: :id,
             fields: [:id, :name],
             redact_fields: [],
             columns: %{id: %{type: :integer}, name: %{type: :string}},
             associations: %{}
           }}
        end

      associations =
        for i <- 1..dimension_count, into: %{} do
          dim = "dim_#{i}" |> String.to_atom()

          {dim,
           %{
             queryable: dim,
             field: dim,
             owner_key: String.to_atom("dim_#{i}_id"),
             related_key: :id
           }}
        end

      joins =
        for i <- 1..dimension_count, into: %{} do
          dim = "dim_#{i}" |> String.to_atom()
          {dim, %{type: :star_dimension, display_field: :name}}
        end

      source_fields =
        [:id, :amount] ++ Enum.map(1..dimension_count, &String.to_atom("dim_#{&1}_id"))

      source_columns =
        Enum.reduce(1..dimension_count, %{id: %{type: :integer}, amount: %{type: :decimal}}, fn i,
                                                                                                acc ->
          Map.put(acc, String.to_atom("dim_#{i}_id"), %{type: :integer})
        end)

      domain = %{
        name: "Wide star schema",
        source: %{
          source_table: "facts",
          primary_key: :id,
          fields: source_fields,
          redact_fields: [],
          columns: source_columns,
          associations: associations
        },
        schemas: dimensions,
        joins: joins
      }

      {sql, _params} =
        domain
        |> Selecto.configure(mock_conn())
        |> Selecto.select(["dim_1_display", "dim_20_display", "amount"])
        |> Selecto.group_by(["dim_1_display", "dim_20_display"])
        |> Selecto.to_sql()

      assert is_binary(sql)
      assert String.contains?(sql, "dim_1")
      assert String.contains?(sql, "dim_20")
    end
  end
end
