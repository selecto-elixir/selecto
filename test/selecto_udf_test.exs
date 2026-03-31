defmodule Selecto.UDFTest do
  use ExUnit.Case, async: true

  alias Selecto.Expr, as: X
  alias Selecto.TypeSystem

  defp domain do
    %{
      name: "UDF test",
      source: %{
        source_table: "products",
        primary_key: :id,
        fields: [:id, :name, :nickname, :active],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          nickname: %{type: :string},
          active: %{type: :boolean}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      functions: %{
        "similarity" => %{
          kind: :scalar,
          sql_name: "public.similarity",
          args: [
            %{name: :left, type: :string, source: :selector},
            %{name: :right, type: :string, source: :value}
          ],
          returns: :float,
          allowed_in: [:select, :order_by]
        },
        "matches_name" => %{
          kind: :predicate,
          sql_name: "public.matches_name",
          args: [
            %{name: :name, type: :string, source: :selector},
            %{name: :pattern, type: :string, source: :value}
          ],
          returns: :boolean,
          allowed_in: [:filter]
        },
        "nearby_points" => %{
          kind: :table,
          sql_name: "gis.nearby_points",
          args: [
            %{name: :origin, type: :string, source: :selector},
            %{name: :radius_m, type: :integer, source: :value}
          ],
          returns: %{
            columns: %{
              id: %{type: :integer},
              distance_m: %{type: :float}
            }
          },
          allowed_in: [:lateral, :query_member]
        }
      }
    }
  end

  defp selecto do
    Selecto.configure(domain(), :mock_connection)
  end

  test "compiles scalar UDF selectors with bound value args" do
    assert Selecto.udf("similarity", ["name", "Acme"]) == X.udf("similarity", ["name", "Acme"])

    query =
      selecto()
      |> Selecto.select([
        X.as(Selecto.udf("similarity", ["name", "Acme"]), "name_similarity")
      ])

    {sql, params} = Selecto.to_sql(query)

    assert sql =~ ~r/public\.similarity\s*\(/i
    assert sql =~ ~r/select/i
    assert params == ["Acme"]
  end

  test "compiles predicate UDF filters with bound params" do
    query =
      selecto()
      |> Selecto.select(["name"])
      |> Selecto.filter(Selecto.udf("matches_name", ["name", "Acme%"]))

    {sql, params} = Selecto.to_sql(query)

    assert sql =~ ~r/public\.matches_name\s*\(/i
    assert sql =~ ~r/where/i
    assert params == ["Acme%"]
  end

  test "rejects select-only use of filter UDFs" do
    assert_raise ArgumentError, ~r/not allowed in :select/, fn ->
      selecto()
      |> Selecto.select([X.udf("matches_name", ["name", "Acme%"])])
    end
  end

  test "infers scalar UDF return types from registry metadata" do
    assert {:ok, :float} = TypeSystem.infer_type(selecto(), X.udf("similarity", ["name", "Acme"]))

    assert {:ok, :boolean} =
             TypeSystem.infer_type(selecto(), X.udf("matches_name", ["name", "Acme%"]))
  end

  test "compiles table UDF lateral joins and registers returned columns" do
    assert Selecto.udf_table("nearby_points", ["name", 500]) ==
             {:udf_table, "nearby_points", ["name", 500]}

    query =
      selecto()
      |> Selecto.lateral_join(
        :left,
        Selecto.udf_table("nearby_points", ["name", 500]),
        "nearby_points"
      )
      |> Selecto.select(["name", "nearby_points.distance_m"])

    {sql, params} = Selecto.to_sql(query)

    assert sql =~ ~r/gis\.nearby_points\s*\(/i
    assert sql =~ ~r/join\s+lateral/i
    assert sql =~ ~r/nearby_points\.distance_m/i
    assert params == [500]
  end

  test "compiles named lateral members backed by table UDFs" do
    query =
      selecto_with_query_member()
      |> Selecto.with_lateral(:nearby_lookup)
      |> Selecto.select(["name", "nearby_lookup.distance_m"])

    {sql, params} = Selecto.to_sql(query)

    assert sql =~ ~r/gis\.nearby_points\s*\(/i
    assert sql =~ ~r/nearby_lookup\.distance_m/i
    assert params == [250]
  end

  test "with_lateral accepts direct table UDF sources" do
    query =
      selecto()
      |> Selecto.with_lateral(Selecto.udf_table("nearby_points", ["name", 750]),
        as: "nearby_direct",
        join_type: :left
      )
      |> Selecto.select(["name", "nearby_direct.distance_m"])

    {sql, params} = Selecto.to_sql(query)

    assert sql =~ ~r/gis\.nearby_points\s*\(/i
    assert sql =~ ~r/nearby_direct\.distance_m/i
    assert params == [750]
  end

  defp selecto_with_query_member do
    domain()
    |> Map.put(:query_members, %{
      laterals: %{
        nearby_lookup: %{
          source: Selecto.udf_table("nearby_points", ["name", 250]),
          as: "nearby_lookup",
          join_type: :left
        }
      }
    })
    |> Selecto.configure(:mock_connection)
  end
end
