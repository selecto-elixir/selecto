defmodule Selecto.Builder.LateralJoinTest do
  use ExUnit.Case, async: true

  alias Selecto.Advanced.LateralJoin.Spec
  alias Selecto.Builder.LateralJoin
  alias Selecto.SQL.Params

  defp to_sql(iodata) do
    {sql, _params} = Params.finalize(iodata)
    sql
  end

  test "table-function lateral join builds iodata" do
    spec = %Spec{
      id: "lat1",
      join_type: :inner,
      subquery_builder: nil,
      table_function: {:unnest, "film.special_features"},
      alias: "features",
      correlation_refs: ["film.special_features"],
      validated: true
    }

    {sql_iodata, params} = LateralJoin.build_lateral_join(spec)
    sql = to_sql(sql_iodata)

    assert sql =~ "JOIN LATERAL"
    assert sql =~ "UNNEST(film.special_features)"
    assert params == []
  end

  test "build_lateral_joins returns all SQL parts" do
    specs = [
      %Spec{id: "lat1", join_type: :inner, subquery_builder: nil, table_function: {:unnest, "film.special_features"}, alias: "features", correlation_refs: [], validated: true},
      %Spec{id: "lat2", join_type: :left, subquery_builder: nil, table_function: {:function, :generate_series, [1, 5]}, alias: "numbers", correlation_refs: [], validated: true}
    ]

    {sql_parts, params} = LateralJoin.build_lateral_joins(specs)
    assert length(sql_parts) == 2
    assert is_list(params)
  end

  test "integrates lateral joins into base SQL" do
    base_sql = ["SELECT film.title", " FROM film"]

    specs = [
      %Spec{id: "lat1", join_type: :left, subquery_builder: nil, table_function: {:unnest, "film.special_features"}, alias: "features", correlation_refs: [], validated: true}
    ]

    {updated_sql, _params} = LateralJoin.integrate_lateral_joins_sql(base_sql, specs)
    combined = IO.iodata_to_binary(updated_sql)

    assert combined =~ "JOIN LATERAL"
    assert combined =~ "features"
  end
end
