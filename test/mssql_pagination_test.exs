defmodule Selecto.MSSQLPaginationTest do
  use ExUnit.Case, async: true

  test "standard queries use OFFSET FETCH pagination for MSSQL" do
    query =
      domain()
      |> Selecto.configure(:mock_connection, adapter: SelectoDBMSSQL.Adapter, validate: false)
      |> Selecto.select(["id", "name"])
      |> Selecto.order_by([{"id", :asc}])
      |> Selecto.limit(2)
      |> Selecto.offset(1)

    {sql, _params} = Selecto.to_sql(query)
    normalized_sql = normalize_sql(sql)

    assert normalized_sql =~ "order by selecto_root.id asc offset 1 rows fetch next 2 rows only"
    refute normalized_sql =~ " limit "
  end

  test "queries without explicit order use MSSQL fallback ordering for pagination" do
    query =
      domain()
      |> Selecto.configure(:mock_connection, adapter: SelectoDBMSSQL.Adapter, validate: false)
      |> Selecto.select(["id"])
      |> Selecto.offset(3)

    {sql, _params} = Selecto.to_sql(query)
    normalized_sql = normalize_sql(sql)

    assert normalized_sql =~ "order by (select 1) offset 3 rows"
  end

  test "set operations use MSSQL OFFSET FETCH pagination" do
    left_query =
      domain()
      |> Selecto.configure(:mock_connection, adapter: SelectoDBMSSQL.Adapter, validate: false)
      |> Selecto.select(["name"])

    right_query =
      domain()
      |> Selecto.configure(:mock_connection, adapter: SelectoDBMSSQL.Adapter, validate: false)
      |> Selecto.select(["name"])

    {sql, _params} =
      left_query
      |> Selecto.union(right_query, all: true)
      |> Selecto.order_by([{"name", :asc}])
      |> Selecto.limit(5)
      |> Selecto.offset(2)
      |> Selecto.to_sql()

    normalized_sql = normalize_sql(sql)

    assert normalized_sql =~ "union all"
    assert normalized_sql =~ "order by selecto_root.name asc"
    assert normalized_sql =~ "offset 2 rows fetch next 5 rows only"
    refute normalized_sql =~ " limit "
  end

  defp normalize_sql(sql) do
    sql
    |> String.downcase()
    |> String.replace(~r/\s+/, " ")
    |> String.trim()
  end

  defp domain do
    %{
      name: "MSSQL Pagination Domain",
      source: %{
        source_table: "items",
        primary_key: :id,
        fields: [:id, :name],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      filters: %{},
      default_selected: ["id", "name"]
    }
  end
end
