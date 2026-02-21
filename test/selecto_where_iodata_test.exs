defmodule Selecto.Builder.Sql.WhereTest do
  use ExUnit.Case

  alias Selecto.Builder.Sql.Where
  alias Selecto.SQL.Params

  @domain %{
    name: "Where Test Domain",
    source: %{
      source_table: "users",
      primary_key: :id,
      fields: [:id, :name, :active, :count, :user_id],
      redact_fields: [],
      columns: %{
        id: %{type: :integer},
        name: %{type: :string},
        active: %{type: :boolean},
        count: %{type: :integer},
        user_id: %{type: :id}
      },
      associations: %{}
    },
    schemas: %{},
    joins: %{}
  }

  defp selecto do
    Selecto.configure(@domain, :mock_connection)
  end

  describe "basic comparisons" do
    test "simple equality" do
      {_joins, iodata, _params} = Where.build(selecto(), {"name", "John"})
      {sql, params} = Params.finalize(iodata)
      assert sql =~ ~r/=\s*\$1/
      assert params == ["John"]
    end

    test "between" do
      {_joins, iodata, _params} = Where.build(selecto(), {"id", {:between, 1, 10}})
      {sql, params} = Params.finalize(iodata)
      assert sql =~ ~r/between\s+\$1\s+and\s+\$2/i
      assert params == [1, 10]
    end

    test "list membership" do
      {_joins, iodata, _params} = Where.build(selecto(), {"id", [1, 2, 3]})
      {sql, params} = Params.finalize(iodata)
      assert sql =~ ~r/any\(\$1\)/i
      assert params == [[1, 2, 3]]
    end
  end

  describe "logical combinations" do
    test "and/or/not render" do
      {_joins, and_iodata, _} = Where.build(selecto(), {:and, [{"name", "John"}, {"active", true}]})
      {and_sql, and_params} = Params.finalize(and_iodata)
      assert and_sql =~ ~r/\sand\s/i
      assert and_params == ["John", true]

      {_joins, or_iodata, _} = Where.build(selecto(), {:or, [{"name", "John"}, {"name", "Jane"}]})
      {or_sql, or_params} = Params.finalize(or_iodata)
      assert or_sql =~ ~r/\sor\s/i
      assert or_params == ["John", "Jane"]

      {_joins, not_iodata, _} = Where.build(selecto(), {:not, {"active", true}})
      {not_sql, not_params} = Params.finalize(not_iodata)
      assert not_sql =~ ~r/not/i
      assert not_params == [true]
    end
  end

  describe "special operators" do
    test "text search and subquery" do
      {_joins, ts_iodata, _} = Where.build(selecto(), {"name", {:text_search, "term"}})
      {ts_sql, ts_params} = Params.finalize(ts_iodata)
      assert ts_sql =~ ~r/websearch_to_tsquery/i
      assert ts_params == ["term"]

      subquery = "SELECT id FROM users WHERE active = true"
      {_joins, sq_iodata, _} = Where.build(selecto(), {"id", {:subquery, :in, subquery, []}})
      {sq_sql, _sq_params} = Params.finalize(sq_iodata)
      assert sq_sql =~ ~r/\sin\s*\(/i
      assert sq_sql =~ subquery
    end

    test "null checks" do
      {_joins, null_iodata, null_params} = Where.build(selecto(), {"name", nil})
      {null_sql, _} = Params.finalize(null_iodata)
      assert null_sql =~ ~r/is\s+null/i
      assert null_params == []

      {_joins, nn_iodata, nn_params} = Where.build(selecto(), {"name", :not_null})
      {nn_sql, _} = Params.finalize(nn_iodata)
      assert nn_sql =~ ~r/is\s+not\s+null/i
      assert nn_params == []
    end
  end

  describe "type conversion" do
    test "integer and id values cast" do
      {_joins, int_iodata, _} = Where.build(selecto(), {"count", {"=", "123"}})
      {_sql, int_params} = Params.finalize(int_iodata)
      assert int_params == [123]

      {_joins, id_iodata, _} = Where.build(selecto(), {"user_id", {"=", "456"}})
      {_sql, id_params} = Params.finalize(id_iodata)
      assert id_params == [456]
    end
  end
end
