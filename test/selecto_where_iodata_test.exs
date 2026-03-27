defmodule Selecto.Builder.Sql.WhereTest do
  use ExUnit.Case

  alias Selecto.Builder.Sql.Where
  alias Selecto.SQL.Params

  @domain %{
    name: "Where Test Domain",
    source: %{
      source_table: "users",
      primary_key: :id,
      fields: [:id, :name, :description, :active, :count, :user_id, :created_at],
      redact_fields: [],
      columns: %{
        id: %{type: :integer},
        name: %{type: :string},
        description: %{type: :string},
        active: %{type: :boolean},
        count: %{type: :integer},
        user_id: %{type: :id},
        created_at: %{type: :date}
      },
      associations: %{}
    },
    schemas: %{},
    joins: %{}
  }

  defp selecto do
    Selecto.configure(@domain, :mock_connection)
  end

  defp sqlite_fts_selecto do
    selecto = Map.put(selecto(), :adapter, SelectoDBSQLite.Adapter)

    selecto
    |> put_in([Access.key(:config), Access.key(:columns), "name"], %{
      name: "Where Test Domain: Name Search",
      type: :fts5,
      field: :name,
      requires_join: :selecto_root,
      colid: "name"
    })
    |> put_in([Access.key(:config), Access.key(:columns), "description"], %{
      name: "Where Test Domain: Description Search",
      type: :fts5,
      field: :description,
      requires_join: :selecto_root,
      colid: "description"
    })
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
      {_joins, and_iodata, _} =
        Where.build(selecto(), {:and, [{"name", "John"}, {"active", true}]})

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

      mysql_selecto = Map.put(selecto(), :adapter, SelectoDBMySQL.Adapter)

      {_joins, mysql_ts_iodata, _} =
        Where.build(mysql_selecto, {"name", {:text_search, "wireless charger"}})

      {mysql_ts_sql, mysql_ts_params} =
        Params.finalize(mysql_ts_iodata, adapter: SelectoDBMySQL.Adapter)

      assert mysql_ts_sql =~ ~r/MATCH\(.*name.*\) AGAINST \(\? IN NATURAL LANGUAGE MODE\)/i
      assert mysql_ts_params == ["wireless charger"]

      {_joins, mysql_multi_ts_iodata, _} =
        Where.build(mysql_selecto, {["name", "description"], {:text_search, "wireless charger"}})

      {mysql_multi_ts_sql, mysql_multi_ts_params} =
        Params.finalize(mysql_multi_ts_iodata, adapter: SelectoDBMySQL.Adapter)

      assert mysql_multi_ts_sql =~
               ~r/MATCH\(.*name.*, .*description.*\) AGAINST \(\? IN NATURAL LANGUAGE MODE\)/i

      assert mysql_multi_ts_params == ["wireless charger"]

      {_joins, mysql_boolean_ts_iodata, _} =
        Where.build(
          mysql_selecto,
          {"name", {:text_search, "+wireless -charger", [mode: :boolean]}}
        )

      {mysql_boolean_ts_sql, mysql_boolean_ts_params} =
        Params.finalize(mysql_boolean_ts_iodata, adapter: SelectoDBMySQL.Adapter)

      assert mysql_boolean_ts_sql =~ ~r/MATCH\(.*name.*\) AGAINST \(\? IN BOOLEAN MODE\)/i
      assert mysql_boolean_ts_params == ["+wireless -charger"]

      assert Selecto.AdapterSupport.supports_feature?(
               SelectoDBMySQL.Adapter,
               :text_search_boolean
             )

      assert Selecto.AdapterSupport.supports_feature?(
               SelectoDBMySQL.Adapter,
               :text_search_boolean_mode
             )

      {_joins, mysql_expansion_ts_iodata, _} =
        Where.build(
          mysql_selecto,
          {"name", {:text_search, "wireless charger", [mode: :query_expansion]}}
        )

      {mysql_expansion_ts_sql, mysql_expansion_ts_params} =
        Params.finalize(mysql_expansion_ts_iodata, adapter: SelectoDBMySQL.Adapter)

      assert mysql_expansion_ts_sql =~
               ~r/MATCH\(.*name.*\) AGAINST \(\? IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION\)/i

      assert mysql_expansion_ts_params == ["wireless charger"]

      assert Selecto.AdapterSupport.supports_feature?(
               SelectoDBMySQL.Adapter,
               :text_search_query_expansion
             )

      assert Selecto.AdapterSupport.supports_feature?(
               SelectoDBMySQL.Adapter,
               :text_search_query_expansion_mode
             )

      {_joins, mysql_keyword_ts_iodata, _} =
        Where.build(
          mysql_selecto,
          {"name",
           {:text_search,
            [query: "wireless charger", fields: ["name", "description"], mode: :boolean]}}
        )

      {mysql_keyword_ts_sql, mysql_keyword_ts_params} =
        Params.finalize(mysql_keyword_ts_iodata, adapter: SelectoDBMySQL.Adapter)

      assert mysql_keyword_ts_sql =~
               ~r/MATCH\(.*name.*, .*description.*\) AGAINST \(\? IN BOOLEAN MODE\)/i

      assert mysql_keyword_ts_params == ["wireless charger"]

      {_joins, pg_boolean_legacy_iodata, _} =
        Where.build(selecto(), {"name", {:text_search, "term", [mode: :boolean]}})

      {pg_boolean_legacy_sql, pg_boolean_legacy_params} =
        Params.finalize(pg_boolean_legacy_iodata)

      assert pg_boolean_legacy_sql =~ ~r/name\s+@@\s+to_tsquery\(\$1\)/i
      assert pg_boolean_legacy_params == ["term"]

      assert_raise RuntimeError, ~r/does not support this text search mode/, fn ->
        Where.build(selecto(), {"name", {:text_search, "term", [mode: :query_expansion]}})
      end

      assert_raise ArgumentError, ~r/requires a :query option/, fn ->
        Where.build(mysql_selecto, {"name", {:text_search, [mode: :boolean]}})
      end

      sqlite_selecto = Map.put(selecto(), :adapter, SelectoDBSQLite.Adapter)

      assert_raise RuntimeError, ~r/requires an FTS5-configured field/i, fn ->
        Where.build(sqlite_selecto, {"name", {:text_search, "term"}})
      end

      {_joins, sqlite_fts_iodata, _} =
        Where.build(sqlite_fts_selecto(), {"name", {:text_search, "term"}})

      {sqlite_fts_sql, sqlite_fts_params} =
        Params.finalize(sqlite_fts_iodata, adapter: SelectoDBSQLite.Adapter)

      assert sqlite_fts_sql =~ ~r/name\s+MATCH\s+\?/i
      assert sqlite_fts_params == ["term"]

      {_joins, sqlite_multi_fts_iodata, _} =
        Where.build(
          sqlite_fts_selecto(),
          {["name", "description"], {:text_search, "wireless charger"}}
        )

      {sqlite_multi_fts_sql, sqlite_multi_fts_params} =
        Params.finalize(sqlite_multi_fts_iodata, adapter: SelectoDBSQLite.Adapter)

      assert sqlite_multi_fts_sql =~ ~r/name\s+MATCH\s+\?/i
      assert sqlite_multi_fts_sql =~ ~r/description\s+MATCH\s+\?/i
      assert sqlite_multi_fts_sql =~ ~r/\sOR\s/i
      assert sqlite_multi_fts_params == ["wireless charger", "wireless charger"]

      {_joins, sqlite_boolean_fts_iodata, _} =
        Where.build(sqlite_fts_selecto(), {"name", {:text_search, "term", [mode: :boolean]}})

      {sqlite_boolean_fts_sql, sqlite_boolean_fts_params} =
        Params.finalize(sqlite_boolean_fts_iodata, adapter: SelectoDBSQLite.Adapter)

      assert sqlite_boolean_fts_sql =~ ~r/name\s+MATCH\s+\?/i
      assert sqlite_boolean_fts_params == ["term"]

      {_joins, pg_web_alias_iodata, _} =
        Where.build(selecto(), {"name", {:text_search, "term", [mode: :web]}})

      {pg_web_alias_sql, pg_web_alias_params} = Params.finalize(pg_web_alias_iodata)

      assert pg_web_alias_sql =~ ~r/name\s+@@\s+websearch_to_tsquery\(\$1\)/i
      assert pg_web_alias_params == ["term"]

      {_joins, pg_plain_iodata, _} =
        Where.build(selecto(), {"name", {:text_search, "term", [mode: :plain]}})

      {pg_plain_sql, pg_plain_params} = Params.finalize(pg_plain_iodata)

      assert pg_plain_sql =~ ~r/name\s+@@\s+plainto_tsquery\(\$1\)/i
      assert pg_plain_params == ["term"]

      {_joins, pg_phrase_iodata, _} =
        Where.build(selecto(), {"name", {:text_search, "term", [mode: :phrase]}})

      {pg_phrase_sql, pg_phrase_params} = Params.finalize(pg_phrase_iodata)

      assert pg_phrase_sql =~ ~r/name\s+@@\s+phraseto_tsquery\(\$1\)/i
      assert pg_phrase_params == ["term"]

      {_joins, pg_natural_iodata, _} =
        Where.build(selecto(), {"name", {:text_search, "term", [mode: :natural]}})

      {pg_natural_sql, pg_natural_params} = Params.finalize(pg_natural_iodata)

      assert pg_natural_sql =~ ~r/name\s+@@\s+plainto_tsquery\(\$1\)/i
      assert pg_natural_params == ["term"]

      {_joins, pg_boolean_iodata, _} =
        Where.build(selecto(), {"name", {:text_search, "foo & bar", [mode: :boolean]}})

      {pg_boolean_sql, pg_boolean_params} = Params.finalize(pg_boolean_iodata)

      assert pg_boolean_sql =~ ~r/name\s+@@\s+to_tsquery\(\$1\)/i
      assert pg_boolean_params == ["foo & bar"]

      assert_raise RuntimeError, ~r/does not support this text search mode/i, fn ->
        Where.build(
          sqlite_fts_selecto(),
          {"name", {:text_search, "term", [mode: :query_expansion]}}
        )
      end

      subquery = "SELECT id FROM users WHERE active = true"
      {_joins, sq_iodata, _} = Where.build(selecto(), {"id", {:subquery, :in, subquery, []}})
      {sq_sql, _sq_params} = Params.finalize(sq_iodata)
      assert sq_sql =~ ~r/\sin\s*\(/i
      assert sq_sql =~ subquery

      param_subquery = "SELECT id FROM users WHERE name = $1"

      {_joins, psq_iodata, psq_params} =
        Where.build(selecto(), {"id", {:subquery, :in, param_subquery, ["Jane"]}})

      {psq_sql, psq_finalized_params} = Params.finalize(psq_iodata)
      assert psq_sql =~ ~r/name\s*=\s*\$1/i
      assert psq_finalized_params == ["Jane"]
      assert psq_params == ["Jane"]

      structured_subquery =
        Selecto.configure(@domain, :mock_connection)
        |> Selecto.select(["id"])
        |> Selecto.filter({"name", "Jane"})

      {_joins, structured_sq_iodata, _} =
        Where.build(selecto(), {"id", {:subquery, :in, structured_subquery}})

      {structured_sq_sql, structured_sq_params} = Params.finalize(structured_sq_iodata)
      assert structured_sq_sql =~ ~r/in\s*\(\s*select/i
      assert structured_sq_sql =~ ~r/from\s+users\s+subq_root_users/i
      assert structured_sq_params == ["Jane"]
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

    test "exists, raw_sql_filter, and empty conjunction" do
      {_joins, exists_iodata, exists_params} =
        Where.build(selecto(), {:exists, "SELECT 1", ["x"]})

      {exists_sql, exists_finalized_params} = Params.finalize(exists_iodata)
      assert exists_sql =~ ~r/exists\s*\(/i
      assert exists_finalized_params == []
      assert exists_params == ["x"]

      {_joins, param_exists_iodata, param_exists_params} =
        Where.build(selecto(), {:exists, "SELECT 1 FROM users WHERE name = $1", ["x"]})

      {param_exists_sql, param_exists_finalized_params} = Params.finalize(param_exists_iodata)
      assert param_exists_sql =~ ~r/name\s*=\s*\$1/i
      assert param_exists_finalized_params == ["x"]
      assert param_exists_params == ["x"]

      structured_exists_subquery =
        Selecto.configure(@domain, :mock_connection)
        |> Selecto.select(["id"])
        |> Selecto.filter({"active", true})

      {_joins, structured_exists_iodata, _} =
        Where.build(selecto(), {:exists, structured_exists_subquery})

      {structured_exists_sql, structured_exists_params} =
        Params.finalize(structured_exists_iodata)

      assert structured_exists_sql =~ ~r/exists\s*\(\s*select/i
      assert structured_exists_sql =~ ~r/from\s+users\s+subq_root_users/i
      assert structured_exists_params == [true]

      {_joins, raw_iodata, raw_params} =
        Where.build(selecto(), {:raw_sql_filter, [" users.id = 1 "]})

      {raw_sql, _} = Params.finalize(raw_iodata)
      assert String.contains?(raw_sql, "users.id = 1")
      assert raw_params == []

      assert {[], [], []} = Where.build(selecto(), {:and, []})
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

  describe "additional branches" do
    test "datetime between uses half-open range" do
      {_joins, iodata, _params} =
        Where.build(
          selecto(),
          {"created_at", {:between, "2024-01-01", "2024-02-01"}}
        )

      {sql, params} = Params.finalize(iodata)
      assert sql =~ ~r/>=\s*\$1/
      assert sql =~ ~r/<\s*\$2/
      assert length(params) == 2
    end

    test "alternate operators and string operators compile" do
      {_joins, gt_iodata, _} = Where.build(selecto(), {"count", {:gt, 10}})
      {gt_sql, gt_params} = Params.finalize(gt_iodata)
      assert gt_sql =~ ~r/>\s*\$1/
      assert gt_params == [10]

      {_joins, eq_iodata, _} = Where.build(selecto(), {"count", {"=", "12"}})
      {eq_sql, eq_params} = Params.finalize(eq_iodata)
      assert eq_sql =~ ~r/=\s*\$1/
      assert eq_params == [12]
    end

    test "mysql in/not_in paths" do
      mysql_selecto = Map.put(selecto(), :adapter, SelectoDBMySQL.Adapter)

      {_joins, in_iodata, _} = Where.build(mysql_selecto, {"id", {:in, [1, 2]}})
      {in_sql, _} = Params.finalize(in_iodata)
      assert in_sql =~ ~r/\sin\s*\(/i

      {_joins, not_in_iodata, _} = Where.build(mysql_selecto, {"id", {:not_in, [1, 2]}})
      {not_in_sql, _} = Params.finalize(not_in_iodata)
      assert not_in_sql =~ ~r/not\s+in\s*\(/i
    end

    test "unrecognized filters raise meaningful errors" do
      assert_raise RuntimeError, ~r/Bucket ranges string was passed as a filter/, fn ->
        Where.build(selecto(), "10-20,21+")
      end

      assert_raise RuntimeError, ~r/Unrecognized filter structure/, fn ->
        Where.build(selecto(), %{bad: :shape})
      end
    end
  end
end
