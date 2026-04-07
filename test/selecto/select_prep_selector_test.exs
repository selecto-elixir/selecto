defmodule Selecto.SelectPrepSelectorTest do
  use ExUnit.Case, async: true

  alias Selecto.Builder.Sql.Select
  alias Selecto.SQL.Params

  defp selecto do
    domain = %{
      name: "Select prep",
      source: %{
        source_table: "users",
        primary_key: :id,
        fields: [:id, :name, :active, :created_at],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          active: %{type: :boolean},
          created_at: %{type: :date}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }

    Selecto.configure(domain, :mock_connection)
  end

  defp mssql_selecto do
    selecto()
    |> Map.put(:adapter, SelectoDBMSSQL.Adapter)
  end

  defp finalize(iodata) do
    Params.finalize(iodata)
  end

  defp finalize_mssql(iodata) do
    Params.finalize(iodata, adapter: SelectoDBMSSQL.Adapter)
  end

  test "parameterized scalar selectors" do
    {int_iodata, _join, int_params} = Select.prep_selector(selecto(), 7)
    {int_sql, finalized_int_params} = finalize(int_iodata)
    assert int_sql =~ ~r/^\$1$/
    assert int_params == [7]
    assert finalized_int_params == [7]

    {float_iodata, _join, float_params} = Select.prep_selector(selecto(), 7.5)
    {float_sql, finalized_float_params} = finalize(float_iodata)
    assert float_sql =~ ~r/^\$1$/
    assert float_params == [7.5]
    assert finalized_float_params == [7.5]

    {bool_iodata, _join, bool_params} = Select.prep_selector(selecto(), false)
    {bool_sql, finalized_bool_params} = finalize(bool_iodata)
    assert bool_sql =~ ~r/^\$1$/
    assert bool_params == [false]
    assert finalized_bool_params == [false]
  end

  test "literal and built-in function selectors" do
    {star, _join, []} = Select.prep_selector(selecto(), {:literal, "*"})
    assert IO.iodata_to_binary(star) == "*"

    {lit, _join, []} = Select.prep_selector(selecto(), {:literal, "O'Reilly"})
    assert IO.iodata_to_binary(lit) == "'O''Reilly'"

    {lit_pos, _join, []} = Select.prep_selector(selecto(), {:literal_position, 2})
    assert IO.iodata_to_binary(lit_pos) == "2"

    {count_sql, _join, []} = Select.prep_selector(selecto(), {:count})
    assert IO.iodata_to_binary(count_sql) =~ "count(*)"

    {raw_sql, _join, []} = Select.prep_selector(selecto(), {:raw_sql, "COALESCE(users.name, '')"})
    assert IO.iodata_to_binary(raw_sql) == "COALESCE(users.name, '')"
  end

  test "wrapper functions and special tuple selectors" do
    {upper_sql, _join, []} = Select.prep_selector(selecto(), {:upper, "name"})
    assert IO.iodata_to_binary(upper_sql) =~ ~r/upper\(/i

    {distinct_sql, _join, []} = Select.prep_selector(selecto(), {:count_distinct, "name"})
    assert IO.iodata_to_binary(distinct_sql) =~ "COUNT(DISTINCT"

    {coalesce_sum_sql, _join, coalesce_sum_params} =
      Select.prep_selector(selecto(), {:sum, {:coalesce, ["id", 0]}})

    {coalesce_sum_sql_text, finalized_coalesce_sum_params} = finalize(coalesce_sum_sql)
    assert coalesce_sum_sql_text =~ ~r/sum\(/i
    assert coalesce_sum_sql_text =~ ~r/coalesce\(/i
    assert coalesce_sum_params == [0]
    assert finalized_coalesce_sum_params == [0]

    {to_char_sql, _join, []} =
      Select.prep_selector(selecto(), {:to_char, {"created_at", "YYYY-MM"}})

    assert IO.iodata_to_binary(to_char_sql) =~ ~r/to_char\(/i

    {concat_sql, _join, []} =
      Select.prep_selector(selecto(), {:concat, ["name", {:literal, "!"}]})

    assert IO.iodata_to_binary(concat_sql) =~ ~r/concat\(/i
  end

  test "func selector DSL supports multi-arg, distinct, filter, and alias options" do
    {agg_sql, _join, []} =
      Select.prep_selector(selecto(), {:func, "string_agg", ["name", {:literal, ", "}]})

    assert IO.iodata_to_binary(agg_sql) =~ ~r/string_agg\(/i

    {distinct_sql, _join, []} =
      Select.prep_selector(selecto(), {:func, "COUNT", ["DISTINCT", "name"]})

    assert IO.iodata_to_binary(distinct_sql) =~ ~r/COUNT\(DISTINCT/i

    {filter_sql, _join, filter_params} =
      Select.prep_selector(selecto(), {:func, "COUNT", ["*"], filter: [{"active", true}]})

    {filter_sql_text, finalized_filter_params} = finalize(filter_sql)
    assert filter_sql_text =~ ~r/FILTER \(where/i
    assert is_list(filter_params)
    assert true in finalized_filter_params

    {built_sql, _join, _params, as_alias} =
      Select.build(selecto(), {:func, "COUNT", ["*"], as: "total_count"}, %{})

    assert as_alias == "total_count"
    assert IO.iodata_to_binary(built_sql) == "COUNT(*)"
  end

  test "mssql rewrites aggregate FILTER clauses to CASE expressions" do
    {count_sql, _join, _count_params} =
      Select.prep_selector(mssql_selecto(), {:func, "COUNT", ["*"], filter: [{"active", true}]})

    {count_sql_text, _finalized_count_params} = finalize_mssql(count_sql)
    assert count_sql_text =~ ~r/COUNT\(CASE WHEN/i
    refute count_sql_text =~ ~r/FILTER \(where/i

    {avg_sql, _join, _avg_params} =
      Select.prep_selector(mssql_selecto(), {:avg, "created_at", {"active", true}})

    {avg_sql_text, _finalized_avg_params} = finalize_mssql(avg_sql)
    assert avg_sql_text =~ ~r/AVG\(CASE WHEN/i
    refute avg_sql_text =~ ~r/FILTER \(where/i
  end

  test "filtered count selectors compile for boolean true and false" do
    {true_count_sql, _join, true_count_params} =
      Select.prep_selector(selecto(), {:count, "active", {"active", true}})

    {true_count_sql_text, finalized_true_count_params} = finalize(true_count_sql)
    assert true_count_sql_text =~ ~r/FILTER \(where/i
    assert true in finalized_true_count_params
    assert true_count_params == []

    {false_count_sql, _join, false_count_params} =
      Select.prep_selector(selecto(), {:count, "active", {"active", false}})

    {false_count_sql_text, finalized_false_count_params} = finalize(false_count_sql)
    assert false_count_sql_text =~ ~r/FILTER \(where/i
    assert false in finalized_false_count_params
    assert false_count_params == []
  end

  test "mssql raises on unsupported aggregate FILTER shapes" do
    assert_raise RuntimeError,
                 ~r/MSSQL does not support this aggregate FILTER shape yet/,
                 fn ->
                   Select.prep_selector(
                     mssql_selecto(),
                     {:func, "jsonb_object_agg", ["name", {:literal, "x"}],
                      filter: [{"active", true}]}
                   )
                 end
  end

  test "func selector DSL handles mixed literal and field aggregate args" do
    {json_obj_sql, _join, []} =
      Select.prep_selector(selecto(), {:func, "jsonb_object_agg", ["name", {:literal, "x"}]})

    assert IO.iodata_to_binary(json_obj_sql) =~ ~r/jsonb_object_agg\(/i

    {string_agg_sql, _join, []} =
      Select.prep_selector(selecto(), {:func, "string_agg", ["name", {:literal, ", "}]})

    assert IO.iodata_to_binary(string_agg_sql) =~ ~r/string_agg\(/i
  end

  test "subquery and case selectors" do
    {subquery_sql, _join, subquery_params} =
      Select.prep_selector(selecto(), {:subquery, "SELECT 1", [10]})

    assert IO.iodata_to_binary(subquery_sql) == "SELECT 1"
    assert subquery_params == [10]

    case_expr = {:case, [{{"active", true}, {:literal, "yes"}}], {:literal, "no"}}
    {case_sql, _join, _params} = Select.prep_selector(selecto(), case_expr)
    {case_text, _case_params} = Params.finalize(case_sql)
    assert case_text =~ ~r/case/i
    assert case_text =~ ~r/when/i
    assert case_text =~ ~r/else/i
  end

  test "dynamic column resolution and error branch" do
    with_dynamic = update_in(selecto().set, &Map.put(&1, :dynamic_columns, %{"dyn_col" => true}))
    {dyn_sql, dyn_join, []} = Select.prep_selector(with_dynamic, "dyn_col")
    assert IO.iodata_to_binary(dyn_sql) == "dyn_col"
    assert dyn_join == :selecto_root

    assert_raise RuntimeError, ~r/Field 'missing_field' not found/, fn ->
      Select.prep_selector(selecto(), "missing_field")
    end

    assert_raise RuntimeError, ~r/Unsupported selector type/, fn ->
      Select.prep_selector(selecto(), %{bad: :selector})
    end
  end
end
