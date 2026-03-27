defmodule Selecto.Builder.JsonOperationsTest do
  use ExUnit.Case, async: true

  alias Selecto.Advanced.JsonOperations
  alias Selecto.Builder.JsonOperations, as: BuilderJsonOperations

  test "mysql rejects postgres-only json_contained filters explicitly" do
    spec =
      JsonOperations.create_json_operation(:json_contained, "metadata",
        value: %{"price_band" => "premium"}
      )

    assert_raise RuntimeError, ~r/does not support this JSON operation/, fn ->
      BuilderJsonOperations.build_json_filter(spec, adapter: SelectoDBMySQL.Adapter)
    end
  end

  test "mysql rejects postgres-only jsonb select operations explicitly" do
    spec = JsonOperations.create_json_operation(:jsonb_agg, "metadata", as: "metadata_items")

    assert_raise RuntimeError, ~r/does not support this JSON operation/, fn ->
      BuilderJsonOperations.build_json_select(spec, adapter: SelectoDBMySQL.Adapter)
    end
  end

  test "mssql rejects unsupported json aggregate and construction operations explicitly" do
    agg_spec = JsonOperations.create_json_operation(:json_agg, "metadata", as: "metadata_items")

    assert_raise RuntimeError, ~r/does not support this JSON operation/, fn ->
      BuilderJsonOperations.build_json_select(agg_spec, adapter: SelectoDBMSSQL.Adapter)
    end

    build_spec =
      JsonOperations.create_json_operation(:json_build_object, nil, value: [{"color", "red"}])

    assert_raise RuntimeError, ~r/does not support this JSON operation/, fn ->
      BuilderJsonOperations.build_json_select(build_spec, adapter: SelectoDBMSSQL.Adapter)
    end
  end

  test "mssql rejects unsupported json containment direction explicitly" do
    spec =
      JsonOperations.create_json_operation(:json_contained, "metadata",
        value: %{"price_band" => "premium"}
      )

    assert_raise RuntimeError, ~r/does not support this JSON operation/, fn ->
      BuilderJsonOperations.build_json_filter(spec, adapter: SelectoDBMSSQL.Adapter)
    end
  end

  test "sqlite rejects unsupported json containment and aggregate operations explicitly" do
    contains_spec =
      JsonOperations.create_json_operation(:json_contains, "metadata",
        value: %{"price_band" => "premium"}
      )

    contains_sql =
      BuilderJsonOperations.build_json_filter(contains_spec, adapter: SelectoDBSQLite.Adapter)
      |> IO.iodata_to_binary()

    assert contains_sql =~ ~r/json_extract\("metadata", '\$\.price_band'\) = 'premium'/i

    agg_spec = JsonOperations.create_json_operation(:json_agg, "metadata", as: "metadata_items")

    assert_raise RuntimeError, ~r/does not support this JSON operation/, fn ->
      BuilderJsonOperations.build_json_select(agg_spec, adapter: SelectoDBSQLite.Adapter)
    end
  end

  test "sqlite supports json_typeof and json_array_length selects" do
    typeof_spec =
      JsonOperations.create_json_operation(:json_typeof, "metadata",
        path: "$.details.priority",
        as: "priority_type"
      )

    array_length_spec =
      JsonOperations.create_json_operation(:json_array_length, "metadata",
        path: "$.items",
        as: "item_count"
      )

    typeof_sql =
      BuilderJsonOperations.build_json_select(typeof_spec, adapter: SelectoDBSQLite.Adapter)
      |> IO.iodata_to_binary()

    array_length_sql =
      BuilderJsonOperations.build_json_select(array_length_spec, adapter: SelectoDBSQLite.Adapter)
      |> IO.iodata_to_binary()

    assert typeof_sql =~ ~r/json_type\("metadata", '\$\.details\.priority'\)/i
    assert typeof_sql =~ ~r/as "priority_type"/i
    assert array_length_sql =~ ~r/json_array_length\("metadata", '\$\.items'\)/i
    assert array_length_sql =~ ~r/as "item_count"/i
  end
end
