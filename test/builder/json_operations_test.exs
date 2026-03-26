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
end
