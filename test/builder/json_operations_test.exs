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
end
