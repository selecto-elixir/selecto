defmodule Selecto.SQLFunctionsTest do
  use ExUnit.Case, async: true

  alias Selecto.SQL.Functions
  alias Selecto.SQL.Params

  @test_domain %{
    name: "SQL Functions Test Domain",
    source: %{
      source_table: "products",
      primary_key: :id,
      fields: [:id, :name, :description, :price, :category, :created_at, :tags],
      redact_fields: [],
      columns: %{
        id: %{type: :integer},
        name: %{type: :string},
        description: %{type: :string},
        price: %{type: :decimal},
        category: %{type: :string},
        created_at: %{type: :utc_datetime},
        tags: %{type: {:array, :string}}
      },
      associations: %{}
    },
    schemas: %{},
    default_selected: ["id", "name"],
    joins: %{},
    filters: %{}
  }

  setup do
    {:ok, selecto: Selecto.configure(@test_domain, :mock_connection)}
  end

  defp render({iodata, _joins, params}) do
    {sql, finalized_params} = Params.finalize(iodata)
    {sql, finalized_params ++ params}
  end

  describe "string functions" do
    test "basic string functions compile", %{selecto: selecto} do
      for selector <- [
            {:substr, "description", 1, 50},
            {:trim, "name"},
            {:upper, "category"},
            {:lower, "category"},
            {:length, "name"}
          ] do
        assert {sql, _params} = selecto |> Functions.prep_advanced_selector(selector) |> render()
        assert is_binary(sql)
      end
    end

    test "replace emits params", %{selecto: selecto} do
      {sql, params} =
        selecto
        |> Functions.prep_advanced_selector(
          {:replace, "description", {:literal, "old"}, {:literal, "new"}}
        )
        |> render()

      assert sql =~ ~r/replace\(/i
      assert "old" in params
      assert "new" in params
    end
  end

  describe "math/date/array functions" do
    test "common math and date functions compile", %{selecto: selecto} do
      selectors = [
        {:abs, "price"},
        {:round, "price"},
        {:sqrt, "price"},
        {:random},
        {:now},
        {:date_trunc, {:literal, "month"}, "created_at"},
        {:array_agg, "category"},
        {:array_length, "tags"},
        {:unnest, "tags"}
      ]

      for selector <- selectors do
        assert {sql, _params} = selecto |> Functions.prep_advanced_selector(selector) |> render()
        assert is_binary(sql)
      end
    end

    test "parametrized functions emit params", %{selecto: selecto} do
      assert {_sql, power_params} =
               selecto
               |> Functions.prep_advanced_selector({:power, "price", {:literal, 2}})
               |> render()

      assert 2 in power_params

      assert {_sql, arr_params} =
               selecto
               |> Functions.prep_advanced_selector({:array_to_string, "tags", {:literal, ", "}})
               |> render()

      assert Enum.any?(arr_params, fn
               ", " -> true
               {:literal, ", "} -> true
               {:literal, value} when value == ", " -> true
               {key, value} when key == :literal and value == ", " -> true
               _ -> false
             end)
    end
  end

  describe "window functions" do
    test "window functions compile", %{selecto: selecto} do
      selectors = [
        {:window, {:row_number}, over: []},
        {:window, {:rank}, over: [order_by: ["price"]]},
        {:window, {:lag, "price"}, over: [partition_by: ["category"]]},
        {:window, {:lead, "price", 2}, over: [partition_by: ["category"]]},
        {:window, {:ntile, 4}, over: [order_by: ["price"]]}
      ]

      for selector <- selectors do
        assert {sql, _params} = selecto |> Functions.prep_advanced_selector(selector) |> render()
        assert sql =~ ~r/over\s*\(/i
      end
    end
  end

  describe "conditional functions" do
    test "iif function emits branch params", %{selecto: selecto} do
      {sql, params} =
        selecto
        |> Functions.prep_advanced_selector({
          :iif,
          {"price", :gt, {:literal, 100}},
          {:literal, "expensive"},
          {:literal, "affordable"}
        })
        |> render()

      assert sql =~ ~r/case\s+when/i
      assert "expensive" in params
      assert "affordable" in params
    end

    test "decode function compiles", %{selecto: selecto} do
      mappings = [
        {{"category", :eq, {:literal, "electronics"}}, {:literal, "tech"}},
        {{"category", :eq, {:literal, "books"}}, {:literal, "literature"}}
      ]

      {sql, _params} =
        selecto
        |> Functions.prep_advanced_selector({:decode, "category", mappings})
        |> render()

      assert sql =~ ~r/decode\(/i
    end
  end

  test "unsupported selector returns nil", %{selecto: selecto} do
    assert Functions.prep_advanced_selector(selecto, {:unknown_function, "field"}) == nil
  end
end
