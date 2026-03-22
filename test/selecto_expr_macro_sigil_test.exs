defmodule Selecto.ExprMacroSigilTest do
  use ExUnit.Case, async: true

  import Selecto.ExprMacros
  import Selecto.Sigil

  test "where macro compiles filter AST with pinned values" do
    status = "active"
    min_price = 100

    assert where(status == ^status and price >= ^min_price) ==
             {:and, [{"status", "active"}, {"price", {:gte, 100}}]}
  end

  test "where macro supports functions and null checks" do
    pattern = "%chair%"

    assert where(not is_nil(deleted_at) or ilike(name, ^pattern)) ==
             {:or, [{"deleted_at", :not_null}, {"name", {:ilike, "%chair%"}}]}
  end

  test "where macro supports starts_with and ends_with helpers" do
    prefix = "Ch"
    suffix = "air"

    assert where(starts_with(name, ^prefix) and ends_with(name, ^suffix)) ==
             {:and, [{"name", {:like, "Ch%"}}, {"name", {:like, "%air"}}]}
  end

  test "select macro compiles helper-friendly selector lists" do
    assert select([name, sum(price), coalesce(nickname, name), as(count(), "total")]) == [
             {:field, "name"},
             {:func, "SUM", [{:field, "price"}]},
             {:coalesce, [{:field, "nickname"}, {:field, "name"}]},
             {:field, {:count, "*"}, "total"}
           ]
  end

  test "select macro compiles window and json helper selectors" do
    assert select([
             window(row_number(),
               over: [partition_by: [status], order_by: [desc(price)]],
               as: "row_num"
             ),
             json_extract_text(metadata, "$.warehouse.zone", as: "zone"),
             json_agg(name, as: "names"),
             json_object_agg(id, name, as: "name_map")
           ]) == [
             {:window, {:row_number},
              over: [partition_by: ["status"], order_by: [{"price", :desc}]], as: "row_num"},
             {:json_extract_text, "metadata", "$.warehouse.zone", as: "zone"},
             {:json_agg, "name", as: "names"},
             {:json_object_agg, "id", "name", as: "name_map"}
           ]
  end

  test "uppercase ~SELECTO sigil compiles filter AST" do
    min_price = 50
    pattern = "%lamp%"

    assert ~SELECTO"price >= ^min_price and ilike(name, ^pattern)" ==
             {:and, [{"price", {:gte, 50}}, {"name", {:ilike, "%lamp%"}}]}
  end

  test "uppercase ~SELECTO sigil supports dotted fields" do
    status = "paid"

    assert ~SELECTO"orders.status == ^status" == {"orders.status", "paid"}
  end

  test "uppercase ~SELECTO sigil reports unsupported functions clearly" do
    assert_raise ArgumentError,
                 ~r/Unsupported Selecto filter expression: unknown_fun\(name\)/,
                 fn ->
                   Code.eval_quoted(quote(do: ~SELECTO"unknown_fun(name)"), [], __ENV__)
                 end
  end
end
