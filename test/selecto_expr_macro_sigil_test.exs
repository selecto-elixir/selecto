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

  test "where macro supports text, array, and existence helpers" do
    term = "wireless charger"
    tags = ["featured", "clearance"]

    assert where(
             text_search(name, ^term) and array_overlap(tags, ^tags) and
               field_exists(metadata.zone)
           ) ==
             {:and,
              [
                {"name", {:text_search, "wireless charger"}},
                {:array_overlap, "tags", ["featured", "clearance"]},
                {"metadata.zone", :exists}
              ]}
  end

  test "where macro supports multi-field text_search helpers" do
    term = "wireless charger"

    assert where(text_search([name, description], ^term)) ==
             {[
                "name",
                "description"
              ], {:text_search, "wireless charger"}}
  end

  test "where macro supports text_search mode options" do
    term = "+wireless -charger"

    assert where(text_search([name, description], ^term, mode: :boolean)) ==
             {[
                "name",
                "description"
              ], {:text_search, "+wireless -charger", mode: :boolean}}
  end

  test "where macro supports text_search query expansion mode" do
    term = "wireless charger"

    assert where(text_search(name, ^term, mode: :query_expansion)) ==
             {"name", {:text_search, "wireless charger", mode: :query_expansion}}
  end

  test "where macro supports not_in and array_contains helpers" do
    statuses = ["cancelled", "returned"]

    assert where(not_in(status, ^statuses) and array_contains(tags, ["featured"])) ==
             {:and,
              [
                {"status", {:not_in, ["cancelled", "returned"]}},
                {:array_contains, "tags", ["featured"]}
              ]}
  end

  test "select macro compiles helper-friendly selector lists" do
    assert select([name, sum(price), coalesce(nickname, name), as(count(), "total")]) == [
             {:field, "name"},
             {:func, "SUM", [{:field, "price"}]},
             {:coalesce, [{:field, "nickname"}, {:field, "name"}]},
             {:field, {:count, "*"}, "total"}
           ]
  end

  test "select macro supports additional selector helpers" do
    status = "active"

    assert select([
             field(customer.name),
             lit("Open"),
             count_distinct(customer.id),
             case_when([{status == ^status, "Open"}], "Other"),
             greatest(total, discount_total),
             nullif(nickname, name)
           ]) == [
             {:field, "customer.name"},
             {:literal, "Open"},
             {:count_distinct, {:field, "customer.id"}},
             {:case, [{{"status", "active"}, {:literal, "Open"}}], {:literal, "Other"}},
             {:greatest, [{:field, "total"}, {:field, "discount_total"}]},
             {:nullif, [{:field, "nickname"}, {:field, "name"}]}
           ]
  end

  test "select macro supports expanded aggregate and wrapper helpers" do
    assert select([
             count_distinct(status),
             stddev(price),
             variance(price),
             concat(nickname, " / ", name),
             least(price, discount_total)
           ]) == [
             {:count_distinct, {:field, "status"}},
             {:func, :stddev, [{:field, "price"}]},
             {:func, :variance, [{:field, "price"}]},
             {:concat, [{:field, "nickname"}, " / ", {:field, "name"}]},
             {:least, [{:field, "price"}, {:field, "discount_total"}]}
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

  test "order_by macro compiles fields, selector expressions, and null ordering helpers" do
    assert order_by([
             price,
             desc_nulls_last(total),
             coalesce(nickname, name),
             asc(customer.name)
           ]) == [
             "price",
             {"total", :desc_nulls_last},
             {:coalesce, [{:field, "nickname"}, {:field, "name"}]},
             {"customer.name", :asc}
           ]
  end

  test "order_by macro supports wrapper selector expressions" do
    assert order_by([
             desc(greatest(total, discount_total)),
             least(price, discount_total)
           ]) == [
             {{:greatest, [{:field, "total"}, {:field, "discount_total"}]}, :desc},
             {:least, [{:field, "price"}, {:field, "discount_total"}]}
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

  test "uppercase ~SELECTO sigil supports new filter helpers" do
    term = "wireless charger"

    assert ~SELECTO"text_search(name, ^term) and field_exists(metadata.zone)" ==
             {:and,
              [
                {"name", {:text_search, "wireless charger"}},
                {"metadata.zone", :exists}
              ]}
  end

  test "uppercase ~SELECTO sigil supports multi-field text_search" do
    term = "wireless charger"
    prefix = "act"

    assert ~SELECTO"text_search([name, description], ^term) and starts_with(status, ^prefix)" ==
             {:and,
              [
                {[
                   "name",
                   "description"
                 ], {:text_search, "wireless charger"}},
                {"status", {:like, "act%"}}
              ]}
  end

  test "uppercase ~SELECTO sigil supports text_search mode options" do
    term = "+wireless -charger"

    assert ~SELECTO"text_search([name, description], ^term, mode: :boolean)" ==
             {[
                "name",
                "description"
              ], {:text_search, "+wireless -charger", mode: :boolean}}
  end

  test "uppercase ~SELECTO sigil supports text_search query expansion mode" do
    term = "wireless charger"

    assert ~SELECTO"text_search(name, ^term, mode: :query_expansion)" ==
             {"name", {:text_search, "wireless charger", mode: :query_expansion}}
  end

  test "uppercase ~SELECTO sigil reports unsupported functions clearly" do
    assert_raise ArgumentError,
                 ~r/Unsupported Selecto filter expression: unknown_fun\(name\)/,
                 fn ->
                   Code.eval_quoted(quote(do: ~SELECTO"unknown_fun(name)"), [], __ENV__)
                 end
  end
end
