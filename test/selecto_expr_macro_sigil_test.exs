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
end
