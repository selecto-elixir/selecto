defmodule Selecto.Performance.OptimizerTest do
  use ExUnit.Case, async: true

  alias Selecto.Performance.Optimizer

  test "returns suggestions for basic query shape" do
    selecto = %{
      source: %{source_table: "users"},
      select: ["*"],
      filters: %{"name" => {:like, "%john%"}},
      joins: %{},
      group_by: [],
      order_by: [],
      limit: nil
    }

    assert {:ok, suggestions} = Optimizer.suggest_optimizations(selecto)
    assert is_list(suggestions)
  end

  test "handles join-heavy query input" do
    selecto = %{
      source: %{source_table: "orders"},
      select: ["*"],
      filters: %{},
      joins: %{a: %{}, b: %{}, c: %{}},
      group_by: [],
      order_by: [],
      limit: 100
    }

    assert {:ok, suggestions} = Optimizer.suggest_optimizations(selecto)
    assert is_list(suggestions)
  end
end
