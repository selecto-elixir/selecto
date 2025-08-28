defmodule Selecto.Schema.FilterTest do
  use ExUnit.Case
  alias Selecto.Schema.Filter

  describe "configure_filters/2" do
    test "returns filters unchanged" do
      filters = [
        {:eq, "status", "active"},
        {:like, "name", "%test%"},
        {:gt, "age", 18}
      ]
      dep = %{some: "dependency"}

      result = Filter.configure_filters(filters, dep)

      assert result == filters
    end

    test "handles empty filters list" do
      filters = []
      dep = %{}

      result = Filter.configure_filters(filters, dep)

      assert result == []
    end

    test "works with various filter formats" do
      filters = [
        %{field: "status", operator: :eq, value: "active"},
        {:between, "created_at", ~D[2023-01-01], ~D[2023-12-31]},
        {:in, "category_id", [1, 2, 3]}
      ]
      dep = nil

      result = Filter.configure_filters(filters, dep)

      assert result == filters
    end
  end
end