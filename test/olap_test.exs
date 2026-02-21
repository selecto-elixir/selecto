defmodule Selecto.OlapTest do
  use ExUnit.Case, async: true

test "SQL builder modules load" do
    assert Code.ensure_loaded?(Selecto.Builder.Sql)
  end
end
