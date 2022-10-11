defmodule ListableTest do
  use ExUnit.Case
  doctest Listable

  test "greets the world" do
    assert Listable.hello() == :world
  end
end
