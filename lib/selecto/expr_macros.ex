defmodule Selecto.ExprMacros do
  @moduledoc """
  Macro sugar for Selecto expressions.

  The initial scope is filter composition via `where/1`, which compiles a small
  Elixir expression subset into Selecto's existing filter AST.
  """

  defmacro where(expression) do
    Selecto.ExprCompiler.compile_filter!(expression)
  end

  defmacro select(expression) do
    Selecto.ExprCompiler.compile_select!(expression)
  end

  defmacro order_by(expression) do
    Selecto.ExprCompiler.compile_order!(expression)
  end
end
