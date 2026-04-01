defmodule Selecto.ExprMacros do
  @moduledoc """
  Macro sugar for Selecto expressions.

  These macros compile a small Elixir expression subset into Selecto's existing
  filter, selector, order, and group AST shapes.
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

  defmacro group_by(expression) do
    Selecto.ExprCompiler.compile_group!(expression)
  end
end
