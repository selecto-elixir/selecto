defmodule Selecto.Sigil do
  @moduledoc """
  Sigils for Selecto expression authoring.

  `~SELECTO` currently supports filter expressions and compiles them into the
  same normalized AST used by `Selecto.Query.filter/2`.
  """

  defmacro sigil_SELECTO({:<<>>, _meta, [string]}, _modifiers) when is_binary(string) do
    string
    |> Selecto.ExprCompiler.parse_filter!()
    |> Selecto.ExprCompiler.compile_filter!()
  end

  defmacro sigil_SELECTO(term, _modifiers) do
    raise ArgumentError,
          "~SELECTO expects a literal string body, got: #{Macro.to_string(term)}"
  end
end
