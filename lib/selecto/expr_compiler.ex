defmodule Selecto.ExprCompiler do
  @moduledoc false

  @supported_filter_functions [
    :between,
    :contains,
    :ends_with,
    :ilike,
    :in,
    :is_nil,
    :like,
    :starts_with
  ]

  def compile_filter!(ast) do
    do_compile_filter(ast)
  end

  def compile_select!(ast) do
    do_compile_select(ast)
  end

  def parse_filter!(string, opts \\ []) when is_binary(string) do
    Code.string_to_quoted!(string, opts)
  rescue
    error in [SyntaxError, TokenMissingError] ->
      raise ArgumentError,
            "Invalid Selecto filter expression: #{Exception.message(error)}"
  end

  defp do_compile_filter({:and, _, [left, right]}) do
    quote do
      Selecto.Expr.compact_and([
        unquote(do_compile_filter(left)),
        unquote(do_compile_filter(right))
      ])
    end
  end

  defp do_compile_filter({:or, _, [left, right]}) do
    quote do
      Selecto.Expr.compact_or([
        unquote(do_compile_filter(left)),
        unquote(do_compile_filter(right))
      ])
    end
  end

  defp do_compile_filter({:not, _, [{:is_nil, _, [field_ast]}]}) do
    field_name = compile_field_ref!(field_ast)

    quote do
      Selecto.Expr.not_null(unquote(field_name))
    end
  end

  defp do_compile_filter({:not, _, [expression]}) do
    quote do
      apply(Selecto.Expr, :not, [unquote(do_compile_filter(expression))])
    end
  end

  defp do_compile_filter({operator, _, [left, right]})
       when operator in [:==, :!=, :>, :>=, :<, :<=] do
    compile_comparison(operator, left, right)
  end

  defp do_compile_filter({:ilike, _, [field_ast, value_ast]}) do
    compile_call(:ilike, [field_ast, value_ast])
  end

  defp do_compile_filter({:like, _, [field_ast, value_ast]}) do
    compile_call(:like, [field_ast, value_ast])
  end

  defp do_compile_filter({:contains, _, [field_ast, value_ast]}) do
    compile_call(:contains, [field_ast, value_ast])
  end

  defp do_compile_filter({:starts_with, _, [field_ast, value_ast]}) do
    compile_call(:starts_with, [field_ast, value_ast])
  end

  defp do_compile_filter({:ends_with, _, [field_ast, value_ast]}) do
    compile_call(:ends_with, [field_ast, value_ast])
  end

  defp do_compile_filter({:in, _, [field_ast, value_ast]}) do
    compile_call(:in, [field_ast, value_ast])
  end

  defp do_compile_filter({:between, _, [field_ast, min_ast, max_ast]}) do
    field_name = compile_field_ref!(field_ast)
    min_value = compile_value!(min_ast)
    max_value = compile_value!(max_ast)

    quote do
      Selecto.Expr.between(unquote(field_name), unquote(min_value), unquote(max_value))
    end
  end

  defp do_compile_filter({:is_nil, _, [field_ast]}) do
    field_name = compile_field_ref!(field_ast)

    quote do
      Selecto.Expr.is_null(unquote(field_name))
    end
  end

  defp do_compile_filter(ast) do
    raise_unsupported_filter!(ast)
  end

  defp do_compile_select(list_ast) when is_list(list_ast) do
    quote do
      [unquote_splicing(Enum.map(list_ast, &do_compile_select_item/1))]
    end
  end

  defp do_compile_select(ast) do
    do_compile_select_item(ast)
  end

  defp do_compile_select_item({name, _, context})
       when is_atom(name) and (is_atom(context) or is_nil(context)) do
    field_name = Atom.to_string(name)

    quote do
      Selecto.Expr.field(unquote(field_name))
    end
  end

  defp do_compile_select_item({:field, _, [field_ast]}) do
    field_name = compile_field_ref!(field_ast)

    quote do
      Selecto.Expr.field(unquote(field_name))
    end
  end

  defp do_compile_select_item({:as, _, [expression_ast, alias_ast]}) do
    alias_value = compile_select_value!(alias_ast)

    quote do
      Selecto.Expr.as(unquote(do_compile_select_item(expression_ast)), unquote(alias_value))
    end
  end

  defp do_compile_select_item({:count, _, []}) do
    quote do
      Selecto.Expr.count("*")
    end
  end

  defp do_compile_select_item({:coalesce, _, args}) when is_list(args) do
    quote do
      Selecto.Expr.coalesce([unquote_splicing(Enum.map(args, &compile_select_value!/1))])
    end
  end

  defp do_compile_select_item({fun_name, _, args})
       when fun_name in [:avg, :count, :max, :min, :sum] and is_list(args) do
    quote do
      apply(Selecto.Expr, unquote(fun_name), [
        unquote_splicing(Enum.map(args, &compile_select_value!/1))
      ])
    end
  end

  defp do_compile_select_item(ast) do
    raise ArgumentError,
          "Unsupported Selecto select expression: #{Macro.to_string(ast)}"
  end

  defp raise_unsupported_filter!(ast) do
    supported = Enum.map_join(@supported_filter_functions, ", ", &to_string/1)

    raise ArgumentError,
          "Unsupported Selecto filter expression: #{Macro.to_string(ast)}. Supported helpers: #{supported}"
  end

  defp compile_comparison(operator, field_ast, nil) when operator in [:==, :!=] do
    field_name = compile_field_ref!(field_ast)

    case operator do
      :== ->
        quote do
          Selecto.Expr.is_null(unquote(field_name))
        end

      :!= ->
        quote do
          Selecto.Expr.not_null(unquote(field_name))
        end
    end
  end

  defp compile_comparison(operator, field_ast, value_ast) do
    field_name = compile_field_ref!(field_ast)
    value = compile_value!(value_ast)

    expr_fun =
      case operator do
        :== -> :eq
        :!= -> :neq
        :> -> :gt
        :>= -> :gte
        :< -> :lt
        :<= -> :lte
      end

    quote do
      apply(Selecto.Expr, unquote(expr_fun), [unquote(field_name), unquote(value)])
    end
  end

  defp compile_call(fun_name, [field_ast, value_ast]) do
    field_name = compile_field_ref!(field_ast)
    value = compile_value!(value_ast)

    quote do
      apply(Selecto.Expr, unquote(fun_name), [unquote(field_name), unquote(value)])
    end
  end

  defp compile_field_ref!({name, _, context})
       when is_atom(name) and (is_atom(context) or is_nil(context)) do
    Atom.to_string(name)
  end

  defp compile_field_ref!({{:., _, [left, right]}, _, []}) when is_atom(right) do
    left_name = compile_field_ref!(left)
    left_name <> "." <> Atom.to_string(right)
  end

  defp compile_field_ref!(ast) do
    raise ArgumentError,
          "Expected a field reference in Selecto expression, got: #{Macro.to_string(ast)}"
  end

  defp compile_value!({:^, _, [value_ast]}), do: value_ast

  defp compile_value!({name, _, context} = ast)
       when is_atom(name) and (is_atom(context) or is_nil(context)) do
    raise ArgumentError,
          "Unpinned value `#{Macro.to_string(ast)}` in Selecto expression. Use ^value for runtime values."
  end

  defp compile_value!(ast), do: ast

  defp compile_select_value!({name, _, context})
       when is_atom(name) and (is_atom(context) or is_nil(context)) do
    field_name = Atom.to_string(name)

    quote do
      Selecto.Expr.field(unquote(field_name))
    end
  end

  defp compile_select_value!({:^, _, [value_ast]}), do: value_ast
  defp compile_select_value!(ast) when is_tuple(ast), do: do_compile_select_item(ast)
  defp compile_select_value!(ast), do: ast
end
