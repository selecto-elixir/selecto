defmodule Selecto.ExprCompiler do
  @moduledoc false

  @supported_filter_functions [
    :array_contains,
    :array_contained,
    :array_eq,
    :array_overlap,
    :between,
    :contains,
    :ends_with,
    :field_exists,
    :ilike,
    :in,
    :is_nil,
    :like,
    :not_in,
    :starts_with,
    :text_search
  ]

  @order_directions [
    :asc,
    :desc,
    :asc_nulls_first,
    :asc_nulls_last,
    :desc_nulls_first,
    :desc_nulls_last
  ]

  @wrapper_selector_functions [:concat, :greatest, :least, :nullif]

  def compile_filter!(ast) do
    do_compile_filter(ast)
  end

  def compile_select!(ast) do
    do_compile_select(ast)
  end

  def compile_order!(ast) do
    do_compile_order(ast)
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

  defp do_compile_filter({:not_in, _, [field_ast, value_ast]}) do
    compile_call(:not_in, [field_ast, value_ast])
  end

  defp do_compile_filter({:text_search, _, [field_ast, value_ast]}) do
    compile_call(:text_search, [field_ast, value_ast])
  end

  defp do_compile_filter({:field_exists, _, [field_ast]}) do
    field_name = compile_field_ref!(field_ast)

    quote do
      Selecto.Expr.field_exists(unquote(field_name))
    end
  end

  defp do_compile_filter({operator, _, [field_ast, value_ast]})
       when operator in [:array_contains, :array_contained, :array_eq, :array_overlap] do
    compile_call(operator, [field_ast, value_ast])
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

  defp do_compile_order(list_ast) when is_list(list_ast) do
    quote do
      [unquote_splicing(Enum.map(list_ast, &compile_order_expr!/1))]
    end
  end

  defp do_compile_order(ast) do
    compile_order_expr!(ast)
  end

  defp do_compile_select_item({name, _, context})
       when is_atom(name) and (is_atom(context) or is_nil(context)) do
    field_name = Atom.to_string(name)

    quote do
      Selecto.Expr.field(unquote(field_name))
    end
  end

  defp do_compile_select_item({{:., _, _}, _, []} = field_ast) do
    field_name = compile_field_ref!(field_ast)

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

  defp do_compile_select_item({:lit, _, [value_ast]}) do
    quote do
      Selecto.Expr.lit(unquote(compile_literal_or_value!(value_ast)))
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

  defp do_compile_select_item({:count_distinct, _, [value_ast]}) do
    quote do
      {:count_distinct, unquote(compile_select_value!(value_ast))}
    end
  end

  defp do_compile_select_item({:case_when, _, [pairs_ast]}) when is_list(pairs_ast) do
    quote do
      Selecto.Expr.case_when(unquote(compile_case_pairs!(pairs_ast)))
    end
  end

  defp do_compile_select_item({:case_when, _, [pairs_ast, else_ast]}) when is_list(pairs_ast) do
    quote do
      Selecto.Expr.case_when(
        unquote(compile_case_pairs!(pairs_ast)),
        unquote(compile_case_result!(else_ast))
      )
    end
  end

  defp do_compile_select_item({:window, _, [window_call_ast, opts_ast]}) when is_list(opts_ast) do
    {function_name, arguments} = compile_window_call!(window_call_ast)
    normalized_opts = compile_window_opts!(opts_ast)

    quote do
      Selecto.Expr.window(unquote(function_name), unquote(arguments), unquote(normalized_opts))
    end
  end

  defp do_compile_select_item({fun_name, _, args})
       when fun_name in [:json_extract, :json_extract_text] and is_list(args) do
    compile_json_extract(fun_name, args)
  end

  defp do_compile_select_item({fun_name, _, args})
       when fun_name in [:json_agg] and is_list(args) do
    compile_json_agg(fun_name, args)
  end

  defp do_compile_select_item({fun_name, _, args})
       when fun_name in [:json_object_agg] and is_list(args) do
    compile_json_object_agg(fun_name, args)
  end

  defp do_compile_select_item({:coalesce, _, args}) when is_list(args) do
    quote do
      Selecto.Expr.coalesce([unquote_splicing(Enum.map(args, &compile_select_value!/1))])
    end
  end

  defp do_compile_select_item({fun_name, _, args})
       when fun_name in @wrapper_selector_functions and is_list(args) do
    quote do
      {unquote(fun_name), [unquote_splicing(Enum.map(args, &compile_select_value!/1))]}
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

  defp compile_select_value!({fun_name, _, [arg]}) when fun_name in [:asc, :desc] do
    quote do
      Selecto.Expr.unquote(fun_name)(unquote(compile_select_value!(arg)))
    end
  end

  defp compile_select_value!(ast) when is_tuple(ast), do: do_compile_select_item(ast)
  defp compile_select_value!(ast), do: ast

  defp compile_window_call!({function_name, _, args})
       when is_atom(function_name) and is_list(args) do
    {function_name, Enum.map(args, &compile_select_value!/1)}
  end

  defp compile_window_call!(ast) do
    raise ArgumentError,
          "Unsupported Selecto window expression: #{Macro.to_string(ast)}"
  end

  defp compile_window_opts!(opts) do
    Enum.map(opts, fn
      {:over, over_opts} when is_list(over_opts) ->
        {:over, compile_over_opts!(over_opts)}

      {:as, alias_name} ->
        {:as, alias_name}

      other ->
        other
    end)
  end

  defp compile_over_opts!(over_opts) do
    Enum.map(over_opts, fn
      {:partition_by, fields} ->
        {:partition_by, Enum.map(List.wrap(fields), &compile_field_string!/1)}

      {:order_by, orders} ->
        {:order_by, Enum.map(List.wrap(orders), &compile_order_expr!/1)}

      {:frame, frame_spec} ->
        {:frame, frame_spec}

      other ->
        other
    end)
  end

  defp compile_order_expr!({direction, _, [value_ast]}) when direction in @order_directions do
    quote do
      Selecto.Expr.unquote(direction)(unquote(compile_order_value!(value_ast)))
    end
  end

  defp compile_order_expr!(ast) do
    compile_order_value!(ast)
  end

  defp compile_json_extract(fun_name, [column_ast, path_ast]) do
    quote do
      Selecto.Expr.unquote(fun_name)(
        unquote(compile_field_string!(column_ast)),
        unquote(compile_literal_or_value!(path_ast))
      )
    end
  end

  defp compile_json_extract(fun_name, [column_ast, path_ast, opts_ast]) when is_list(opts_ast) do
    quote do
      Selecto.Expr.unquote(fun_name)(
        unquote(compile_field_string!(column_ast)),
        unquote(compile_literal_or_value!(path_ast)),
        unquote(compile_json_opts!(opts_ast))
      )
    end
  end

  defp compile_json_extract(fun_name, args) do
    raise ArgumentError,
          "Unsupported Selecto #{fun_name} expression: #{Macro.to_string({fun_name, [], args})}"
  end

  defp compile_json_agg(fun_name, [field_ast]) do
    quote do
      Selecto.Expr.unquote(fun_name)(unquote(compile_field_string!(field_ast)))
    end
  end

  defp compile_json_agg(fun_name, [field_ast, opts_ast]) when is_list(opts_ast) do
    quote do
      Selecto.Expr.unquote(fun_name)(
        unquote(compile_field_string!(field_ast)),
        unquote(compile_json_opts!(opts_ast))
      )
    end
  end

  defp compile_json_object_agg(fun_name, [key_ast, value_ast]) do
    quote do
      Selecto.Expr.unquote(fun_name)(
        unquote(compile_field_string!(key_ast)),
        unquote(compile_field_string!(value_ast))
      )
    end
  end

  defp compile_json_object_agg(fun_name, [key_ast, value_ast, opts_ast]) when is_list(opts_ast) do
    quote do
      Selecto.Expr.unquote(fun_name)(
        unquote(compile_field_string!(key_ast)),
        unquote(compile_field_string!(value_ast)),
        unquote(compile_json_opts!(opts_ast))
      )
    end
  end

  defp compile_json_object_agg(fun_name, args) do
    raise ArgumentError,
          "Unsupported Selecto #{fun_name} expression: #{Macro.to_string({fun_name, [], args})}"
  end

  defp compile_json_opts!(opts) do
    Enum.map(opts, fn
      {:as, alias_name} -> {:as, alias_name}
      other -> other
    end)
  end

  defp compile_field_string!(ast) do
    compile_field_ref!(ast)
  end

  defp compile_case_pairs!(pairs_ast) when is_list(pairs_ast) do
    Enum.map(pairs_ast, fn
      {condition_ast, result_ast} ->
        quote do
          {unquote(do_compile_filter(condition_ast)), unquote(compile_case_result!(result_ast))}
        end

      other ->
        raise ArgumentError,
              "Expected {condition, result} pairs in case_when/2, got: #{Macro.to_string(other)}"
    end)
  end

  defp compile_case_result!(result_ast), do: compile_select_value!(result_ast)

  defp compile_order_value!({name, _, context})
       when is_atom(name) and (is_atom(context) or is_nil(context)) do
    compile_field_string!({name, [], context})
  end

  defp compile_order_value!({{:., _, _}, _, []} = field_ast) do
    compile_field_string!(field_ast)
  end

  defp compile_order_value!(ast) when is_tuple(ast), do: do_compile_select_item(ast)
  defp compile_order_value!(ast), do: compile_literal_or_value!(ast)

  defp compile_literal_or_value!({:^, _, [value_ast]}), do: value_ast
  defp compile_literal_or_value!(ast), do: ast
end
