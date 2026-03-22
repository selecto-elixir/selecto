defmodule Selecto.QueryValidator do
  @moduledoc false

  @skip_function_args ["*", "DISTINCT", "ALL"]
  @skip_function_arg_atoms [:*, :distinct, :all]
  @group_wrapper_keys [:rollup]
  @order_directions [
    :asc,
    :desc,
    :asc_nulls_first,
    :asc_nulls_last,
    :desc_nulls_first,
    :desc_nulls_last
  ]

  @spec validate_selectors!(
          Selecto.Types.t(),
          [Selecto.Types.selector()] | Selecto.Types.selector()
        ) ::
          :ok
  def validate_selectors!(selecto, selectors) when is_list(selectors) do
    Enum.each(selectors, &validate_selector!(selecto, &1))
  end

  def validate_selectors!(selecto, selector) do
    validate_selector!(selecto, selector)
  end

  @spec validate_filters!(Selecto.Types.t(), [Selecto.Types.filter()] | Selecto.Types.filter()) ::
          :ok
  def validate_filters!(selecto, filters) when is_list(filters) do
    Enum.each(filters, &validate_filter!(selecto, &1))
  end

  def validate_filters!(selecto, filter) do
    validate_filter!(selecto, filter)
  end

  @spec validate_order_specs!(Selecto.Types.t(), [term()] | term()) :: :ok
  def validate_order_specs!(selecto, order_specs) when is_list(order_specs) do
    Enum.each(order_specs, &validate_order_spec!(selecto, &1))
  end

  def validate_order_specs!(selecto, order_spec) do
    validate_order_spec!(selecto, order_spec)
  end

  @spec validate_group_specs!(Selecto.Types.t(), [term()] | term()) :: :ok
  def validate_group_specs!(selecto, group_specs) when is_list(group_specs) do
    if keyword_group_specs?(group_specs) do
      Enum.each(group_specs, fn
        {:rollup, groups} -> validate_group_specs!(selecto, groups)
        {_key, groups} -> validate_group_specs!(selecto, groups)
      end)
    else
      Enum.each(group_specs, &validate_group_spec!(selecto, &1))
    end
  end

  def validate_group_specs!(selecto, group_spec) do
    validate_group_spec!(selecto, group_spec)
  end

  defp validate_selector!(selecto, selector) when is_binary(selector) or is_atom(selector) do
    validate_field_reference!(selecto, selector)
  end

  defp validate_selector!(_selecto, selector)
       when is_integer(selector) or is_float(selector) or is_boolean(selector) or is_nil(selector) do
    :ok
  end

  defp validate_selector!(_selecto, {:literal, _value}), do: :ok
  defp validate_selector!(_selecto, {:literal_position, _value}), do: :ok
  defp validate_selector!(_selecto, {:raw_sql, _sql}), do: :ok
  defp validate_selector!(_selecto, {:subquery, _sql, _params}), do: :ok
  defp validate_selector!(_selecto, {:count}), do: :ok
  defp validate_selector!(_selecto, {:count, "*"}), do: :ok

  defp validate_selector!(selecto, {:count, "*", filter}) do
    validate_filter!(selecto, filter)
  end

  defp validate_selector!(selecto, {:field, selector}) do
    validate_field_selector!(selecto, selector)
  end

  defp validate_selector!(selecto, {:field, selector, _as}) do
    validate_field_selector!(selecto, selector)
  end

  defp validate_selector!(selecto, {:to_char, {field, _format}}) do
    validate_selector!(selecto, field)
  end

  defp validate_selector!(selecto, {:extract, field, _format}) do
    validate_selector!(selecto, field)
  end

  defp validate_selector!(selecto, {:array_length, field}) do
    validate_selector!(selecto, field)
  end

  defp validate_selector!(selecto, {:cardinality, field}) do
    validate_selector!(selecto, field)
  end

  defp validate_selector!(selecto, {:array_ndims, field}) do
    validate_selector!(selecto, field)
  end

  defp validate_selector!(selecto, {:array_dims, field}) do
    validate_selector!(selecto, field)
  end

  defp validate_selector!(selecto, {:unnest, field}) do
    validate_selector!(selecto, field)
  end

  defp validate_selector!(selecto, {:array, values}) when is_list(values) do
    Enum.each(values, &validate_function_arg!(selecto, &1))
  end

  defp validate_selector!(selecto, {:array_cat, left, right}) do
    validate_selector!(selecto, left)
    validate_function_arg!(selecto, right)
  end

  defp validate_selector!(selecto, {:array_to_string, value, delimiter}) do
    validate_selector!(selecto, value)
    validate_function_arg!(selecto, delimiter)
  end

  defp validate_selector!(selecto, {:string_to_array, value, delimiter}) do
    validate_function_arg!(selecto, value)
    validate_function_arg!(selecto, delimiter)
  end

  defp validate_selector!(selecto, {:array_append, left, right}) do
    validate_selector!(selecto, left)
    validate_function_arg!(selecto, right)
  end

  defp validate_selector!(selecto, {:array_prepend, left, right}) do
    validate_function_arg!(selecto, left)
    validate_selector!(selecto, right)
  end

  defp validate_selector!(selecto, {:array_fill, value, dims}) do
    validate_function_arg!(selecto, value)
    validate_function_arg!(selecto, dims)
  end

  defp validate_selector!(selecto, {:array_remove, left, right}) do
    validate_selector!(selecto, left)
    validate_function_arg!(selecto, right)
  end

  defp validate_selector!(selecto, {:array_position, left, right}) do
    validate_selector!(selecto, left)
    validate_function_arg!(selecto, right)
  end

  defp validate_selector!(selecto, {:array_positions, left, right}) do
    validate_selector!(selecto, left)
    validate_function_arg!(selecto, right)
  end

  defp validate_selector!(selecto, {:array_replace, array, search, replacement}) do
    validate_selector!(selecto, array)
    validate_function_arg!(selecto, search)
    validate_function_arg!(selecto, replacement)
  end

  defp validate_selector!(selecto, {:array_position, array, search, start}) do
    validate_selector!(selecto, array)
    validate_function_arg!(selecto, search)
    validate_function_arg!(selecto, start)
  end

  defp validate_selector!(selecto, {:count_age_bucket, field, _min, _max}) do
    validate_selector!(selecto, field)
  end

  defp validate_selector!(selecto, {:count_age_bucket_other, field, bucket_ranges}) do
    validate_selector!(selecto, field)
    Enum.each(List.wrap(bucket_ranges), &validate_function_arg!(selecto, &1))
  end

  defp validate_selector!(selecto, {:count_bucket, field, _min, _max}) do
    validate_selector!(selecto, field)
  end

  defp validate_selector!(selecto, {:count_bucket_other, field, bucket_ranges}) do
    validate_selector!(selecto, field)
    Enum.each(List.wrap(bucket_ranges), &validate_function_arg!(selecto, &1))
  end

  defp validate_selector!(selecto, {:func, _func_name, args}) when is_list(args) do
    Enum.each(args, &validate_function_arg!(selecto, &1))
  end

  defp validate_selector!(selecto, {:func, _func_name, args, opts})
       when is_list(args) and is_list(opts) do
    Enum.each(args, &validate_function_arg!(selecto, &1))

    case Keyword.get(opts, :filter) do
      nil -> :ok
      filters -> validate_filters!(selecto, filters)
    end
  end

  defp validate_selector!(selecto, {:case, pairs}) when is_list(pairs) do
    Enum.each(pairs, &validate_case_pair!(selecto, &1))
  end

  defp validate_selector!(selecto, {:case, pairs, else_clause}) when is_list(pairs) do
    Enum.each(pairs, &validate_case_pair!(selecto, &1))
    validate_case_result!(selecto, else_clause)
  end

  defp validate_selector!(selecto, {func, fields})
       when func in [:concat, :coalesce, :greatest, :least, :nullif] and is_list(fields) do
    Enum.each(fields, &validate_function_arg!(selecto, &1))
  end

  defp validate_selector!(_selecto, {func}) when is_atom(func), do: :ok

  defp validate_selector!(selecto, {func, selector}) when is_atom(func) do
    validate_selector!(selecto, selector)
  end

  defp validate_selector!(selecto, {func, selector, filter}) when is_atom(func) do
    validate_selector!(selecto, selector)
    validate_filter!(selecto, filter)
  end

  defp validate_selector!(selecto, {:custom_sql, _sql_template, field_mappings})
       when is_map(field_mappings) do
    field_mappings
    |> Map.values()
    |> Enum.each(&validate_field_reference!(selecto, &1))
  end

  defp validate_selector!(_selecto, _selector), do: :ok

  defp validate_filter!(selecto, {:and, filters}) when is_list(filters) do
    Enum.each(filters, &validate_filter!(selecto, &1))
  end

  defp validate_filter!(selecto, {:or, filters}) when is_list(filters) do
    Enum.each(filters, &validate_filter!(selecto, &1))
  end

  defp validate_filter!(selecto, {:not, filter}) do
    validate_filter!(selecto, filter)
  end

  defp validate_filter!(selecto, {op, field, values})
       when op in [:array_contains, :array_contained, :array_overlap, :array_eq] and
              (is_binary(field) or is_atom(field)) and is_list(values) do
    validate_field_reference!(selecto, field)
  end

  defp validate_filter!(_selecto, {:exists, _query}), do: :ok
  defp validate_filter!(_selecto, {:exists, _query, _params}), do: :ok
  defp validate_filter!(_selecto, {:raw_sql_filter, _filter_iodata}), do: :ok

  defp validate_filter!(selecto, {field, _value}) when is_binary(field) or is_atom(field) do
    validate_field_reference!(selecto, field)
  end

  defp validate_filter!(selecto, {field, _operator, _value})
       when is_binary(field) or is_atom(field) do
    validate_field_reference!(selecto, field)
  end

  defp validate_filter!(_selecto, _filter), do: :ok

  defp validate_order_spec!(selecto, {{:case, _when_clauses, _else_clause} = case_spec, dir})
       when dir in @order_directions do
    validate_selector!(selecto, case_spec)
  end

  defp validate_order_spec!(selecto, {{:case, _when_clauses} = case_spec, dir})
       when dir in @order_directions do
    validate_selector!(selecto, case_spec)
  end

  defp validate_order_spec!(selecto, {dir, order}) when dir in @order_directions do
    validate_selector!(selecto, order)
  end

  defp validate_order_spec!(selecto, {order, dir}) when dir in @order_directions do
    validate_selector!(selecto, order)
  end

  defp validate_order_spec!(selecto, order) do
    validate_selector!(selecto, order)
  end

  defp validate_group_spec!(selecto, {:rollup, groups}) do
    validate_group_specs!(selecto, groups)
  end

  defp validate_group_spec!(selecto, group) do
    validate_selector!(selecto, group)
  end

  defp validate_function_arg!(_selecto, arg) when arg in @skip_function_arg_atoms, do: :ok
  defp validate_function_arg!(_selecto, arg) when arg in @skip_function_args, do: :ok

  defp validate_function_arg!(selecto, arg) when is_binary(arg) or is_atom(arg) do
    if field_reference_like?(arg) do
      validate_field_reference!(selecto, arg)
    else
      :ok
    end
  end

  defp validate_function_arg!(selecto, arg) when is_tuple(arg) do
    validate_selector!(selecto, arg)
  end

  defp validate_function_arg!(selecto, arg) when is_list(arg) do
    Enum.each(arg, &validate_function_arg!(selecto, &1))
  end

  defp validate_function_arg!(_selecto, _arg), do: :ok

  defp keyword_group_specs?(group_specs) when is_list(group_specs) do
    Keyword.keyword?(group_specs) and
      Enum.all?(group_specs, fn {key, _value} -> key in @group_wrapper_keys end)
  end

  defp validate_case_pair!(selecto, {condition, result}) do
    validate_filter!(selecto, condition)
    validate_case_result!(selecto, result)
  end

  defp validate_case_pair!(_selecto, _pair), do: :ok

  defp validate_case_result!(selecto, result)
       when is_tuple(result) or is_binary(result) or is_atom(result) do
    validate_selector!(selecto, result)
  end

  defp validate_case_result!(_selecto, _result), do: :ok

  defp validate_field_selector!(_selecto, {:raw_sql, _sql}), do: :ok
  defp validate_field_selector!(selecto, selector), do: validate_selector!(selecto, selector)

  defp validate_field_reference!(selecto, field_ref)
       when is_binary(field_ref) or is_atom(field_ref) do
    json_field_ref = if is_atom(field_ref), do: Atom.to_string(field_ref), else: field_ref

    cond do
      dynamic_field?(selecto, field_ref) ->
        :ok

      computed_alias?(selecto, field_ref) ->
        :ok

      match?(
        {:jsonb, _, [_ | _]},
        Selecto.Jsonb.parse_field_reference(json_field_ref, selecto.config)
      ) ->
        :ok

      Selecto.field(selecto, field_ref) != nil ->
        :ok

      true ->
        raise_field_error!(selecto, field_ref)
    end
  end

  defp validate_field_reference!(selecto, field_ref) when is_tuple(field_ref) do
    raise_field_error!(selecto, field_ref)
  end

  defp dynamic_field?(selecto, field_ref) do
    dynamic_columns =
      selecto
      |> Map.get(:set, %{})
      |> Map.get(:dynamic_columns, %{})

    field_name = to_string(field_ref)

    Map.has_key?(dynamic_columns, field_name) or Map.has_key?(dynamic_columns, field_ref)
  end

  defp field_reference_like?(field_ref) when is_atom(field_ref), do: true

  defp field_reference_like?(field_ref) when is_binary(field_ref) do
    Regex.match?(~r/^[A-Za-z_][A-Za-z0-9_]*(?::[^.]+)*(?:\.[A-Za-z_][A-Za-z0-9_]*)?$/, field_ref)
  end

  defp field_reference_like?(_field_ref), do: false

  defp computed_alias?(selecto, field_ref) do
    field_name = to_string(field_ref)

    json_aliases =
      selecto
      |> Map.get(:set, %{})
      |> Map.get(:json_selects, [])
      |> Enum.map(&Map.get(&1, :alias))

    array_aliases =
      selecto
      |> Map.get(:set, %{})
      |> Map.get(:array_operations, [])
      |> Enum.map(&Map.get(&1, :alias))

    window_aliases =
      selecto
      |> Map.get(:set, %{})
      |> Map.get(:window_functions, [])
      |> Enum.map(&Map.get(&1, :alias))

    field_name in (json_aliases ++ array_aliases ++ window_aliases)
  end

  defp raise_field_error!(selecto, field_ref) do
    case Selecto.resolve_field(selecto, field_ref) do
      {:ok, _field_info} ->
        :ok

      {:error, %{message: message}} ->
        raise ArgumentError, message

      {:error, error} ->
        raise ArgumentError, inspect(error)
    end
  end
end
