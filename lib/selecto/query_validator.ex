defmodule Selecto.QueryValidator do
  @moduledoc false

  alias Selecto.Advanced.ArrayOperations.Spec, as: ArraySpec
  alias Selecto.Advanced.JsonOperations.Spec, as: JsonSpec
  alias Selecto.Window.Spec, as: WindowSpec

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

  @spec validate_window_spec!(Selecto.Types.t(), WindowSpec.t()) :: :ok
  def validate_window_spec!(selecto, %WindowSpec{} = window_spec) do
    validate_window_arguments!(selecto, window_spec.function, window_spec.arguments)
    validate_group_specs!(selecto, window_spec.partition_by || [])
    validate_order_specs!(selecto, window_spec.order_by || [])
  end

  @spec validate_json_specs!(Selecto.Types.t(), [JsonSpec.t()] | JsonSpec.t()) :: :ok
  def validate_json_specs!(selecto, json_specs) when is_list(json_specs) do
    Enum.each(json_specs, &validate_json_spec!(selecto, &1))
  end

  def validate_json_specs!(selecto, %JsonSpec{} = json_spec) do
    validate_json_spec!(selecto, json_spec)
  end

  @spec validate_array_specs!(Selecto.Types.t(), [ArraySpec.t()] | ArraySpec.t()) :: :ok
  def validate_array_specs!(selecto, array_specs) when is_list(array_specs) do
    Enum.each(array_specs, &validate_array_spec!(selecto, &1))
  end

  def validate_array_specs!(selecto, %ArraySpec{} = array_spec) do
    validate_array_spec!(selecto, array_spec)
  end

  @spec validate_unnest_source!(Selecto.Types.t(), term()) :: :ok
  def validate_unnest_source!(selecto, source) do
    validate_array_column!(selecto, source)
  end

  @spec validate_table_source!(Selecto.Types.t(), atom() | String.t()) :: :ok
  def validate_table_source!(selecto, source) when is_binary(source) or is_atom(source) do
    validate_field_reference!(selecto, source)
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

  defp validate_selector!(selecto, {:udf, function_id, args}) when is_list(args) do
    validate_udf_call!(selecto, function_id, args, :select)
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

  defp validate_selector!(selecto, {:custom_sql, _sql_template, field_mappings})
       when is_map(field_mappings) do
    field_mappings
    |> Map.values()
    |> Enum.each(&validate_field_reference!(selecto, &1))
  end

  defp validate_selector!(_selecto, {func}) when is_atom(func), do: :ok

  defp validate_selector!(selecto, {func, selector}) when is_atom(func) do
    validate_selector!(selecto, selector)
  end

  defp validate_selector!(selecto, {func, selector, filter}) when is_atom(func) do
    validate_selector!(selecto, selector)
    validate_filter!(selecto, filter)
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

  defp validate_filter!(selecto, {:udf, function_id, args}) when is_list(args) do
    validate_udf_call!(selecto, function_id, args, :filter)
  end

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

  defp validate_order_spec!(selecto, {{:udf, function_id, args}, dir})
       when dir in @order_directions and is_list(args) do
    validate_udf_call!(selecto, function_id, args, :order_by)
  end

  defp validate_order_spec!(selecto, {dir, order}) when dir in @order_directions do
    validate_selector!(selecto, order)
  end

  defp validate_order_spec!(selecto, {order, dir}) when dir in @order_directions do
    validate_selector!(selecto, order)
  end

  defp validate_order_spec!(selecto, order) do
    case order do
      {:udf, function_id, args} when is_list(args) ->
        validate_udf_call!(selecto, function_id, args, :order_by)

      _ ->
        validate_selector!(selecto, order)
    end
  end

  defp validate_group_spec!(selecto, {:rollup, groups}) do
    validate_group_specs!(selecto, groups)
  end

  defp validate_group_spec!(selecto, {:grouping_set, groups}) do
    validate_group_specs!(selecto, groups)
  end

  defp validate_group_spec!(selecto, {:udf, function_id, args}) when is_list(args) do
    validate_udf_call!(selecto, function_id, args, :group_by)
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

  defp validate_udf_call!(selecto, function_id, args, call_site) do
    spec =
      case Selecto.UDF.fetch(selecto, function_id) do
        {:ok, spec} -> spec
        :error -> raise ArgumentError, "Unknown UDF '#{Selecto.UDF.normalize_id(function_id)}'"
      end

    allowed_in = Map.get(spec, :allowed_in) || Map.get(spec, "allowed_in") || []
    kind = Map.get(spec, :kind) || Map.get(spec, "kind")
    spec_args = Map.get(spec, :args) || Map.get(spec, "args") || []

    allowed_for_call_site? =
      case call_site do
        :lateral -> :lateral in allowed_in or :query_member in allowed_in
        other -> other in allowed_in
      end

    if not allowed_for_call_site? do
      raise ArgumentError,
            "UDF '#{Selecto.UDF.normalize_id(function_id)}' is not allowed in :#{call_site}. Allowed: #{inspect(allowed_in)}"
    end

    if call_site == :filter and kind != :predicate do
      raise ArgumentError,
            "UDF '#{Selecto.UDF.normalize_id(function_id)}' must be kind :predicate to be used in filters"
    end

    if call_site == :lateral and kind != :table do
      raise ArgumentError,
            "UDF '#{Selecto.UDF.normalize_id(function_id)}' must be kind :table to be used in lateral joins"
    end

    if length(args) != length(spec_args) do
      raise ArgumentError,
            "UDF '#{Selecto.UDF.normalize_id(function_id)}' expects #{length(spec_args)} argument(s), got #{length(args)}"
    end

    Enum.zip(args, spec_args)
    |> Enum.each(fn {arg, arg_spec} -> validate_udf_arg!(selecto, arg, arg_spec) end)
  end

  defp validate_udf_arg!(selecto, arg, arg_spec) do
    case Map.get(arg_spec, :source) || Map.get(arg_spec, "source") do
      :selector ->
        validate_selector!(selecto, arg)

      :value ->
        validate_udf_value_arg!(arg)

      :literal ->
        validate_udf_literal_arg!(arg)

      other ->
        raise ArgumentError, "Unsupported UDF arg source: #{inspect(other)}"
    end
  end

  defp validate_udf_value_arg!({:param, _value}), do: :ok
  defp validate_udf_value_arg!({:literal, _value}), do: :ok

  defp validate_udf_value_arg!(arg)
       when is_binary(arg) or is_atom(arg) or is_number(arg) or is_boolean(arg) or is_nil(arg) or
              is_list(arg) or is_map(arg),
       do: :ok

  defp validate_udf_value_arg!(arg) do
    raise ArgumentError,
          "UDF value args must be raw values, {:param, value}, or {:literal, value}. Got: #{inspect(arg)}"
  end

  defp validate_udf_literal_arg!({:literal, _value}), do: :ok

  defp validate_udf_literal_arg!(arg)
       when is_binary(arg) or is_atom(arg) or is_number(arg) or is_boolean(arg) or is_nil(arg),
       do: :ok

  defp validate_udf_literal_arg!(arg) do
    raise ArgumentError,
          "UDF literal args must be literal values or {:literal, value}. Got: #{inspect(arg)}"
  end

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

  defp validate_window_arguments!(selecto, function, arguments) do
    case {function, List.wrap(arguments)} do
      {func, _args} when func in [:row_number, :rank, :dense_rank, :percent_rank, :cume_dist] ->
        :ok

      {:ntile, _args} ->
        :ok

      {:count, []} ->
        :ok

      {:count, ["*" | _rest]} ->
        :ok

      {:count, [:* | _rest]} ->
        :ok

      {func, [field | _rest]}
      when func in [
             :lag,
             :lead,
             :first_value,
             :last_value,
             :nth_value,
             :sum,
             :avg,
             :count,
             :min,
             :max,
             :stddev,
             :variance
           ] ->
        validate_function_arg!(selecto, field)

      {_function, args} ->
        Enum.each(args, &validate_function_arg!(selecto, &1))
    end
  end

  defp validate_json_spec!(selecto, %JsonSpec{} = spec) do
    validate_json_column!(selecto, spec.column)
    validate_json_column!(selecto, spec.key_field)
    validate_json_column!(selecto, spec.value_field)
  end

  defp validate_json_column!(_selecto, nil), do: :ok

  defp validate_json_column!(selecto, column)
       when is_binary(column) or is_atom(column) or is_tuple(column) do
    validate_array_column!(selecto, column)
  end

  defp validate_json_column!(_selecto, _column), do: :ok

  defp validate_array_spec!(selecto, %ArraySpec{} = spec) do
    validate_array_column!(selecto, spec.column)

    case spec.order_by do
      order_specs when is_list(order_specs) -> validate_order_specs!(selecto, order_specs)
      _ -> :ok
    end
  end

  defp validate_array_column!(_selecto, nil), do: :ok

  defp validate_array_column!(selecto, column) when is_binary(column) or is_atom(column) do
    validate_field_reference!(selecto, column)
  end

  defp validate_array_column!(selecto, column) when is_tuple(column) do
    validate_selector!(selecto, column)
  end

  defp validate_array_column!(_selecto, _column), do: :ok

  defp validate_field_selector!(_selecto, {:raw_sql, _sql}), do: :ok
  defp validate_field_selector!(selecto, selector), do: validate_selector!(selecto, selector)

  defp validate_field_reference!(selecto, field_ref)
       when is_binary(field_ref) or is_atom(field_ref) do
    json_field_ref = if is_atom(field_ref), do: Atom.to_string(field_ref), else: field_ref

    cond do
      not field_validation_enabled?(selecto) ->
        :ok

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

  defp field_validation_enabled?(selecto) do
    case Map.get(selecto, :config) do
      %{} = config ->
        map_size(config) > 0

      _ ->
        false
    end
  end

  defp field_reference_like?(field_ref) when is_atom(field_ref), do: true

  defp field_reference_like?(field_ref) when is_binary(field_ref) do
    Regex.match?(~r/^[A-Za-z_][A-Za-z0-9_]*(?::[^.]+)*(?:\.[A-Za-z_][A-Za-z0-9_]*)?$/, field_ref)
  end

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
