defmodule Selecto.Expr do
  @moduledoc """
  Ergonomic constructors for Selecto filter and selector AST.

  This module returns the tuple and list shapes that Selecto already accepts,
  making dynamic query composition easier without introducing a new execution
  path.
  """

  import Kernel, except: [in: 2, not: 1]

  alias Selecto.Query

  defguardp wrapper_function(func)
            when func == :concat or func == :coalesce or func == :greatest or func == :least or
                   func == :nullif

  defguardp basic_sort_direction(direction) when direction == :asc or direction == :desc

  @typedoc "A helper expression that normalizes into Selecto's existing AST."
  @type expr :: term()

  @doc "Normalizes helper-friendly expression forms into Selecto AST."
  @spec normalize(expr()) :: term()
  def normalize(expr) when is_list(expr) do
    if Keyword.keyword?(expr) do
      Enum.map(expr, fn {key, value} -> {key, normalize(value)} end)
    else
      Enum.map(expr, &normalize/1)
    end
  end

  def normalize({:as, expression, alias_name}), do: as(expression, alias_name)
  def normalize({:eq, field, value}), do: eq(field, value)
  def normalize({:neq, field, value}), do: neq(field, value)
  def normalize({:not_eq, field, value}), do: neq(field, value)
  def normalize({:gt, field, value}), do: gt(field, value)
  def normalize({:gte, field, value}), do: gte(field, value)
  def normalize({:lt, field, value}), do: lt(field, value)
  def normalize({:lte, field, value}), do: lte(field, value)
  def normalize({:like, field, value}), do: like(field, value)
  def normalize({:ilike, field, value}), do: ilike(field, value)
  def normalize({:contains, field, value}), do: contains(field, value)
  def normalize({:starts_with, field, value}), do: starts_with(field, value)
  def normalize({:ends_with, field, value}), do: ends_with(field, value)
  def normalize({:is_null, field}), do: is_null(field)
  def normalize({:not_null, field}), do: not_null(field)
  def normalize({:between, field, min, max}), do: between(field, min, max)
  def normalize({:in, field, values}), do: unquote(:in)(field, values)
  def normalize({:not_in, field, values}), do: not_in(field, values)
  def normalize({:text_search, field, value}), do: text_search(field, value)
  def normalize({:field_exists, field}), do: field_exists(field)
  def normalize({:array_contains, field, values}), do: array_contains(field, values)
  def normalize({:array_contained, field, values}), do: array_contained(field, values)
  def normalize({:array_overlap, field, values}), do: array_overlap(field, values)
  def normalize({:array_eq, field, values}), do: array_eq(field, values)
  def normalize({:json_contains, column, value}), do: json_contains(column, value)
  def normalize({:json_path_exists, column, path}), do: json_path_exists(column, path)
  def normalize({:exists, query}), do: exists(query)
  def normalize({:exists, query, params}), do: exists(query, params)
  def normalize({:subquery_in, field, query}), do: subquery_in(field, query)
  def normalize({:subquery_in, field, query, params}), do: subquery_in(field, query, params)
  def normalize({:asc, expression}), do: asc(expression)
  def normalize({:desc, expression}), do: desc(expression)
  def normalize({:asc_nulls_first, expression}), do: asc_nulls_first(expression)
  def normalize({:asc_nulls_last, expression}), do: asc_nulls_last(expression)
  def normalize({:desc_nulls_first, expression}), do: desc_nulls_first(expression)
  def normalize({:desc_nulls_last, expression}), do: desc_nulls_last(expression)
  def normalize({:rollup, groups}), do: rollup(groups)

  def normalize({:window, window_call, opts}) when is_list(opts) do
    {:window, normalize_window_call(window_call), normalize_window_opts(opts)}
  end

  def normalize({:and, filters}) when is_list(filters) do
    unquote(:and)(Enum.map(filters, &normalize/1))
  end

  def normalize({:or, filters}) when is_list(filters) do
    unquote(:or)(Enum.map(filters, &normalize/1))
  end

  def normalize({:not, filter}) do
    unquote(:not)(normalize(filter))
  end

  def normalize({:field, selector}) do
    {:field, normalize_selector_input(selector)}
  end

  def normalize({:field, selector, alias_name}) do
    {:field, normalize_selector_input(selector), alias_name}
  end

  def normalize({:count_distinct, selector}) do
    {:count_distinct, normalize_selector_input(selector)}
  end

  def normalize({:func, function_name, args}) when is_list(args) do
    {:func, function_name, Enum.map(args, &normalize_selector_input/1)}
  end

  def normalize({:func, function_name, args, opts}) when is_list(args) and is_list(opts) do
    normalized_opts =
      case Keyword.fetch(opts, :filter) do
        {:ok, filter} -> Keyword.put(opts, :filter, normalize(filter))
        :error -> opts
      end

    {:func, function_name, Enum.map(args, &normalize_selector_input/1), normalized_opts}
  end

  def normalize({func, fields}) when wrapper_function(func) do
    {func, Enum.map(List.wrap(fields), &normalize_selector_input/1)}
  end

  def normalize({:case, pairs}) when is_list(pairs) do
    {:case, Enum.map(pairs, &normalize_case_pair/1)}
  end

  def normalize({:case, pairs, else_clause}) when is_list(pairs) do
    {:case, Enum.map(pairs, &normalize_case_pair/1), normalize_case_result(else_clause)}
  end

  def normalize(expr), do: expr

  @doc "Wraps a field reference or selector expression."
  @spec field(term()) :: tuple()
  def field(selector), do: {:field, normalize_selector_input(selector)}

  @doc "Wraps a literal selector value."
  @spec lit(term()) :: tuple()
  def lit(value), do: {:literal, value}

  @doc "Builds a generic function selector."
  @spec func(String.t(), term()) :: tuple()
  def func(function_name, args \\ []) do
    {:func, function_name, Enum.map(List.wrap(args), &normalize_selector_input/1)}
  end

  @doc "Builds `COUNT(*)` or `COUNT(field)` selectors."
  @spec count(term()) :: tuple()
  def count(value \\ "*")

  def count("*"), do: {:count, "*"}
  def count(:*), do: {:count, "*"}
  def count(value), do: func("COUNT", [value])

  @doc "Builds `COUNT(DISTINCT ...)` selectors."
  @spec count_distinct(term()) :: tuple()
  def count_distinct(value), do: {:count_distinct, normalize_selector_input(value)}

  @doc "Builds a `SUM(...)` selector."
  @spec sum(term()) :: tuple()
  def sum(value), do: func("SUM", [value])

  @doc "Builds an `AVG(...)` selector."
  @spec avg(term()) :: tuple()
  def avg(value), do: func("AVG", [value])

  @doc "Builds a `MIN(...)` selector."
  @spec min(term()) :: tuple()
  def min(value), do: func("MIN", [value])

  @doc "Builds a `MAX(...)` selector."
  @spec max(term()) :: tuple()
  def max(value), do: func("MAX", [value])

  @doc "Builds a `STDDEV(...)` selector."
  @spec stddev(term()) :: tuple()
  def stddev(value), do: func(:stddev, [value])

  @doc "Builds a `VARIANCE(...)` selector."
  @spec variance(term()) :: tuple()
  def variance(value), do: func(:variance, [value])

  @doc "Builds a `COALESCE(...)` selector."
  @spec coalesce([term()]) :: tuple()
  def coalesce(values) when is_list(values) do
    {:coalesce, Enum.map(values, &normalize_selector_input/1)}
  end

  @doc "Builds a `CONCAT(...)` selector."
  @spec concat([term()]) :: tuple()
  def concat(values) when is_list(values) do
    {:concat, Enum.map(values, &normalize_selector_input/1)}
  end

  @doc "Builds a `GREATEST(...)` selector."
  @spec greatest([term()] | term(), term() | nil) :: tuple()
  def greatest(values, maybe_second \\ nil)

  def greatest(values, nil) when is_list(values) do
    {:greatest, Enum.map(values, &normalize_selector_input/1)}
  end

  def greatest(left, right) do
    greatest([left, right])
  end

  @doc "Builds a `LEAST(...)` selector."
  @spec least([term()] | term(), term() | nil) :: tuple()
  def least(values, maybe_second \\ nil)

  def least(values, nil) when is_list(values) do
    {:least, Enum.map(values, &normalize_selector_input/1)}
  end

  def least(left, right) do
    least([left, right])
  end

  @doc "Builds a `NULLIF(...)` selector."
  @spec nullif(term(), term()) :: tuple()
  def nullif(left, right) do
    {:nullif, Enum.map([left, right], &normalize_selector_input/1)}
  end

  @doc "Builds a CASE expression using `{filter, result}` pairs."
  @spec case_when([{term(), term()}], term()) :: tuple()
  def case_when(pairs, else_clause \\ nil) when is_list(pairs) do
    {:case, Enum.map(pairs, &normalize_case_pair/1), normalize_case_result(else_clause)}
  end

  @doc "Adds an alias to any selector expression."
  @spec as(term(), String.t() | atom()) :: tuple()
  def as(expression, alias_name) do
    {:field, normalize_selector_input(expression), to_string(alias_name)}
  end

  @doc "Builds an equality filter."
  @spec eq(term(), term()) :: tuple()
  def eq(field, value), do: {field, value}

  @doc "Builds a not-equal filter."
  @spec neq(term(), term()) :: tuple()
  def neq(field, value), do: {field, {:ne, value}}

  @doc "Builds a greater-than filter."
  @spec gt(term(), term()) :: tuple()
  def gt(field, value), do: {field, {:gt, value}}

  @doc "Builds a greater-than-or-equal filter."
  @spec gte(term(), term()) :: tuple()
  def gte(field, value), do: {field, {:gte, value}}

  @doc "Builds a less-than filter."
  @spec lt(term(), term()) :: tuple()
  def lt(field, value), do: {field, {:lt, value}}

  @doc "Builds a less-than-or-equal filter."
  @spec lte(term(), term()) :: tuple()
  def lte(field, value), do: {field, {:lte, value}}

  @doc "Builds a `LIKE` filter."
  @spec like(term(), term()) :: tuple()
  def like(field, value), do: {field, {:like, value}}

  @doc "Builds an `ILIKE` filter."
  @spec ilike(term(), term()) :: tuple()
  def ilike(field, value), do: {field, {:ilike, value}}

  @doc "Builds a contains filter using Selecto's existing `:contains` operator."
  @spec contains(term(), term()) :: tuple()
  def contains(field, value), do: {field, {:contains, value}}

  @doc "Builds a prefix `LIKE` filter."
  @spec starts_with(term(), String.t()) :: tuple()
  def starts_with(field, value), do: like(field, "#{value}%")

  @doc "Builds a suffix `LIKE` filter."
  @spec ends_with(term(), String.t()) :: tuple()
  def ends_with(field, value), do: like(field, "%#{value}")

  @doc "Builds an `IS NULL` filter."
  @spec is_null(term()) :: tuple()
  def is_null(field), do: {field, nil}

  @doc "Builds an `IS NOT NULL` filter."
  @spec not_null(term()) :: tuple()
  def not_null(field), do: {field, :not_null}

  @doc "Builds a `BETWEEN` filter."
  @spec between(term(), term(), term()) :: tuple()
  def between(field, min, max), do: {field, {:between, min, max}}

  @doc "Builds an `IN (...)` filter."
  def unquote(:in)(field, values), do: {field, {:in, values}}

  @doc "Builds a `NOT IN (...)` filter."
  @spec not_in(term(), [term()]) :: tuple()
  def not_in(field, values), do: {field, {:not_in, values}}

  @doc "Builds a full-text search filter using Selecto's `:text_search` operator."
  @spec text_search(term(), term()) :: tuple()
  def text_search(field, value), do: {field, {:text_search, value}}

  @doc "Builds a field-path existence filter for JSONB paths or non-null fields."
  @spec field_exists(term()) :: tuple()
  def field_exists(field), do: {field, :exists}

  @doc "Builds an array contains filter."
  @spec array_contains(term(), [term()]) :: tuple()
  def array_contains(field, values), do: {:array_contains, field, values}

  @doc "Builds an array contained-by filter."
  @spec array_contained(term(), [term()]) :: tuple()
  def array_contained(field, values), do: {:array_contained, field, values}

  @doc "Builds an array overlap filter."
  @spec array_overlap(term(), [term()]) :: tuple()
  def array_overlap(field, values), do: {:array_overlap, field, values}

  @doc "Builds an array equality filter."
  @spec array_eq(term(), [term()]) :: tuple()
  def array_eq(field, values), do: {:array_eq, field, values}

  @doc "Builds a JSON contains filter for `Selecto.json_filter/2`."
  @spec json_contains(term(), term()) :: tuple()
  def json_contains(column, value), do: {:json_contains, column, value}

  @doc "Builds a JSON path-exists filter for `Selecto.json_filter/2`."
  @spec json_path_exists(term(), term()) :: tuple()
  def json_path_exists(column, path), do: {:json_path_exists, column, path}

  @doc "Builds an `EXISTS (...)` filter."
  @spec exists(term(), [term()]) :: tuple()
  def exists(query, params \\ [])

  def exists(query, []), do: {:exists, query}
  def exists(query, params), do: {:exists, query, params}

  @doc "Builds a field `IN (subquery)` filter."
  @spec subquery_in(term(), term(), [term()]) :: tuple()
  def subquery_in(field, query, params \\ [])

  def subquery_in(field, query, []), do: {field, {:subquery, :in, query}}
  def subquery_in(field, query, params), do: {field, {:subquery, :in, query, params}}

  @doc "Builds an ascending order spec."
  @spec asc(term()) :: tuple()
  def asc(expression), do: {normalize_selector_input(expression), :asc}

  @doc "Builds a descending order spec."
  @spec desc(term()) :: tuple()
  def desc(expression), do: {normalize_selector_input(expression), :desc}

  @doc "Builds an ascending order spec with nulls first."
  @spec asc_nulls_first(term()) :: tuple()
  def asc_nulls_first(expression), do: {normalize_selector_input(expression), :asc_nulls_first}

  @doc "Builds an ascending order spec with nulls last."
  @spec asc_nulls_last(term()) :: tuple()
  def asc_nulls_last(expression), do: {normalize_selector_input(expression), :asc_nulls_last}

  @doc "Builds a descending order spec with nulls first."
  @spec desc_nulls_first(term()) :: tuple()
  def desc_nulls_first(expression), do: {normalize_selector_input(expression), :desc_nulls_first}

  @doc "Builds a descending order spec with nulls last."
  @spec desc_nulls_last(term()) :: tuple()
  def desc_nulls_last(expression), do: {normalize_selector_input(expression), :desc_nulls_last}

  @doc "Builds a `GROUP BY ROLLUP(...)` keyword spec."
  @spec rollup(term()) :: keyword()
  def rollup(groups), do: [rollup: normalize(groups)]

  @doc "Builds a window-function selector for `Selecto.select/2`."
  @spec window(atom(), term(), keyword()) :: tuple()
  def window(function, arguments \\ [], opts \\ []) when is_atom(function) and is_list(opts) do
    {:window, normalize_window_call(function, arguments), normalize_window_opts(opts)}
  end

  @doc "Builds a frame tuple for window expressions."
  @spec frame(:rows | :range, term(), term()) :: tuple()
  def frame(type, start_bound, end_bound), do: {type, start_bound, end_bound}

  @doc "Builds a JSON extract tuple for `Selecto.json_select/2` or `Selecto.json_order_by/2`."
  def json_extract(column, path, opts_or_direction \\ [])

  def json_extract(column, path, direction) when basic_sort_direction(direction) do
    {:json_extract, column, path, direction}
  end

  def json_extract(column, path, opts) when is_list(opts) and opts != [] do
    {:json_extract, column, path, normalize_json_opts(opts)}
  end

  def json_extract(column, path, _opts), do: {:json_extract, column, path}

  @doc "Builds a JSON text extract tuple for `Selecto.json_select/2` or `Selecto.json_order_by/2`."
  def json_extract_text(column, path, opts_or_direction \\ [])

  def json_extract_text(column, path, direction) when basic_sort_direction(direction) do
    {:json_extract_text, column, path, direction}
  end

  def json_extract_text(column, path, opts) when is_list(opts) and opts != [] do
    {:json_extract_text, column, path, normalize_json_opts(opts)}
  end

  def json_extract_text(column, path, _opts), do: {:json_extract_text, column, path}

  @doc "Builds a JSON aggregate tuple for `Selecto.json_select/2`."
  def json_agg(field, opts \\ [])

  def json_agg(field, opts) when is_list(opts) and opts != [] do
    {:json_agg, field, normalize_json_opts(opts)}
  end

  def json_agg(field, _opts), do: {:json_agg, field}

  @doc "Builds a JSON object aggregate tuple for `Selecto.json_select/2`."
  def json_object_agg(key_field, value_field, opts \\ [])

  def json_object_agg(key_field, value_field, opts) when is_list(opts) and opts != [] do
    {:json_object_agg, key_field, value_field, normalize_json_opts(opts)}
  end

  def json_object_agg(key_field, value_field, _opts),
    do: {:json_object_agg, key_field, value_field}

  @doc "Builds an `AND` filter group."
  def unquote(:and)(filters) when is_list(filters), do: {:and, Enum.map(filters, &normalize/1)}

  @doc "Builds an `OR` filter group."
  def unquote(:or)(filters) when is_list(filters), do: {:or, Enum.map(filters, &normalize/1)}

  @doc "Builds a negated filter."
  def unquote(:not)(filter), do: {:not, normalize(filter)}

  @doc "Builds an expression only when the value is present."
  @spec when_present(term(), (term() -> term())) :: term() | nil
  def when_present(value, fun) when is_function(fun, 1) do
    if blank_value?(value) do
      nil
    else
      normalize(fun.(value))
    end
  end

  @doc "Alias for `when_present/2`."
  @spec maybe(term(), (term() -> term())) :: term() | nil
  def maybe(value, fun), do: when_present(value, fun)

  @doc "Drops nil/empty fragments and wraps the remainder in an `AND` group."
  @spec compact_and([term()]) :: term() | nil
  def compact_and(filters) when is_list(filters), do: compact_filters(filters, :and)

  @doc "Drops nil/empty fragments and wraps the remainder in an `OR` group."
  @spec compact_or([term()]) :: term() | nil
  def compact_or(filters) when is_list(filters), do: compact_filters(filters, :or)

  @doc "Conditionally applies a query pipeline function."
  @spec pipe_if(term(), term(), (term() -> term())) :: term()
  def pipe_if(query, condition, fun) when is_function(fun, 1) do
    case condition do
      false -> query
      nil -> query
      _ -> fun.(query)
    end
  end

  @doc "Appends a filter when a normalized filter is present."
  @spec append_filter(Selecto.Types.t(), term()) :: Selecto.Types.t()
  def append_filter(selecto, filter) do
    case filter do
      nil -> selecto
      [] -> selecto
      value -> Query.filter(selecto, normalize(value))
    end
  end

  @doc "Appends a selector when a normalized selector is present."
  @spec append_select(Selecto.Types.t(), term()) :: Selecto.Types.t()
  def append_select(selecto, selector) do
    case selector do
      nil -> selecto
      [] -> selecto
      value -> Query.select(selecto, normalize(value))
    end
  end

  @doc "Compacts a list of filters with `AND` semantics and appends it to the query."
  @spec merge_where(Selecto.Types.t(), [term()]) :: Selecto.Types.t()
  def merge_where(selecto, filters) when is_list(filters) do
    append_filter(selecto, compact_and(filters))
  end

  @doc "Appends an order-by expression when present."
  @spec append_order_by(Selecto.Types.t(), term()) :: Selecto.Types.t()
  def append_order_by(selecto, order_spec) do
    case order_spec do
      nil -> selecto
      [] -> selecto
      value -> Query.order_by(selecto, normalize(value))
    end
  end

  @doc "Appends a group-by expression when present."
  @spec append_group_by(Selecto.Types.t(), term()) :: Selecto.Types.t()
  def append_group_by(selecto, group_spec) do
    case group_spec do
      nil -> selecto
      [] -> selecto
      value -> Query.group_by(selecto, normalize(value))
    end
  end

  @doc "Conditionally appends an order-by expression when the value is present."
  @spec maybe_order_by(Selecto.Types.t(), term(), (term() -> term())) :: Selecto.Types.t()
  def maybe_order_by(selecto, value, fun) when is_function(fun, 1) do
    case when_present(value, fun) do
      nil -> selecto
      order_spec -> append_order_by(selecto, order_spec)
    end
  end

  @doc "Conditionally appends a group-by expression when the value is present."
  @spec maybe_group_by(Selecto.Types.t(), term(), (term() -> term())) :: Selecto.Types.t()
  def maybe_group_by(selecto, value, fun) when is_function(fun, 1) do
    case when_present(value, fun) do
      nil -> selecto
      group_spec -> append_group_by(selecto, group_spec)
    end
  end

  defp compact_filters(filters, conjunction) do
    compacted =
      filters
      |> Enum.flat_map(&flatten_filter(&1, conjunction))
      |> Enum.reject(&empty_fragment?/1)

    case compacted do
      [] -> nil
      [filter] -> filter
      many -> {conjunction, many}
    end
  end

  defp flatten_filter(nil, _conjunction), do: []
  defp flatten_filter([], _conjunction), do: []

  defp flatten_filter({conjunction, filters}, conjunction) when is_list(filters) do
    Enum.flat_map(filters, &flatten_filter(&1, conjunction))
  end

  defp flatten_filter(filter, _conjunction), do: [normalize(filter)]

  defp empty_fragment?(nil), do: true
  defp empty_fragment?([]), do: true
  defp empty_fragment?({:and, []}), do: true
  defp empty_fragment?({:or, []}), do: true
  defp empty_fragment?(_fragment), do: false

  defp blank_value?(nil), do: true
  defp blank_value?(""), do: true
  defp blank_value?(value) when is_binary(value), do: String.trim(value) == ""
  defp blank_value?(value) when is_list(value), do: value == []
  defp blank_value?(value) when is_map(value), do: map_size(value) == 0
  defp blank_value?(_value), do: false

  defp normalize_case_pair({condition, result}) do
    {normalize(condition), normalize_case_result(result)}
  end

  defp normalize_case_pair(other), do: other

  defp normalize_case_result(result) when is_binary(result), do: lit(result)
  defp normalize_case_result(result) when is_tuple(result), do: normalize(result)
  defp normalize_case_result(result) when is_list(result), do: normalize(result)
  defp normalize_case_result(result), do: result

  defp normalize_selector_input({:as, expression, alias_name}), do: as(expression, alias_name)
  defp normalize_selector_input({:eq, _, _} = expression), do: normalize(expression)
  defp normalize_selector_input({:neq, _, _} = expression), do: normalize(expression)
  defp normalize_selector_input({:not_eq, _, _} = expression), do: normalize(expression)
  defp normalize_selector_input({:gt, _, _} = expression), do: normalize(expression)
  defp normalize_selector_input({:gte, _, _} = expression), do: normalize(expression)
  defp normalize_selector_input({:lt, _, _} = expression), do: normalize(expression)
  defp normalize_selector_input({:lte, _, _} = expression), do: normalize(expression)
  defp normalize_selector_input({:like, _, _} = expression), do: normalize(expression)
  defp normalize_selector_input({:ilike, _, _} = expression), do: normalize(expression)
  defp normalize_selector_input({:contains, _, _} = expression), do: normalize(expression)
  defp normalize_selector_input({:starts_with, _, _} = expression), do: normalize(expression)
  defp normalize_selector_input({:ends_with, _, _} = expression), do: normalize(expression)
  defp normalize_selector_input({:is_null, _} = expression), do: normalize(expression)
  defp normalize_selector_input({:not_null, _} = expression), do: normalize(expression)
  defp normalize_selector_input({:between, _, _, _} = expression), do: normalize(expression)
  defp normalize_selector_input({:in, _, _} = expression), do: normalize(expression)

  defp normalize_selector_input({:window, window_call, opts}) when is_list(opts),
    do: normalize({:window, window_call, opts})

  defp normalize_selector_input({:field, selector}),
    do: {:field, normalize_selector_input(selector)}

  defp normalize_selector_input({:field, selector, alias_name}),
    do: {:field, normalize_selector_input(selector), alias_name}

  defp normalize_selector_input({:func, function_name, args}) when is_list(args) do
    {:func, function_name, Enum.map(args, &normalize_selector_input/1)}
  end

  defp normalize_selector_input({:func, function_name, args, opts})
       when is_list(args) and is_list(opts) do
    {:func, function_name, Enum.map(args, &normalize_selector_input/1), opts}
  end

  defp normalize_selector_input({func, fields}) when wrapper_function(func) do
    {func, Enum.map(List.wrap(fields), &normalize_selector_input/1)}
  end

  defp normalize_selector_input(other), do: other

  defp normalize_window_call(function, arguments) when is_atom(function) do
    [function | Enum.map(List.wrap(arguments), &normalize_selector_input/1)]
    |> List.to_tuple()
  end

  defp normalize_window_call(window_call) when is_tuple(window_call) do
    window_call
    |> Tuple.to_list()
    |> Enum.map(fn
      function when is_atom(function) -> function
      argument -> normalize_selector_input(argument)
    end)
    |> List.to_tuple()
  end

  defp normalize_window_opts(opts) do
    Enum.map(opts, fn
      {:over, over_opts} when is_list(over_opts) -> {:over, normalize_over_opts(over_opts)}
      {key, value} when key == :as or key == :filter -> {key, normalize(value)}
      {key, value} -> {key, value}
    end)
  end

  defp normalize_over_opts(over_opts) do
    Enum.map(over_opts, fn
      {:partition_by, fields} ->
        {:partition_by, Enum.map(List.wrap(fields), &normalize_selector_input/1)}

      {:order_by, orders} ->
        {:order_by, Enum.map(List.wrap(orders), &normalize/1)}

      {:frame, frame_spec} ->
        {:frame, frame_spec}

      other ->
        other
    end)
  end

  defp normalize_json_opts(opts) do
    Enum.map(opts, fn
      {:as, alias_name} -> {:as, to_string(alias_name)}
      other -> other
    end)
  end
end
