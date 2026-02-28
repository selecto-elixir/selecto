defmodule Selecto.Builder.Sql.Select do
  @moduledoc """
  SELECT-expression compiler for Selecto query AST values.

  This module normalizes selector terms (fields, literals, functions, CASE,
  subqueries, JSON/array helpers, and custom SQL fragments) into iodata SQL
  fragments alongside required joins and bind parameters.
  """

  import Selecto.Builder.Sql.Helpers

  alias Selecto.Jsonb

  ### TODO alter prep_selector to return the data type

  @doc """
  Process field selectors with various formats:

  Custom SQL support:
    {:custom_sql, sql_template, field_mappings} - safely handle custom column SQL with field validation

  Standard formats:

    "field" # - plain old field from one of the tables
    {:field, field } #- same as above disamg for predicate second+ position
    {:literal, "value"} #- for literal values
    {:literal, 1.0}
    {:literal, 1}
    {:literal, datetime} etc
    {:func, SELECTOR}
    {:count, *} (for count(*))
    {:func, SELECTOR, SELECTOR}
    {:func, SELECTOR, SELECTOR, SELECTOR} #...
    {:extract, part, SELECTOR}
    {:case, [PREDICATE, SELECTOR, ..., :else, SELECTOR]}
    {:coalese, [SELECTOR, SELECTOR, ...]}
    {:greatest, [SELECTOR, SELECTOR, ...]}
    {:least, [SELECTOR, SELECTOR, ...]}
    {:nullif, [SELECTOR, LITERAL_SELECTOR]} #LITERAL_SELECTOR means naked value treated as lit not field
    {:subquery, [SELECTOR, SELECTOR, ...], PREDICATE}
  """

  ### TODO ability to select distinct on count( field )...

  # Custom SQL clause (Phase 1 safety implementation)
  def prep_selector(selecto, {:custom_sql, sql_template, field_mappings})
      when is_binary(sql_template) do
    # Validate that all referenced fields exist
    available_fields = get_available_fields(selecto)
    validate_field_references(selecto, field_mappings, available_fields)

    # Replace field placeholders with actual field references
    safe_sql = substitute_field_references(sql_template, field_mappings, selecto)

    # Return as safe iodata (no parameters for now - Phase 1 safety only)
    {[safe_sql], :selecto_root, []}
  end

  # Phase 4: iodata-based prep_selector/2 functions (now main functions)
  def prep_selector(selecto, val) when is_integer(val) do
    prep_selector(selecto, val, %{})
  end

  def prep_selector(selecto, val) when is_float(val) do
    prep_selector(selecto, val, %{})
  end

  def prep_selector(selecto, val) when is_boolean(val) do
    prep_selector(selecto, val, %{})
  end

  def prep_selector(selecto, {:count}) do
    prep_selector(selecto, {:count}, %{})
  end

  def prep_selector(selecto, {:count, "*"}) do
    prep_selector(selecto, {:count, "*"}, %{})
  end

  def prep_selector(selecto, {:count, "*", filter}) do
    prep_selector(selecto, {:count, "*", filter}, %{})
  end

  def prep_selector(selecto, {:subquery, dynamic, params}) do
    prep_selector(selecto, {:subquery, dynamic, params}, %{})
  end

  def prep_selector(selecto, {:case, pairs}) when is_list(pairs) do
    prep_selector(selecto, {:case, pairs}, %{})
  end

  def prep_selector(selecto, {:case, pairs, else_clause}) when is_list(pairs) do
    prep_selector(selecto, {:case, pairs, else_clause}, %{})
  end

  # Handle new CASE expression specifications
  def prep_selector(selecto, {:case, case_spec}) do
    prep_selector(selecto, {:case, case_spec}, %{})
  end

  # Handle searched CASE expression specifications
  def prep_selector(selecto, {:case_when, case_spec}) do
    prep_selector(selecto, {:case_when, case_spec}, %{})
  end

  def prep_selector(selecto, {func, fields})
      when func in [:concat, :coalesce, :greatest, :least, :nullif] do
    prep_selector(selecto, {func, fields}, %{})
  end

  def prep_selector(selecto, {:extract, field, format}) do
    prep_selector(selecto, {:extract, field, format}, %{})
  end

  # Handle array_length function (2-tuple)
  def prep_selector(selecto, {:array_length, _} = selector) do
    prep_selector(selecto, selector, %{})
  end

  # Handle other 2-tuple array functions
  def prep_selector(selecto, {:cardinality, _} = selector) do
    prep_selector(selecto, selector, %{})
  end

  def prep_selector(selecto, {:array_ndims, _} = selector) do
    prep_selector(selecto, selector, %{})
  end

  def prep_selector(selecto, {:array_dims, _} = selector) do
    prep_selector(selecto, selector, %{})
  end

  def prep_selector(selecto, {:unnest, _} = selector) do
    prep_selector(selecto, selector, %{})
  end

  # Handle array functions specifically before generic 3-tuple handler
  # Array construction - build array from values
  def prep_selector(selecto, {:array, values}) when is_list(values) do
    prep_selector(selecto, {:array, values}, %{})
  end

  # These have a third argument that is NOT a filter
  def prep_selector(selecto, {:array_cat, _, _} = selector) do
    prep_selector(selecto, selector, %{})
  end

  def prep_selector(selecto, {:array_to_string, _, _} = selector) do
    prep_selector(selecto, selector, %{})
  end

  def prep_selector(selecto, {:string_to_array, _, _} = selector) do
    prep_selector(selecto, selector, %{})
  end

  # Handle 3-tuple array functions before generic handler
  def prep_selector(selecto, {:array_append, _, _} = selector) do
    prep_selector(selecto, selector, %{})
  end

  def prep_selector(selecto, {:array_prepend, _, _} = selector) do
    prep_selector(selecto, selector, %{})
  end

  def prep_selector(selecto, {:array_fill, _, _} = selector) do
    prep_selector(selecto, selector, %{})
  end

  def prep_selector(selecto, {:array_remove, _, _} = selector) do
    prep_selector(selecto, selector, %{})
  end

  def prep_selector(selecto, {:array_position, _, _} = selector) do
    prep_selector(selecto, selector, %{})
  end

  def prep_selector(selecto, {:array_positions, _, _} = selector) do
    prep_selector(selecto, selector, %{})
  end

  # Handle 4-tuple array functions
  def prep_selector(selecto, {:array_replace, _, _, _} = selector) do
    prep_selector(selecto, selector, %{})
  end

  def prep_selector(selecto, {:array_position, _, _, _} = selector) do
    prep_selector(selecto, selector, %{})
  end

  # Generic 3-tuple handler - assumes third element is a filter
  def prep_selector(selecto, {func, field, filter}) when is_atom(func) do
    prep_selector(selecto, {func, field, filter}, %{})
  end

  def prep_selector(selecto, {:literal, value}) when is_integer(value) do
    prep_selector(selecto, {:literal, value}, %{})
  end

  # Special case for rollup position literals that should not be parameterized
  def prep_selector(selecto, {:literal_position, value}) when is_integer(value) do
    prep_selector(selecto, {:literal_position, value}, %{})
  end

  # Special case for string literals that should not be parameterized (e.g., in concat)
  def prep_selector(selecto, {:literal_string, value}) when is_bitstring(value) do
    prep_selector(selecto, {:literal_string, value}, %{})
  end

  def prep_selector(selecto, {:literal, value}) when is_bitstring(value) do
    prep_selector(selecto, {:literal, value}, %{})
  end

  def prep_selector(selecto, {:to_char, {field, format}}) do
    prep_selector(selecto, {:to_char, {field, format}}, %{})
  end

  def prep_selector(selecto, {:raw_sql, sql}) when is_binary(sql) do
    prep_selector(selecto, {:raw_sql, sql}, %{})
  end

  # Add 2-argument versions for bucket aggregate types
  def prep_selector(selecto, {:count_age_bucket, field, min, max}) do
    prep_selector(selecto, {:count_age_bucket, field, min, max}, %{})
  end

  def prep_selector(selecto, {:count_age_bucket_other, field, bucket_ranges}) do
    prep_selector(selecto, {:count_age_bucket_other, field, bucket_ranges}, %{})
  end

  def prep_selector(selecto, {:count_bucket, field, min, max}) do
    prep_selector(selecto, {:count_bucket, field, min, max}, %{})
  end

  def prep_selector(selecto, {:count_bucket_other, field, bucket_ranges}) do
    prep_selector(selecto, {:count_bucket_other, field, bucket_ranges}, %{})
  end

  def prep_selector(selecto, {:field, selector}) do
    prep_selector(selecto, {:field, selector}, %{})
  end

  def prep_selector(selecto, {func}) when is_atom(func) do
    prep_selector(selecto, {func}, %{})
  end

  def prep_selector(selecto, {func, selector}) when is_atom(func) do
    prep_selector(selecto, {func, selector}, %{})
  end

  def prep_selector(selecto, selector) when is_binary(selector) do
    prep_selector(selecto, selector, %{})
  end

  def prep_selector(selecto, selector) when is_atom(selector) do
    prep_selector(selecto, Atom.to_string(selector), %{})
  end

  def prep_selector(selecto, selector) do
    prep_selector(selecto, selector, %{})
  end

  # Phase 4: iodata-based prep_selector/3 functions
  def prep_selector(_selecto, val, _pivot_aliases) when is_integer(val) do
    {{:param, val}, :selecto_root, [val]}
  end

  def prep_selector(_selecto, val, _pivot_aliases) when is_float(val) do
    {{:param, val}, :selecto_root, [val]}
  end

  def prep_selector(_selecto, val, _pivot_aliases) when is_boolean(val) do
    {{:param, val}, :selecto_root, [val]}
  end

  def prep_selector(_selecto, {:count}, _pivot_aliases) do
    {["count(*)"], :selecto_root, []}
  end

  def prep_selector(_selecto, {:count, "*"}, _pivot_aliases) do
    {["count(*)"], :selecto_root, []}
  end

  def prep_selector(selecto, {:count, "*", filter}, pivot_aliases) do
    prep_selector(selecto, {:count, {:literal, "*"}, filter}, pivot_aliases)
  end

  def prep_selector(_selecto, {:subquery, dynamic, params}, _pivot_aliases) do
    # Dynamic subquery is already in the right format
    {[dynamic], [], params}
  end

  def prep_selector(selecto, {:case, pairs}, pivot_aliases) when is_list(pairs) do
    prep_selector(selecto, {:case, pairs, nil}, pivot_aliases)
  end

  def prep_selector(selecto, {:case, pairs, else_clause}, pivot_aliases) when is_list(pairs) do
    {sel_parts, join, par} =
      Enum.reduce(
        pairs,
        {[], [], []},
        fn {filter, selector}, {s, j, p} ->
          {join_w, filters_iodata, param_w} =
            Selecto.Builder.Sql.Where.build(selecto, {:and, List.wrap(filter)})

          {sel_iodata, join_s, param_s} = prep_selector(selecto, selector, pivot_aliases)

          when_clause = ["when ", filters_iodata, " then ", sel_iodata]

          {s ++ [when_clause], j ++ List.wrap(join_s) ++ List.wrap(join_w),
           p ++ param_w ++ param_s}
        end
      )

    case else_clause do
      nil ->
        case_iodata = ["case ", Enum.intersperse(sel_parts, " "), " end"]
        {case_iodata, join, par}

      _ ->
        {sel_else_iodata, join_s, param_s} = prep_selector(selecto, else_clause, pivot_aliases)

        case_iodata = [
          "case ",
          Enum.intersperse(sel_parts, " "),
          " else ",
          sel_else_iodata,
          " end"
        ]

        {case_iodata, join ++ List.wrap(join_s), par ++ param_s}
    end
  end

  def prep_selector(selecto, {:case, case_spec}, _pivot_aliases) do
    # Use the CaseExpression builder for the new specification format
    {case_sql, params} = Selecto.Builder.CaseExpression.build_case_for_select(case_spec, selecto)
    {case_sql, [], params}
  end

  def prep_selector(selecto, {:case_when, case_spec}, _pivot_aliases) do
    # Use the CaseExpression builder for the new specification format
    {case_sql, params} = Selecto.Builder.CaseExpression.build_case_for_select(case_spec, selecto)
    {case_sql, [], params}
  end

  def prep_selector(selecto, {func, fields}, pivot_aliases)
      when func in [:concat, :coalesce, :greatest, :least, :nullif] do
    # Special handling for CONCAT to avoid PostgreSQL parameter type issues
    processed_fields =
      if func == :concat do
        Enum.map(List.wrap(fields), fn
          {:literal, value} when is_bitstring(value) ->
            # Convert string literals to non-parameterized form for CONCAT
            {:literal_string, value}

          other ->
            other
        end)
      else
        List.wrap(fields)
      end

    {sel_parts, join, param} =
      Enum.reduce(processed_fields, {[], [], []}, fn f, {select, join, param} ->
        {s_iodata, j, p} = prep_selector(selecto, f, pivot_aliases)
        {select ++ [s_iodata], join ++ List.wrap(j), param ++ p}
      end)

    func_name = Atom.to_string(func)
    func_iodata = [func_name, "( ", Enum.intersperse(sel_parts, ", "), " )"]
    {func_iodata, join, param}
  end

  def prep_selector(selecto, {:extract, field, format}, pivot_aliases) do
    {sel_iodata, join, param} = prep_selector(selecto, field, pivot_aliases)
    check_string(format)
    extract_iodata = ["extract( ", format, " from  ", sel_iodata, ")"]
    {extract_iodata, join, param}
  end

  def prep_selector(selecto, {:array_length, _} = selector, _pivot_aliases) do
    # Delegate to Functions module
    case Selecto.SQL.Functions.prep_advanced_selector(selecto, selector) do
      nil -> raise "array_length function not properly implemented"
      result -> result
    end
  end

  def prep_selector(selecto, {:cardinality, _} = selector, _pivot_aliases) do
    case Selecto.SQL.Functions.prep_advanced_selector(selecto, selector) do
      nil -> raise "cardinality function not properly implemented"
      result -> result
    end
  end

  def prep_selector(selecto, {:array_ndims, _} = selector, _pivot_aliases) do
    case Selecto.SQL.Functions.prep_advanced_selector(selecto, selector) do
      nil -> raise "array_ndims function not properly implemented"
      result -> result
    end
  end

  def prep_selector(selecto, {:array_dims, _} = selector, _pivot_aliases) do
    case Selecto.SQL.Functions.prep_advanced_selector(selecto, selector) do
      nil -> raise "array_dims function not properly implemented"
      result -> result
    end
  end

  def prep_selector(selecto, {:unnest, _} = selector, _pivot_aliases) do
    case Selecto.SQL.Functions.prep_advanced_selector(selecto, selector) do
      nil -> raise "unnest function not properly implemented"
      result -> result
    end
  end

  def prep_selector(selecto, {:array, values}, _pivot_aliases) when is_list(values) do
    # Build ARRAY[val1, val2, ...] expression
    {values_iodata, values_params} =
      values
      |> Enum.map(fn value ->
        case value do
          v when is_binary(v) or is_number(v) or is_boolean(v) ->
            {{:param, v}, [v]}

          nil ->
            {"NULL", []}

          field when is_binary(field) ->
            # Could be a field reference
            {sel, _join, param} = prep_selector(selecto, field)
            {sel, param}

          expr when is_tuple(expr) ->
            # Complex expression
            {sel, _join, param} = prep_selector(selecto, expr)
            {sel, param}
        end
      end)
      |> Enum.reduce({[], []}, fn {io, p}, {acc_io, acc_p} ->
        {acc_io ++ [io], acc_p ++ p}
      end)

    array_elements = Enum.intersperse(values_iodata, ", ")
    iodata = ["ARRAY[", array_elements, "]"]

    {iodata, [], values_params}
  end

  def prep_selector(selecto, {:array_cat, _, _} = selector, _pivot_aliases) do
    # Delegate to Functions module
    case Selecto.SQL.Functions.prep_advanced_selector(selecto, selector) do
      nil -> raise "array_cat function not properly implemented"
      result -> result
    end
  end

  def prep_selector(selecto, {:array_to_string, _, _} = selector, _pivot_aliases) do
    # Delegate to Functions module
    case Selecto.SQL.Functions.prep_advanced_selector(selecto, selector) do
      nil -> raise "array_to_string function not properly implemented"
      result -> result
    end
  end

  def prep_selector(selecto, {:string_to_array, _, _} = selector, _pivot_aliases) do
    # Delegate to Functions module
    case Selecto.SQL.Functions.prep_advanced_selector(selecto, selector) do
      nil -> raise "string_to_array function not properly implemented"
      result -> result
    end
  end

  def prep_selector(selecto, {:array_append, _, _} = selector, _pivot_aliases) do
    case Selecto.SQL.Functions.prep_advanced_selector(selecto, selector) do
      nil -> raise "array_append function not properly implemented"
      result -> result
    end
  end

  def prep_selector(selecto, {:array_prepend, _, _} = selector, _pivot_aliases) do
    case Selecto.SQL.Functions.prep_advanced_selector(selecto, selector) do
      nil -> raise "array_prepend function not properly implemented"
      result -> result
    end
  end

  def prep_selector(selecto, {:array_fill, _, _} = selector, _pivot_aliases) do
    case Selecto.SQL.Functions.prep_advanced_selector(selecto, selector) do
      nil -> raise "array_fill function not properly implemented"
      result -> result
    end
  end

  def prep_selector(selecto, {:array_remove, _, _} = selector, _pivot_aliases) do
    case Selecto.SQL.Functions.prep_advanced_selector(selecto, selector) do
      nil -> raise "array_remove function not properly implemented"
      result -> result
    end
  end

  def prep_selector(selecto, {:array_position, _, _} = selector, _pivot_aliases) do
    case Selecto.SQL.Functions.prep_advanced_selector(selecto, selector) do
      nil -> raise "array_position function not properly implemented"
      result -> result
    end
  end

  def prep_selector(selecto, {:array_positions, _, _} = selector, _pivot_aliases) do
    case Selecto.SQL.Functions.prep_advanced_selector(selecto, selector) do
      nil -> raise "array_positions function not properly implemented"
      result -> result
    end
  end

  def prep_selector(selecto, {:array_replace, _, _, _} = selector, _pivot_aliases) do
    case Selecto.SQL.Functions.prep_advanced_selector(selecto, selector) do
      nil -> raise "array_replace function not properly implemented"
      result -> result
    end
  end

  def prep_selector(selecto, {:array_position, _, _, _} = selector, _pivot_aliases) do
    case Selecto.SQL.Functions.prep_advanced_selector(selecto, selector) do
      nil -> raise "array_position with start function not properly implemented"
      result -> result
    end
  end

  # Handle count_age_bucket_other BEFORE the generic 3-element tuple handler
  # The third element is bucket_ranges configuration, not a filter
  def prep_selector(selecto, {:count_age_bucket_other, field, bucket_ranges}, pivot_aliases) do
    {field_iodata, join, param} = prep_selector(selecto, field, pivot_aliases)

    # Parse ranges to build the "Other" condition
    ranges = parse_bucket_ranges_simple(bucket_ranges)

    conditions =
      Enum.map(ranges, fn
        {min, max, _} when is_integer(min) and is_integer(max) ->
          if min == max do
            [
              "EXTRACT(DAY FROM AGE(CURRENT_DATE, ",
              field_iodata,
              ")) != ",
              Integer.to_string(min)
            ]
          else
            [
              "NOT (EXTRACT(DAY FROM AGE(CURRENT_DATE, ",
              field_iodata,
              ")) >= ",
              Integer.to_string(min),
              " AND EXTRACT(DAY FROM AGE(CURRENT_DATE, ",
              field_iodata,
              ")) <= ",
              Integer.to_string(max),
              ")"
            ]
          end

        {min, :infinity, _} ->
          ["EXTRACT(DAY FROM AGE(CURRENT_DATE, ", field_iodata, ")) < ", Integer.to_string(min)]

        {:negative_infinity, max, _} ->
          ["EXTRACT(DAY FROM AGE(CURRENT_DATE, ", field_iodata, ")) > ", Integer.to_string(max)]

        _ ->
          nil
      end)
      |> Enum.reject(&is_nil/1)

    case_sql =
      if Enum.empty?(conditions) do
        ["COUNT(*)"]
      else
        ["COUNT(CASE WHEN ", Enum.intersperse(conditions, " AND "), " THEN 1 END)"]
      end

    {case_sql, join, param}
  end

  # Handle count_bucket_other BEFORE the generic 3-element tuple handler
  # The third element is bucket_ranges configuration, not a filter
  def prep_selector(selecto, {:count_bucket_other, field, bucket_ranges}, pivot_aliases) do
    {field_iodata, join, param} = prep_selector(selecto, field, pivot_aliases)

    # Parse ranges to build the "Other" condition
    ranges = parse_bucket_ranges_simple(bucket_ranges)

    conditions =
      Enum.map(ranges, fn
        {min, max, _} when is_integer(min) and is_integer(max) ->
          if min == max do
            [field_iodata, " != ", Integer.to_string(min)]
          else
            [
              "NOT (",
              field_iodata,
              " >= ",
              Integer.to_string(min),
              " AND ",
              field_iodata,
              " <= ",
              Integer.to_string(max),
              ")"
            ]
          end

        {min, :infinity, _} ->
          [field_iodata, " < ", Integer.to_string(min)]

        {:negative_infinity, max, _} ->
          [field_iodata, " > ", Integer.to_string(max)]

        _ ->
          nil
      end)
      |> Enum.reject(&is_nil/1)

    case_sql =
      if Enum.empty?(conditions) do
        ["COUNT(*)"]
      else
        ["COUNT(CASE WHEN ", Enum.intersperse(conditions, " AND "), " THEN 1 END)"]
      end

    {case_sql, join, param}
  end

  def prep_selector(selecto, {func, field, filter}, pivot_aliases) when is_atom(func) do
    {sel_iodata, join, param} = prep_selector(selecto, field, pivot_aliases)

    {join_w, filters_iodata, param_w} =
      Selecto.Builder.Sql.Where.build(selecto, {:and, List.wrap(filter)})

    func_name = Atom.to_string(func) |> check_string()
    filter_iodata = [func_name, "(", sel_iodata, ") FILTER (where ", filters_iodata, ")"]
    {filter_iodata, List.wrap(join) ++ List.wrap(join_w), param ++ param_w}
  end

  # {:literal, value} should NOT be parameterized - render as SQL literal
  # Special case for "*" used in COUNT(*) and similar functions
  def prep_selector(_selecto, {:literal, "*"}, _pivot_aliases) do
    {["*"], :selecto_root, []}
  end

  # Integer literals - render as raw numbers in SQL
  def prep_selector(_selecto, {:literal, value}, _pivot_aliases) when is_integer(value) do
    {[Integer.to_string(value)], :selecto_root, []}
  end

  # String literals - render as SQL-escaped string literals with quotes
  def prep_selector(_selecto, {:literal, value}, _pivot_aliases) when is_bitstring(value) do
    {["'", String.replace(value, "'", "''"), "'"], :selecto_root, []}
  end

  # Float literals - render as raw floats in SQL
  def prep_selector(_selecto, {:literal, value}, _pivot_aliases) when is_float(value) do
    {[Float.to_string(value)], :selecto_root, []}
  end

  # Boolean literals - render as SQL boolean literals
  def prep_selector(_selecto, {:literal, true}, _pivot_aliases) do
    {["TRUE"], :selecto_root, []}
  end

  def prep_selector(_selecto, {:literal, false}, _pivot_aliases) do
    {["FALSE"], :selecto_root, []}
  end

  # NULL literal
  def prep_selector(_selecto, {:literal, nil}, _pivot_aliases) do
    {["NULL"], :selecto_root, []}
  end

  # {:literal_position, value} is for positional numbers (e.g., ORDER BY 1)
  def prep_selector(_selecto, {:literal_position, value}, _pivot_aliases)
      when is_integer(value) do
    {[Integer.to_string(value)], :selecto_root, []}
  end

  # {:literal_string, value} is an alias for {:literal, value} when it's a string
  # Keeping for backwards compatibility
  def prep_selector(_selecto, {:literal_string, value}, _pivot_aliases)
      when is_bitstring(value) do
    {["'", String.replace(value, "'", "''"), "'"], :selecto_root, []}
  end

  def prep_selector(selecto, {:to_char, {field, format}}, pivot_aliases) do
    {sel_iodata, join, param} = prep_selector(selecto, field, pivot_aliases)
    to_char_iodata = ["to_char(", sel_iodata, ", ", single_wrap(format), ")"]
    {to_char_iodata, join, param}
  end

  def prep_selector(selecto, {:field, selector}, pivot_aliases) do
    prep_selector(selecto, selector, pivot_aliases)
  end

  def prep_selector(_selecto, {func}, _pivot_aliases) when is_atom(func) do
    func_name = Atom.to_string(func) |> check_string()
    {[func_name, "()"], :selecto_root, []}
  end

  def prep_selector(_selecto, {:raw_sql, sql}, _pivot_aliases) when is_binary(sql) do
    # For raw SQL, just return it as-is
    # This is used for bucket CASE expressions
    {[sql], :selecto_root, []}
  end

  # Handle count_age_bucket for age-based buckets
  def prep_selector(selecto, {:count_age_bucket, field, min, max}, pivot_aliases) do
    {field_iodata, join, param} = prep_selector(selecto, field, pivot_aliases)

    case_sql =
      cond do
        min == max ->
          [
            "COUNT(CASE WHEN EXTRACT(DAY FROM AGE(CURRENT_DATE, ",
            field_iodata,
            ")) = ",
            Integer.to_string(min),
            " THEN 1 END)"
          ]

        max == :infinity ->
          [
            "COUNT(CASE WHEN EXTRACT(DAY FROM AGE(CURRENT_DATE, ",
            field_iodata,
            ")) >= ",
            Integer.to_string(min),
            " THEN 1 END)"
          ]

        min == :negative_infinity ->
          [
            "COUNT(CASE WHEN EXTRACT(DAY FROM AGE(CURRENT_DATE, ",
            field_iodata,
            ")) <= ",
            Integer.to_string(max),
            " THEN 1 END)"
          ]

        true ->
          [
            "COUNT(CASE WHEN EXTRACT(DAY FROM AGE(CURRENT_DATE, ",
            field_iodata,
            ")) >= ",
            Integer.to_string(min),
            " AND EXTRACT(DAY FROM AGE(CURRENT_DATE, ",
            field_iodata,
            ")) <= ",
            Integer.to_string(max),
            " THEN 1 END)"
          ]
      end

    {case_sql, join, param}
  end

  # DUPLICATE REMOVED - This handler is now placed BEFORE the generic 3-element tuple handler
  # to ensure it matches before the generic handler tries to treat bucket_ranges as a filter

  # Handle count_bucket for numeric buckets
  def prep_selector(selecto, {:count_bucket, field, min, max}, pivot_aliases) do
    {field_iodata, join, param} = prep_selector(selecto, field, pivot_aliases)

    case_sql =
      cond do
        min == max ->
          ["COUNT(CASE WHEN ", field_iodata, " = ", Integer.to_string(min), " THEN 1 END)"]

        max == :infinity ->
          ["COUNT(CASE WHEN ", field_iodata, " >= ", Integer.to_string(min), " THEN 1 END)"]

        min == :negative_infinity ->
          ["COUNT(CASE WHEN ", field_iodata, " <= ", Integer.to_string(max), " THEN 1 END)"]

        true ->
          [
            "COUNT(CASE WHEN ",
            field_iodata,
            " >= ",
            Integer.to_string(min),
            " AND ",
            field_iodata,
            " <= ",
            Integer.to_string(max),
            " THEN 1 END)"
          ]
      end

    {case_sql, join, param}
  end

  # DUPLICATE REMOVED - This handler is now placed BEFORE the generic 3-element tuple handler
  # to ensure it matches before the generic handler tries to treat bucket_ranges as a filter

  # Special handling for count_distinct - generates COUNT(DISTINCT column) not count_distinct(column)
  def prep_selector(selecto, {:count_distinct, selector}, pivot_aliases) do
    {sel_iodata, join, param} = prep_selector(selecto, selector, pivot_aliases)
    func_call_iodata = ["COUNT(DISTINCT ", sel_iodata, ")"]
    {func_call_iodata, join, param}
  end

  def prep_selector(selecto, {func, selector}, pivot_aliases) when is_atom(func) do
    {sel_iodata, join, param} = prep_selector(selecto, selector, pivot_aliases)
    func_name = Atom.to_string(func) |> check_string()
    func_call_iodata = [func_name, "(", sel_iodata, ")"]
    {func_call_iodata, join, param}
  end

  def prep_selector(selecto, selector, pivot_aliases)
      when is_binary(selector) or is_atom(selector) do
    selector = if is_atom(selector), do: Atom.to_string(selector), else: selector

    # First check if this is a JSONB path (e.g., "attributes.color")
    domain = selecto.config

    case Jsonb.parse_field_reference(selector, domain) do
      {:jsonb, column, path} ->
        # This is a JSONB path - generate extraction SQL
        prep_jsonb_selector(selecto, column, path, pivot_aliases)

      {:regular, _} ->
        # Not a JSONB path, use standard field resolution
        prep_regular_selector(selecto, selector, pivot_aliases)
    end
  end

  def prep_selector(selecto, selector, _pivot_aliases) do
    # Try advanced SQL functions first
    case Selecto.SQL.Functions.prep_advanced_selector(selecto, selector) do
      nil ->
        # Not an advanced function, fall back to error
        raise "Unsupported selector type: #{inspect(selector)}. Supported types: atoms, tuples with functions, strings, and literals."

      result ->
        result
    end
  end

  # Handle JSONB path selectors - extract value from JSONB column
  defp prep_jsonb_selector(selecto, column, path, pivot_aliases) do
    domain = selecto.config

    # Get schema info for type casting
    path_schema = Jsonb.get_path_schema(domain, column, path)
    field_type = if path_schema, do: Map.get(path_schema, :type), else: nil
    cast = Jsonb.pg_cast_for_type(field_type)

    # Get the table alias
    conf = Selecto.field(selecto, column)

    join_alias =
      if conf do
        Map.get(pivot_aliases, conf.requires_join, conf.requires_join)
      else
        :selecto_root
      end

    # Build the extraction expression
    table_alias_str = get_table_alias_string(selecto, join_alias)

    extraction =
      Jsonb.build_extraction(column, path,
        as_text: true,
        table_alias: table_alias_str,
        cast: cast
      )

    requires_join = if conf, do: conf.requires_join, else: :selecto_root
    {[extraction], requires_join, []}
  end

  # Standard field resolution (non-JSONB)
  defp prep_regular_selector(selecto, selector, pivot_aliases) do
    # First check if it's a dynamic column (from UNNEST, CTE, etc.)
    set = Map.get(selecto, :set) || %{}
    dynamic_columns = if is_map(set), do: Map.get(set, :dynamic_columns, %{}), else: %{}

    conf =
      if Map.has_key?(dynamic_columns, selector) do
        # Create a minimal field config for dynamic columns
        %{
          name: selector,
          field: selector,
          requires_join: nil,
          select: nil
        }
      else
        Selecto.field(selecto, selector)
      end

    # Handle case where field configuration doesn't exist
    if conf == nil do
      raise build_missing_field_error(selecto, selector, dynamic_columns)
    end

    case Map.get(conf, :select) do
      nil ->
        # Use the database field name (field property) instead of display name (name property)
        field_name = Map.get(conf, :field, conf.name)

        # For dynamic columns, use them directly without table qualification
        if Map.has_key?(dynamic_columns, selector) do
          # Dynamic columns from UNNEST don't need table qualification
          {[field_name], :selecto_root, []}
        else
          # Check if we have a pivot alias for this join
          join_alias = Map.get(pivot_aliases, conf.requires_join, conf.requires_join)
          field_iodata = [build_selector_string(selecto, join_alias, field_name)]
          {field_iodata, conf.requires_join, []}
        end

      sub when is_binary(sub) ->
        # If the select value is a string, treat it as literal SQL
        # This handles cases like "string_agg(tags[name], ', ')" from tagging configurations
        {[sub], conf.requires_join || :selecto_root, []}

      sub ->
        # For other selector types, process recursively
        prep_selector(selecto, sub, pivot_aliases)
    end
  end

  defp build_missing_field_error(selecto, selector, dynamic_columns) do
    selector_str = to_string(selector)

    available_fields =
      (Map.keys(selecto.config.columns || %{}) ++ Map.keys(dynamic_columns))
      |> Enum.map(&to_string/1)
      |> Enum.uniq()
      |> Enum.sort()

    suggestions =
      case Selecto.field_suggestions(selecto, selector_str) do
        [] -> fuzzy_suggestions(selector_str, available_fields, 5)
        list -> list
      end

    computed_aliases = computed_aliases(selecto)
    computed_alias_hint? = selector_str in computed_aliases

    suggestion_text =
      case suggestions do
        [] -> "No close field suggestions found."
        list -> "Did you mean: #{Enum.join(list, ", ")}"
      end

    computed_hint_text =
      if computed_alias_hint? do
        "\nHint: '#{selector_str}' matches a computed alias. In selectors/shapes use explicit source expressions (for example {:field, \"metadata.some_path\", \"#{selector_str}\"})."
      else
        ""
      end

    "Field '#{selector_str}' not found in selecto configuration. " <>
      suggestion_text <>
      computed_hint_text <>
      "\nAvailable fields: #{inspect(available_fields)}"
  end

  defp fuzzy_suggestions(term, candidates, limit) do
    candidates
    |> Enum.map(fn candidate ->
      {candidate, String.jaro_distance(String.downcase(candidate), String.downcase(term))}
    end)
    |> Enum.sort_by(fn {_candidate, score} -> score end, :desc)
    |> Enum.filter(fn {_candidate, score} -> score >= 0.72 end)
    |> Enum.take(limit)
    |> Enum.map(fn {candidate, _score} -> candidate end)
  end

  defp computed_aliases(selecto) do
    json_aliases =
      selecto.set
      |> Map.get(:json_selects, [])
      |> Enum.map(&Map.get(&1, :alias))

    array_aliases =
      selecto.set
      |> Map.get(:array_operations, [])
      |> Enum.map(&Map.get(&1, :alias))

    window_aliases =
      selecto.set
      |> Map.get(:window_functions, [])
      |> Enum.map(&Map.get(&1, :alias))

    (json_aliases ++ array_aliases ++ window_aliases)
    |> Enum.filter(&is_binary/1)
    |> Enum.uniq()
  end

  # Get the string representation of a table alias for JSONB extraction
  defp get_table_alias_string(_selecto, nil), do: nil
  defp get_table_alias_string(_selecto, :selecto_root), do: "selecto_root"
  defp get_table_alias_string(_selecto, alias) when is_binary(alias), do: alias
  defp get_table_alias_string(_selecto, alias) when is_atom(alias), do: Atom.to_string(alias)

  ### make the builder build the dynamic so we can use same parts for SQL

  # Future feature: subquery and array operations
  # See advanced SQL functions library for current implementation

  # # CASE ... {:case, %{{...filter...}}=>val, cond2=>val, :else=>val}}
  # def build(selecto, {:case, _field, _case_map}) do
  #   {query, aliases}
  # end

  # TODO - other data types- float, decimal

  # Case for func call with field as arg
  ## Check for SQL INJ TODO
  ## TODO allow for func call args
  ## TODO variant for 2 arg aggs eg string_agg, jsonb_object_agg, Grouping
  ## ^^ and mixed lit/field args - field as list?

  # Phase 4: iodata-based build functions (now main functions)

  # build/2 functions
  def build(selecto, {:row, fields, as}) do
    build(selecto, {:row, fields, as}, %{})
  end

  def build(selecto, {:field, field, as}) do
    build(selecto, {:field, field, as}, %{})
  end

  def build(selecto, field) do
    build(selecto, field, %{})
  end

  # build/3 functions
  def build(selecto, {:row, fields, as}, pivot_aliases) do
    {select_parts, join, param} =
      Enum.reduce(List.wrap(fields), {[], [], []}, fn f, {select, join, param} ->
        {s_iodata, j, p} = prep_selector(selecto, f, pivot_aliases)
        {select ++ [s_iodata], join ++ List.wrap(j), param ++ p}
      end)

    row_iodata = ["row( ", Enum.intersperse(select_parts, ", "), " )"]
    {row_iodata, join, param, as}
  end

  def build(selecto, {:field, field, as}, pivot_aliases) do
    {select_iodata, join, param} = prep_selector(selecto, field, pivot_aliases)
    {select_iodata, join, param, as}
  end

  def build(selecto, field, pivot_aliases) do
    {select_iodata, join, param} = prep_selector(selecto, field, pivot_aliases)
    {select_iodata, join, param, UUID.uuid4()}
  end

  # build/4 functions
  def build(selecto, field, as, pivot_aliases) do
    {select_iodata, join, param} = prep_selector(selecto, field, pivot_aliases)
    {select_iodata, join, param, as}
  end

  # Phase 1: Custom Column Safety Helper Functions

  defp get_available_fields(selecto) do
    # Get all available fields from source and joins
    source_fields = Map.keys(selecto.config.columns || %{})
    join_fields = get_join_fields(selecto.config.joins || %{})
    # New: CTE field availability
    cte_fields = get_cte_fields(selecto)

    source_fields ++ join_fields ++ cte_fields
  end

  defp get_join_fields(joins) do
    Enum.flat_map(joins, fn {join_id, join_config} ->
      case Map.get(join_config, :fields, %{}) do
        fields when is_map(fields) -> Map.keys(fields)
        _ -> []
      end
      |> Enum.map(&"#{join_id}.#{&1}")
    end)
  end

  defp get_cte_fields(selecto) do
    selecto
    |> Map.get(:set, %{})
    |> Map.get(:ctes, [])
    |> Enum.flat_map(fn cte_spec ->
      cte_name = Map.get(cte_spec, :name)
      cte_columns = Map.get(cte_spec, :columns, [])

      if is_binary(cte_name) and is_list(cte_columns) do
        Enum.map(cte_columns, fn col -> "#{cte_name}.#{col}" end)
      else
        []
      end
    end)
  end

  defp validate_field_references(selecto, field_mappings, available_fields) do
    # Ensure all field references in mappings exist
    Enum.each(field_mappings, fn {_placeholder, field_ref} ->
      case validate_field_exists(selecto, field_ref, available_fields) do
        :ok ->
          :ok

        {:error, reason} ->
          raise ArgumentError, "Invalid field reference '#{field_ref}' in custom SQL: #{reason}"
      end
    end)
  end

  defp validate_field_exists(selecto, field_ref, available_fields) do
    cond do
      field_ref in available_fields ->
        :ok

      String.contains?(field_ref, ".") ->
        # Check if it's a valid qualified field reference, including permissive CTE fields
        case String.split(field_ref, ".", parts: 2) do
          [prefix, field_name] ->
            cond do
              Enum.any?(available_fields, &(&1 == field_ref)) ->
                :ok

              cte_exists?(selecto, prefix) and cte_field_allowed?(selecto, prefix, field_name) ->
                :ok

              true ->
                {:error, "field not found in available joins/ctes"}
            end

          _ ->
            {:error, "invalid qualified field format"}
        end

      true ->
        {:error, "field not found in source columns"}
    end
  end

  defp cte_exists?(selecto, cte_name) do
    selecto
    |> Map.get(:set, %{})
    |> Map.get(:ctes, [])
    |> Enum.any?(fn cte -> Map.get(cte, :name) == cte_name end)
  end

  defp cte_field_allowed?(selecto, cte_name, field_name) do
    cte_spec =
      selecto
      |> Map.get(:set, %{})
      |> Map.get(:ctes, [])
      |> Enum.find(fn cte -> Map.get(cte, :name) == cte_name end)

    case cte_spec do
      nil ->
        false

      spec ->
        case Map.get(spec, :columns) do
          nil -> true
          [] -> true
          cols when is_list(cols) -> field_name in Enum.map(cols, &to_string/1)
          _ -> true
        end
    end
  end

  defp substitute_field_references(sql_template, field_mappings, selecto) do
    # Safely replace {{field}} placeholders with actual field references
    Enum.reduce(field_mappings, sql_template, fn {placeholder, field_ref}, acc_sql ->
      safe_field_reference = build_safe_field_reference(field_ref, selecto)
      String.replace(acc_sql, "{{#{placeholder}}}", safe_field_reference)
    end)
  end

  defp build_safe_field_reference(field_ref, _selecto) do
    # Phase 1: Basic field reference building
    # Phase 2+: More sophisticated reference building with join aliases
    cond do
      String.contains?(field_ref, ".") -> field_ref
      true -> "selecto_root.#{field_ref}"
    end
  end

  # Simple bucket range parser for use in SQL generation
  defp parse_bucket_ranges_simple(ranges_string) when is_binary(ranges_string) do
    ranges_string
    |> String.split(",")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.map(&parse_single_range_simple/1)
    |> Enum.reject(&is_nil/1)
  end

  defp parse_bucket_ranges_simple(_), do: []

  defp parse_single_range_simple(range) do
    cond do
      # Single value like "1"
      String.match?(range, ~r/^\d+$/) ->
        val = String.to_integer(range)
        {val, val, "#{val}"}

      # Range like "2-5"
      String.match?(range, ~r/^\d+-\d+$/) ->
        [min_str, max_str] = String.split(range, "-")
        min = String.to_integer(min_str)
        max = String.to_integer(max_str)
        {min, max, "#{min}-#{max}"}

      # Open-ended range like "15+"
      String.match?(range, ~r/^\d+\+$/) ->
        min = range |> String.replace("+", "") |> String.to_integer()
        {min, :infinity, "#{min}+"}

      # Open-ended range like "-5" (up to 5)
      String.match?(range, ~r/^-\d+$/) ->
        max = range |> String.replace("-", "") |> String.to_integer()
        {:negative_infinity, max, "≤#{max}"}

      true ->
        nil
    end
  end
end
