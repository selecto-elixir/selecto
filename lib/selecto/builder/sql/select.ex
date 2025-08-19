defmodule Selecto.Builder.Sql.Select do
  import Selecto.Builder.Sql.Helpers

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
    validate_field_references(sql_template, field_mappings, available_fields)

    # Replace field placeholders with actual field references
    safe_sql = substitute_field_references(sql_template, field_mappings, selecto)

    # Return as safe iodata (no parameters for now - Phase 1 safety only)
    {[safe_sql], :selecto_root, []}
  end

  # Phase 4: iodata-based prep_selector functions (now main functions)
  def prep_selector(_selecto, val) when is_integer(val) do
    {{:param, val}, :selecto_root, [val]}
  end

  def prep_selector(_selecto, val) when is_float(val) do
    {{:param, val}, :selecto_root, [val]}
  end

  def prep_selector(_selecto, val) when is_boolean(val) do
    {{:param, val}, :selecto_root, [val]}
  end

  def prep_selector(_selecto, {:count}) do
    {["count(*)"], :selecto_root, []}
  end

  def prep_selector(_selecto, {:count, "*"}) do
    {["count(*)"], :selecto_root, []}
  end

  def prep_selector(selecto, {:count, "*", filter}) do
    prep_selector(selecto, {:count, {:literal, "*"}, filter})
  end

  def prep_selector(_selecto, {:subquery, dynamic, params}) do
    # Dynamic subquery is already in the right format
    {[dynamic], [], params}
  end

  def prep_selector(selecto, {:case, pairs}) when is_list(pairs) do
    prep_selector(selecto, {:case, pairs, nil})
  end

  def prep_selector(selecto, {:case, pairs, else_clause}) when is_list(pairs) do
    {sel_parts, join, par} =
      Enum.reduce(
        pairs,
        {[], [], []},
        fn {filter, selector}, {s, j, p} ->
          {join_w, filters_iodata, param_w} =
            Selecto.Builder.Sql.Where.build(selecto, {:and, List.wrap(filter)})

          {sel_iodata, join_s, param_s} = prep_selector(selecto, selector)

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
        {sel_else_iodata, join_s, param_s} = prep_selector(selecto, else_clause)

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

  def prep_selector(selecto, {func, fields})
      when func in [:concat, :coalesce, :greatest, :least, :nullif] do
    {sel_parts, join, param} =
      Enum.reduce(List.wrap(fields), {[], [], []}, fn f, {select, join, param} ->
        {s_iodata, j, p} = prep_selector(selecto, f)
        {select ++ [s_iodata], join ++ List.wrap(j), param ++ p}
      end)

    func_name = Atom.to_string(func)
    func_iodata = [func_name, "( ", Enum.intersperse(sel_parts, ", "), " )"]
    {func_iodata, join, param}
  end

  def prep_selector(selecto, {:extract, field, format}) do
    {sel_iodata, join, param} = prep_selector(selecto, field)
    check_string(format)
    extract_iodata = ["extract( ", format, " from  ", sel_iodata, ")"]
    {extract_iodata, join, param}
  end

  def prep_selector(selecto, {func, field, filter}) when is_atom(func) do
    {sel_iodata, join, param} = prep_selector(selecto, field)

    {join_w, filters_iodata, param_w} =
      Selecto.Builder.Sql.Where.build(selecto, {:and, List.wrap(filter)})

    func_name = Atom.to_string(func) |> check_string()
    filter_iodata = [func_name, "(", sel_iodata, ") FILTER (where ", filters_iodata, ")"]
    {filter_iodata, List.wrap(join) ++ List.wrap(join_w), param ++ param_w}
  end

  def prep_selector(_selecto, {:literal, value}) when is_integer(value) do
    {[{:param, value}], :selecto_root, [value]}
  end

  # Special case for rollup position literals that should not be parameterized
  def prep_selector(_selecto, {:literal_position, value}) when is_integer(value) do
    {[Integer.to_string(value)], :selecto_root, []}
  end

  # Special case for string literals that should not be parameterized (e.g., in concat)
  def prep_selector(_selecto, {:literal_string, value}) when is_bitstring(value) do
    {["'", String.replace(value, "'", "''"), "'"], :selecto_root, []}
  end

  def prep_selector(_selecto, {:literal, value}) when is_bitstring(value) do
    {[{:param, value}], :selecto_root, [value]}
  end

  def prep_selector(selecto, {:to_char, {field, format}}) do
    {sel_iodata, join, param} = prep_selector(selecto, field)
    to_char_iodata = ["to_char(", sel_iodata, ", ", single_wrap(format), ")"]
    {to_char_iodata, join, param}
  end

  def prep_selector(selecto, {:field, selector}) do
    prep_selector(selecto, selector)
  end

  def prep_selector(_selecto, {func}) when is_atom(func) do
    func_name = Atom.to_string(func) |> check_string()
    {[func_name, "()"], :selecto_root, []}
  end

  def prep_selector(selecto, {func, selector}) when is_atom(func) do
    {sel_iodata, join, param} = prep_selector(selecto, selector)
    func_name = Atom.to_string(func) |> check_string()
    func_call_iodata = [func_name, "(", sel_iodata, ")"]
    {func_call_iodata, join, param}
  end

  def prep_selector(selecto, selector) when is_binary(selector) do
    conf = Selecto.field(selecto, selector)

    # Handle case where field configuration doesn't exist
    if conf == nil do
      raise "Field '#{selector}' not found in selecto configuration. Available fields: #{inspect(Map.keys(selecto.config.columns || %{}))}"
    end

    case Map.get(conf, :select) do
      nil ->
        field_iodata = [build_selector_string(selecto, conf.requires_join, conf.name)]
        {field_iodata, conf.requires_join, []}

      sub when is_binary(sub) ->
        # If the select value is a string, treat it as literal SQL
        # This handles cases like "string_agg(tags[name], ', ')" from tagging configurations
        {[sub], conf.requires_join || :selecto_root, []}

      sub ->
        # For other selector types, process recursively
        prep_selector(selecto, sub)
    end
  end

  def prep_selector(selecto, selector) when is_atom(selector) do
    # Convert atom field names to strings and process like binary selectors
    prep_selector(selecto, Atom.to_string(selector))
  end

  def prep_selector(selecto, selector) do
    # Try advanced SQL functions first
    case Selecto.SQL.Functions.prep_advanced_selector(selecto, selector) do
      nil ->
        # Not an advanced function, fall back to error
        raise "Unsupported selector type: #{inspect(selector)}. Supported types: atoms, tuples with functions, strings, and literals."

      result ->
        result
    end
  end

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
  def build(selecto, {:row, fields, as}) do
    {select_parts, join, param} =
      Enum.reduce(List.wrap(fields), {[], [], []}, fn f, {select, join, param} ->
        {s_iodata, j, p} = prep_selector(selecto, f)
        {select ++ [s_iodata], join ++ List.wrap(j), param ++ p}
      end)

    row_iodata = ["row( ", Enum.intersperse(select_parts, ", "), " )"]
    {row_iodata, join, param, as}
  end

  def build(selecto, {:field, field, as}) do
    {select_iodata, join, param} = prep_selector(selecto, field)
    {select_iodata, join, param, as}
  end

  def build(selecto, field) do
    {select_iodata, join, param} = prep_selector(selecto, field)
    {select_iodata, join, param, UUID.uuid4()}
  end

  def build(selecto, field, as) do
    {select_iodata, join, param} = prep_selector(selecto, field)
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

  defp get_cte_fields(_selecto) do
    # Phase 1: Stub - Phase 2+ will implement CTE field detection
    []
  end

  defp validate_field_references(_sql_template, field_mappings, available_fields) do
    # Ensure all field references in mappings exist
    Enum.each(field_mappings, fn {_placeholder, field_ref} ->
      case validate_field_exists(field_ref, available_fields) do
        :ok ->
          :ok

        {:error, reason} ->
          raise ArgumentError, "Invalid field reference '#{field_ref}' in custom SQL: #{reason}"
      end
    end)
  end

  defp validate_field_exists(field_ref, available_fields) do
    cond do
      field_ref in available_fields ->
        :ok

      String.contains?(field_ref, ".") ->
        # Check if it's a valid qualified field reference
        if Enum.any?(available_fields, &String.starts_with?(&1, field_ref)) do
          :ok
        else
          {:error, "field not found in available joins"}
        end

      true ->
        {:error, "field not found in source columns"}
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
end
