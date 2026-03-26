defmodule Selecto.Builder.Sql.Where do
  @moduledoc """
  WHERE-clause compiler for Selecto predicate expressions.

  Predicates are translated into iodata fragments plus bind parameters and join
  dependencies. The builder supports nested boolean logic, comparison operators,
  NULL checks, ranges, text search, subqueries, EXISTS clauses, JSON predicates,
  and raw prebuilt filter fragments.
  """

  import Selecto.Builder.Sql.Helpers

  alias Selecto.AdapterSQL
  alias Selecto.AdapterSupport
  alias Selecto.Builder.Sql.Select
  alias Selecto.Error
  alias Selecto.Jsonb

  # Predicate formats supported:
  #   {SELECTOR} # for boolean fields
  #   {SELECTOR, nil} #is null
  #   {SELECTOR, :not_nil} #is not null
  #   {SELECTOR, SELECTOR} #=
  #   {SELECTOR, [SELECTOR2, ...]}# in ()
  #   {SELECTOR, {comp, SELECTOR2}} #<= etc
  #   {SELECTOR, {:between, SELECTOR2, SELECTOR2}
  #   {:not, PREDICATE}
  #   {:and, [PREDICATES]}
  #   {:or, [PREDICATES]}
  #   {SELECTOR, :in, SUBQUERY}
  #   {SELECTOR, comp, {:subquery, :any, SUBQUERY}}  ## Or :all
  #   {:exists, SUBQUERY}

  # Extract the actual database field name from the field reference or configuration
  defp extract_database_field(field, conf) do
    case field do
      # For joined fields like "category.updated_at"
      field_str when is_binary(field_str) ->
        if String.contains?(field_str, ".") do
          # Extract just the field name from "table.field" format
          String.split(field_str, ".") |> List.last()
        else
          # Use field from conf or fallback to the field string itself
          Map.get(conf, :field, field_str)
        end

      # For atom fields
      field_atom when is_atom(field_atom) ->
        Map.get(conf, :field, Atom.to_string(field_atom))

      # Default case
      _ ->
        Map.get(conf, :field, to_string(field))
    end
  end

  # Handle CASE expressions in WHERE clause
  def build(selecto, {:case, when_clauses, else_clause}) when is_list(when_clauses) do
    {case_iodata, joins, params} = build_case_expression(selecto, when_clauses, else_clause)
    {joins, [" ", case_iodata, " "], params}
  end

  def build(selecto, {:case, when_clauses}) when is_list(when_clauses) do
    build(selecto, {:case, when_clauses, nil})
  end

  def build(selecto, {fields, {:text_search, value}}) when is_list(fields) do
    build_text_search(selecto, fields, value)
  end

  def build(selecto, {fields, {:text_search, value, opts}})
      when is_list(fields) and is_list(opts) do
    build_text_search(selecto, fields, value, opts)
  end

  def build(selecto, {field, {:text_search, value}}) do
    build_text_search(selecto, [field], value)
  end

  def build(selecto, {field, {:text_search, value, opts}}) when is_list(opts) do
    build_text_search(selecto, [field], value, opts)
  end

  def build(selecto, {field, {:subquery, :in, %Selecto{} = query_selecto}}) do
    conf = field_conf(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)
    query_iodata = selecto_subquery_to_iodata(query_selecto)

    {List.wrap(conf.requires_join) ++ List.wrap(join),
     [" ", sel, " in ", in_subquery_fragment(query_iodata), " "], param}
  end

  def build(selecto, {field, {:subquery, :in, query, params}}) do
    conf = field_conf(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)
    query_iodata = convert_sql_placeholders_to_iodata(query, params)

    {List.wrap(conf.requires_join) ++ List.wrap(join),
     [" ", sel, " in ", in_subquery_fragment(query_iodata), " "], param ++ params}
  end

  def build(selecto, {field, {:subquery, :in, query}}) do
    conf = field_conf(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)

    {List.wrap(conf.requires_join) ++ List.wrap(join),
     [" ", sel, " in ", in_subquery_fragment(query), " "], param}
  end

  def build(selecto, {field, comp, {:subquery, agg, %Selecto{} = query_selecto}})
      when agg in [:any, :all] do
    conf = field_conf(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)
    query_iodata = selecto_subquery_to_iodata(query_selecto)

    {List.wrap(conf.requires_join) ++ List.wrap(join),
     [" ", sel, " ", comp, " ", to_string(agg), " (", query_iodata, ") "], param}
  end

  def build(selecto, {field, comp, {:subquery, agg, query, params}}) when agg in [:any, :all] do
    conf = field_conf(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)
    query_iodata = convert_sql_placeholders_to_iodata(query, params)

    {List.wrap(conf.requires_join) ++ List.wrap(join),
     [" ", sel, " ", comp, " ", to_string(agg), " (", query_iodata, ") "], param ++ params}
  end

  def build(selecto, {field, comp, {:subquery, agg, query}}) when agg in [:any, :all] do
    conf = field_conf(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)

    {List.wrap(conf.requires_join) ++ List.wrap(join),
     [" ", sel, " ", comp, " ", to_string(agg), " (", query, ") "], param}
  end

  def build(_selecto, {:exists, %Selecto{} = query_selecto}) do
    query_iodata = selecto_subquery_to_iodata(query_selecto)
    {[], [" exists (", query_iodata, ") "], []}
  end

  def build(_selecto, {:exists, query, params}) do
    query_iodata = convert_sql_placeholders_to_iodata(query, params)
    {[], [" exists (", query_iodata, ") "], params}
  end

  def build(_selecto, {:exists, query}) do
    {[], [" exists (", query, ") "], []}
  end

  def build(selecto, {:not, filter}) do
    {j, c, p} = build(selecto, filter)
    {j, ["not ( ", c, " ) "], p}
  end

  def build(selecto, {conj, filters}) when conj in [:and, :or] do
    # Handle empty filter list - return empty result to avoid empty WHERE clauses
    if Enum.empty?(filters) do
      {[], [], []}
    else
      {joins, clauses, param_chunks} =
        Enum.reduce(filters, {[], [], []}, fn
          f, {joins, clauses, param_chunks} ->
            {j, c, p} = build(selecto, f)
            {[j | joins], [c | clauses], [p | param_chunks]}
        end)

      joins = Enum.reverse(joins)
      clauses = Enum.reverse(clauses)
      params = param_chunks |> Enum.reverse() |> List.flatten()

      clause_parts = Enum.map(clauses, fn c -> ["(", c, ")"] end)
      conj_str = " #{conj} "

      final_clause = [
        "(",
        Enum.intersperse(clause_parts, conj_str),
        ")"
      ]

      {joins, final_clause, params}
    end
  end

  # Prebuilt SQL filters (used by JSON and other advanced operations).
  def build(_selecto, {:raw_sql_filter, filter_iodata}) do
    {[], [" ", filter_iodata, " "], []}
  end

  # Handle :between with list format [{min, max}]
  # For datetime types, use >= start AND < end for better boundary handling
  def build(selecto, {field, {:between, [min, max]}}) do
    conf = field_conf(selecto, field)
    # Extract actual field name, not display name
    field_name = extract_database_field(field, conf)

    # Use half-open interval for datetime types to avoid precision issues
    if conf.type in [:date, :naive_datetime, :utc_datetime, :datetime] do
      {conf.requires_join,
       [
         " (",
         build_selector_string(selecto, conf.requires_join, field_name),
         " >= ",
         {:param, to_type(conf.type, min)},
         " and ",
         build_selector_string(selecto, conf.requires_join, field_name),
         " < ",
         {:param, to_type(conf.type, max)},
         ") "
       ], []}
    else
      # Use standard BETWEEN for non-datetime types
      {conf.requires_join,
       [
         " ",
         build_selector_string(selecto, conf.requires_join, field_name),
         " between ",
         {:param, to_type(conf.type, min)},
         " and ",
         {:param, to_type(conf.type, max)},
         " "
       ], []}
    end
  end

  # Handle :between with separate min, max parameters
  # For datetime types, use >= start AND < end for better boundary handling
  def build(selecto, {field, {:between, min, max}}) do
    conf = field_conf(selecto, field)
    # Extract actual field name, not display name
    field_name = extract_database_field(field, conf)

    # Use half-open interval for datetime types to avoid precision issues
    if conf.type in [:date, :naive_datetime, :utc_datetime, :datetime] do
      {conf.requires_join,
       [
         " (",
         build_selector_string(selecto, conf.requires_join, field_name),
         " >= ",
         {:param, to_type(conf.type, min)},
         " and ",
         build_selector_string(selecto, conf.requires_join, field_name),
         " < ",
         {:param, to_type(conf.type, max)},
         ") "
       ], []}
    else
      # Use standard BETWEEN for non-datetime types
      {conf.requires_join,
       [
         " ",
         build_selector_string(selecto, conf.requires_join, field_name),
         " between ",
         {:param, to_type(conf.type, min)},
         " and ",
         {:param, to_type(conf.type, max)},
         " "
       ], []}
    end
  end

  def build(selecto, {field, {comp, value}}) when comp in [:like, :ilike] do
    # ### Value must have a % in it to work!
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(join), [" ", sel, " ", to_string(comp), " ", {:param, value}, " "], param}
  end

  def build(selecto, {field, {comp, {:ref, ref_field}}})
      when comp in [:=, :!=, :<, :>, :<=, :>=, :gt, :lt, :gte, :lte, :eq, :ne] do
    conf = field_conf(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)
    {ref_sel, ref_join, ref_params} = Select.prep_selector(selecto, {:ref, ref_field})

    sql_op =
      case comp do
        :gt -> ">"
        :lt -> "<"
        :gte -> ">="
        :lte -> "<="
        :eq -> "="
        :ne -> "!="
        other -> to_string(other)
      end

    {List.wrap(conf.requires_join) ++ List.wrap(join) ++ List.wrap(ref_join),
     [" ", sel, " ", sql_op, " ", ref_sel, " "], param ++ ref_params}
  end

  def build(selecto, {field, {comp, {:ref, ref_field}}}) when comp in ~w[= != < > <= >=] do
    conf = field_conf(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)
    {ref_sel, ref_join, ref_params} = Select.prep_selector(selecto, {:ref, ref_field})

    {List.wrap(conf.requires_join) ++ List.wrap(join) ++ List.wrap(ref_join),
     [" ", sel, " ", comp, " ", ref_sel, " "], param ++ ref_params}
  end

  def build(selecto, {field, {:not_like, value}}) do
    # NOT LIKE filter - value should already have % wildcards
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(join), [" ", sel, " NOT LIKE ", {:param, value}, " "], param}
  end

  def build(selecto, {field, {comp, value}})
      when comp in [:=, :!=, :<, :>, :<=, :>=, :gt, :lt, :gte, :lte, :eq, :ne] do
    conf = field_conf(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)

    # Convert alternative operator names to SQL operators
    sql_op =
      case comp do
        :gt -> ">"
        :lt -> "<"
        :gte -> ">="
        :lte -> "<="
        :eq -> "="
        :ne -> "!="
        other -> to_string(other)
      end

    {List.wrap(conf.requires_join) ++ List.wrap(join),
     [" ", sel, " ", sql_op, " ", {:param, to_type(conf.type, value)}, " "], param}
  end

  def build(selecto, {field, {comp, value}}) when comp in ~w[= != < > <= >=] do
    conf = field_conf(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)

    {List.wrap(conf.requires_join) ++ List.wrap(join),
     [" ", sel, " ", comp, " ", {:param, to_type(conf.type, value)}, " "], param}
  end

  def build(selecto, {field, {:in, list}}) when is_list(list) do
    conf = field_conf(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)

    typed_values = Enum.map(list, fn i -> to_type(conf.type, i) end)
    in_clause = build_list_clause(selecto, sel, typed_values, false)

    {List.wrap(conf.requires_join) ++ List.wrap(join), in_clause, param}
  end

  def build(selecto, {field, {:not_in, list}}) when is_list(list) do
    conf = field_conf(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)

    typed_values = Enum.map(list, fn i -> to_type(conf.type, i) end)
    not_in_clause = build_list_clause(selecto, sel, typed_values, true)

    {List.wrap(conf.requires_join) ++ List.wrap(join), not_in_clause, param}
  end

  def build(selecto, {field, list}) when is_list(list) do
    conf = field_conf(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)

    typed_values = Enum.map(list, fn i -> to_type(conf.type, i) end)
    in_clause = build_list_clause(selecto, sel, typed_values, false)

    {List.wrap(conf.requires_join) ++ List.wrap(join), in_clause, param}
  end

  def build(selecto, {field, {:not, nil}}) do
    # Handle {:not, nil} as "IS NOT NULL"
    build(selecto, {field, :not_null})
  end

  def build(selecto, {field, :not_null}) do
    conf = field_conf(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(conf.requires_join) ++ List.wrap(join), [" ", sel, " is not null "], param}
  end

  def build(selecto, {field, value}) when is_nil(value) do
    conf = field_conf(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(conf.requires_join) ++ List.wrap(join), [" ", sel, " is null "], param}
  end

  def build(selecto, {field, {:ref, ref_field}}) do
    conf = field_conf(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)
    {ref_sel, ref_join, ref_params} = Select.prep_selector(selecto, {:ref, ref_field})

    {List.wrap(conf.requires_join) ++ List.wrap(join) ++ List.wrap(ref_join),
     [" ", sel, " = ", ref_sel, " "], param ++ ref_params}
  end

  # Handle array filter specifications (must come before generic {field, value})
  def build(selecto, {:array_filter, spec}) do
    {sql, params} = Selecto.Advanced.ArrayOperations.to_sql(spec, [], selecto)
    # Convert params to iodata with markers
    iodata = convert_array_sql_to_iodata(sql, params)
    {[], iodata, params}
  end

  # Array contains - checks if array contains all specified elements
  def build(selecto, {:array_contains, field, values}) when is_list(values) do
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(join), [" ", sel, " @> ", {:param, values}, " "], param ++ [values]}
  end

  # Array contained - checks if array is contained by specified elements
  def build(selecto, {:array_contained, field, values}) when is_list(values) do
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(join), [" ", sel, " <@ ", {:param, values}, " "], param ++ [values]}
  end

  # Array overlap - checks if arrays have any common elements
  def build(selecto, {:array_overlap, field, values}) when is_list(values) do
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(join), [" ", sel, " && ", {:param, values}, " "], param ++ [values]}
  end

  # Array equality - checks if arrays are equal
  def build(selecto, {:array_eq, field, values}) when is_list(values) do
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(join), [" ", sel, " = ", {:param, values}, " "], param ++ [values]}
  end

  # ---------------------------------------------------------------------------
  # JSONB Operations with dot notation
  # ---------------------------------------------------------------------------

  # JSONB contains - check if JSONB column contains value
  def build(selecto, {field, {:jsonb_contains, value}}) when is_map(value) do
    domain = selecto.config

    case Jsonb.parse_field_reference(field, domain) do
      {:jsonb, column, []} ->
        # Contains on the whole column
        json_value = Jason.encode!(value)

        {[],
         [
           " ",
           build_selector_string(selecto, :selecto_root, column),
           " @> ",
           {:param, json_value},
           "::jsonb "
         ], []}

      {:jsonb, column, path} ->
        # Contains on a nested path
        json_value = Jason.encode!(value)

        extraction =
          Jsonb.build_extraction(column, path,
            as_text: false,
            table_alias: get_root_alias(selecto),
            adapter: Map.get(selecto, :adapter, Selecto.AdapterSupport.default_adapter())
          )

        {[], [" ", extraction, " @> ", {:param, json_value}, "::jsonb "], []}

      {:regular, _} ->
        # Not a JSONB field, try to use as regular contains
        json_value = Jason.encode!(value)
        {sel, join, param} = Select.prep_selector(selecto, field)
        {List.wrap(join), [" ", sel, " @> ", {:param, json_value}, "::jsonb "], param}
    end
  end

  # JSONB key exists - check if key exists in JSONB
  def build(selecto, {field, :exists}) do
    domain = selecto.config

    case Jsonb.parse_field_reference(field, domain) do
      {:jsonb, column, path} when path != [] ->
        exists_expr =
          Jsonb.build_key_exists(column, path,
            table_alias: get_root_alias(selecto),
            adapter: Map.get(selecto, :adapter, Selecto.AdapterSupport.default_adapter())
          )

        {[], [" ", exists_expr, " "], []}

      _ ->
        # Not a JSONB path, treat as "is not null"
        build(selecto, {field, :not_null})
    end
  end

  # JSONB array contains - check if JSONB array contains value(s)
  def build(selecto, {field, {:contains, value}}) do
    domain = selecto.config

    case Jsonb.parse_field_reference(field, domain) do
      {:jsonb, column, path} ->
        contains_expr =
          Jsonb.build_array_contains(column, path, value,
            table_alias: get_root_alias(selecto),
            adapter: Map.get(selecto, :adapter, Selecto.AdapterSupport.default_adapter())
          )

        {[], [" ", contains_expr, " "], []}

      {:regular, _} ->
        # Fallback to array contains for regular array columns
        build(selecto, {:array_contains, field, List.wrap(value)})
    end
  end

  # JSONB array contains all - check if JSONB array contains all values
  def build(selecto, {field, {:contains_all, values}}) when is_list(values) do
    domain = selecto.config

    case Jsonb.parse_field_reference(field, domain) do
      {:jsonb, column, path} ->
        contains_expr =
          Jsonb.build_array_contains_all(column, path, values,
            table_alias: get_root_alias(selecto),
            adapter: Map.get(selecto, :adapter, Selecto.AdapterSupport.default_adapter())
          )

        {[], [" ", contains_expr, " "], []}

      {:regular, _} ->
        # Fallback to array contains for regular array columns
        build(selecto, {:array_contains, field, values})
    end
  end

  # JSONB path with comparison operators
  def build(selecto, {field, {comp, value}})
      when is_binary(field) and
             comp in [:gt, :lt, :gte, :lte, :eq, :ne, :=, :!=, :<, :>, :<=, :>=] do
    domain = selecto.config

    case Jsonb.parse_field_reference(field, domain) do
      {:jsonb, column, path} ->
        build_jsonb_comparison(selecto, column, path, comp, value)

      {:regular, _} ->
        # Not a JSONB field, use regular comparison
        build_regular_comparison(selecto, field, comp, value)
    end
  end

  # JSONB path with :in operator
  def build(selecto, {field, {:in, list}}) when is_binary(field) and is_list(list) do
    domain = selecto.config

    case Jsonb.parse_field_reference(field, domain) do
      {:jsonb, column, path} ->
        build_jsonb_in(selecto, column, path, list)

      {:regular, _} ->
        # Not a JSONB field, use regular IN
        conf = field_conf(selecto, field)
        {sel, join, param} = Select.prep_selector(selecto, field)
        typed_values = Enum.map(list, fn i -> to_type(conf.type, i) end)
        in_clause = build_list_clause(selecto, sel, typed_values, false)

        {List.wrap(conf.requires_join) ++ List.wrap(join), in_clause, param}
    end
  end

  # JSONB path with :between operator
  def build(selecto, {field, {:between, min, max}}) when is_binary(field) do
    domain = selecto.config

    case Jsonb.parse_field_reference(field, domain) do
      {:jsonb, column, path} ->
        build_jsonb_between(selecto, column, path, min, max)

      {:regular, _} ->
        # Not a JSONB field, delegate to regular between handler
        conf = field_conf(selecto, field)
        field_name = extract_database_field(field, conf)

        if conf.type in [:date, :naive_datetime, :utc_datetime, :datetime] do
          {conf.requires_join,
           [
             " (",
             build_selector_string(selecto, conf.requires_join, field_name),
             " >= ",
             {:param, to_type(conf.type, min)},
             " and ",
             build_selector_string(selecto, conf.requires_join, field_name),
             " < ",
             {:param, to_type(conf.type, max)},
             ") "
           ], []}
        else
          {conf.requires_join,
           [
             " ",
             build_selector_string(selecto, conf.requires_join, field_name),
             " between ",
             {:param, to_type(conf.type, min)},
             " and ",
             {:param, to_type(conf.type, max)},
             " "
           ], []}
        end
    end
  end

  # Generic field = value (with JSONB dot notation support)
  def build(selecto, {field, value}) when is_binary(field) do
    domain = selecto.config

    case Jsonb.parse_field_reference(field, domain) do
      {:jsonb, column, path} ->
        build_jsonb_equality(selecto, column, path, value)

      {:regular, _} ->
        {sel, join, param} = Select.prep_selector(selecto, field)
        {List.wrap(join), [" ", sel, " = ", {:param, value}, " "], param}
    end
  end

  def build(selecto, {field, value}) do
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(join), [" ", sel, " = ", {:param, value}, " "], param}
  end

  def build(_sel, other) do
    require Logger

    # Determine the type safely without logging the actual value (which may contain sensitive data)
    type_name =
      cond do
        is_binary(other) -> "binary"
        is_tuple(other) -> "tuple"
        is_list(other) -> "list"
        is_map(other) -> "map"
        true -> "other"
      end

    # Check if this is a bucket_ranges string that shouldn't be here
    if is_binary(other) && String.match?(other, ~r/^\d+-\d+,\d+\+$|^\d+,\d+-\d+|\d+\+/) do
      Logger.error(
        "WHERE builder received bucket_ranges string as filter (value redacted for security)"
      )

      Logger.error(
        "This likely means aggregate or group_by bucket_ranges are being incorrectly passed as filters"
      )

      raise "WHERE clause builder error: Bucket ranges string was passed as a filter. This should not happen - bucket ranges should be handled by aggregate processing, not WHERE clauses."
    end

    # Log type info only, not the actual value which may contain sensitive data
    Logger.error("WHERE builder received unrecognized filter structure of type: #{type_name}")
    Logger.error("Debug: Enable debug logging with safe inspection for more details")

    raise "WHERE clause builder error: Unrecognized filter structure (type: #{type_name}). This usually means an aggregate or filter configuration is generating an invalid filter format."
  end

  defp build_list_clause(selecto, selector, typed_values, negate?) do
    if AdapterSQL.array_any_comparison?(selecto) do
      if negate? do
        [" NOT (", selector, " = ANY(", {:param, typed_values}, ")) "]
      else
        [" ", selector, " = ANY(", {:param, typed_values}, ") "]
      end
    else
      operator = if negate?, do: " NOT IN (", else: " IN ("
      [" ", selector, operator, {:param, typed_values}, ") "]
    end
  end

  # ---------------------------------------------------------------------------
  # JSONB Helper Functions
  # ---------------------------------------------------------------------------

  defp build_jsonb_equality(selecto, column, path, value) do
    domain = selecto.config
    path_schema = Jsonb.get_path_schema(domain, column, path)
    field_type = if path_schema, do: Map.get(path_schema, :type), else: nil
    cast = Jsonb.pg_cast_for_type(field_type)

    extraction =
      Jsonb.build_extraction(column, path,
        as_text: true,
        table_alias: get_root_alias(selecto),
        cast: cast,
        adapter: Map.get(selecto, :adapter, Selecto.AdapterSupport.default_adapter())
      )

    # For nil values, check if the key exists and is null vs key doesn't exist
    if is_nil(value) do
      {[], [" (", extraction, " IS NULL) "], []}
    else
      {[], [" ", extraction, " = ", {:param, to_string(value)}, " "], []}
    end
  end

  defp build_jsonb_comparison(selecto, column, path, comp, value) do
    domain = selecto.config
    path_schema = Jsonb.get_path_schema(domain, column, path)
    field_type = if path_schema, do: Map.get(path_schema, :type), else: nil
    cast = Jsonb.pg_cast_for_type(field_type)

    extraction =
      Jsonb.build_extraction(column, path,
        as_text: true,
        table_alias: get_root_alias(selecto),
        cast: cast,
        adapter: Map.get(selecto, :adapter, Selecto.AdapterSupport.default_adapter())
      )

    sql_op =
      case comp do
        :gt -> ">"
        :lt -> "<"
        :gte -> ">="
        :lte -> "<="
        :eq -> "="
        :ne -> "!="
        other -> to_string(other)
      end

    # Cast value appropriately for comparison
    param_value =
      case field_type do
        t when t in [:integer, :decimal, :float] -> value
        _ -> to_string(value)
      end

    {[], [" ", extraction, " ", sql_op, " ", {:param, param_value}, " "], []}
  end

  defp build_jsonb_in(selecto, column, path, list) do
    domain = selecto.config
    path_schema = Jsonb.get_path_schema(domain, column, path)
    field_type = if path_schema, do: Map.get(path_schema, :type), else: nil
    cast = Jsonb.pg_cast_for_type(field_type)

    extraction =
      Jsonb.build_extraction(column, path,
        as_text: true,
        table_alias: get_root_alias(selecto),
        cast: cast,
        adapter: Map.get(selecto, :adapter, Selecto.AdapterSupport.default_adapter())
      )

    # Convert list values to appropriate types
    typed_list =
      case field_type do
        t when t in [:integer, :decimal, :float] -> list
        _ -> Enum.map(list, &to_string/1)
      end

    {[], [" ", extraction, " = ANY(", {:param, typed_list}, ") "], []}
  end

  defp build_jsonb_between(selecto, column, path, min, max) do
    domain = selecto.config
    path_schema = Jsonb.get_path_schema(domain, column, path)
    field_type = if path_schema, do: Map.get(path_schema, :type), else: nil
    cast = Jsonb.pg_cast_for_type(field_type)

    extraction =
      Jsonb.build_extraction(column, path,
        as_text: true,
        table_alias: get_root_alias(selecto),
        cast: cast,
        adapter: Map.get(selecto, :adapter, Selecto.AdapterSupport.default_adapter())
      )

    {[], [" ", extraction, " BETWEEN ", {:param, min}, " AND ", {:param, max}, " "], []}
  end

  defp build_regular_comparison(selecto, field, comp, value) do
    conf = field_conf(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)

    sql_op =
      case comp do
        :gt -> ">"
        :lt -> "<"
        :gte -> ">="
        :lte -> "<="
        :eq -> "="
        :ne -> "!="
        other -> to_string(other)
      end

    {List.wrap(conf.requires_join) ++ List.wrap(join),
     [" ", sel, " ", sql_op, " ", {:param, to_type(conf.type, value)}, " "], param}
  end

  defp get_root_alias(selecto) do
    # Get the table alias for the root table
    Map.get(selecto, :root_alias, "selecto_root")
  end

  # Build CASE expression for WHERE clause
  defp build_case_expression(selecto, when_clauses, else_clause) do
    # Collect joins and params from all parts
    {when_parts, all_joins, all_params} =
      when_clauses
      |> Enum.reduce({[], [], []}, fn {condition, result}, {parts_acc, joins_acc, params_acc} ->
        # Build the condition (could be complex filter)
        {cond_joins, cond_iodata, cond_params} = build(selecto, condition)

        # Build the result (could be a filter expression or boolean)
        {result_joins, result_iodata, result_params} =
          case result do
            true ->
              {[], ["TRUE"], []}

            false ->
              {[], ["FALSE"], []}

            nil ->
              {[], ["NULL"], []}

            result when is_tuple(result) ->
              build(selecto, result)

            result when is_binary(result) or is_number(result) ->
              {[], [{:param, result}], [result]}
          end

        when_part = ["WHEN ", cond_iodata, " THEN ", result_iodata]

        {parts_acc ++ [when_part], joins_acc ++ List.wrap(cond_joins) ++ List.wrap(result_joins),
         params_acc ++ cond_params ++ result_params}
      end)

    # Build ELSE clause
    {else_joins, else_iodata, else_params} =
      case else_clause do
        nil ->
          {[], [], []}

        true ->
          {[], [" ELSE TRUE"], []}

        false ->
          {[], [" ELSE FALSE"], []}

        else_clause when is_tuple(else_clause) ->
          {ej, ei, ep} = build(selecto, else_clause)
          {ej, [" ELSE ", ei], ep}

        else_clause when is_binary(else_clause) or is_number(else_clause) ->
          {[], [" ELSE ", {:param, else_clause}], [else_clause]}
      end

    case_iodata = ["CASE ", Enum.intersperse(when_parts, " ")] ++ else_iodata ++ [" END"]

    {case_iodata, all_joins ++ List.wrap(else_joins), all_params ++ else_params}
  end

  # Helper to convert array SQL with params to iodata format
  defp convert_array_sql_to_iodata(sql, params) do
    # Replace $1, $2, etc. with {:param, value} markers
    params
    |> Enum.with_index(1)
    |> Enum.reduce([sql], fn {value, idx}, acc ->
      Enum.flat_map(acc, fn
        s when is_binary(s) ->
          String.split(s, "$#{idx}", parts: 2)
          |> case do
            [before_text] -> [before_text]
            [before_text, after_text] -> [before_text, {:param, value}, after_text]
          end

        other ->
          [other]
      end)
    end)
  end

  defp to_type(:id, value) when is_integer(value) do
    value
  end

  defp to_type(:id, value) when is_bitstring(value) do
    String.to_integer(value)
  end

  defp to_type(:integer, value) when is_integer(value) do
    value
  end

  defp to_type(:integer, value) when is_bitstring(value) do
    String.to_integer(value)
  end

  defp to_type(_t, val) do
    val
  end

  defp field_conf(selecto, field) do
    case fast_config_column(selecto, field) do
      nil -> Selecto.field(selecto, field)
      conf -> conf
    end
  end

  defp fast_config_column(selecto, field) do
    columns = Map.get(selecto.config, :columns, %{})

    conf =
      Map.get(columns, field) ||
        case safe_existing_atom(field) do
          nil -> nil
          atom_key -> Map.get(columns, atom_key)
        end

    case conf do
      nil ->
        nil

      value ->
        value
        |> Map.put_new(:requires_join, :selecto_root)
        |> Map.put_new(:field, field_name(value, field))
    end
  end

  defp field_name(conf, field) do
    case Map.get(conf, :field) do
      nil -> Map.get(conf, :name, field)
      value -> value
    end
  end

  defp safe_existing_atom(field) when is_binary(field) do
    try do
      String.to_existing_atom(field)
    rescue
      ArgumentError -> nil
    end
  end

  defp safe_existing_atom(_field), do: nil

  defp build_text_search(selecto, fields, value, opts \\ []) when is_list(fields) do
    adapter = Map.get(selecto, :adapter)
    adapter_name = AdapterSupport.adapter_name(adapter)
    mode = Keyword.get(opts, :mode)

    compiled_fields =
      Enum.map(fields, fn field ->
        conf = field_conf(selecto, field)
        field_name = extract_database_field(field, conf)
        {conf, field_name}
      end)

    joins =
      compiled_fields
      |> Enum.flat_map(fn {conf, _field_name} -> List.wrap(conf.requires_join) end)
      |> Enum.uniq()

    selectors =
      Enum.map(compiled_fields, fn {conf, field_name} ->
        build_selector_string(selecto, conf.requires_join, field_name)
      end)

    cond do
      adapter_name == :mysql and AdapterSupport.supports_feature?(adapter, :text_search) and
          length(selectors) == 1 ->
        {joins,
         [
           " MATCH(",
           hd(selectors),
           ") AGAINST (",
           {:param, value},
           mysql_text_search_mode_sql(adapter, mode)
         ], []}

      adapter_name == :mysql and
          AdapterSupport.supports_feature?(adapter, :text_search_multi_field) ->
        {joins,
         [
           " MATCH(",
           Enum.intersperse(selectors, ", "),
           ") AGAINST (",
           {:param, value},
           mysql_text_search_mode_sql(adapter, mode)
         ], []}

      length(selectors) > 1 ->
        raise_text_search_error(
          "Adapter does not support multi-field text search",
          adapter_name,
          :text_search_multi_field,
          fields
        )

      AdapterSupport.supports_feature?(adapter, :text_search) ->
        ensure_supported_text_search_mode!(adapter_name, mode, fields)
        [selector] = selectors

        {joins, [" ", selector, " @@ websearch_to_tsquery(", {:param, value}, ") "], []}

      true ->
        raise_text_search_error(
          "Adapter does not support text search",
          adapter_name,
          :text_search,
          fields
        )
    end
  end

  defp raise_text_search_error(message, adapter_name, unsupported_feature, fields) do
    error =
      Error.validation_error(message, %{
        adapter: adapter_name,
        fields: fields,
        unsupported_feature: unsupported_feature
      })

    raise Error.to_exception(error)
  end

  defp mysql_text_search_mode_sql(adapter, mode) do
    case mode do
      nil ->
        " IN NATURAL LANGUAGE MODE) "

      :natural ->
        " IN NATURAL LANGUAGE MODE) "

      :boolean ->
        if AdapterSupport.supports_feature?(adapter, :text_search_boolean_mode) do
          " IN BOOLEAN MODE) "
        else
          raise_text_search_error(
            "Adapter does not support boolean-mode text search",
            AdapterSupport.adapter_name(adapter),
            :text_search_boolean_mode,
            []
          )
        end

      :query_expansion ->
        if AdapterSupport.supports_feature?(adapter, :text_search_query_expansion_mode) do
          " IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION) "
        else
          raise_text_search_error(
            "Adapter does not support query-expansion text search",
            AdapterSupport.adapter_name(adapter),
            :text_search_query_expansion_mode,
            []
          )
        end

      other ->
        raise_text_search_error(
          "Unsupported text search mode",
          AdapterSupport.adapter_name(adapter),
          :text_search_mode,
          [other]
        )
    end
  end

  defp ensure_supported_text_search_mode!(adapter_name, mode, fields)
       when mode in [nil, :websearch] do
    _ = {adapter_name, fields}
    :ok
  end

  defp ensure_supported_text_search_mode!(adapter_name, mode, fields) do
    raise_text_search_error(
      "Adapter does not support this text search mode",
      adapter_name,
      :text_search_mode,
      [{:mode, mode} | fields]
    )
  end

  defp convert_sql_placeholders_to_iodata(sql, params)
       when is_binary(sql) and is_list(params) do
    values_by_index =
      params
      |> Enum.with_index(1)
      |> Map.new(fn {value, idx} -> {idx, value} end)

    Regex.split(~r/(\$\d+)/, sql, include_captures: true, trim: false)
    |> Enum.map(fn part ->
      case Regex.run(~r/^\$(\d+)$/, part, capture: :all_but_first) do
        [idx] ->
          case Map.fetch(values_by_index, String.to_integer(idx)) do
            {:ok, value} -> {:param, value}
            :error -> part
          end

        _ ->
          part
      end
    end)
  end

  defp convert_sql_placeholders_to_iodata(sql, _params), do: sql

  defp selecto_subquery_to_iodata(%Selecto{} = query_selecto) do
    {sql, params} = Selecto.to_sql(query_selecto)

    sql
    |> rewrite_subquery_root_alias(build_subquery_root_alias(query_selecto))
    |> convert_sql_placeholders_to_iodata(params)
  end

  defp build_subquery_root_alias(query_selecto) do
    table_segment =
      query_selecto
      |> Selecto.source_table()
      |> normalize_alias_segment("source")

    "subq_root_#{table_segment}"
  end

  defp normalize_alias_segment(value, fallback) do
    normalized =
      value
      |> to_string()
      |> String.downcase()
      |> String.replace(~r/[^a-z0-9]+/u, "_")
      |> String.trim("_")

    if normalized == "", do: fallback, else: normalized
  end

  defp rewrite_subquery_root_alias(subquery_sql, alias_name) when is_binary(subquery_sql) do
    Regex.replace(~r/\bselecto_root\b/u, subquery_sql, alias_name)
  end

  # Ensure `IN` subqueries are wrapped in parentheses.
  # If caller already provides parentheses, keep them as-is.
  defp in_subquery_fragment(query) when is_binary(query) do
    trimmed = String.trim(query)

    if String.starts_with?(trimmed, "(") and String.ends_with?(trimmed, ")") do
      trimmed
    else
      ["(", query, ")"]
    end
  end

  defp in_subquery_fragment(query), do: ["(", query, ")"]
end
