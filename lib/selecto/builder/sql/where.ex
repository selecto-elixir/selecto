defmodule Selecto.Builder.Sql.Where do
  import Selecto.Builder.Sql.Helpers

  alias ElixirSense.Log
  alias Selecto.Builder.Sql.Select

  @doc """
      {SELECTOR} # for boolean fields
      {SELECTOR, nil} #is null
      {SELECTOR, :not_nil} #is not null
      {SELECTOR, SELECTOR} #=
      {SELECTOR, [SELECTOR2, ...]}# in ()
      {SELECTOR, {comp, SELECTOR2}} #<= etc
      {SELECTOR, {:between, SELECTOR2, SELECTOR2}
      {:not, PREDICATE}
      {:and, [PREDICATES]}
      {:or, [PREDICATES]}
      {SELECTOR, :in, SUBQUERY}
      {SELECTOR, comp, {:subquery, :any, SUBQUERY}}  ## Or :all
      {:exists, SUBQUERY}
  """

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

  def build(selecto, {field, {:text_search, value}}) do
    conf = Selecto.field(selecto, field)
    # Extract actual field name, not display name
    field_name = extract_database_field(field, conf)

    {conf.requires_join,
     [
       " ",
       build_selector_string(selecto, conf.requires_join, field_name),
       " @@ websearch_to_tsquery(",
       {:param, value},
       ") "
     ], []}
  end

  def build(selecto, {field, {:subquery, :in, query, params}}) do
    conf = Selecto.field(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)

    {List.wrap(conf.requires_join) ++ List.wrap(join), [" ", sel, " in ", query, " "],
     param ++ params}
  end

  def build(selecto, {field, {:subquery, :in, query}}) do
    conf = Selecto.field(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(conf.requires_join) ++ List.wrap(join), [" ", sel, " in ", query, " "], param}
  end

  def build(selecto, {field, comp, {:subquery, agg, query, params}}) when agg in [:any, :all] do
    conf = Selecto.field(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)

    {List.wrap(conf.requires_join) ++ List.wrap(join),
     [" ", sel, " ", comp, " ", to_string(agg), " (", query, ") "], param ++ params}
  end

  def build(selecto, {field, comp, {:subquery, agg, query}}) when agg in [:any, :all] do
    conf = Selecto.field(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)

    {List.wrap(conf.requires_join) ++ List.wrap(join),
     [" ", sel, " ", comp, " ", to_string(agg), " (", query, ") "], param}
  end

  def build(_selecto, {:exists, query, params}) do
    {[], [" exists (", query, ") "], params}
  end

  def build(_selecto, {:exists, query}) do
    {[], [" exists (", query, ") "], []}
  end

  def build(selecto, {:not, filter}) do
    {j, c, p} = build(selecto, filter)
    {j, ["not ( ", c, " ) "], p}
  end

  def build(selecto, {conj, filters}) when conj in [:and, :or] do
    require Logger
    ### output call stack here
    Logger.debug("Call stack:\n" <> Exception.format_stacktrace())
    Logger.debug("=== WHERE.build processing #{conj} with #{length(filters)} filters ===")
    Logger.debug("Filters: #{inspect(filters, pretty: true)}")

    # Handle empty filter list - return empty result to avoid empty WHERE clauses
    if Enum.empty?(filters) do
      {[], [], []}
    else
      {joins, clauses, params} =
        Enum.reduce(filters, {[], [], []}, fn
          f, {joins, clauses, params} ->
            Logger.debug("Processing individual filter: #{inspect(f, pretty: true)}")
            {j, c, p} = build(selecto, f)
            {joins ++ [j], clauses ++ [c], params ++ p}
        end)

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

  # Handle :between with list format [{min, max}]
  # For datetime types, use >= start AND < end for better boundary handling
  def build(selecto, {field, {:between, [min, max]}}) do
    conf = Selecto.field(selecto, field)
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
    conf = Selecto.field(selecto, field)
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

  def build(selecto, {field, {comp, value}}) when comp in [:=, :!=, :<, :>, :<=, :>=, :gt, :lt, :gte, :lte, :eq, :ne] do
    conf = Selecto.field(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)

    # Convert alternative operator names to SQL operators
    sql_op = case comp do
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
    conf = Selecto.field(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)

    {List.wrap(conf.requires_join) ++ List.wrap(join),
     [" ", sel, " ", comp, " ", {:param, to_type(conf.type, value)}, " "], param}
  end

  def build(selecto, {field, {:in, list}}) when is_list(list) do
    conf = Selecto.field(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)

    # Use adapter-specific syntax for IN/ANY
    adapter = Map.get(selecto, :adapter, Selecto.DB.PostgreSQL)
    in_clause = case adapter do
      Selecto.DB.MySQL -> [" ", sel, " IN (", {:param, Enum.map(list, fn i -> to_type(conf.type, i) end)}, ") "]
      Selecto.DB.MariaDB -> [" ", sel, " IN (", {:param, Enum.map(list, fn i -> to_type(conf.type, i) end)}, ") "]
      _ -> [" ", sel, " = ANY(", {:param, Enum.map(list, fn i -> to_type(conf.type, i) end)}, ") "]
    end

    {List.wrap(conf.requires_join) ++ List.wrap(join), in_clause, param}
  end

  def build(selecto, {field, list}) when is_list(list) do
    conf = Selecto.field(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)

    # Use adapter-specific syntax for IN/ANY
    adapter = Map.get(selecto, :adapter, Selecto.DB.PostgreSQL)
    in_clause = case adapter do
      Selecto.DB.MySQL -> [" ", sel, " IN (", {:param, Enum.map(list, fn i -> to_type(conf.type, i) end)}, ") "]
      Selecto.DB.MariaDB -> [" ", sel, " IN (", {:param, Enum.map(list, fn i -> to_type(conf.type, i) end)}, ") "]
      _ -> [" ", sel, " = ANY(", {:param, Enum.map(list, fn i -> to_type(conf.type, i) end)}, ") "]
    end

    {List.wrap(conf.requires_join) ++ List.wrap(join), in_clause, param}
  end

  def build(selecto, {field, {:not, nil}}) do
    # Handle {:not, nil} as "IS NOT NULL"
    build(selecto, {field, :not_null})
  end

  def build(selecto, {field, :not_null}) do
    conf = Selecto.field(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(conf.requires_join) ++ List.wrap(join), [" ", sel, " is not null "], param}
  end

  def build(selecto, {field, value}) when is_nil(value) do
    conf = Selecto.field(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(conf.requires_join) ++ List.wrap(join), [" ", sel, " is null "], param}
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

  def build(selecto, {field, value}) do
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(join), [" ", sel, " = ", {:param, value}, " "], param}
  end

  def build(_sel, other) do
    require Logger

    # Check if this is a bucket_ranges string that shouldn't be here
    if is_binary(other) && String.match?(other, ~r/^\d+-\d+,\d+\+$|^\d+,\d+-\d+|\d+\+/) do
      Logger.error("WHERE builder received bucket_ranges string as filter: #{inspect(other)}")
      Logger.error("This likely means aggregate or group_by bucket_ranges are being incorrectly passed as filters")
      raise "WHERE clause builder error: Bucket ranges string #{inspect(other)} was passed as a filter. This should not happen - bucket ranges should be handled by aggregate processing, not WHERE clauses."
    end

    Logger.error("WHERE builder received unrecognized filter structure: #{inspect(other, pretty: true)}")
    Logger.error("Type: #{inspect(is_binary(other) && "binary" || is_tuple(other) && "tuple" || is_list(other) && "list" || is_map(other) && "map" || "other")}")
    raise "WHERE clause builder error: Unrecognized filter structure #{inspect(other)}. This usually means an aggregate or filter configuration is generating an invalid filter format."
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
            true -> {[], ["TRUE"], []}
            false -> {[], ["FALSE"], []}
            nil -> {[], ["NULL"], []}
            result when is_tuple(result) -> build(selecto, result)
            result when is_binary(result) or is_number(result) ->
              {[], [{:param, result}], [result]}
          end

        when_part = ["WHEN ", cond_iodata, " THEN ", result_iodata]

        {parts_acc ++ [when_part],
         joins_acc ++ List.wrap(cond_joins) ++ List.wrap(result_joins),
         params_acc ++ cond_params ++ result_params}
      end)

    # Build ELSE clause
    {else_joins, else_iodata, else_params} =
      case else_clause do
        nil -> {[], [], []}
        true -> {[], [" ELSE TRUE"], []}
        false -> {[], [" ELSE FALSE"], []}
        else_clause when is_tuple(else_clause) ->
          {ej, ei, ep} = build(selecto, else_clause)
          {ej, [" ELSE ", ei], ep}
        else_clause when is_binary(else_clause) or is_number(else_clause) ->
          {[], [" ELSE ", {:param, else_clause}], [else_clause]}
      end

    case_iodata = ["CASE ", Enum.intersperse(when_parts, " ")] ++ else_iodata ++ [" END"]

    {case_iodata,
     all_joins ++ List.wrap(else_joins),
     all_params ++ else_params}
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
        other -> [other]
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
end
