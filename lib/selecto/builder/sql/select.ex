defmodule Selecto.Builder.Sql.Select do
  import Selecto.Helpers

  ### TODO alter prep_selector to return the data type

  @doc """
  new format...
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

  def prep_selector(_selecto, val) when is_integer(val) do
    {val, :selecto_root, []}
  end

  def prep_selector(_selecto, val) when is_float(val) do
    {val, :selecto_root, []}
  end

  def prep_selector(_selecto, val) when is_boolean(val) do
    {val, :selecto_root, []}
  end

  def prep_selector(_selecto, {:count}) do
    {"count(*)", :selecto_root, []}
  end

  def prep_selector(selecto, {:count, "*", filter}) do
    prep_selector(selecto, {:count, {:literal, "*"}, filter})
  end

  def prep_selector(_selecto, {:subquery, dynamic, params}) do
    {dynamic, [], params}
  end

  def prep_selector(selecto, {:case, pairs}) when is_list(pairs) do
    prep_selector(selecto, {:case, pairs, nil})
  end

  def prep_selector(selecto, {:case, pairs, else_clause}) when is_list(pairs) do
    {sel, join, par} =
      Enum.reduce(
        pairs,
        {[], [], []},
        fn {filter, selector}, {s, j, p} ->
          {join_w, filters, param_w} =
            Selecto.Builder.Sql.Where.build(selecto, {:and, List.wrap(filter)})

          {sel, join_s, param_s} = prep_selector(selecto, selector)

          {s ++ ["when #{filters} then #{sel}"], j ++ List.wrap(join_s) ++ List.wrap(join_w),
           p ++ param_w ++ param_s}
        end
      )

    case else_clause do
      nil ->
        {"case #{Enum.join(sel, " ")} end", join, par}

      _ ->
        {sel_else, join_s, param_s} = prep_selector(selecto, else_clause)

        {"case #{Enum.join(sel, " ")} else #{sel_else} end", join ++ List.wrap(join_s),
         par ++ param_s}
    end
  end

  def prep_selector(selecto, {func, fields})
      when func in [:concat, :coalesce, :greatest, :least, :nullif] do
    {sel, join, param} =
      Enum.reduce(List.wrap(fields), {[], [], []}, fn f, {select, join, param} ->
        {s, j, p} = prep_selector(selecto, f)
        {select ++ [s], join ++ List.wrap(j), param ++ p}
      end)

    {"#{func}( #{Enum.join(sel, ", ")} )", join, param}
  end

  def prep_selector(selecto, {:extract, field, format}) do
    {sel, join, param} = prep_selector(selecto, field)
    check_string(format)
    {"extract( #{format} from  #{sel})", join, param}
  end

  def prep_selector(selecto, {func, field, filter}) when is_atom(func) do
    {sel, join, param} = prep_selector(selecto, field)

    {join_w, filters, param_w} =
      Selecto.Builder.Sql.Where.build(selecto, {:and, List.wrap(filter)})

    func = Atom.to_string(func) |> check_string()

    {"#{func}(#{sel}) FILTER (where #{filters})", List.wrap(join) ++ List.wrap(join_w),
     param ++ param_w}
  end

  def prep_selector(_selecto, {:literal, value}) when is_integer(value) do
    {"#{value}", :selecto_root, []}
  end

  def prep_selector(_selecto, {:literal, value}) when is_bitstring(value) do
    {"#{single_wrap(value)}", :selecto_root, []}
  end

  def prep_selector(selecto, {:to_char, {field, format}}) do
    {sel, join, param} = prep_selector(selecto, field)
    {"to_char(#{sel}, #{single_wrap(format)})", join, param}
  end

  def prep_selector(selecto, {:field, selector}) do
    prep_selector(selecto, selector)
  end

  def prep_selector(_selecto, {func}) when is_atom(func) do
    func = Atom.to_string(func) |> check_string()
    {"#{func}()", :selecto_root, []}
  end

  def prep_selector(selecto, {func, selector}) when is_atom(func) do
    {sel, join, param} = prep_selector(selecto, selector)
    func = Atom.to_string(func) |> check_string()
    {"#{func}(#{sel})", join, param}
  end

  def prep_selector(selecto, selector) when is_binary(selector) do
    conf = selecto.config.columns[selector]

    case Map.get(conf, :select) do
      nil ->
        {"#{double_wrap(conf.requires_join)}.#{double_wrap(conf.field)}", conf.requires_join, []}

      sub ->
        prep_selector(selecto, sub)
    end
  end

  def prep_selector(_sel, selc) do
    IO.inspect(selc)
    raise "ERror"
  end

  ### make the builder build the dynamic so we can use same parts for SQL

  # def build(selecto, {:subquery, func, field}) do
  #   conf = selecto.config.columns[field]

  #   join = selecto.config.joins[conf.requires_join]
  #   my_func = check_string( Atom.to_string(func) )
  #   my_key = Atom.to_string(join.my_key)
  #   my_field = Atom.to_string(conf.field)

  #   #TODO
  # end

  # ARRAY - auto gen array from otherwise denorm'ing selects using postgres 'array' func
  # ---- eg {"array", "item_orders", select: ["item[name]", "item_orders[quantity]"], filters: [{item[type], "Pin"}]}
  # ---- postgres has functions to put those into json!
  # to select the items into an array and apply the filter to the subq. Would ahve to be something that COULD join
  # to one of the main query joins
  # TODOs
  # def build(selecto, {:array, _field, _selects}) do
  #   {query, aliases}
  # end

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

  def build(selecto, {:row, fields, as}) do
    {select, join, param} =
      Enum.reduce(List.wrap(fields), {[], [], []}, fn f, {select, join, param} ->
        {s, j, p} = prep_selector(selecto, f)
        {select ++ [s], join ++ List.wrap(j), param ++ p}
      end)

    {"row( #{Enum.join(select, ", ")} )", join, param, as}
  end

  def build(selecto, {:field, field, as}) do
    {select, join, param} = prep_selector(selecto, field)
    {select, join, param, as}
  end

  ### regular old fields. Allow atoms?
  def build(selecto, field) do
    {select, join, param} = prep_selector(selecto, field)
    {select, join, param, UUID.uuid4()}
  end

  def build(selecto, field, as) do
    {select, join, param} = prep_selector(selecto, field)
    {select, join, param, as}
  end
end
