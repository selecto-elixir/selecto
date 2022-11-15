defmodule Selecto.Builder.Sql.Select do


  import Selecto.Helpers

  ### make the builder build the dynamic so we can use same parts for SQL
  def build(selecto, {:subquery, {:dyn, as, dynamic}}) do

    #TODO
  end


  def build(selecto, {:subquery, func, field}) do
    conf = selecto.config.columns[field]

    join = selecto.config.joins[conf.requires_join]
    my_func = check_string( Atom.to_string(func) )
    my_key = Atom.to_string(join.my_key)
    my_field = Atom.to_string(conf.field)

    #TODO
  end

  # ARRAY - auto gen array from otherwise denorm'ing selects using postgres 'array' func
  # ---- eg {"array", "item_orders", select: ["item[name]", "item_orders[quantity]"], filters: [{item[type], "Pin"}]}
  # ---- postgres has functions to put those into json!
  # to select the items into an array and apply the filter to the subq. Would ahve to be something that COULD join
  # to one of the main query joins
  #TODOs
  # def build(selecto, {:array, _field, _selects}) do
  #   {query, aliases}
  # end

  # # COALESCE ... ??
  # def build(selecto, {:coalesce, _field, _selects}) do
  #   {query, aliases}
  # end

  # # CASE ... {:case, %{{...filter...}}=>val, cond2=>val, :else=>val}}
  # def build(selecto, {:case, _field, _case_map}) do
  #   {query, aliases}
  # end

  def build(selecto, {:extract, field, format}) do
    conf = selecto.config.columns[field]
    as = "#{format} from #{field}"

    check_string(format)
#TODO
  end

  def build(selecto, {:to_char, {field, format}, as}) do
    conf = selecto.config.columns[field]
    #TODO
  end

  def build(selecto, {:literal, name, value}) when is_integer(value) do
    {"#{value}", :selecto_root, [], name}
  end
  def build(selecto, {:literal, name, value}) when is_bitstring(value) do
    {"#{single_wrap(value)}", :selecto_root, [], name}
  end
  #TODO more types ... refactor out 'literal' processing

  ### works with any func/agg of normal form with no as
  def build(selecto, {func, field}) when is_atom(func) do
    use_as = "#{func}(#{field})"
    build(selecto, {func, field, use_as})
  end

  ## Case of literal value arg
  def build(selecto, {func, {:literal, literal}, as}) when is_atom(func) and is_integer(literal) do
    func = Atom.to_string(func) |> check_string()
    {"#{func}(#{literal})", :selecto_root, [], as}
  end
  def build(selecto, {func, {:literal, literal}, as}) when is_atom(func) and is_bitstring(literal) do
    func = Atom.to_string(func) |> check_string()
    {"#{func}(#{ single_wrap( literal ) })", :selecto_root, [], as}
  end
  #TODO - other data types- float, decimal

  # Case for func call with field as arg
  ## Check for SQL INJ TODO
  ## TODO allow for func call args
  ## TODO variant for 2 arg aggs eg string_agg, jsonb_object_agg, Grouping
  ## ^^ and mixed lit/field args - field as list?

  def build(selecto, {func, field, as}) when is_atom(func) do
    conf = selecto.config.columns[field]
    func = Atom.to_string(func) |> check_string()
    {"#{func}(#{double_wrap(conf.requires_join)}.#{double_wrap(conf.field)})", conf.requires_join, [], as}

  end

  # Case of 'count(*)' which we can just ref as count
  def build(selecto, {:count}) do
    {"count(*)", nil, [], "count"}
  end

  # case of other non-arg funcs eg now()
  def build(selecto, {func}) when is_atom(func) do
    func = Atom.to_string(func) |> check_string()
    {"#{func}()", nil, [], func}
  end

  ### regular old fields. Allow atoms?
  def build(selecto, field) when is_binary(field) do
    conf = selecto.config.columns[field]
    conf.requires_join
    ### SQL, JOIN, PARAMS, FIELD
    {"#{double_wrap(conf.requires_join)}.#{double_wrap(conf.field)}", conf.requires_join, [], field}
  end




end