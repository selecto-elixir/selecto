defmodule Selecto.Builder.Sql.Where do

  import Selecto.Helpers

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
    {SELECTOR, comp, :any, SUBQUERY}
    {SELECTOR, comp, :all, SUBQUERY}
    {:exists, SUBQUERY}
"""

  def build(selecto, {field, {:subquery, :in, query, params}}) do
    conf = selecto.config.columns[field]
    {conf.requires_join, "#{double_wrap(conf.requires_join)}.#{double_wrap(conf.field)} in #{query}", params}
  end

  def build(selecto, {:not, filter}) do
    {j, c, p} = build(selecto, filter)
    {j, "not ( #{c} ) ", p}
    #Joins, clause, params
  end

  def build(selecto, {conj, filters}) when conj in [:and, :or] do
    {joins, clauses, params} = Enum.reduce(filters, {[],[],[]}, fn
      f, {joins, clauses, params} ->
        {j, c, p} = build(selecto, f)
        {joins ++ [j], clauses ++ [c], params ++ p}
    end)
    {joins, "(#{ Enum.join(Enum.map(clauses, fn c -> "(#{c})" end), " #{conj} ")})" , params}
    #Joins, clause, params
  end

  # {:subquery, :in, query} ->
  #   dynamic([{^table, a}], field(a, ^field) in ^query )

  def build(selecto, {field, {:between, min, max}}) do
    conf = selecto.config.columns[field]
    {conf.requires_join, " #{double_wrap(conf.requires_join)}.#{double_wrap(conf.field)} between ^SelectoParam^ and ^SelectoParam^ ", [min, max]}
  end

  def build(selecto, {field, {comp, value}}) when comp in [:like, :ilike] do
    conf = selecto.config.columns[field]
    ### Value must have a % in it to work!
    ### TODO sanitize like value in caller
    {conf.requires_join, " #{double_wrap(conf.requires_join)}.#{double_wrap(conf.field)} #{comp} ^SelectoParam^ ", [value]}
  end

  def build(selecto, {field, {comp, value}}) when comp in ~w[= != < > <= >=] do
    conf = selecto.config.columns[field]
    {conf.requires_join, " #{double_wrap(conf.requires_join)}.#{double_wrap(conf.field)} #{comp} ^SelectoParam^ ", [value]}
  end

  def build(selecto, {field, list}) when is_list(list) do
    conf = selecto.config.columns[field]
    {conf.requires_join, " #{double_wrap(conf.requires_join)}.#{double_wrap(conf.field)} = ANY(^SelectoParam^) ", [list]}
  end

  def build(selecto, {field, :not_null}) do
    conf = selecto.config.columns[field]
    {conf.requires_join, " #{double_wrap(conf.requires_join)}.#{double_wrap(conf.field)} is not null ", []}
  end

  def build(selecto, {field, value}) when is_nil(value) do
    conf = selecto.config.columns[field]
    {conf.requires_join, " #{double_wrap(conf.requires_join)}.#{double_wrap(conf.field)} is null ", []}
  end

  def build(selecto, {field, value}) do
    conf = selecto.config.columns[field]
    {conf.requires_join, " #{double_wrap(conf.requires_join)}.#{double_wrap(conf.field)} = ^SelectoParam^ ", [value]}
  end

end
