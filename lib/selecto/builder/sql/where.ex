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


  def build(selecto, {:not, filter}) do
    {j, c, p} = build(selecto, filter)
    IO.inspect({j, "not " <> c <> "", p})
    #Joins, clause, params
  end

  def build(selecto, {conj, filters}) when conj in [:and, :or] do
    IO.inspect(filters, label: "filters")
    {joins, clauses, params} = Enum.reduce(filters, {[],[],[]}, fn
      f, {joins, clauses, params} ->
        {j, c, p} = build(selecto, f)
        {joins ++ [j], clauses ++ [c], params ++ p} |> IO.inspect(label: "build and")
    end)
    IO.inspect({joins, "(" <> Enum.join(clauses, ")\n     #{conj}  (") <> ")", params})
    #Joins, clause, params
  end

  def build(selecto, {field, value}) do
    IO.puts(field)
    conf = selecto.config.columns[field]
    IO.inspect(conf)
    {conf.requires_join, " #{double_wrap(conf.requires_join)}.#{double_wrap(conf.field)} = ? ", [value]}
  end

  def build(selecto, {field, value}) do
    IO.puts(field)
    conf = selecto.config.columns[field]
    IO.inspect(conf)
    {conf.requires_join, " #{double_wrap(conf.requires_join)}.#{double_wrap(conf.field)} = ? ", [value]}
  end

end
