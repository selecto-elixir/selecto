defmodule Selecto.Builder.Sql.Where do

  import Selecto.Helpers






  def build(selecto, {conj, filters}) when conj in [:and, :or] do
    IO.inspect(filters, label: "filters")
    {joins, clauses, params} = Enum.reduce(filters, {[],[],[]}, fn
      f, {joins, clauses, params} ->
        {j, c, p} = build(selecto, f)
        {joins ++ [j], clauses ++ [c], params ++ p}
    end)
    IO.inspect({joins, Enum.join(clauses, "\n#{conj}\n"), params})
    #Joins, clause, params
  end

  def build(selecto, {field, value}) do
    IO.puts(field)
    conf = selecto.config.columns[field]
    IO.inspect(conf)
    {conf.requires_join, " #{double_wrap(conf.requires_join)}.#{double_wrap(conf.field)} = ? ", [value]}
  end

end
