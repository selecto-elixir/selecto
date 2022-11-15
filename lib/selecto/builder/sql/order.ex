defmodule Selecto.Builder.Sql.Order do

  def order(selecto, {dir, order}) when is_atom(dir) do
    {c, j, p, a} = Selecto.Builder.Sql.Select.build(selecto, order)
    {j, "#{c} #{dir}", p}
  end

  def order(selecto, [dir, order]) when is_binary(dir) do
    {c, j, p, a} = Selecto.Builder.Sql.Select.build(selecto, order)
    {j, "#{c} #{dir}", p}
  end


  def order(selecto, order_by) when is_bitstring(order_by) do
    order(selecto, ["asc nulls first", order_by])
  end

  def order(_s, leftover) do
    IO.inspect(leftover, label: "Leftover")
    {[],"", []}
  end

  def build(selecto) do
    IO.inspect(selecto.set.order_by)
    {joins, clauses, params} =
      selecto.set.order_by
      |> Enum.reduce({[], [], []},
        fn g, {joins, clauses, params} ->
          {j, c, p} = order(selecto, g)
          {joins ++ [j], clauses ++ [c], params ++ p}
        end
      )
    {joins, Enum.join(clauses, ", "), params}
  end




end
