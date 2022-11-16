defmodule Selecto.Builder.Sql.Order do


  @dirs %{
    asc: "asc",
    desc: "desc",
    asc_nulls_first: "asc nulls first"
  }

  @dir_list [
    :asc,:desc,:asc_nulls_first

  ]

  def order(selecto, {dir, order}) when dir in @dir_list do
    {c, j, p, a} = Selecto.Builder.Sql.Select.build(selecto, order)
    {j, "#{c} #{@dirs[dir]}", p}
  end

  def order(selecto, order_by) do
    order(selecto, {:asc_nulls_first, order_by})
  end

  def order(_s, leftover) do
    IO.inspect(leftover, label: "Leftover Order By!")
    {[],"", []}
  end

  def build(selecto) do
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
