defmodule Selecto.Builder.Sql.Order do
  @dirs %{
    asc: "asc",
    desc: "desc",
    asc_nulls_first: "asc nulls first",
    asc_nulls_last: "asc nulls last",
    desc_nulls_first: "desc nulls first",
    desc_nulls_last: "desc nulls last"
  }

  @dir_list [
    :asc,
    :desc,
    :asc_nulls_first,
    :asc_nulls_last,
    :desc_nulls_first,
    :desc_nulls_last
  ]

  def order(selecto, {dir, order}) when dir in @dir_list do
    {c, j, p, _a} = Selecto.Builder.Sql.Select.build(selecto, order)
    #### I think this will break for a parameterized col...
    {j, "#{c} #{@dirs[dir]}", p}
  end

  def order(selecto, order_by) do
    order(selecto, {:asc_nulls_first, order_by})
  end

  def build(selecto) do
    {joins, clauses, params} =
      selecto.set.order_by
      |> Enum.reduce(
        {[], [], []},
        fn g, {joins, clauses, params} ->
          {j, c, p} = order(selecto, g)
          {joins ++ [j], clauses ++ [c], params ++ p}
        end
      )

    {joins, Enum.join(clauses, ", "), params}
  end
end
