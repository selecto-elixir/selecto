defmodule Selecto.Builder.Sql.Group do
  def group(selecto, rollup: groups) do
    {joins, clauses, params} = group(selecto, groups)
    {joins, "rollup( #{clauses} )", params}
  end

  def group(selecto, groups) when is_list(groups) do
    {joins, clauses, params} =
      groups
      |> Enum.reduce(
        {[], [], []},
        fn g, {joins, clauses, params} ->
          {j, c, p} = group(selecto, g)
          {joins ++ [j], clauses ++ [c], params ++ p}
        end
      )

    {joins, Enum.join(clauses, ", "), params}
  end

  def group(selecto, group_by) do
    {c, j, p, a} = Selecto.Builder.Sql.Select.build(selecto, group_by)
    {j, c, p}
  end

  def build(selecto) do
    group(selecto, selecto.set.group_by)
  end
end
