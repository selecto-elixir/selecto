defmodule Selecto.Builder.Sql.Group do
  @moduledoc """
  GROUP BY clause builder for Selecto queries.

  Group expressions are compiled through `Selecto.Builder.Sql.Select` and can be
  emitted either as standard grouped columns or wrapped in SQL `ROLLUP(...)`.
  """

  alias Selecto.AdapterSQL

  def group(selecto, rollup: groups) do
    {joins, clauses_iodata, params} = group(selecto, groups)

    {joins, AdapterSQL.rollup_sql(selecto, clauses_iodata), params}
  end

  def group(selecto, {:grouping_set, groups}) do
    {joins, clauses_iodata, params} = group(selecto, groups)
    {joins, ["(", clauses_iodata, ")"], params}
  end

  def group(selecto, groups) when is_list(groups) do
    {joins, clauses_iodata, params} =
      groups
      |> Enum.reduce(
        {[], [], []},
        fn g, {joins, clauses, params} ->
          {j, c, p} = group(selecto, g)
          {joins ++ [j], clauses ++ [c], params ++ p}
        end
      )

    # Join clauses with ", " separator as iodata
    clause_parts = Enum.intersperse(clauses_iodata, ", ")
    {joins, clause_parts, params}
  end

  def group(selecto, group_by) do
    {c, j, p, _a} = Selecto.Builder.Sql.Select.build(selecto, group_by)
    {j, c, p}
  end

  def build(selecto) do
    group(selecto, selecto.set.group_by)
  end
end
