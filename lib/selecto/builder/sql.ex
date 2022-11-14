defmodule Selecto.Builder.Sql do


  alias Selecto.Builder.Joins


  def table_aliases() do

  end


  def build(selecto, opts) do


    {aliases, sel_joins, select_clause, select_params} = build_select(selecto)
    {filter_joins, where_clause, where_params} = build_where(selecto)
    {group_by_joins, group_by_clause, group_params} = build_group_by(selecto)
    {order_by_joins, order_by_clause, order_params} = build_order_by(selecto)


    {"", aliases, select_clause}
  end

  @doc """
  selecto = Selecto.configure(SelectoTest.Repo, SelectoTestWeb.PagilaLive.selecto_domain())
  selecto = Selecto.select(selecto, ["actor_id", "film[film_id]"])
  selecto |> Selecto.Builder.Sql.build([])
  """

  defp wrap(str) do
    ## TODO do not allow non- \w_ here
    ~s["#{str}"]
  end

  defp build_select(selecto) do
    {aliases, joins, selects, params } = selecto.set.selected
      |> Enum.map(fn s -> Selecto.Builder.Sql.Select.build(selecto, s) end)
      |> Enum.reduce({[],[],[],[]},
        fn {f, j, p, as}, {aliases, joins, selects, params} ->
          {aliases ++ [as], joins ++ [j], selects ++ ["#{wrap(j)}.#{wrap(f)}"], params ++ [p]}
      end)



    {aliases,joins,Enum.join(selects, ", "), params}
  end

  defp build_where(selecto) do
    {[],"", []}
  end

  defp build_group_by(selecto) do
    {[],"",[]}
  end

  defp build_order_by(selecto) do
    {[],"", []}
  end



end
