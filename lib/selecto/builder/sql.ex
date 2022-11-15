defmodule Selecto.Builder.Sql do


  alias Selecto.Builder.Joins


  def table_aliases() do

  end


  def build(selecto, opts) do


    {aliases, sel_joins, select_clause, select_params} = build_select(selecto)
    {filter_joins, where_clause, where_params} = build_where(selecto)
    {group_by_joins, group_by_clause, group_params} = build_group_by(selecto)
    {order_by_joins, order_by_clause, order_params} = build_order_by(selecto)

    joins_in_order = Selecto.Builder.Join.get_join_order(selecto.config.joins, sel_joins ++ filter_joins ++ group_by_joins ++ order_by_joins) |> IO.inspect

    {from_clause, params} = build_from(selecto, joins_in_order)

    sql = ~s"""
      select #{select_clause}
      from   #{Enum.join(from_clause, " ")}
      where  #{where_clause}

    """

    IO.puts(sql)

    {"", aliases, select_clause}
  end

  @doc """
  selecto = Selecto.configure(SelectoTest.Repo, SelectoTestWeb.PagilaLive.selecto_domain())
  selecto = Selecto.select(selecto, ["actor_id", "film[film_id]", {:literal, "TLIT", 1}])
  selecto = Selecto.filter(selecto, [{"actor_id", 1}])
  selecto |> Selecto.Builder.Sql.build([])
  """

  defp build_from(selecto, joins) do
    Enum.reduce(joins, {[],[]}, fn
      :selecto_root, {fc, p} ->
        {fc ++ [~s[#{selecto.config.source_table} "selecto_root"]], p}

      join, {fc, p} ->
        config = selecto.config.joins[join]
        {fc ++ [~s[left join #{config.source} "#{join}" on "#{join}"."#{config.my_key}" = "#{config.requires_join}"."#{config.owner_key}"]], p}


    end)

  end

  defp build_select(selecto) do
    {aliases, joins, selects, params } = selecto.set.selected
      |> Enum.map(fn s -> Selecto.Builder.Sql.Select.build(selecto, s) end)
      |> Enum.reduce({[],[],[],[]},
        fn {f, j, p, as}, {aliases, joins, selects, params} ->
          {aliases ++ [as], joins ++ [j], selects ++ [f], params ++ [p]}
      end)

    {aliases,joins,Enum.join(selects, ", "), params}
  end

  defp build_where(selecto) do
    Selecto.Builder.Sql.Where.build(selecto, {:and, selecto.set.filtered})
  end

  defp build_group_by(selecto) do
    {[],"",[]}
  end

  defp build_order_by(selecto) do
    {[],"", []}
  end



end
