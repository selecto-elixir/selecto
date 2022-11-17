defmodule Selecto.Builder.Sql do
  alias Selecto.Builder.Joins

  def build(selecto, opts) do

    {aliases, sel_joins, select_clause, select_params} = build_select(selecto)
    {filter_joins, where_clause, where_params} = build_where(selecto)
    {group_by_joins, group_by_clause, group_params} = build_group_by(selecto) #TODO
    {order_by_joins, order_by_clause, order_params} = build_order_by(selecto) #TODO

    joins_in_order = Selecto.Builder.Join.get_join_order(selecto.config.joins,
      List.flatten(sel_joins ++ filter_joins ++ group_by_joins ++ order_by_joins) )

    {from_clause, from_params} = build_from(selecto, joins_in_order)

    sql = "
        select #{select_clause}
        from #{from_clause}
    "

    sql = case where_clause do
      "()" -> sql
      _ -> sql <> "
        where #{where_clause}
      "
    end
    sql = case group_by_clause do
      "" -> sql
      _ -> sql <> "
        group by #{group_by_clause}
      "
    end
    sql = case order_by_clause do
      "" -> sql
      _ -> sql <> "
        order by #{order_by_clause}
      "
    end
    params = select_params ++ from_params ++ where_params ++ group_params ++ order_params

    params_num = Enum.with_index(params) |> Enum.map(fn {_, index} -> "$#{index+1}" end)
    ## replace ^SelectoParam^ with $1 etc. There has to be a better way???? TODO use 1.. params length
    sql = String.split(sql, "^SelectoParam^")
      |> Enum.zip(params_num ++ [""])
      |> Enum.map(fn {a,b}->[a,b] end)
      |> List.flatten()
      |> Enum.join("")

    params = select_params ++ from_params ++ where_params ++ group_params ++ order_params

    {sql, aliases, params}
  end

  @doc """
  selecto = Selecto.configure(SelectoTest.Repo, SelectoTestWeb.PagilaLive.selecto_domain())
  selecto = Selecto.select(selecto, ["actor_id", "film[film_id]", {:literal, "TLIT", 1}])
  selecto = Selecto.filter(selecto, [{:not,
                                          {:or, [{"actor_id", [1,2]},
                                                  {"actor_id", 3}
                                                ]
                                          }
                                      }])
  selecto |> Selecto.execute([])

  selecto = Selecto.configure(SelectoTest.Repo, SelectoTestWeb.PagilaLive.selecto_domain())
  selecto = Selecto.select(selecto, {:count, "first_name", "cnt", {"first_name", {"!=", "DAN"}}})
            Selecto.execute(selecto, [])
  """

  defp build_from(selecto, joins) do
    Enum.reduce(joins, {[],[]}, fn
      :selecto_root, {fc, p} ->
        {fc ++ [~s[#{selecto.config.source_table} "selecto_root"]], p}
      join, {fc, p} ->
        config = selecto.config.joins[join]
        {fc ++ [~s[left join #{config.source} "#{join}" on "#{join}"."#{config.my_key}" = "#{config.requires_join}"."#{config.owner_key}"]],
        p}
      end)
  end

  defp build_select(selecto) do
    {aliases, joins, selects, params } = selecto.set.selected
      |> Enum.map(fn s -> Selecto.Builder.Sql.Select.build(selecto, s) end)
      |> Enum.reduce({[],[],[],[]},
        fn {f, j, p, as}, {aliases, joins, selects, params} ->
          {aliases ++ [as], joins ++ [j], selects ++ [f], params ++ p}
      end)

    {aliases,joins,Enum.join(selects, ", "), params}
  end

  defp build_where(selecto) do
    Selecto.Builder.Sql.Where.build(selecto, {:and, Map.get(selecto.domain, :required_filters, []) ++ selecto.set.filtered})
  end

  defp build_group_by(selecto) do
    Selecto.Builder.Sql.Group.build(selecto)
  end

  defp build_order_by(selecto) do
    Selecto.Builder.Sql.Order.build(selecto)
  end



end
