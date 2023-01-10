defmodule Selecto.Builder.Sql do

  import Selecto.Helpers

  def build(selecto, _opts) do
    {aliases, sel_joins, select_clause, select_params} = build_select(selecto)
    {filter_joins, where_clause, where_params} = build_where(selecto)
    {group_by_joins, group_by_clause, group_params} = build_group_by(selecto)
    {order_by_joins, order_by_clause, order_params} = build_order_by(selecto)

    joins_in_order =
      Selecto.Builder.Join.get_join_order(
        selecto.config.joins,
        List.flatten(sel_joins ++ filter_joins ++ group_by_joins ++ order_by_joins)
      )

    {from_clause, from_params} = build_from(selecto, joins_in_order)

    sql = "
        select #{select_clause}
        from #{from_clause}
    "

    sql =
      case where_clause do
        "()" -> sql
        _ -> sql <> "
        where #{where_clause}
      "
      end

    sql =
      case group_by_clause do
        "" -> sql
        _ -> sql <> "
        group by #{group_by_clause}
      "
      end

    sql =
      if String.contains?(group_by_clause, "rollup") do
        case order_by_clause do
          "" -> sql
          _ -> "select * from (" <> sql <> ") as rollupfix
        order by #{order_by_clause}
      "
        end
      else
        case order_by_clause do
          "" -> sql
          _ -> sql <> "
        order by #{order_by_clause}
      "
        end
      end

    params = select_params ++ from_params ++ where_params ++ group_params ++ order_params

    params_num = Enum.with_index(params) |> Enum.map(fn {_, index} -> "$#{index + 1}" end)

    ## replace ^SelectoParam^ with $1 etc. There has to be a better way???? TODO use 1.. params length
    sql =
      String.split(sql, "^SelectoParam^")
      |> Enum.zip(params_num ++ [""])
      |> Enum.map(fn {a, b} -> [a, b] end)
      |> List.flatten()
      |> Enum.join("")

    params = select_params ++ from_params ++ where_params ++ group_params ++ order_params

    {sql, aliases, params}
  end

  #rework to allow parameterized joins, CTEs etc TODO
  defp build_from(selecto, joins) do
    Enum.reduce(joins, {[], []}, fn
      :selecto_root, {fc, p} ->
        {fc ++ [~s[#{selecto.config.source_table} #{build_join_string(selecto, "selecto_root")}]], p}

      join, {fc, p} ->
        config = selecto.config.joins[join]

        {fc ++
           [
             ~s[ left join #{config.source} #{build_join_string(selecto, join)} on #{build_selector_string(selecto, join, config.my_key)} = #{build_selector_string(selecto, config.requires_join, config.owner_key)}]
           ], p}
    end)
  end

  defp build_select(selecto) do
    {aliases, joins, selects, params} =
      selecto.set.selected
      |> Enum.map(fn s -> Selecto.Builder.Sql.Select.build(selecto, s) end)
      |> Enum.reduce(
        {[], [], [], []},
        fn {f, j, p, as}, {aliases, joins, selects, params} ->
          {aliases ++ [as], joins ++ [j], selects ++ [f], params ++ p}
        end
      )

    {aliases, joins, Enum.join(selects, ", "), params}
  end

  defp build_where(selecto) do
    Selecto.Builder.Sql.Where.build(
      selecto,
      {:and, Map.get(Selecto.domain(selecto), :required_filters, []) ++ selecto.set.filtered}
    )
  end

  defp build_group_by(selecto) do
    Selecto.Builder.Sql.Group.build(selecto)
  end

  defp build_order_by(selecto) do
    Selecto.Builder.Sql.Order.build(selecto)
  end
end
