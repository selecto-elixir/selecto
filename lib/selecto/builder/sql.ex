defmodule Selecto.Builder.Sql do


  alias Selecto.Builder.Joins


  def table_aliases() do

  end


  def build(selecto, opts) do


    {aliases, sel_joins, select_clause} = build_select(selecto)
    {filter_joins, where_clause} = build_where(selecto)
    {group_by_joins, group_by_clause} = build_group_by(selecto)
    {order_by_joins, order_by_clause} = build_order_by(selecto)


    {"", aliases}
  end

  defp build_select(selecto) do
    selecto.set.selected
      |> Enum.reduce([], fn
        s, acc ->
          case Selecto.Builder.Sql.Select.build(selecto, s) do
            [] = r -> acc ++ r
            r -> acc ++ [r]
          end
      end)






    {[],[],""}
  end

  defp build_where(selecto) do
    {[],""}
  end

  defp build_group_by(selecto) do
    {[],""}
  end

  defp build_order_by(selecto) do
    {[],""}
  end



end
