defmodule Selecto.Builder.Sql do


  alias Selecto.Builder.Joins


  def build(selecto, opts) do


    {aliases, sel_joins, select_clause} = build_select(selecto)
    {filter_joins, where_clause} = build_where(selecto)
    {group_by_joins, group_by_clause} = build_group_by(selecto)
    {order_by_joins, order_by_clause} = build_order_by(selecto)


    {"", aliases}
  end

  defp build_select(selecto) do
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
