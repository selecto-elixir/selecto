defmodule Selecto.Builder.Ecto do
  alias Selecto.Builder.Ecto.{Select, Joins, Filter, Group}
  alias Selecto.Builder.Join
  import Ecto.Query

  @doc """
    Returns an Ecto.Query with all your filters and selections added..eventually!
  """
  def gen_query(selecto, opts \\ []) do
    # IO.puts("Gen Query")

    {results_type, opts} = Keyword.pop(opts, :results_type, :maps)

    from_selects = Join.from_selects(selecto.config.columns, selecto.set.selected)
    filters_to_use = Map.get(selecto.domain, :required_filters, []) ++ selecto.set.filtered
    filtered_by_join = Join.from_filters(selecto.config, filters_to_use)

    joins_from_order_by =
      Join.from_selects(
        selecto.config.columns,
        Enum.map(selecto.set.order_by, fn
          {_dir, field} -> field
          field -> field
        end)
      )

    joins_from_group_by = Join.from_selects(selecto.config.columns, selecto.set.group_by)

    ## We select nothing from the initial query because we are going to select_merge everything and
    ## if we don't select empty map here, it will include the full * of our source!
    query = from(root in selecto.domain.source, as: :selecto_root, select: %{})

    ##### If we are GROUP BY and have AGGREGATES that live on a join path with any :many
    ##### cardinality we have to force the aggregates to subquery

    {query, aliases} =
      Join.get_join_order(
        selecto.config.joins,
        Enum.uniq(from_selects ++ filtered_by_join ++ joins_from_order_by ++ joins_from_group_by)
      )
      |> Enum.reduce(query, fn j, acc -> Joins.apply_join(selecto.config, acc, j) end)
      |> Select.apply_selections(selecto.config, selecto.set.selected)

    # IO.inspect(query, label: "Second Last")

    query =
      query
      |> Filter.apply_filters(selecto.config, filters_to_use)
      |> Group.apply_group_by(selecto.config, selecto.set.group_by)
      |> Select.apply_order_by(selecto.config, selecto.set.order_by)

    # IO.inspect(query, label: "Last")

    {query, aliases}
  end
end
