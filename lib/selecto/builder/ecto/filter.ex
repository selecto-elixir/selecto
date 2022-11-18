defmodule Selecto.Builder.Ecto.Filter do
  import Ecto.Query

  # Thanks to https://medium.com/swlh/how-to-write-a-nested-and-or-query-using-elixirs-ecto-library-b7755de79b80
  defp combine_fragments_with_and(fragments) do
    conditions = false

    Enum.reduce(fragments, conditions, fn fragment, conditions ->
      if !conditions do
        dynamic([q], ^fragment)
      else
        dynamic([q], ^conditions and ^fragment)
      end
    end)
  end

  defp combine_fragments_with_or(fragments) do
    conditions = false

    Enum.reduce(fragments, conditions, fn fragment, conditions ->
      if !conditions do
        dynamic([q], ^fragment)
      else
        dynamic([q], ^conditions or ^fragment)
      end
    end)
  end

  def apply_filters(query, config, filters) do
    filter =
      Enum.map(filters, fn f ->
        filters_recurse(config, f)
      end)
      |> combine_fragments_with_and()

    query |> where(^filter)
  end

  defp filters_recurse(config, {:or, filters}) do
    Enum.map(filters, fn f ->
      filters_recurse(config, f)
    end)
    |> combine_fragments_with_or()
  end

  defp filters_recurse(config, {:and, filters}) do
    Enum.map(filters, fn f ->
      filters_recurse(config, f)
    end)
    |> combine_fragments_with_and()
  end

  ### TODO add :not

  defp filters_recurse(config, {name, val}) do
    def = config.columns[name]
    table = def.requires_join
    field = def.field

    ### how to allow function calls/subqueries in field and val?
    case val do
      x when is_nil(x) ->
        dynamic([{^table, a}], is_nil(field(a, ^field)))

      x when is_bitstring(x) or is_number(x) or is_boolean(x) ->
        dynamic([{^table, a}], field(a, ^field) == ^val)

      x when is_list(x) ->
        dynamic([{^table, a}], field(a, ^field) in ^val)

      # TODO not-in

      # sucks to not be able to do these 6 in one with a fragment!
      {x, v} when x == "!=" ->
        dynamic([{^table, a}], field(a, ^field) != ^v)

      {x, v} when x == "<" ->
        dynamic([{^table, a}], field(a, ^field) < ^v)

      {x, v} when x == ">" ->
        dynamic([{^table, a}], field(a, ^field) > ^v)

      {x, v} when x == "<=" ->
        dynamic([{^table, a}], field(a, ^field) <= ^v)

      {x, v} when x == ">=" ->
        dynamic([{^table, a}], field(a, ^field) >= ^v)

      {:between, min, max} ->
        dynamic([{^table, a}], fragment("? between ? and ?", field(a, ^field), ^min, ^max))

      :not_true ->
        dynamic([{^table, a}], not field(a, ^field))

      {:like, v} ->
        dynamic([{^table, a}], like(field(a, ^field), ^v))

      {:ilike, v} ->
        dynamic([{^table, a}], ilike(field(a, ^field), ^v))

      {:subquery, :in, query} ->
        dynamic([{^table, a}], field(a, ^field) in ^query)

      _ ->
        raise "Filter Recurse not implemented for #{inspect(val)}"
        # date shortcuts (:today, :tomorrow, :last_week, etc )

        # {:case, %{filter=>}}
        # {:exists, etc-, subq} # how to do subq????
    end
  end
end
