defmodule Selecto.Builder.Ecto.Join do

  import Ecto.Query

  # get a map of joins to list of selected
  def from_selects(fields, selected) do
    selected
    |> Enum.map(fn
      {:array, _n, sels} -> sels
      {:coalesce, _n, sels} -> sels
      {:case, _n, case_map} -> Map.values(case_map)
      {:literal, _a, _b} -> []
      {_f, {s, _d}, _p} -> s
      {_f, s, _p} -> s
      {_f, s} -> s
      {_f} -> nil
      s -> s
    end)
    |> List.flatten()
    |> Enum.filter(fn
      {:literal, _s} -> false
      s -> not is_nil(s) and Map.get(fields, s)
    end)
    |> Enum.reduce(%{}, fn e, acc ->
      Map.put(acc, fields[e].requires_join, 1)
    end)
    |> Map.keys()
  end

  ### We walk the joins pushing deps in front of joins recursively, then flatten and uniq to make final list
  def get_join_order(joins, requested_joins) do
    requested_joins
    |> Enum.map(fn j ->
      case Map.get(joins, j, %{}) |> Map.get(:requires_join, nil) do
        nil ->
          j

        req ->
          [get_join_order(joins, [req]), req, j]
      end
    end)
    |> List.flatten()
    |> Enum.uniq()
  end



  # Can only give us the joins.. make this recurse and handle :or, :and, etc
  def from_filters(config, filters) do
    filters
    |> Enum.reduce(%{}, fn
      {:or, list}, acc ->
        Map.merge(
          acc,
          Enum.reduce(from_filters(config, list), %{}, fn i, acc -> Map.put(acc, i, 1) end)
        )
      {:and, list}, acc ->
        Map.merge(
          acc,
          Enum.reduce(from_filters(config, list), %{}, fn i, acc -> Map.put(acc, i, 1) end)
        )

      {fil, _val}, acc ->
          Map.put(acc, config.columns[fil].requires_join, 1)

    end)
    |> Map.keys()
  end

    # apply the join to the query
  # we don't need to join root!
  def apply_join(_config, query, :selecto_root) do
    query
  end

  def apply_join(config, query, join) do

    join_map = config.joins[join]

    case join_map do
      # %{ through_path: path } ->
      #   IO.inspect(path)
      #   query
      _ ->
        from({^join_map.requires_join, par} in query,
          left_join: b in ^join_map.i_am,
          as: ^join,
          on: field(par, ^join_map.owner_key) == field(b, ^join_map.my_key)
        )
    end
  end


end
