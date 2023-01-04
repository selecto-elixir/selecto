defmodule Selecto.Builder.Join do

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
end
