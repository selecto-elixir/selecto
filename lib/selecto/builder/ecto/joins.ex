defmodule Selecto.Builder.Ecto.Joins do
  import Ecto.Query

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
