defmodule Selecto.Schema.Join do
  # selecto meta join can edit, add, alter this join!

  defp normalize_joins(source, domain, joins, dep) do
    #IO.inspect(joins, label: "Normalize")

    Enum.reduce(joins, [], fn
      {id, %{type: :cte} = config}, acc ->
        acc = acc ++ [Selecto.Schema.Join.configure_cte(id, source, config, dep)]

        case Map.get(config, :joins) do
          ## how to add joins here
          _ -> acc
        end

      {id, config}, acc ->
        ### Todo allow this to be non-configured assoc
        association = source.__schema__(:association, id)
        acc = acc ++ [Selecto.Schema.Join.configure(id, association, config, dep)]
        #|> IO.inspect(label: "After conf")
        case Map.get(config, :joins) do
          nil -> acc
          _ -> acc ++ normalize_joins(association.queryable, domain, config.joins, id)
        end
    end)
  end

  # we consume the join tree (atom/list) to a flat map of joins then into a map
  def recurse_joins(source, domain) do
    normalize_joins(source, domain, domain.joins, :selecto_root)
    |> List.flatten()
    |> Enum.reduce(%{}, fn j, acc -> Map.put(acc, j.id, j) end)
  end

  def configure_cte(id, source, config, dep) do
    ### TODO for CTEs etc
    join = %{
      id: id,


    }
  end

##TODO
  def configure(id, %{through: through} = association, config, dep) do
    trail = Map.get(association, :through)
    start = association.owner
    {path, target} = trail |> Enum.reduce({[], start},
      fn assoc, {acc, start} ->
        step_assoc = start.__schema__(:association, assoc)
        target =
        {acc ++ [%{
          id: assoc,
          association: step_assoc
        }], step_assoc.queryable}
      end
    )
    %{
      joined_from: association.owner,
      # assoc: association,
      cardinality: association.cardinality,
      owner_key: association.owner_key,
      #my_key: association.related_key,
      through: Map.get(association, :through),
      through_path: path,
      #source: association.queryable.__schema__(:source),
      id: id,
      name: Map.get(config, :name, id),
      ## probably don't need 'where'
      requires_join: dep,
      filters: Map.get(config, :filters, %{}),
      fields:
        Selecto.Schema.Column.configure_columns(
          association.field,
          target.__schema__(:fields) --
            target.__schema__(:redact_fields),
          target,
          config
        )
    }
  end

  def configure(id, %{queryable: queryable} = association, config, dep) do

    #IO.inspect(association)
    %{
      joined_from: association.owner,
      # assoc: association,
      cardinality: association.cardinality,
      owner_key: association.owner_key,
      my_key: association.related_key,
      source: association.queryable.__schema__(:source),
      id: id,
      name: Map.get(config, :name, id),
      ## probably don't need 'where'
      requires_join: dep,
      filters: Map.get(config, :filters, %{}),
      fields:
        Selecto.Schema.Column.configure_columns(
          association.field,
          association.queryable.__schema__(:fields) --
            association.queryable.__schema__(:redact_fields),
          association.queryable,
          config
        )
    }
  end
end
