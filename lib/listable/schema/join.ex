defmodule Listable.Schema.Join do
  # listable meta join can edit, add, alter this join!

  defp normalize_joins(source, domain, joins, dep) do
    Enum.reduce( joins, [], fn {id, config}, acc ->
      ### Todo allow this to be non-configured assoc
      association = source.__schema__(:association, id)
      acc = acc ++ [Listable.Schema.Join.configure(id, association, config, dep)]
      case Map.get(config, :joins) do
        nil -> acc
        _ -> acc ++ normalize_joins(association.queryable, domain, config.joins, id)
      end
    end)
  end

  # we consume the join tree (atom/list) to a flat map of joins then into a map
  def recurse_joins(source, domain) do
    normalize_joins(source, domain, domain.joins, :listable_root)
    |> List.flatten()
    |> Enum.reduce(%{}, fn j, acc -> Map.put(acc, j.id, j) end)
  end

  def configure(id, association, config, dep) do
    IO.puts("configuring #{ association.field}")
    join = %{
      i_am: association.queryable,
      joined_from: association.owner,
      # assoc: association,
      cardinality: association.cardinality,
      owner_key: association.owner_key,
      my_key: association.related_key,
      id: id,
      name: config.name,
      ## probably don't need 'where'
      requires_join: dep,
      fields:
        Listable.Schema.Column.configure_columns(
          association.field,
          association.queryable.__schema__(:fields) --
            association.queryable.__schema__(:redact_fields),
          association.queryable
        )
    }
    if function_exported?(join.i_am, :listable_meta_join, 1) do
      join.i_am.listable_meta_join(join)
    else
      join
    end

  end

end
