defmodule Listable.Schema.Join do
  # listable meta join can edit, add, alter this join!

    ### move this to the join module
  def configure(_domain, association, config, dep) do
    IO.puts("configuring #{ association.field}")
    join = %{
      i_am: association.queryable,
      joined_from: association.owner,
      # assoc: association,
      cardinality: association.cardinality,
      owner_key: association.owner_key,
      my_key: association.related_key,
      id: association.field,
      name: config.name,
      ## probably don't need 'where'
      requires_join: dep,
      fields:
        Listable.walk_fields(
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
