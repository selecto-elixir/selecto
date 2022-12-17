defmodule Selecto.Schema.Join do
  # selecto meta join can edit, add, alter this join!

  @doc """
  # Join Types ideas

  Done
  - dimension - this type of join the local table has an ID that points to a table with an ID and name and that name could easily just be a member of this table. So we will make special filters and columns.

  Planned
  - self - Joining into self we only want to grab columns that are asked for
  - through - this type of join has interesting tables on both sides, but probably nothing interesting in itself. Let's SKIP adding columns from this table unless they are requested in the columns map
  - parameterized - these are joins that can be repeated with different rows on the far side, like a flag or tag table

  ...
  - one_to_one - Like a lookup but there are more interesting cols on the far side, we will treat normally. Also, default
  - one_to_many - Will treat like a one to one
  - belongs_to - Will treat like one-to-one

  """

  defp normalize_joins(source, domain, joins, dep) do
    # IO.inspect(joins, label: "Normalize")

    Enum.reduce(joins, [], fn
      {id, config}, acc ->
        ### Todo allow this to be non-configured assoc
        association = source.__schema__(:association, id)
        acc = acc ++ [Selecto.Schema.Join.configure(id, association, config, dep, source)]
        # |> IO.inspect(label: "After conf")
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

  ## TODO this does not work yet!
  def configure(id, %{through: through} = association, config, dep, from_source) do
    trail = Map.get(association, :through)
    start = association.owner

    {path, target} =
      trail
      |> Enum.reduce(
        {[], start},
        fn assoc, {acc, start} ->
          step_assoc = start.__schema__(:association, assoc)

          target =
            {acc ++
               [
                 %{
                   id: assoc,
                   association: step_assoc
                 }
               ], step_assoc.queryable}
        end
      )

    %{
      # joined_from: association.owner,
      # assoc: association,
      # cardinality: association.cardinality,
      owner_key: association.owner_key,
      # my_key: association.related_key,
      through: Map.get(association, :through),
      through_path: path,
      # source: association.queryable.__schema__(:source),
      id: id,
      name: Map.get(config, :name, id),
      ## probably don't need 'where'
      requires_join: dep,
      filters: Selecto.Schema.Filter.configure_filters(Map.get(config, :filters, %{}), dep),
      # this will bring in custom columns
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

  ### Custom TODO
  # def configure(id, %{type: :custom} = config, dep) do
  #   ### this join does not have an association

  # end


  ### Dimension table join
  def configure(id, %{queryable: queryable} = association, %{type: :dimension} = config, dep, from_source) do
    #dimension table, has one 'name-ish' value to display, and then the Local reference would provide ID filtering.
    # So create a field for group-by that displays NAME and filters by ID

    name = Map.get(config, :name, id)

    from_field = case dep do
      :selecto_root -> "#{association.owner_key}"
      _ -> "#{dep}[#{association.owner_key}]"
    end

    config = Map.put(config, :custom_columns, Map.get(config, :custom_columns, %{}) |> Map.put(
        "#{id}", %{ ## we will use the nane of the join's association!
          name: name,
          ### concat_ws?
          select: "#{association.field}[#{config.dimension_value}]",
          ### we will always get a tuple of select + group_by_filter_select here
          group_by_format: fn {a, _id}, _def -> a end,
          group_by_filter: from_field,
          group_by_filter_select: ["#{association.field}[#{config.dimension_value}]", from_field ]
        }
      )
    )

    %{
      owner_key: association.owner_key,
      my_key: association.related_key,
      source: association.queryable.__schema__(:source),
      id: id,
      name: name,
      ## probably don't need 'where'
      requires_join: dep,
      filters: Map.get(config, :filters, %{}),
      fields:
        Selecto.Schema.Column.configure_columns(
          association.field,
          [config.dimension_value],
          association.queryable,
          config
        )
    }
  end



  ### Regular
  def configure(id, %{queryable: queryable} = association, config, dep, from_source) do
    %{
      # joined_from: association.owner, #Not used?
      # assoc: association,
      # cardinality: association.cardinality,
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
