defmodule Selecto.Schema.Join do
  # selecto meta join can edit, add, alter this join!

  @moduledoc """
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

  ### source - a schema name such as SelectoTest.Store.Film
  ### joins - the joins map from this join structure

  # we consume the join tree (atom/list) to a flat map of joins then into a map
  def recurse_joins(source, domain) do
    normalize_joins(source, domain.joins, :selecto_root)
    |> List.flatten()
    |> Enum.reduce(%{}, fn j, acc -> Map.put(acc, j.id, j) end)
  end


  defp normalize_joins(source, joins, parent) do
    # IO.inspect(joins, label: "Normalize")

    Enum.reduce(joins, [], fn
      {id, %{non_assoc: true} = config}, acc ->
        acc = acc ++ [Selecto.Schema.Join.configure(id, config, parent, source)]
        case Map.get(config, :joins) do
          nil -> acc
          _ -> acc ++ normalize_joins(config.source, config.joins, id)
        end

      {id, config}, acc ->
        ### Todo allow this to be non-configured assoc
        association = source.__schema__(:association, id)
        acc = acc ++ [configure(id, association, config, parent, source)]
        case Map.get(config, :joins) do
          nil -> acc
          _ -> acc ++ normalize_joins(association.queryable, config.joins, id)
        end
    end)
  end


  #### Non-assoc joins
  defp configure(_id, _config, _dep, _from_source) do
  end

  ## TODO this does not work yet!
  defp configure(_id, %{through: _through} = _association, _config, _dep, _from_source) do
    ### we are going to expand the through but only add the

    ##??????
  end

  ### Custom TODO
  # defp configure(id, %{type: :custom} = config, dep) do
  #   ### this join does not have an association

  # end


  ### Dimension table join
  defp configure(id, %{queryable: _queryable} = association, %{type: :dimension} = config, parent, from_source) do
    #dimension table, has one 'name-ish' value to display, and then the Local reference would provide ID filtering.
    # So create a field for group-by that displays NAME and filters by ID

    name = Map.get(config, :name, id)

    from_field = case parent do
      :selecto_root -> "#{association.owner_key}"
      _ -> "#{parent}[#{association.owner_key}]"
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
      config: config,
      from_source: from_source,
      owner_key: association.owner_key,
      my_key: association.related_key,
      source: association.queryable.__schema__(:source),
      id: id,
      name: name,
      ## probably don't need 'where'
      requires_join: parent,
      filters: Map.get(config, :filters, %{}),
      fields:
        Selecto.Schema.Column.configure_columns(
          association.field,
          [config.dimension_value],
          association.queryable,
          config
        )
    } |> parameterize()
  end



  ### Regular
  defp configure(id, association, config, parent, from_source) do
    std_config(id, association, config, parent, from_source)
  end

  # defp min_config(id, %{queryable: _queryable} = association, config, parent, from_source) do
  #   %{
  #     config: config,
  #     from_source: from_source,
  #     owner_key: association.owner_key,
  #     my_key: association.related_key,
  #     source: association.queryable.__schema__(:source),
  #     id: id,
  #     name: Map.get(config, :name, id),
  #     ## probably don't need 'where'
  #     requires_join: parent,
  #     filters: make_filters(config),

  #   } |> parameterize()
  # end




  defp std_config(id, %{queryable: _queryable} = association, config, parent, from_source) do
    %{
      config: config,
      from_source: from_source,
      owner_key: association.owner_key,
      my_key: association.related_key,
      source: association.queryable.__schema__(:source),
      id: id,
      name: Map.get(config, :name, id),
      ## probably don't need 'where'
      requires_join: parent,
      filters: make_filters(config),
      fields:
        Selecto.Schema.Column.configure_columns(
          association.field,
          association.queryable.__schema__(:fields) --
            association.queryable.__schema__(:redact_fields),
          association.queryable,
          config
        )
    } |> parameterize()
  end


  defp parameterize(join) do
    join
  end


  defp make_filters(config) do
    Map.get(config, :filters, %{})
  end

end
