defmodule Listable do
  defstruct [:repo, :domain, :config, :set]

  import Ecto.Query

  alias Listable.Schema.Column

  @doc """
  TODO
    alias selects in case 2 cols have same name
    allow intermediate joins to not have selects (part_group[id])
    filters
    order by
    group by
    aggregates

    combine all 'selects' to allow more efficient retr?

  Mebbie:
    windows?
    ability to add synthetic root, joins, filters, columns
  """


  @moduledoc """
  Documentation for `Listable`.
  """

  @doc """
    Generate a listable structure from this Repo following
    the instructinos in Domain

  """
  def configure(repo, domain) do
    %Listable{
      repo: repo,
      domain: domain,
      config: walk_config(domain),
      set: %{
        selected: domain.selected,
        filtered: domain.filters,
        order_by: [],
        #group_by: [],
      }
    }
  end

  ### move this to the join module
  defp configure_join(association, dep) do
    %{
      i_am: association.queryable,
      joined_from: association.owner,
      #assoc: association,
      cardinality: association.cardinality,
      owner_key: association.owner_key,
      my_key: association.related_key,
      name: association.field,
      ## probably don't need 'where'
      requires_join: dep,
      fields:  walk_fields(association.field,
        association.queryable.__schema__(:fields) -- association.queryable.__schema__(:redact_fields),
        association.queryable)
    }
    |> Listable.Schema.Join.configure()
  end

  ### This is f'n weird feels like it should only take half as many!
  defp normalize_joins(source, [assoc, subs | joins ], dep ) when is_atom(assoc) and is_list(subs) do
    association = source.__schema__(:association, assoc)
    [configure_join(association, dep),
      normalize_joins(association.queryable, subs, assoc)] ++ normalize_joins(source, joins, dep)
  end
  defp normalize_joins(source, [assoc, subs ], dep ) when is_atom(assoc) and is_list(subs) do
    association = source.__schema__(:association, assoc)
    [configure_join(association, dep),
      normalize_joins(association.queryable, subs, assoc)]
  end
  defp normalize_joins(source, [assoc | joins ], dep ) when is_atom(assoc)  do
    association = source.__schema__(:association, assoc)
    [configure_join(association, dep)] ++ normalize_joins(source, joins, dep)
  end
  defp normalize_joins(source, [assoc], dep) when is_atom(assoc)  do
    association = source.__schema__(:association, assoc)
    [configure_join(association, dep)]
  end
  defp normalize_joins(_, _, _) do
    []
  end

  # we consume the join tree (atom/list) to a flat map of joins
  defp recurse_joins(source, joins) do
    List.flatten(normalize_joins(source, joins, :listable_root))
    |> Enum.reduce(%{}, fn j, acc -> Map.put(acc, j.name, j)  end)
  end

  # generate the listable configuration
  defp walk_config(%{source: source} = domain) do
    primary_key = source.__schema__(:primary_key)
    fields = walk_fields(:listable_root, source.__schema__(:fields) -- source.__schema__(:redact_fields), source)
    joins = recurse_joins(source, domain.joins)

    fields = List.flatten( [fields | Enum.map(Map.values(joins), fn e -> e.fields end) ] )
      |> Enum.reduce( %{} ,fn m, acc -> Map.merge(acc, m) end)

    %{
      primary_key: primary_key,
      columns: fields,
      joins: joins
    }
    |> flatten_config( )
    #|> IO.inspect()
  end

  #Configure columns
  defp walk_fields(join, fields, source) do
    fields |> Enum.map( &Column.configure(&1, join, source) )
    |> Map.new()
  end

  ### Put filters/columns/joins in one level with join meta
  defp flatten_config(config) do
    config
  end

  ### todo - make these more flexible
  def select( listable, fields ) do
    put_in( listable.set.selected, listable.set.selected ++ fields)
  end

  ### Make sure to tupleize these!
  def filter( listable, filters ) do
    put_in( listable.set.filtered, listable.set.filtered ++ filters)
  end

  def order_by( listable, orders) do
    put_in( listable.set.order_by, listable.set.order_by ++ orders)
  end

    #  from [listable_root: a] in query
    #  select: map( a, ^Enum.map(selections, fn s -> s.field end))
    #select_merge: map(b, ^Enum.map(selections, fn s -> s.field end))

  defp apply_selections( query, config, selected ) do

    selected
    |> Enum.with_index()
    |> Enum.reduce( query, fn {s, i}, acc ->
      conf = config.columns[s]
      case i do
        0 -> from {^conf.requires_join, owner} in acc,
          select: map(owner, ^[conf.field])
        _ -> from {^conf.requires_join, owner} in acc,
          select_merge: map(owner, ^[conf.field])
      end

    end)
   # from {^join_map.requires_join, par} in query,


  end

  def gen_query( listable ) do
    IO.puts("Gen Query")

    selected_by_join = selected_by_join(listable.config.columns, listable.set.selected )
    filtered_by_join = filter_by_join(listable.config, listable.set.filtered)

    query = from root in listable.domain.source, as: :listable_root

    get_join_order(listable.config.joins, Map.keys(selected_by_join) ++ Map.keys(filtered_by_join))
      |> Enum.reduce(query, fn j, acc ->
        apply_join(listable.config, acc, j)
      end )
      |> apply_selections(listable.config, listable.set.selected)
      |> apply_filters(listable.config, listable.set.filtered)

  end

  ### Not sure how to do this. hmmmm
  #defp filters_recurse(config, query, {mod, filter_list}) when is_atom(mod) and is_list(filter_list) do
  #  query
  #end

  defp filters_recurse(config, query, {name, val}) do
    def = config.columns[name]
    table = def.requires_join
    field = def.field

    case val do
      x when is_nil(x) ->
        from [{^table, a}] in query,
        where: is_nil( field(a, ^field) )
      x when is_bitstring(x) or is_number(x) or is_boolean(x) ->
        from [{^table, a}] in query,
        where: field(a, ^field) == ^val
      x when is_list(x) ->
        from [{^table, a}] in query,
        where: field(a, ^field) in ^val

    end
  end

  defp apply_filters(query, config, filters ) do
    Enum.reduce(filters, query, fn f, acc ->
        filters_recurse(config, acc, f)
      end
    )
  end


  #we don't need to join root!
  defp apply_join( _config, query, :listable_root ) do
    query
  end


  #apply the join to the query
  defp apply_join( config, query, join ) do
    join_map = config.joins[join]
    from {^join_map.requires_join, par} in query,
      left_join: b in ^join_map.i_am,
      as: ^join,
      on: field(par, ^join_map.owner_key) == field(b, ^join_map.my_key)
  end

  ### We walk the joins pushing deps in front of joins recursively, then flatten and uniq to make final list
  defp get_join_order(joins, requested_joins) do
    requested_joins
    |> Enum.map(
      fn j ->
        case Map.get( joins, j, %{} ) |> Map.get(:requires_join, nil) do
          nil -> j
          req ->
            [get_join_order(joins, [req]), req, j]
        end
      end
    )
    |> List.flatten()
    |> Enum.uniq()
  end

  #Can only give us the joins.. make this recurse and handle :or, :and, etc
  defp filter_by_join(config, filters) do
    filters
      |> Enum.reduce( %{}, fn {fil, _val}, acc ->
          field_def = config.columns[fil]
          Map.put(acc, field_def.requires_join, 1 )
        end
      )
  end

  #get a map of joins to list of selected
  defp selected_by_join(fields, selected) do
    selected
      |> Enum.reduce( %{}, fn e, acc ->
        field_def = fields[e]
        Map.put( acc, field_def.requires_join, Map.get(acc, field_def.requires_join, []) ++ [field_def] )
      end)
  end

  #make it go
  def execute( listable ) do
    IO.puts("Execute Query")
    listable
      |> gen_query
      |> listable.repo.all()
      |> IO.inspect( label: "Results")
  end

end
