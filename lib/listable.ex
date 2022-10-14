defmodule Listable do
  defstruct [:repo, :domain, :config, :set]

  import Ecto.Query

  alias Listable.Schema.Column

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
        filtered: [ domain.filters ],
        order_by: [],
        #group_by: [],
      }
    }
  end

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
  ### This is f'n weird, fix it TODO allow user: [:profiles] !
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


  defp recurse_joins(source, joins) do
    List.flatten(normalize_joins(source, joins, :listable_root))
    |> Enum.reduce(%{}, fn j, acc -> Map.put(acc, j.name, j)  end)

  end

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
  end

  defp walk_fields(join, fields, source) do
    fields |> Enum.map( &Column.configure(&1, join, source) )
    |> Map.new()
  end

  ### Put filters/columns/joins in one level with join meta
  defp flatten_config(config) do
    config
  end

  def select( listable, fields ) do
    put_in( listable.set.selected, listable.set.selected ++ fields)
  end

  def filter( listable, filters ) do
    put_in( listable.set.filtered, listable.set.filtered ++ filters)
  end

  def order_by( listable, orders) do
    put_in( listable.set.order_by, listable.set.order_by ++ orders)
  end

  def gen_query( listable ) do
    IO.puts("Gen Query")


    selected_by_join = selected_by_join(listable.config.columns, listable.set.selected ) |> IO.inspect()
    filtered_by_join = filter_by_join()

    #sel =  Enum.reduce( selected_by_join.listable_root, %{}, fn s, acc -> Map.put(acc, s.colid, s.field) end)
    query = from root in listable.domain.source, as: :listable_root #, select: ^sel


    query = get_join_order(listable.config.joins, Map.keys(selected_by_join) ++ Map.keys(filtered_by_join))
      |> IO.inspect(label: "Join Order")
      |> Enum.reduce(query, fn j, acc ->
        apply_join(listable.config.joins, acc, j,
          Map.get(selected_by_join, j, %{}),
          Map.get(filtered_by_join, j, %{}))
      end )
      |> IO.inspect()




    query |> IO.inspect( struct: false, label: "Query")
  end

  defp apply_join( joins, query, :listable_root, selections, filters ) do
    fields = Enum.map(selections, fn s -> s.field end)
    IO.inspect(selections, label: "Joins")
    from [listable_root: a] in query,
      select: map( a, ^fields)
  end


  defp apply_join( joins, query, join, selections, filters ) do
    fields = Enum.map(selections, fn s -> s.field end)
    join_map = joins[join]
    join_repo = join_map.i_am
    parent_id = join_map.owner_key
    my_id = join_map.my_key
    source = join_map.requires_join
    from {^source, par} in query,
      join: b in ^join_repo,
      as: ^join,
      on: field(par, ^parent_id) == field(b, ^my_id)

  end

  defp get_join_order(joins, requested_joins) do
    ## look at each requested join, if it has a requires_join, push recursive call to get_join_order in front of it!

    requested_joins
    |> Enum.map(
      fn j ->
        case Map.get( joins, j, %{} ) |> Map.get(:requires_joins, nil) do
          nil -> j
          req -> [get_join_order(joins, [req]) | j]
        end
      end
    )
    |> List.flatten()
  end

  defp filter_by_join() do
    %{}
  end
  defp selected_by_join(fields, selected) do
    selected
      |> Enum.reduce( %{}, fn e, acc ->
        field_def = fields[e]
        Map.put( acc, field_def.requires_join, Map.get(acc, field_def.requires_join, []) ++ [field_def] )
      end)
  end

  def execute( listable ) do
    IO.puts("Execute Query")
    listable
      |> gen_query
      |> listable.repo.all()
      |> IO.inspect()
  end

end
