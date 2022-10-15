defmodule Listable do
  defstruct [:repo, :domain, :config, :set]

  import Ecto.Query

  alias Listable.Schema.Column

  @moduledoc """
  Documentation for `Listable,` a query writer and report generator for Elixir/Ecto

    TODO
    filters (complex queries)
    order by
    group by
    aggregates

    select into tuple or list instead of map more efficient?
    ability to add synthetic root, joins, filters, columns

  Mebbie:
    windows? CTEs?

  """

  @doc """
    Generate a listable structure from this Repo following
    the instructinos in Domain map
    TODO struct-ize the domain map?
  """
  def configure(repo, domain) do
    %Listable{
      repo: repo,
      domain: domain,
      config: walk_config(domain),
      set: %{
        selected: Map.get(domain, :required_selected, []),
        filtered: Map.get(domain, :required_filters,[]),
        order_by: Map.get(domain, :required_order_by,[]),
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
    #|> IO.inspect()
  end

  #Configure columns
  defp walk_fields(join, fields, source) do
    fields
    |> Enum.map( &Column.configure(&1, join, source) )
    |> Map.new()
  end


  @doc """
    add a field to the Select list. Send in a list of field names
    TODO allow to send single, and special forms..
  """
  def select( listable, fields ) do
    put_in( listable.set.selected, listable.set.selected ++ fields)
  end

  @doc """
    add a filter to listable. Send in a tuple with field name and filter value
  """
  def filter( listable, filters ) do
    put_in( listable.set.filtered, listable.set.filtered ++ filters)
  end

  ### TODO
  def order_by( listable, orders) do
    put_in( listable.set.order_by, listable.set.order_by ++ orders)
  end

  ### Make this cleaner
  defp apply_order_by(query, config, order_bys) do
    order_bys = order_bys
    |> Enum.map( fn
      {dir, field} -> {dir, field}
      field -> {:asc, field}
    end)
    |> Enum.map( fn
      {dir, field} -> {dir, dynamic([{^config.columns[field].requires_join, owner}], field(owner, ^config.columns[field].field))}
    end
    )
    from query,
      order_by: ^order_bys
  end

  ### applies the selections to the query
  defp apply_selections( query, config, selected ) do
    selected
    |> Enum.with_index()
    |> Enum.reduce( query, fn {s, i}, acc ->
      conf = config.columns[s]
      case i do    ### Can I do this in One select: ?
        0 -> from {^conf.requires_join, owner} in acc,
          select:       %{^s => field(owner, ^conf.field) }
        _ -> from {^conf.requires_join, owner} in acc,
          select_merge: %{^s => field(owner, ^conf.field) }
      end
    end)
   # from {^join_map.requires_join, par} in query,
  end

  @doc """
    Returns an Ecto.Query with all your filters and selections added
  """
  def gen_query( listable ) do
    IO.puts("Gen Query")

    selected_by_join = selected_by_join(listable.config.columns, listable.set.selected )
    filtered_by_join = filter_by_join(listable.config, listable.set.filtered)
    order_by_by_join = selected_by_join(listable.config.columns, Enum.map(
      listable.set.order_by,
      fn
        {_dir, field} -> field
        field -> field
      end
    ))

    query = from root in listable.domain.source, as: :listable_root

    get_join_order(listable.config.joins,
      Map.keys(selected_by_join) ++ Map.keys(filtered_by_join) ++ Map.keys(order_by_by_join)
    )
      |> Enum.reduce(query, fn j, acc -> apply_join(listable.config, acc, j) end )
      |> apply_selections(listable.config, listable.set.selected)
      |> apply_filters(listable.config, listable.set.filtered)
      |> apply_order_by(listable.config, listable.set.order_by)

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
      #todo add more options here
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

  @doc """
    Generate and run the query, returning list of maps (for now...)
  """
  def execute( listable ) do
    IO.puts("Execute Query")
    listable
      |> gen_query
      |> listable.repo.all()
      |> IO.inspect( label: "Results")
  end

end
