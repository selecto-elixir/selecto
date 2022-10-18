defmodule Listable do
  defstruct [:repo, :domain, :config, :set]

  import Ecto.Query

  alias Listable.Schema.Column

  @moduledoc """
  Documentation for `Listable,` a query writer and report generator for Elixir/Ecto

    TODO
    filters (complex queries)
    having
    json/embeds/arrays/maps?
       json:  tablen[field].somejsonkey tablen[field][index].somekey...


    distinct

    select into tuple or list instead of map more efficient?
    ability to add synthetic root, joins, filters, columns

    union, union all, intersect, intersect all
    -- pass in lists of alternative filters
    -- allow multiple unions

    limit, offset

    subqueries :D

    Multitenant w schema/prefix

  Mebbie:
    windows?
    CTEs? recursive?
    first, last?? as limit, reverse_order

  ERROR CHECKS
   -- Has association by right name?


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
        filtered: Map.get(domain, :required_filters, []),
        order_by: Map.get(domain, :required_order_by, []),
        group_by: Map.get(domain, :required_group_by, [])
      }
    }
  end

  ### move this to the join module
  defp configure_join(domain, association, dep) do
    %{
      i_am: association.queryable,
      joined_from: association.owner,
      # assoc: association,
      cardinality: association.cardinality,
      owner_key: association.owner_key,
      my_key: association.related_key,
      name: association.field,
      ## probably don't need 'where'
      requires_join: dep,
      fields:
        walk_fields(
          association.field,
          association.queryable.__schema__(:fields) --
            association.queryable.__schema__(:redact_fields),
          association.queryable
        )
    }
    |> Listable.Schema.Join.configure(domain)
  end

  ### This is f'n weird feels like it should only take half as many!
  defp normalize_joins(source,domain,  [assoc, subs | joins], dep)
       when is_atom(assoc) and is_list(subs) do
    association = source.__schema__(:association, assoc)

    [configure_join(domain, association, dep), normalize_joins(association.queryable, domain, subs, assoc)] ++
      normalize_joins(source, domain, joins, dep)
  end

  defp normalize_joins(source,domain,  [assoc, subs], dep) when is_atom(assoc) and is_list(subs) do
    association = source.__schema__(:association, assoc)
    [configure_join(domain, association, dep), normalize_joins(association.queryable, domain, subs, assoc)]
  end

  defp normalize_joins(source,domain,  [assoc | joins], dep) when is_atom(assoc) do
    association = source.__schema__(:association, assoc)
    [configure_join(domain, association, dep)] ++ normalize_joins(source, domain, joins, dep)
  end

  defp normalize_joins(source, domain, [assoc], dep) when is_atom(assoc) do
    association = source.__schema__(:association, assoc)
    [configure_join(domain, association, dep)]
  end

  defp normalize_joins(_, _, _, _) do
    []
  end

  # we consume the join tree (atom/list) to a flat map of joins
  defp recurse_joins(source, domain) do
    normalize_joins(source, domain, domain.joins, :listable_root)
    |> List.flatten()
    |> Enum.reduce(%{}, fn j, acc -> Map.put(acc, j.name, j) end)
  end

  # generate the listable configuration
  defp walk_config(%{source: source} = domain) do
    primary_key = source.__schema__(:primary_key)

    fields =
      walk_fields(
        :listable_root,
        source.__schema__(:fields) -- source.__schema__(:redact_fields),
        source
      )

    joins = recurse_joins(source, domain)

    fields =
      List.flatten([fields | Enum.map(Map.values(joins), fn e -> e.fields end)])
      |> Enum.reduce(%{}, fn m, acc -> Map.merge(acc, m) end)

    %{
      primary_key: primary_key,
      columns: fields,
      joins: joins
    }
  end

  # Configure columns
  defp walk_fields(join, fields, source) do
    fields
    |> Enum.map(&Column.configure(&1, join, source))
    |> Map.new()
  end

  @doc """
    add a field to the Select list. Send in a list of field names
    TODO allow to send single, and special forms..
  """
  def select(listable, fields) do
    put_in(listable.set.selected, Enum.uniq(listable.set.selected ++ fields))
  end

  #### Selects
  ### Add parameterized select functions...

  ## need more? upper, lower, ???, postgres specifics?

  # ARRAY - auto gen array from otherwise denorm'ing selects using postgres 'array' func
  # ---- eg {"array", "item_orders", select: ["item[name]", "item_orders[quantity]"], filters: [{item[type], "Pin"}]}
  # ---- postgres has functions to put those into json!
  # to select the items into an array and apply the filter to the subq. Would ahve to be something that COULD join
  # to one of the main query joins
  defp apply_selection({query, aliases}, _config, {:array, _field, _selects}) do
    {query, aliases}
  end

  # COALESCE ... ??
  defp apply_selection({query, aliases}, _config, {:coalesce, _field, _selects}) do
    {query, aliases}
  end

  # CASE ... {:case, %{{...filter...}}=>val, cond2=>val, :else=>val}}
  defp apply_selection({query, aliases}, _config, {:case, _field, _case_map}) do
    {query, aliases}
  end

  ## Todo why this does not work with numbers?
  defp apply_selection({query, aliases}, _config, {:literal, name, value}) do
    query = from({:listable_root, owner} in query, select_merge: %{^name => ^value})
    {query, [name | aliases]}
  end

  ### works with any func/agg of normal form
  defp apply_selection({query, aliases}, config, {func, field}) when is_atom(func) do
    use_as = "#{func}(#{field})"
    apply_selection({query, aliases}, config, {func, field, use_as})
  end

  ## Case of literal value arg
  defp apply_selection({query, aliases}, _config, {func, {:literal, field}, as})
       when is_atom(func) do
    func = Atom.to_string(func)

    query =
      from(query,
        select_merge: %{
          ^"#{as}" => fragment("?(?)", literal(^func), ^field)
        }
      )

    {query, [as | aliases]}
  end

  # Case for func call with field as arg
  ## Check for SQL INJ TODO
  ## TODO allow for func call args
  ## TODO variant for 2 arg aggs eg string_agg, jsonb_object_agg, Grouping
  ## ^^ and mixed lit/field args - field as list?
  defp apply_selection({query, aliases}, config, {func, field, as}) when is_atom(func) do
    conf = config.columns[field]
    func = Atom.to_string(func)

    query =
      from({^conf.requires_join, owner} in query,
        select_merge: %{
          ^"#{as}" => fragment("?(?)", literal(^func), field(owner, ^conf.field))
        }
      )

    {query, [as | aliases]}
  end

  # Case of 'count(*)' which we can just ref as count
  defp apply_selection({query, aliases}, _config, {:count}) do
    query = from(query, select_merge: %{"count" => fragment("count(*)")})
    {query, ["count" | aliases]}
  end

  # case of other non-arg funcs eg now()
  defp apply_selection({query, aliases}, _config, {func}) when is_atom(func) do
    func = Atom.to_string(func)
    from(query, select_merge: %{^func => fragment("?()", literal(^func))})
    {query, [func | aliases]}
  end

  ### regular old fields. Allow atoms?
  defp apply_selection({query, aliases}, config, field) when is_binary(field) do
    conf = config.columns[field]

    query =
      from({^conf.requires_join, owner} in query,
        select_merge: %{^field => field(owner, ^conf.field)}
      )

    {query, [field | aliases]}
  end

  ### applies the selections to the query
  defp apply_selections(query, config, selected) do
    {query, aliases} =
      selected
      |> Enum.reduce({query, []}, fn s, acc ->
        apply_selection(acc, config, s)
      end)

    IO.inspect(aliases)
    {query, Enum.reverse(aliases)}
  end

  @doc """
    add a filter to listable. Send in a tuple with field name and filter value
  """
  def filter(listable, filters) do
    put_in(listable.set.filtered, listable.set.filtered ++ filters)
  end

  defp apply_filters(query, config, filters) do
    Enum.reduce(filters, query, fn f, acc ->
      filters_recurse(config, acc, f)
    end)
  end

  ### Move to new module since there will be a lot of pattern matching of atoms here...
  ### Not sure how to do this. hmmmm
  # defp filters_recurse(config, query, {mod, filter_list}) when is_atom(mod) and is_list(filter_list) do
  #  query
  # end
  defp filters_recurse(config, query, {name, val}) do
    def = config.columns[name]
    table = def.requires_join
    field = def.field

    case val do
      x when is_nil(x) ->
        from([{^table, a}] in query,
          where: is_nil(field(a, ^field))
        )

      x when is_bitstring(x) or is_number(x) or is_boolean(x) ->
        from([{^table, a}] in query,
          where: field(a, ^field) == ^val
        )

      x when is_list(x) ->
        from([{^table, a}] in query,
          where: field(a, ^field) in ^val
        )

        # todo add more options here
        # >, >=,<=, <, !=
        # :not_true (false or nil)
        # date shortcuts (:today, :tomorrow, :last_week, etc )
        # {:between, a, b}
        # {:like}, {:ilike}
        # {:or, [filters]}
        # {:and, [filters]}
        # {:case, %{filter=>}}
        # {:exists, subq} # how to do subq????
    end
  end

  @doc """
    Add to the Order By
  """
  def order_by(listable, orders) do
    put_in(listable.set.order_by, listable.set.order_by ++ orders)
  end

  defp apply_order_by(query, config, order_bys) do
    order_bys =
      order_bys
      |> Enum.map(fn
        {dir, field} -> {dir, field}
        field -> {:asc, field}
      end)
      |> Enum.map(fn
        {dir, field} ->
          {dir,
           dynamic(
             [{^config.columns[field].requires_join, owner}],
             field(owner, ^config.columns[field].field)
           )}
      end)

    from(query,
      order_by: ^order_bys
    )
  end

  def group_by(listable, groups) do
    put_in(listable.set.group_by, listable.set.group_by ++ groups)
  end

  defp apply_group_by(query, _config, []) do
    query
  end

  defp apply_group_by(query, config, group_bys) do
    group_bys =
      group_bys
      |> Enum.map(fn
        field ->
          dynamic(
            [{^config.columns[field].requires_join, owner}],
            field(owner, ^config.columns[field].field)
          )
      end)

    from(query,
      group_by: ^group_bys
    )
  end

  @doc """
    Returns an Ecto.Query with all your filters and selections added
  """
  def gen_query(listable) do
    IO.puts("Gen Query")
    selected_by_join = selected_by_join(listable.config.columns, listable.set.selected)
    filtered_by_join = filter_by_join(listable.config, listable.set.filtered)

    order_by_by_join =
      selected_by_join(
        listable.config.columns,
        Enum.map(listable.set.order_by, fn
          {_dir, field} -> field
          field -> field
        end)
      )

    group_by_by_join = selected_by_join(listable.config.columns, listable.set.group_by)

    ## We select nothing from the initial query because we are going to select_merge everything and
    ## if we don't select empty map here, it will include the full * of our source!
    query = from(root in listable.domain.source, as: :listable_root, select: %{})

    {query, aliases} =
      get_join_order(
        listable.config.joins,
        Enum.uniq(selected_by_join ++ filtered_by_join ++ order_by_by_join ++ group_by_by_join)
      )
      |> Enum.reduce(query, fn j, acc -> apply_join(listable.config, acc, j) end)
      |> apply_selections(listable.config, listable.set.selected)

    query =
      query
      |> apply_filters(listable.config, listable.set.filtered)
      |> apply_group_by(listable.config, listable.set.group_by)
      |> apply_order_by(listable.config, listable.set.order_by)

    {query, aliases}
  end

  # apply the join to the query
  # we don't need to join root!
  defp apply_join(_config, query, :listable_root) do
    query
  end

  defp apply_join(config, query, join) do
    join_map = config.joins[join]

    from({^join_map.requires_join, par} in query,
      left_join: b in ^join_map.i_am,
      as: ^join,
      on: field(par, ^join_map.owner_key) == field(b, ^join_map.my_key)
    )
  end

  ### We walk the joins pushing deps in front of joins recursively, then flatten and uniq to make final list
  defp get_join_order(joins, requested_joins) do
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
  defp filter_by_join(config, filters) do
    filters
    |> Enum.reduce(%{}, fn {fil, _val}, acc ->
      Map.put(acc, config.columns[fil].requires_join, 1)
    end)
    |> Map.keys()
  end

  # get a map of joins to list of selected
  defp selected_by_join(fields, selected) do
    selected
    |> Enum.map(fn
      {:array, _n, sels} -> sels
      {:coalesce, _n, sels} -> sels
      {:case, _n, case_map} -> Map.values(case_map)
      {:literal, _a, _b} -> []
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

  @doc """
    Generate and run the query, returning list of maps (for now...)
  """
  def execute(listable) do
    IO.puts("Execute Query")

    {query, aliases} =
      listable
      |> gen_query()

    results =
      query
      |> listable.repo.all()
      |> IO.inspect(label: "Results")

    {results, aliases}
  end

  def available_columns(listable) do
    listable.config.columns
  end

  def available_filters(listable) do
    listable.config.filters
  end
end
