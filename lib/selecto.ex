defmodule Selecto do
  defstruct [:repo, :domain, :config, :set]

  import Ecto.Query

  @moduledoc """
  Documentation for `Selecto,` a query writer and report generator for Elixir/Ecto

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
    Generate a selecto structure from this Repo following
    the instructinos in Domain map
    TODO struct-ize the domain map?
  """
  def configure(repo, domain) do
    %Selecto{
      repo: repo,
      domain: domain,
      config: configure_domain(domain),
      set: %{
        selected: Map.get(domain, :required_selected, []),
        filtered: [],
        order_by: Map.get(domain, :required_order_by, []),
        group_by: Map.get(domain, :required_group_by, [])
      }
    }
  end

  # generate the selecto configuration
  defp configure_domain(%{source: source} = domain) do
    primary_key = source.__schema__(:primary_key)

    fields =
      Selecto.Schema.Column.configure_columns(
        :selecto_root,
        ## Add in keys from domain.columns ...
        source.__schema__(:fields) -- source.__schema__(:redact_fields),
        source,
        domain
      )

    joins = Selecto.Schema.Join.recurse_joins(source, domain)
    ## Combine fields from Joins into fields list
    fields =
      List.flatten([fields | Enum.map(Map.values(joins), fn e -> e.fields end)])
      |> Enum.reduce(%{}, fn m, acc -> Map.merge(m, acc) end)

    ### Extra filters (all normal fields can be a filter)
    filters = Map.get(domain, :filters, %{})

    filters =
      Enum.reduce(
        Map.values(joins),
        filters,
        fn e, acc ->
          Map.merge(Map.get(e, :filters, %{}), acc)
        end
      )

    %{
      primary_key: primary_key,
      columns: fields,
      joins: joins,
      filters: filters,
      domain_data: Map.get(domain, :domain_data)
    }
  end

  @doc """
    add a field to the Select list. Send in a list of field names
    TODO allow to send single, and special forms..
  """
  def select(selecto, fields) when is_list(fields) do
    put_in(selecto.set.selected, Enum.uniq(selecto.set.selected ++ fields))
  end

  def select(selecto, field) do
    Selecto.select(selecto, [field])
  end

  #### Selects
  ### Add parameterized select functions...

  defp check_string( string ) do
    if string |> String.match?(~r/^[^a-zA-Z0-9_]+$/) do
      raise "Invalid String #{string}"
    end
    string
  end

  ## need more? upper, lower, ???, postgres specifics?
  defp apply_selection({query, aliases}, config, {:subquery, func, field}) do
    conf = config.columns[field]

    join = config.joins[conf.requires_join]
    my_func = check_string( Atom.to_string(func) )
    my_key = Atom.to_string(join.my_key)
    my_field = Atom.to_string(conf.field)



    # from a in SelectoTest.Test.SolarSystem, select: {fragment("(select json_agg(planets) from planets where solar_system_id = ?)", a.id)}
    # from a in SelectoTest.Test.SolarSystem, select: {fragment("(select count(id) from planets where solar_system_id = ?)", a.id)}
    as = "#{func}(#{field})"

    dyn = %{
      as =>
        dynamic(
          [{^join.requires_join, par}],
          fragment(
            "(select ?(?) from ? where ? = ?)",
            literal(^my_func),
            literal(^my_field),
            literal(^join.source),
            literal(^my_key),
            par.id
          )
        )
    }

    query = from(a in query, select_merge: ^dyn)
    {query, [as | aliases]}
  end

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

  defp apply_selection({query, aliases}, config, {:extract, field, format}) do
    conf = config.columns[field]
    as = "#{format} from #{field}"

    check_string(format)

    query =
      from({^conf.requires_join, owner} in query,
        select_merge: %{
          ^"#{as}" => fragment("extract(? from ?)", literal(^format), field(owner, ^conf.field))
        }
      )

    {query, [as | aliases]}
  end

  defp apply_selection({query, aliases}, config, {:to_char, {field, format}, as}) do
    conf = config.columns[field]

    query =
      from({^conf.requires_join, owner} in query,
        select_merge: %{
          ^"#{as}" => fragment("to_char(?, ?)", field(owner, ^conf.field), ^format)
        }
      )

    {query, [as | aliases]}
  end

  ## Todo why this does not work with numbers?
  defp apply_selection({query, aliases}, _config, {:literal, name, value}) do
    query = from({:selecto_root, owner} in query, select_merge: %{^name => ^value})
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
    func = Atom.to_string(func) |> check_string()

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
    func = Atom.to_string(func) |> check_string()

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
    func = Atom.to_string(func) |> check_string()
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

    {query, Enum.reverse(aliases)}
  end

  # get a map of joins to list of selected
  defp joins_from_selects(fields, selected) do
    selected
    |> Enum.map(fn
      {:array, _n, sels} -> sels
      {:coalesce, _n, sels} -> sels
      {:case, _n, case_map} -> Map.values(case_map)
      {:literal, _a, _b} -> []
      {_f, {s, _d}, _p} -> s
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
    add a filter to selecto. Send in a tuple with field name and filter value
  """
  def filter(selecto, filters) when is_list(filters) do
    put_in(selecto.set.filtered, selecto.set.filtered ++ filters)
  end

  def filter(selecto, filters) do
    put_in(selecto.set.filtered, selecto.set.filtered ++ [filters])
  end

  # Thanks to https://medium.com/swlh/how-to-write-a-nested-and-or-query-using-elixirs-ecto-library-b7755de79b80
  defp combine_fragments_with_and(fragments) do
    conditions = false

    Enum.reduce(fragments, conditions, fn fragment, conditions ->
      if !conditions do
        dynamic([q], ^fragment)
      else
        dynamic([q], ^conditions and ^fragment)
      end
    end)
  end

  defp combine_fragments_with_or(fragments) do
    conditions = false

    Enum.reduce(fragments, conditions, fn fragment, conditions ->
      if !conditions do
        dynamic([q], ^fragment)
      else
        dynamic([q], ^conditions or ^fragment)
      end
    end)
  end

  defp apply_filters(query, config, filters) do
    filter =
      Enum.map(filters, fn f ->
        filters_recurse(config, f)
      end)
      |> combine_fragments_with_and()

    query |> where(^filter)
  end

  defp filters_recurse(config, {:or, filters}) do
    Enum.map(filters, fn f ->
      filters_recurse(config, f)
    end)
    |> combine_fragments_with_or()
  end

  defp filters_recurse(config, {:and, filters}) do
    Enum.map(filters, fn f ->
      filters_recurse(config, f)
    end)
    |> combine_fragments_with_and()
  end

  ### TODO add :not

  defp filters_recurse(config, {name, val}) do
    def = config.columns[name]
    table = def.requires_join
    field = def.field

    ### how to allow function calls/subqueries in field and val?
    case val do
      x when is_nil(x) ->
        dynamic([{^table, a}], is_nil(field(a, ^field)))

      x when is_bitstring(x) or is_number(x) or is_boolean(x) ->
        dynamic([{^table, a}], field(a, ^field) == ^val)

      x when is_list(x) ->
        dynamic([{^table, a}], field(a, ^field) in ^val)

      # TODO not-in

      # sucks to not be able to do these 6 in one with a fragment!
      {x, v} when x == "!=" ->
        dynamic([{^table, a}], field(a, ^field) != ^v)

      {x, v} when x == "<" ->
        dynamic([{^table, a}], field(a, ^field) < ^v)

      {x, v} when x == ">" ->
        dynamic([{^table, a}], field(a, ^field) > ^v)

      {x, v} when x == "<=" ->
        dynamic([{^table, a}], field(a, ^field) <= ^v)

      {x, v} when x == ">=" ->
        dynamic([{^table, a}], field(a, ^field) >= ^v)

      {:between, min, max} ->
        dynamic([{^table, a}], fragment("? between ? and ?", field(a, ^field), ^min, ^max))

      :not_true ->
        dynamic([{^table, a}], not field(a, ^field))

      {:like, v} ->
          dynamic([{^table, a}], like(field(a, ^field), ^v))

      {:ilike, v} ->
        dynamic([{^table, a}], ilike(field(a, ^field), ^v))

      _ -> raise "Filter Recurse not implemented for #{inspect(val)}"
        # date shortcuts (:today, :tomorrow, :last_week, etc )

        # {:case, %{filter=>}}
        # {:exists, etc-, subq} # how to do subq????
    end
  end

  # Can only give us the joins.. make this recurse and handle :or, :and, etc
  defp joins_from_filters(config, filters) do
    filters
    |> Enum.reduce(%{}, fn
      {:or, list}, acc ->
        Map.merge(
          acc,
          Enum.reduce(joins_from_filters(config, list), %{}, fn i, acc -> Map.put(acc, i, 1) end)
        )

      {fil, _val}, acc ->
        Map.put(acc, config.columns[fil].requires_join, 1)
    end)
    |> Map.keys()
  end

  @doc """
    Add to the Order By
  """
  def order_by(selecto, orders) do
    put_in(selecto.set.order_by, selecto.set.order_by ++ orders)
  end

  defp apply_order_by(query, config, order_bys) do
    order_bys =
      order_bys
      |> Enum.map(fn
        {dir, field} -> {dir, field}
        field -> {:asc_nulls_first, field}
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

  def group_by(selecto, groups) do
    put_in(selecto.set.group_by, selecto.set.group_by ++ groups)
  end



 # From Ecto.OLAP Copyright (c) 2017 Łukasz Jan Niemier THANKS!
  defmacro rollup(columns), do: mkquery(columns, "ROLLUP")

  defp mkquery(data, name) do
    quote do: fragment(unquote(name <> " ?"), unquote(fragment_list(data)))
  end

  defp fragment_list(list) when is_list(list) do
    query = "?" |> List.duplicate(Enum.count(list)) |> Enum.join(",")
    quote do: fragment(unquote("(" <> query <> ")"), unquote_splicing(list))
  end
  ###

  defp apply_group_by(query, _config, [], _) do
    query
  end


  defp recurse_group_by(config, group_by) do
    IO.inspect(group_by)
    case group_by do
      {:extract, field, format} ->
        check_string(format)
        dynamic(
          [{^config.columns[field].requires_join, owner}],
          fragment(
            "extract( ? from ? )",
            literal(^format),
            field(owner, ^config.columns[field].field)
          )
        )
      {:rollup, [a]} ->
        dynamic([], rollup( [^recurse_group_by(config, a)] ) )

      {:rollup, [a, b]} ->
        dynamic([], rollup( [^recurse_group_by(config, a), ^recurse_group_by(config, b) ] ) )

      {:rollup, [a, b, c]} ->
        dynamic([], rollup( [^recurse_group_by(config, a), ^recurse_group_by(config, b),
        ^recurse_group_by(config, c) ] ) )

      {:rollup, [a, b, c, d]} ->
        dynamic([], rollup( [^recurse_group_by(config, a), ^recurse_group_by(config, b),
        ^recurse_group_by(config, c), ^recurse_group_by(config, d) ] ) )


      field ->
        dynamic(
          [{^config.columns[field].requires_join, owner}],
          field(owner, ^config.columns[field].field)
        )
    end

  end

  defp apply_group_by(query, config, group_bys, mode) do
    group_bys =
      group_bys |> Enum.map(fn g -> recurse_group_by(config, g) end)
    case mode do
      _ -> from(query, group_by: ^group_bys )

    end
  end

  @doc """
    Returns an Ecto.Query with all your filters and selections added..eventually!
  """
  def gen_query(selecto, opts \\ []) do
    IO.puts("Gen Query")

    {group_by_type, opts} = Keyword.pop(opts, :group_by_type, :group)

    joins_from_selects = joins_from_selects(selecto.config.columns, selecto.set.selected)
    filters_to_use = Map.get(selecto.domain, :required_filters, []) ++ selecto.set.filtered
    filtered_by_join = joins_from_filters(selecto.config, filters_to_use)

    joins_from_order_by =
      joins_from_selects(
        selecto.config.columns,
        Enum.map(selecto.set.order_by, fn
          {_dir, field} -> field
          field -> field
        end)
      )

    joins_from_group_by = joins_from_selects(selecto.config.columns, selecto.set.group_by)

    ## We select nothing from the initial query because we are going to select_merge everything and
    ## if we don't select empty map here, it will include the full * of our source!
    query = from(root in selecto.domain.source, as: :selecto_root, select: %{})

    ##### If we are GROUP BY and have AGGREGATES that live on a join path with any :many
    ##### cardinality we have to force the aggregates to subquery

    {query, aliases} =
      get_join_order(
        selecto.config.joins,
        Enum.uniq(
          joins_from_selects ++ filtered_by_join ++ joins_from_order_by ++ joins_from_group_by
        )
      )
      |> Enum.reduce(query, fn j, acc -> apply_join(selecto.config, acc, j) end)
      |> apply_selections(selecto.config, selecto.set.selected)

    IO.inspect(query, label: "Second Last")

    query =
      query
      |> apply_filters(selecto.config, filters_to_use)
      |> apply_group_by(selecto.config, selecto.set.group_by, group_by_type)
      |> apply_order_by(selecto.config, selecto.set.order_by)

    IO.inspect(query, label: "Last")

    {query, aliases}
  end

  def gen_sql(selecto) do

  end

  # apply the join to the query
  # we don't need to join root!
  defp apply_join(_config, query, :selecto_root) do
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

  @doc """
    Generate and run the query, returning list of maps (for now...)
  """
  def execute(selecto, opts \\ []) do
    IO.puts("Execute Query")

    {query, aliases} =
      selecto
      |> gen_query(opts)

    IO.inspect(query, label: "Exe")

    results =
      query
      |> selecto.repo.all()
      |> IO.inspect(label: "Results")

    {results, aliases}
  end

  def available_columns(selecto) do
    selecto.config.columns
  end

  def available_filters(selecto) do
    selecto.config.filters
  end
end
