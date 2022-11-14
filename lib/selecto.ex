defmodule Selecto do
  defstruct [:repo, :domain, :config, :set]

  import Ecto.Query
  import Selecto.Helpers

  alias Selecto.Builder.Ecto.{Select, Join, Filter, Group}


  @moduledoc """
  Documentation for `Selecto,` a query writer and report generator for Elixir/Ecto

    TODO

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

    subqueries

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

    ### Extra filters (all normal fields can be a filter) These are custom, which is really passed into Selecto Components to deal with
    filters = Map.get(domain, :filters, %{})

    filters =
      Enum.reduce(
        Map.values(joins),
        filters,
        fn e, acc ->
          Map.merge(Map.get(e, :filters, %{}), acc)
        end
      ) |> Enum.map(fn {f, v} -> {f, Map.put(v, :id, f)} end)
      |> Enum.into(%{})

    %{
      primary_key: primary_key,
      columns: fields,
      joins: joins,
      filters: filters,
      domain_data: Map.get(domain, :domain_data)
    }
  end

  @doc """
    add a field to the Select list. Send in one or a list of field names or selectable tuples
    TODO allow to send single, and special forms..
  """
  def select(selecto, fields) when is_list(fields) do
    put_in(selecto.set.selected, Enum.uniq(selecto.set.selected ++ fields))
  end

  def select(selecto, field) do
    Selecto.select(selecto, [field])
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

  @doc """
    Add to the Order By
  """
  def order_by(selecto, orders) when is_list(orders) do
    put_in(selecto.set.order_by, selecto.set.order_by ++ orders)
  end
  def order_by(selecto, orders) do
    put_in(selecto.set.order_by, selecto.set.order_by ++ [orders])
  end

  @doc """
    Add to the Group By
  """
  def group_by(selecto, groups) do
    put_in(selecto.set.group_by, selecto.set.group_by ++ groups)
  end


  @doc """
    Returns an Ecto.Query with all your filters and selections added..eventually!
  """
  def gen_query(selecto, opts \\ []) do
    #IO.puts("Gen Query")

    {results_type, opts} = Keyword.pop(opts, :results_type, :maps)


    from_selects = Join.from_selects(selecto.config.columns, selecto.set.selected)
    filters_to_use = Map.get(selecto.domain, :required_filters, []) ++ selecto.set.filtered
    filtered_by_join = Join.from_filters(selecto.config, filters_to_use)

    joins_from_order_by =
      Join.from_selects(
        selecto.config.columns,
        Enum.map(selecto.set.order_by, fn
          {_dir, field} -> field
          field -> field
        end)
      )

    joins_from_group_by = Join.from_selects(selecto.config.columns, selecto.set.group_by)

    ## We select nothing from the initial query because we are going to select_merge everything and
    ## if we don't select empty map here, it will include the full * of our source!
    query = from(root in selecto.domain.source, as: :selecto_root, select: %{})

    ##### If we are GROUP BY and have AGGREGATES that live on a join path with any :many
    ##### cardinality we have to force the aggregates to subquery

    {query, aliases} =
      Join.get_join_order(
        selecto.config.joins,
        Enum.uniq(
          from_selects ++ filtered_by_join ++ joins_from_order_by ++ joins_from_group_by
        )
      )
      |> Enum.reduce(query, fn j, acc -> apply_join(selecto.config, acc, j) end)
      |> Select.apply_selections(selecto.config, selecto.set.selected)

    #IO.inspect(query, label: "Second Last")

    query =
      query
      |> Filter.apply_filters(selecto.config, filters_to_use)
      |> Group.apply_group_by(selecto.config, selecto.set.group_by)
      |> Select.apply_order_by(selecto.config, selecto.set.order_by)

    #IO.inspect(query, label: "Last")

    {query, aliases}
  end

  def gen_sql(selecto) do
    #todo!
  end

  # apply the join to the query
  # we don't need to join root!
  defp apply_join(_config, query, :selecto_root) do
    query
  end

  defp apply_join(config, query, join) do

    join_map = config.joins[join]

    case join_map do
      # %{ through_path: path } ->
      #   IO.inspect(path)
      #   query
      _ ->
        from({^join_map.requires_join, par} in query,
          left_join: b in ^join_map.i_am,
          as: ^join,
          on: field(par, ^join_map.owner_key) == field(b, ^join_map.my_key)
        )
    end
  end



  @doc """
    Generate and run the query, returning list of maps (for now...)
  """
  def execute(selecto, opts \\ []) do
    #IO.puts("Execute Query")

    {query, aliases} =
      selecto
      |> gen_query(opts)

    #IO.inspect(query, label: "Exe")

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
