defmodule Selecto do
  defstruct [:repo, :domain, :config, :set]

  import Ecto.Query
  import Selecto.Helpers

  @moduledoc """

  Documentation for `Selecto,` a query writer and report generator for Elixir/Ecto

  """

  @doc """
    Generate a selecto structure from this Repo following
    the instructions in Domain map
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
      source: source,
      source_table: source.__schema__(:source),
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
  def group_by(selecto, groups) when is_list(groups) do
    put_in(selecto.set.group_by, selecto.set.group_by ++ groups)
  end
  def group_by(selecto, groups) do
    put_in(selecto.set.group_by, selecto.set.group_by ++ [groups])
  end


  def gen_sql(selecto) do
    #todo!
  end




  @doc """
    Generate and run the query, returning list of lists, db produces column headers, and provides aliases
  """
  def execute(selecto, opts \\ []) do
    #IO.puts("Execute Query")

    {query, aliases, params} = Selecto.Builder.Sql.build(selecto, opts)
    #IO.inspect(query, label: "Exe")

    {:ok, result} = Ecto.Adapters.SQL.query(selecto.repo, query, params)
#      |> IO.inspect(label: "Results")

    {result.rows, result.columns, aliases}
  end

end
