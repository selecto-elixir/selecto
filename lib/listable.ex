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
    List.flatten(normalize_joins(source, joins, nil))

  end

  defp walk_config(%{source: source} = domain) do
    primary_key = source.__schema__(:primary_key)
    fields = walk_fields(nil, source.__schema__(:fields) -- source.__schema__(:redact_fields), source)

    joins = recurse_joins(source, domain.joins)

    %{
      primary_key: primary_key,
      columns: fields,
      joins: joins
    }
    |> flatten_config( )
    |> IO.inspect
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

    selections = listable.set.selected

    query = from root in listable.domain.source,
      select: map( root, ^selections )
    query |> IO.inspect
  end

  def execute( listable ) do
    IO.puts("Execute Query")
    listable
      |> gen_query
      |> listable.repo.all()
      |> IO.inspect()
  end

end
