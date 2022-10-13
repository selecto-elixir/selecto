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
    q = from u in "users", select: %{id: u.id}
    q |> repo.all() |> IO.inspect()
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

  defp configure_join(assoc, dep \\ nil) do
    %{
      assoc: assoc,
      requires_join: dep,
    }
  end
  ### This is f'n weird, fix it TODO allow user: [:profiles] !
  defp normalize_joins([assoc, subs | joins ], dep ) when is_atom(assoc) and is_list(subs) do
    [configure_join(assoc, dep), normalize_joins(subs, assoc)] ++ normalize_joins(joins, dep)
  end
  defp normalize_joins([assoc, subs ], dep ) when is_atom(assoc) and is_list(subs) do
    [configure_join(assoc, dep), normalize_joins(subs, assoc)]
  end
  defp normalize_joins([assoc | joins ], dep ) when is_atom(assoc)  do
    [configure_join(assoc, dep)] ++ normalize_joins(joins, dep)
  end
  defp normalize_joins([assoc], dep) when is_atom(assoc)  do
    [configure_join(assoc, dep)]
  end

  defp normalize_joins([] = _joins, _) do
    []
  end
  defp normalize_joins(nil, _) do
    []
  end


  defp recurse_joins(_source, joins) do
    List.flatten(normalize_joins(joins, :root)) |> IO.inspect()

  end

  defp walk_config(%{source: source} = domain) do
    primary_key = source.__schema__(:primary_key)
    fields = walk_fields(:root, source.__schema__(:fields), source)

    IO.inspect(primary_key)

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
    Enum.map( fields, &Column.configure(&1, join, source) )
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

  # Example config
  defp listable_domain(event) do
    %{
      source: EventSystems.Registration.Attendee,
      joins: [
        :registrations,
        :stub,
        :package,
        :group,
        user: [:profile],
      ],
      filters: [
        :"registrations.event_id", event.id
      ],
      selected: [ :id, :registrations_id ]
    }
  end


end
