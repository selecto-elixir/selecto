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
        selected: [],
        filtered: [],
        order_by: [],
        #group_by: [],
      }
    }
  end



  defp walk_config(%{source: source} = _domain) do
    primary_key = source.__schema__(:primary_key)
    fields = walk_fields(:root, source.__schema__(:fields), source)
    IO.inspect(primary_key)

    %{
      primary_key: primary_key,
      columns: fields

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


  end

  def execute( listable ) do
    listable |> gen_query |> listable.repo.all()
  end

end
