defmodule Listable do
  defstruct [:repo, :domain, :config]

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
      config: walk_config(domain)
    }
  end



  defp walk_config(%{source: source} = _domain) do
    primary_key = source.__schema__(:primary_key)
    fields = walk_fields(:root, source.__schema__(:fields), source)
    IO.inspect(primary_key)


    flatten_config(
      %{
        primary_key: primary_key,
        columns: fields

      }
    ) |> IO.inspect
  end

  defp walk_fields(join, fields, source) do
    Enum.map( fields, &Column.configure(&1, join, source) )
    |> Map.new()
  end

  ### Put filters/columns/joins in one level with join meta
  defp flatten_config(config) do
    config
  end

end
