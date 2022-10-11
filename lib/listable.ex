defmodule Listable do

  import Ecto.Query


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
    %{
      repo: repo,
      domain: domain
    }
  end

end
