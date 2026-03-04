defmodule Selecto.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Task.Supervisor, name: Selecto.TaskSupervisor},
      {Selecto.ConnectionPool.Runtime, []}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Selecto.Supervisor)
  end
end
