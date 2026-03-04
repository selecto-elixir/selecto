defmodule Selecto.ConnectionPool.Runtime do
  @moduledoc false

  use Supervisor

  @registry Selecto.ConnectionPool.Registry
  @manager_supervisor Selecto.ConnectionPool.ManagerSupervisor

  @spec ensure_started() :: {:ok, pid()} | {:error, term()}
  def ensure_started do
    case Process.whereis(__MODULE__) do
      pid when is_pid(pid) ->
        {:ok, pid}

      nil ->
        case Supervisor.start_link(__MODULE__, [], name: __MODULE__) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          {:error, _} = error -> error
        end
    end
  end

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: @registry},
      {DynamicSupervisor, strategy: :one_for_one, name: @manager_supervisor}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
