defmodule Selecto.TaskSupervisor do
  @moduledoc false

  @name __MODULE__

  @spec ensure_started() :: {:ok, pid()} | {:error, term()}
  def ensure_started do
    case Process.whereis(@name) do
      pid when is_pid(pid) ->
        {:ok, pid}

      nil ->
        case Task.Supervisor.start_link(name: @name) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          {:error, _} = error -> error
        end
    end
  end

  @spec async((-> term())) :: Task.t()
  def async(fun) when is_function(fun, 0) do
    case ensure_started() do
      {:ok, _pid} -> Task.Supervisor.async_nolink(@name, fun)
      {:error, _reason} -> Task.async(fun)
    end
  end

  @spec start_child((-> term())) :: {:ok, pid()} | {:error, term()}
  def start_child(fun) when is_function(fun, 0) do
    case ensure_started() do
      {:ok, _pid} -> Task.Supervisor.start_child(@name, fun)
      {:error, _reason} -> {:ok, spawn(fun)}
    end
  end
end
