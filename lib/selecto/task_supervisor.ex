defmodule Selecto.TaskSupervisor do
  @moduledoc false

  @name __MODULE__

  @spec ensure_started() :: {:ok, pid()} | {:error, term()}
  def ensure_started do
    case Process.whereis(@name) do
      pid when is_pid(pid) ->
        {:ok, pid}

      nil ->
        {:error, :not_started}
    end
  end

  @spec async((-> term())) :: Task.t()
  def async(fun) when is_function(fun, 0) do
    case ensure_started() do
      {:ok, _pid} ->
        Task.Supervisor.async_nolink(@name, fun)

      {:error, reason} ->
        raise RuntimeError, "Selecto task supervisor is unavailable: #{inspect(reason)}"
    end
  end

  @spec start_child((-> term())) :: {:ok, pid()} | {:error, term()}
  def start_child(fun) when is_function(fun, 0) do
    case ensure_started() do
      {:ok, _pid} -> Task.Supervisor.start_child(@name, fun)
      {:error, _reason} = error -> error
    end
  end
end
