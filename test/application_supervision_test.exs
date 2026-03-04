defmodule Selecto.ApplicationSupervisionTest do
  use ExUnit.Case, async: true

  test "application boots core runtime supervisors" do
    assert is_pid(Process.whereis(Selecto.Supervisor))
    assert is_pid(Process.whereis(Selecto.TaskSupervisor))
    assert is_pid(Process.whereis(Selecto.ConnectionPool.Runtime))
    assert is_pid(Process.whereis(Selecto.ConnectionPool.Registry))
    assert is_pid(Process.whereis(Selecto.ConnectionPool.ManagerSupervisor))
  end

  test "runtime ensure_started helpers return active pids" do
    assert {:ok, task_pid} = Selecto.TaskSupervisor.ensure_started()
    assert task_pid == Process.whereis(Selecto.TaskSupervisor)

    assert {:ok, runtime_pid} = Selecto.ConnectionPool.Runtime.ensure_started()
    assert runtime_pid == Process.whereis(Selecto.ConnectionPool.Runtime)
  end
end
