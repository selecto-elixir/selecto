defmodule Mix.Tasks.Selecto.Domain.Compat do
  use Mix.Task

  @shortdoc "Checks canonical Selecto domain compatibility artifacts"
  @moduledoc """
  Validates the canonical example domains against Selecto's package-neutral
  compatibility matrix.

  This task does not connect to a database. It normalizes the canonical domains,
  builds JSON-safe query-contract artifacts, and verifies their semantic ids.

      mix selecto.domain.compat
  """

  @impl Mix.Task
  def run(_args) do
    case Selecto.Domain.Examples.Compatibility.check() do
      :ok ->
        Mix.shell().info("Selecto domain compatibility check passed")

      {:error, errors} ->
        Mix.shell().error("Selecto domain compatibility check failed")

        Enum.each(errors, fn error ->
          Mix.shell().error(
            "#{error.domain}.#{error.key}: expected #{inspect(error.expected)}, got #{inspect(error.actual)}"
          )
        end)

        Mix.raise("domain compatibility check failed")
    end
  end
end
