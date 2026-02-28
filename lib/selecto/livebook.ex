defmodule Selecto.Livebook do
  @moduledoc """
  Convenience helpers for interactive notebooks and demos.
  """

  @type run_result :: {:ok, term()} | {:error, term()}

  @doc """
  Print generated SQL and parameters.
  """
  @spec explain(String.t(), Selecto.t(), keyword()) :: {String.t(), list()}
  def explain(label, selecto, opts \\ []) do
    to_sql_opts = Keyword.get(opts, :to_sql_opts, pretty: true)
    {sql, params} = Selecto.to_sql(selecto, to_sql_opts)

    IO.puts("\n=== #{label} ===")
    IO.puts(String.trim(sql))
    IO.puts("Params: #{inspect(params)}")

    {sql, params}
  end

  @doc """
  Print SQL and execute query with preview information.
  """
  @spec run(String.t(), Selecto.t(), keyword()) :: run_result()
  def run(label, selecto, opts \\ []) do
    explain(label, selecto, opts)
    preview_count = Keyword.get(opts, :preview_count, 10)
    execute_opts = Keyword.get(opts, :execute_opts, [])

    case Selecto.execute(selecto, execute_opts) do
      {:ok, {rows, columns, aliases}} = ok ->
        IO.puts("Rows: #{length(rows)}")
        IO.puts("Columns: #{inspect(columns)}")
        IO.puts("Aliases: #{inspect(aliases)}")
        IO.inspect(Enum.take(rows, preview_count), label: "Preview (up to #{preview_count} rows)")
        ok

      {:ok, other} = ok ->
        IO.inspect(other, label: "Result")
        ok

      {:error, _} = error ->
        IO.inspect(error, label: "Execution error")
        error
    end
  end

  @doc """
  Print SQL and execute shaped query with preview information.
  """
  @spec run_shape(String.t(), Selecto.t(), keyword()) :: run_result()
  def run_shape(label, selecto, opts \\ []) do
    explain(label, selecto, opts)
    preview_count = Keyword.get(opts, :preview_count, 10)

    case Selecto.execute_shape(selecto) do
      {:ok, shaped_rows} = ok ->
        IO.puts("Shaped rows: #{length(shaped_rows)}")

        IO.inspect(Enum.take(shaped_rows, preview_count),
          label: "Shape preview (up to #{preview_count} rows)"
        )

        ok

      {:error, _} = error ->
        IO.inspect(error, label: "Shape execution error")
        error
    end
  end
end
