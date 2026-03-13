defmodule Selecto.Diagnostics do
  @moduledoc """
  Query diagnostics helpers (EXPLAIN / EXPLAIN ANALYZE).
  """

  alias Selecto.Error

  @type explain_result :: %{
          explain_sql: String.t(),
          query_sql: String.t(),
          params: list(),
          columns: [String.t()],
          rows: list(),
          plan_lines: [String.t()]
        }

  @doc """
  Run `EXPLAIN` for a Selecto query.
  """
  @spec explain(Selecto.t(), keyword()) :: {:ok, explain_result()} | {:error, Selecto.Error.t()}
  def explain(selecto, opts \\ []) do
    run_explain(selecto, Keyword.put_new(opts, :analyze, false))
  end

  @doc """
  Run `EXPLAIN ANALYZE` for a Selecto query.
  """
  @spec explain_analyze(Selecto.t(), keyword()) ::
          {:ok, explain_result()} | {:error, Selecto.Error.t()}
  def explain_analyze(selecto, opts \\ []) do
    run_explain(selecto, Keyword.put(opts, :analyze, true))
  end

  @doc """
  Build the explain SQL wrapper.
  """
  @spec build_explain_sql(String.t(), keyword()) :: String.t()
  def build_explain_sql(query_sql, opts \\ []) when is_binary(query_sql) do
    flags =
      []
      |> maybe_true_flag("ANALYZE", Keyword.get(opts, :analyze, false))
      |> maybe_true_flag("VERBOSE", Keyword.get(opts, :verbose, false))
      |> maybe_true_flag("BUFFERS", Keyword.get(opts, :buffers, false))
      |> maybe_true_flag("SETTINGS", Keyword.get(opts, :settings, false))
      |> maybe_true_flag("WAL", Keyword.get(opts, :wal, false))
      |> maybe_false_flag("TIMING", Keyword.get(opts, :timing, true))
      |> maybe_false_flag("COSTS", Keyword.get(opts, :costs, true))
      |> maybe_false_flag("SUMMARY", Keyword.get(opts, :summary, true))
      |> maybe_format_flag(Keyword.get(opts, :format))

    prefix =
      case flags do
        [] -> "EXPLAIN"
        _ -> "EXPLAIN (#{Enum.join(flags, ", ")})"
      end

    "#{prefix} #{query_sql}"
  end

  defp run_explain(selecto, opts) do
    to_sql_opts = Keyword.get(opts, :to_sql_opts, [])
    {query_sql, params} = Selecto.to_sql(selecto, to_sql_opts)
    explain_sql = build_explain_sql(query_sql, opts)

    case execute_raw(selecto, explain_sql, params) do
      {:ok, rows, columns} ->
        plan_lines =
          rows
          |> Enum.map(fn
            [line | _] when is_binary(line) -> line
            other -> inspect(other)
          end)

        {:ok,
         %{
           explain_sql: explain_sql,
           query_sql: query_sql,
           params: params,
           columns: columns,
           rows: rows,
           plan_lines: plan_lines
         }}

      {:error, %Error{} = error} ->
        {:error, error}

      {:error, other} ->
        {:error, Error.from_reason(other)}
    end
  end

  defp execute_raw(selecto, sql, params) do
    adapter = runtime_adapter(selecto)
    connection = runtime_connection(selecto)

    cond do
      Selecto.AdapterSupport.callback_available?(adapter, :execute_raw, 3) ->
        case Kernel.apply(adapter, :execute_raw, [connection, sql, params]) do
          {:ok, result} ->
            {:ok, Map.get(result, :rows, []), Map.get(result, :columns, [])}

          {:error, reason} ->
            {:error, Error.from_reason(reason)}
        end

      selecto.adapter && Selecto.AdapterSupport.callback_available?(adapter, :execute, 4) ->
        case Kernel.apply(adapter, :execute, [connection, sql, params, []]) do
          {:ok, result} ->
            {:ok, Map.get(result, :rows, []), Map.get(result, :columns, [])}

          {:error, reason} ->
            {:error, Error.from_reason(reason)}
        end

      true ->
        {:error,
         Error.connection_error("Raw execution is unavailable for adapter", %{adapter: adapter})}
    end
  rescue
    e ->
      {:error, Error.from_reason(e)}
  end

  defp runtime_connection(%{connection: nil, postgrex_opts: postgrex_opts}), do: postgrex_opts
  defp runtime_connection(%{connection: connection}), do: connection
  defp runtime_connection(%{postgrex_opts: postgrex_opts}), do: postgrex_opts
  defp runtime_connection(_), do: nil

  defp runtime_adapter(%{adapter: nil}), do: Selecto.AdapterSupport.default_adapter()
  defp runtime_adapter(%{adapter: adapter}) when not is_nil(adapter), do: adapter
  defp runtime_adapter(_), do: Selecto.AdapterSupport.default_adapter()

  defp maybe_true_flag(flags, _name, nil), do: flags
  defp maybe_true_flag(flags, _name, false), do: flags
  defp maybe_true_flag(flags, name, true), do: flags ++ [name]

  defp maybe_false_flag(flags, _name, nil), do: flags
  defp maybe_false_flag(flags, _name, true), do: flags
  defp maybe_false_flag(flags, name, false), do: flags ++ ["#{name} false"]

  defp maybe_format_flag(flags, nil), do: flags

  defp maybe_format_flag(flags, format) when format in [:text, :json, :yaml, :xml] do
    flags ++ ["FORMAT #{String.upcase(to_string(format))}"]
  end

  defp maybe_format_flag(flags, _), do: flags
end
