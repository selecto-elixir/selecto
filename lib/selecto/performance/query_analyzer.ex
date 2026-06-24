defmodule Selecto.Performance.QueryAnalyzer do
  @moduledoc """
  Query analysis and optimization using adapter-backed EXPLAIN support.

  Provides deep insights into query performance by:
  - Running EXPLAIN ANALYZE on queries
  - Parsing execution plans
  - Identifying performance bottlenecks
  - Suggesting optimizations
  - Tracking index usage
  """

  @doc """
  Analyze a query using EXPLAIN ANALYZE and return detailed performance information.

  ## Options

  - `:format` - Output format (:text, :json, :xml, :yaml). Default: :json
  - `:analyze` - Actually run the query (default: true)
  - `:buffers` - Include buffer usage (default: true)
  - `:verbose` - Include verbose output (default: false)
  - `:timing` - Include timing information (default: true)
  - `:summary` - Include summary (default: true)
  - `:settings` - Include settings (default: false)

  ## Examples

      iex> QueryAnalyzer.analyze_query(selecto)
      {:ok, %{
        execution_time: 45.2,
        planning_time: 0.8,
        plan: %{...},
        suggestions: [...]
      }}
  """
  def analyze_query(selecto, options \\ []) do
    format = Keyword.get(options, :format, :json)

    with {:ok, explain_query} <- build_explain_query(selecto, options),
         {:ok, result} <- execute_explain(selecto, explain_query),
         {:ok, parsed_plan} <- parse_explain_result(result, format),
         {:ok, analysis} <- analyze_plan(parsed_plan) do
      # Record metrics if collector is running
      if Process.whereis(Selecto.Performance.MetricsCollector) do
        Selecto.Performance.MetricsCollector.record_query(
          generate_analysis_id(),
          Map.merge(analysis, %{type: :explain_analyze})
        )
      end

      {:ok, analysis}
    end
  end

  @doc """
  Get query plan without executing (EXPLAIN only, no ANALYZE).
  """
  def get_query_plan(selecto, options \\ []) do
    options = Keyword.put(options, :analyze, false)
    analyze_query(selecto, options)
  end

  @doc """
  Compare performance of two queries.
  """
  def compare_queries(selecto1, selecto2, options \\ []) do
    with {:ok, analysis1} <- analyze_query(selecto1, options),
         {:ok, analysis2} <- analyze_query(selecto2, options) do
      comparison = %{
        query1: analysis1,
        query2: analysis2,
        performance_diff: %{
          execution_time: analysis2.execution_time - analysis1.execution_time,
          planning_time: analysis2.planning_time - analysis1.planning_time,
          total_cost: analysis2.total_cost - analysis1.total_cost,
          rows_diff: analysis2.actual_rows - analysis1.actual_rows
        },
        recommendations: generate_comparison_recommendations(analysis1, analysis2)
      }

      {:ok, comparison}
    end
  end

  @doc """
  Analyze index usage for a query.
  """
  def analyze_index_usage(selecto, options \\ []) do
    with {:ok, analysis} <- analyze_query(selecto, options) do
      index_info = extract_index_usage(analysis.plan)

      {:ok,
       %{
         indexes_used: index_info.used,
         indexes_missing: index_info.missing,
         index_scans: index_info.scans,
         sequential_scans: index_info.seq_scans,
         recommendations: generate_index_recommendations(index_info)
       }}
    end
  end

  @doc """
  Get statistics for tables used in a query.
  """
  def get_table_statistics(selecto) do
    tables = extract_tables_from_query(selecto)

    stats =
      Enum.map(tables, fn table ->
        with {:ok, stats} <-
               get_table_stats(runtime_adapter(selecto), runtime_connection(selecto), table) do
          {table, stats}
        else
          _ -> {table, %{error: "Could not fetch statistics"}}
        end
      end)

    {:ok, Map.new(stats)}
  end

  # Private functions

  defp build_explain_query(selecto, options) do
    {sql, params} = Selecto.to_sql(selecto)

    # Build EXPLAIN options
    explain_opts = build_explain_options(options)
    explain_query = "EXPLAIN #{explain_opts} #{sql}"

    {:ok, {explain_query, params}}
  end

  defp build_explain_options(options) do
    format = normalize_explain_format(Keyword.get(options, :format, :json))

    opts = []
    opts = if Keyword.get(options, :analyze, true), do: ["ANALYZE" | opts], else: opts
    opts = if Keyword.get(options, :buffers, true), do: ["BUFFERS" | opts], else: opts
    opts = if Keyword.get(options, :verbose, false), do: ["VERBOSE" | opts], else: opts
    opts = if Keyword.get(options, :costs, true), do: ["COSTS" | opts], else: opts
    opts = if Keyword.get(options, :timing, true), do: ["TIMING" | opts], else: opts
    opts = if Keyword.get(options, :summary, true), do: ["SUMMARY" | opts], else: opts
    opts = if Keyword.get(options, :settings, false), do: ["SETTINGS" | opts], else: opts

    format_opt = "FORMAT #{String.upcase(to_string(format))}"

    if opts == [] do
      "(#{format_opt})"
    else
      "(#{Enum.join(opts, ", ")}, #{format_opt})"
    end
  end

  defp normalize_explain_format(format) when format in [:text, :json, :xml, :yaml], do: format

  defp normalize_explain_format(format) when is_binary(format) do
    format
    |> String.trim()
    |> String.downcase()
    |> case do
      "text" -> :text
      "json" -> :json
      "xml" -> :xml
      "yaml" -> :yaml
      _ -> raise ArgumentError, "invalid EXPLAIN format #{inspect(format)}"
    end
  end

  defp normalize_explain_format(format),
    do: raise(ArgumentError, "invalid EXPLAIN format #{inspect(format)}")

  defp execute_explain(selecto, {query, params}) do
    adapter = runtime_adapter(selecto)
    connection = runtime_connection(selecto)

    try do
      cond do
        invalid_runtime_connection?(connection) ->
          {:error, :invalid_connection}

        Selecto.AdapterSupport.callback_available?(adapter, :execute_raw, 3) ->
          case Kernel.apply(adapter, :execute_raw, [connection, query, params]) do
            {:ok, result} -> {:ok, result}
            {:error, reason} -> {:error, {:explain_failed, reason}}
          end

        true ->
          {:error, :invalid_connection}
      end
    rescue
      error -> {:error, {:explain_failed, error}}
    catch
      kind, reason -> {:error, {:explain_failed, {kind, reason}}}
    end
  end

  defp parse_explain_result(%{rows: rows}, :json) do
    case rows do
      [[json_string]] when is_binary(json_string) ->
        case Jason.decode(json_string) do
          {:ok, [plan_data | _]} -> {:ok, plan_data}
          {:error, reason} -> {:error, {:parse_failed, reason}}
        end

      [[plan_data]] when is_map(plan_data) ->
        {:ok, plan_data}

      _ ->
        {:error, :unexpected_explain_format}
    end
  end

  defp parse_explain_result(%{rows: rows}, :text) do
    # Parse text format EXPLAIN output
    text = rows |> List.flatten() |> Enum.join("\n")
    {:ok, parse_text_explain(text)}
  end

  defp parse_text_explain(text) do
    lines = String.split(text, "\n")

    # Extract key metrics from text output
    execution_time = extract_metric(lines, ~r/Execution Time: ([\d.]+) ms/)
    planning_time = extract_metric(lines, ~r/Planning Time: ([\d.]+) ms/)

    %{
      format: :text,
      raw_output: text,
      execution_time: execution_time,
      planning_time: planning_time
    }
  end

  defp extract_metric(lines, regex) do
    Enum.find_value(lines, fn line ->
      case Regex.run(regex, line) do
        [_, value] -> String.to_float(value)
        _ -> nil
      end
    end)
  end

  defp analyze_plan(plan_data) when is_map(plan_data) do
    analysis = %{
      execution_time: plan_data["Execution Time"] || plan_data["Total Runtime"] || 0,
      planning_time: plan_data["Planning Time"] || 0,
      plan: plan_data["Plan"] || plan_data,
      triggers: plan_data["Triggers"] || [],
      jit: plan_data["JIT"] || %{},
      settings: plan_data["Settings"] || %{}
    }

    # Extract detailed metrics from plan
    plan_metrics = analyze_plan_node(analysis.plan)

    # Generate optimization suggestions
    suggestions = generate_suggestions(plan_metrics)

    final_analysis =
      Map.merge(analysis, %{
        total_cost: plan_metrics.total_cost,
        actual_rows: plan_metrics.actual_rows,
        plan_rows: plan_metrics.plan_rows,
        row_estimation_accuracy: calculate_estimation_accuracy(plan_metrics),
        node_types: plan_metrics.node_types,
        slow_nodes: plan_metrics.slow_nodes,
        suggestions: suggestions
      })

    {:ok, final_analysis}
  end

  defp analyze_plan_node(nil),
    do: %{
      total_cost: 0,
      actual_rows: 0,
      plan_rows: 0,
      node_types: [],
      slow_nodes: []
    }

  defp analyze_plan_node(node) when is_map(node) do
    node_type = node["Node Type"] || "Unknown"

    metrics = %{
      total_cost: node["Total Cost"] || 0,
      actual_rows: node["Actual Rows"] || 0,
      plan_rows: node["Plan Rows"] || 0,
      actual_time: node["Actual Total Time"] || 0,
      loops: node["Actual Loops"] || 1,
      node_types: [node_type],
      slow_nodes: []
    }

    # Check if this node is slow
    metrics =
      if metrics.actual_time > 10.0 do
        %{
          metrics
          | slow_nodes: [
              %{
                type: node_type,
                time: metrics.actual_time,
                rows: metrics.actual_rows,
                relation: node["Relation Name"],
                index: node["Index Name"]
              }
            ]
        }
      else
        metrics
      end

    # Recursively analyze child plans
    child_metrics =
      case node["Plans"] do
        plans when is_list(plans) ->
          plans
          |> Enum.map(&analyze_plan_node/1)
          |> merge_plan_metrics()

        _ ->
          %{total_cost: 0, actual_rows: 0, plan_rows: 0, node_types: [], slow_nodes: []}
      end

    merge_plan_metrics([metrics, child_metrics])
  end

  defp merge_plan_metrics(metrics_list) do
    Enum.reduce(
      metrics_list,
      %{
        total_cost: 0,
        actual_rows: 0,
        plan_rows: 0,
        node_types: [],
        slow_nodes: []
      },
      fn metrics, acc ->
        %{
          total_cost: max(acc.total_cost, metrics.total_cost),
          actual_rows: acc.actual_rows + metrics.actual_rows,
          plan_rows: acc.plan_rows + metrics.plan_rows,
          node_types: acc.node_types ++ metrics.node_types,
          slow_nodes: acc.slow_nodes ++ metrics.slow_nodes
        }
      end
    )
  end

  defp calculate_estimation_accuracy(%{actual_rows: actual, plan_rows: planned})
       when planned > 0 do
    accuracy = actual / planned * 100

    cond do
      accuracy < 50 -> :underestimated
      accuracy > 200 -> :overestimated
      true -> :accurate
    end
  end

  defp calculate_estimation_accuracy(_), do: :unknown

  defp generate_suggestions(plan_metrics) do
    suggestions = []

    # Check for sequential scans on large tables
    suggestions =
      if "Seq Scan" in plan_metrics.node_types do
        ["Consider adding indexes to avoid sequential scans" | suggestions]
      else
        suggestions
      end

    # Check for slow nodes
    suggestions =
      suggestions ++
        Enum.map(plan_metrics.slow_nodes, fn node ->
          "Slow #{node.type} operation on #{node.relation || "unknown table"} (#{node.time}ms)"
        end)

    # Check row estimation accuracy
    suggestions =
      case calculate_estimation_accuracy(plan_metrics) do
        :underestimated ->
          ["Query planner underestimated rows - consider updating table statistics" | suggestions]

        :overestimated ->
          ["Query planner overestimated rows - consider updating table statistics" | suggestions]

        _ ->
          suggestions
      end

    suggestions
  end

  defp extract_index_usage(plan) do
    extract_index_info(plan, %{
      used: [],
      missing: [],
      scans: 0,
      seq_scans: 0
    })
  end

  defp extract_index_info(nil, acc), do: acc

  defp extract_index_info(node, acc) when is_map(node) do
    node_type = node["Node Type"] || ""

    acc =
      cond do
        String.contains?(node_type, "Index") ->
          %{acc | used: [node["Index Name"] | acc.used], scans: acc.scans + 1}

        node_type == "Seq Scan" ->
          %{
            acc
            | seq_scans: acc.seq_scans + 1,
              missing: maybe_add_missing_index(node, acc.missing)
          }

        true ->
          acc
      end

    # Process child nodes
    case node["Plans"] do
      plans when is_list(plans) ->
        Enum.reduce(plans, acc, &extract_index_info/2)

      _ ->
        acc
    end
  end

  defp maybe_add_missing_index(node, missing) do
    if node["Filter"] && node["Relation Name"] do
      [
        %{
          table: node["Relation Name"],
          filter: node["Filter"],
          rows_scanned: node["Actual Rows"] || 0
        }
        | missing
      ]
    else
      missing
    end
  end

  defp generate_index_recommendations(index_info) do
    recommendations = []

    recommendations =
      if index_info.seq_scans > 3 do
        ["Multiple sequential scans detected - review indexing strategy" | recommendations]
      else
        recommendations
      end

    recommendations =
      recommendations ++
        Enum.map(index_info.missing, fn missing ->
          "Consider adding index on #{missing.table} for filter: #{missing.filter}"
        end)

    recommendations
  end

  defp generate_comparison_recommendations(analysis1, analysis2) do
    recommendations = []

    time_diff = analysis2.execution_time - analysis1.execution_time

    recommendations =
      cond do
        time_diff > 50 ->
          [
            "Query 2 is significantly slower (#{Float.round(time_diff, 2)}ms difference)"
            | recommendations
          ]

        time_diff < -50 ->
          [
            "Query 2 is significantly faster (#{Float.round(abs(time_diff), 2)}ms improvement)"
            | recommendations
          ]

        true ->
          recommendations
      end

    # Compare node types
    nodes1 = MapSet.new(analysis1.node_types)
    nodes2 = MapSet.new(analysis2.node_types)

    added_nodes = MapSet.difference(nodes2, nodes1) |> MapSet.to_list()
    removed_nodes = MapSet.difference(nodes1, nodes2) |> MapSet.to_list()

    recommendations =
      if added_nodes != [] do
        ["Query 2 introduces: #{Enum.join(added_nodes, ", ")}" | recommendations]
      else
        recommendations
      end

    recommendations =
      if removed_nodes != [] do
        ["Query 2 eliminates: #{Enum.join(removed_nodes, ", ")}" | recommendations]
      else
        recommendations
      end

    recommendations
  end

  defp extract_tables_from_query(selecto) do
    # Extract table names from Selecto struct or map
    source_table =
      get_in(selecto, [:source, :source_table]) ||
        get_in(selecto, [:domain, :source, :source_table]) ||
        get_in(selecto, [:config, :source, :source_table])

    join_map =
      Map.get(selecto, :joins) ||
        get_in(selecto, [:domain, :joins]) ||
        get_in(selecto, [:config, :joins]) ||
        %{}

    join_tables =
      join_map
      |> Enum.map(fn {_alias, join_info} ->
        Map.get(join_info, :source_table) || Map.get(join_info, :table)
      end)
      |> Enum.filter(& &1)

    [source_table | join_tables]
    |> Enum.filter(& &1)
    |> Enum.uniq()
  end

  defp get_table_stats(adapter, conn, table_name) do
    query = """
    SELECT 
      n_live_tup as live_rows,
      n_dead_tup as dead_rows,
      last_vacuum,
      last_autovacuum,
      last_analyze,
      last_autoanalyze,
      vacuum_count,
      autovacuum_count,
      analyze_count,
      autoanalyze_count
    FROM pg_stat_user_tables
    WHERE schemaname = 'public' AND tablename = $1
    """

    case execute_raw_query(adapter, conn, query, [table_name]) do
      {:ok, result} ->
        case result.rows do
          [row] ->
            {:ok,
             %{
               live_rows: Enum.at(row, 0),
               dead_rows: Enum.at(row, 1),
               last_vacuum: Enum.at(row, 2),
               last_autovacuum: Enum.at(row, 3),
               last_analyze: Enum.at(row, 4),
               last_autoanalyze: Enum.at(row, 5),
               vacuum_count: Enum.at(row, 6),
               autovacuum_count: Enum.at(row, 7),
               analyze_count: Enum.at(row, 8),
               autoanalyze_count: Enum.at(row, 9)
             }}

          _ ->
            {:error, :no_stats}
        end

      error ->
        error
    end
  end

  defp execute_raw_query(adapter, conn, query, params) do
    cond do
      invalid_runtime_connection?(conn) ->
        {:error, :invalid_connection}

      Selecto.AdapterSupport.callback_available?(adapter, :execute_raw, 3) ->
        Kernel.apply(adapter, :execute_raw, [conn, query, params])

      true ->
        {:error, :invalid_connection}
    end
  end

  defp runtime_connection(%{connection: nil, postgrex_opts: postgrex_opts}), do: postgrex_opts
  defp runtime_connection(%{connection: connection}), do: connection
  defp runtime_connection(%{postgrex_opts: postgrex_opts}), do: postgrex_opts
  defp runtime_connection(_), do: nil

  defp runtime_adapter(%{adapter: nil}), do: Selecto.AdapterSupport.default_adapter()
  defp runtime_adapter(%{adapter: adapter}) when not is_nil(adapter), do: adapter
  defp runtime_adapter(_), do: Selecto.AdapterSupport.default_adapter()

  defp invalid_runtime_connection?(connection),
    do: is_nil(connection) or is_list(connection) or is_map(connection)

  defp generate_analysis_id do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
  end
end
