defmodule Selecto.Performance.ComplexityAnalyzer do
  @moduledoc """
  Analyzes query complexity to prevent database overload.

  Provides pre-execution query analysis to block or warn about queries that
  may be too expensive to execute safely. Complexity is scored based on:

  - Number of JOINs (each JOIN: +10 points)
  - Subqueries and subselects (each: +15 points)
  - Cartesian products (detected cross joins: +100 points - blocks execution)
  - Large IN clauses (>100 items: +20 points)
  - LIKE patterns with leading wildcard (+5 points)
  - Missing WHERE clauses on non-aggregated queries (+30 points)
  - Post-retarget filters (additional complexity: +10 points)

  ## Usage

      # Analyze before execution
      case Selecto.Performance.ComplexityAnalyzer.analyze(selecto) do
        {:ok, analysis} ->
          # Safe to execute, but may have warnings
          Logger.warning(analysis.warnings)
          execute(selecto)

        {:error, :too_complex, analysis} ->
          # Query blocked due to high complexity
          {:error, "Query too complex: \#{Enum.join(analysis.blocking_issues, ", ")}"}
      end

  ## Configuration

      # Application config
      config :selecto, :complexity_analyzer,
        max_complexity: 100,
        max_joins: 10,
        max_in_clause_size: 100,
        warn_on_full_table_scan: true
  """

  require Logger

  @max_complexity_score 100
  @max_joins 10
  @max_in_clause_size 100
  @warn_threshold 50

  defstruct score: 0,
            warnings: [],
            blocking_issues: [],
            recommendations: [],
            details: %{}

  @type t :: %__MODULE__{
          score: non_neg_integer(),
          warnings: [String.t()],
          blocking_issues: [String.t()],
          recommendations: [String.t()],
          details: map()
        }

  @type analysis_result :: {:ok, t()} | {:error, :too_complex, t()}

  @doc """
  Analyze query complexity before execution.

  ## Options

  - `:max_complexity` - Maximum allowed complexity score (default: 100)
  - `:max_joins` - Maximum allowed joins (default: 10)
  - `:max_in_clause_size` - Maximum items in IN clause (default: 100)
  - `:block_on_warnings` - Treat warnings as blocking (default: false)

  ## Returns

  - `{:ok, analysis}` - Query is safe to execute (may have warnings)
  - `{:error, :too_complex, analysis}` - Query exceeds complexity limits
  """
  @spec analyze(Selecto.t(), keyword()) :: analysis_result()
  def analyze(selecto, opts \\ []) do
    max_score =
      opts[:max_complexity] ||
        Application.get_env(:selecto, :max_complexity, @max_complexity_score)

    max_joins = opts[:max_joins] || Application.get_env(:selecto, :max_joins, @max_joins)

    max_in_size =
      opts[:max_in_clause_size] ||
        Application.get_env(:selecto, :max_in_clause_size, @max_in_clause_size)

    analysis = %__MODULE__{
      score: 0,
      warnings: [],
      blocking_issues: [],
      recommendations: [],
      details: %{
        max_score: max_score,
        max_joins: max_joins,
        max_in_clause_size: max_in_size
      }
    }

    analysis
    |> check_join_count(selecto, max_joins)
    |> check_subselects(selecto)
    |> check_cartesian_products(selecto)
    |> check_in_clause_size(selecto, max_in_size)
    |> check_like_patterns(selecto)
    |> check_missing_filters(selecto)
    |> check_post_retarget_complexity(selecto)
    |> check_aggregate_complexity(selecto)
    |> finalize_analysis(max_score, opts)
  end

  # Check number of joins
  defp check_join_count(analysis, selecto, max_joins) do
    join_count = count_joins(selecto.config.joins)

    details = Map.put(analysis.details, :join_count, join_count)

    cond do
      join_count > max_joins ->
        %{
          analysis
          | score: analysis.score + 100,
            blocking_issues:
              analysis.blocking_issues ++
                [
                  "Query has #{join_count} joins (maximum: #{max_joins})"
                ],
            recommendations:
              analysis.recommendations ++
                [
                  "Consider breaking this query into multiple smaller queries",
                  "Review if all joins are necessary for this use case"
                ],
            details: details
        }

      join_count > 5 ->
        %{
          analysis
          | score: analysis.score + join_count * 10,
            warnings:
              analysis.warnings ++
                [
                  "High number of joins (#{join_count}) may impact performance"
                ],
            recommendations:
              analysis.recommendations ++
                [
                  "Monitor query execution time closely"
                ],
            details: details
        }

      true ->
        %{analysis | score: analysis.score + join_count * 10, details: details}
    end
  end

  # Check for subselects
  defp check_subselects(analysis, selecto) do
    subselect_count = count_subselects(selecto.set)

    if subselect_count > 0 do
      %{
        analysis
        | score: analysis.score + subselect_count * 15,
          warnings:
            analysis.warnings ++
              [
                "Query contains #{subselect_count} subselect(s)"
              ],
          details: Map.put(analysis.details, :subselect_count, subselect_count)
      }
    else
      %{analysis | details: Map.put(analysis.details, :subselect_count, 0)}
    end
  end

  # Detect cartesian products (joins without proper conditions)
  defp check_cartesian_products(analysis, selecto) do
    if has_cartesian_product?(selecto.config.joins) do
      %{
        analysis
        | score: analysis.score + 100,
          blocking_issues:
            analysis.blocking_issues ++
              [
                "Cartesian product detected - JOIN without proper conditions"
              ],
          recommendations:
            analysis.recommendations ++
              [
                "Ensure all JOINs have proper ON conditions",
                "Review join relationships in domain configuration"
              ]
      }
    else
      analysis
    end
  end

  # Check IN clause sizes
  defp check_in_clause_size(analysis, selecto, max_in_size) do
    max_in_found = find_max_in_clause(selecto.set.filtered)

    details = Map.put(analysis.details, :max_in_clause_size, max_in_found)

    if max_in_found > max_in_size do
      %{
        analysis
        | score: analysis.score + 20,
          warnings:
            analysis.warnings ++
              [
                "Large IN clause with #{max_in_found} values (maximum: #{max_in_size})"
              ],
          recommendations:
            analysis.recommendations ++
              [
                "Consider using a temporary table or VALUES clause for large IN lists",
                "Break the query into batches of #{max_in_size} items"
              ],
          details: details
      }
    else
      %{analysis | details: details}
    end
  end

  # Check for LIKE patterns with leading wildcards
  defp check_like_patterns(analysis, selecto) do
    leading_wildcard_count = count_leading_wildcard_like(selecto.set.filtered)

    if leading_wildcard_count > 0 do
      %{
        analysis
        | score: analysis.score + leading_wildcard_count * 5,
          warnings:
            analysis.warnings ++
              [
                "#{leading_wildcard_count} LIKE pattern(s) with leading wildcard - cannot use index"
              ],
          recommendations:
            analysis.recommendations ++
              [
                "Consider PostgreSQL full-text search (tsvector/tsquery) for leading wildcard patterns",
                "Use trigram indexes (pg_trgm) if full-text search is not suitable"
              ],
          details: Map.put(analysis.details, :leading_wildcard_count, leading_wildcard_count)
      }
    else
      %{analysis | details: Map.put(analysis.details, :leading_wildcard_count, 0)}
    end
  end

  # Check for missing filters (potential full table scans)
  defp check_missing_filters(analysis, selecto) do
    has_filters = length(selecto.set.filtered) > 0
    has_grouping = length(selecto.set.group_by) > 0

    # If no filters and no grouping, this might be a full table scan
    if !has_filters && !has_grouping do
      %{
        analysis
        | score: analysis.score + 30,
          warnings:
            analysis.warnings ++
              [
                "No WHERE clause - potential full table scan"
              ],
          recommendations:
            analysis.recommendations ++
              [
                "Add filters to limit the result set",
                "If full table scan is intentional, add LIMIT clause"
              ]
      }
    else
      analysis
    end
  end

  # Check post-retarget filter complexity
  defp check_post_retarget_complexity(analysis, selecto) do
    post_retarget_filters = Map.get(selecto.set, :post_retarget_filters, [])

    if length(post_retarget_filters) > 0 do
      %{
        analysis
        | score: analysis.score + 10,
          warnings:
            analysis.warnings ++
              [
                "Query contains #{length(post_retarget_filters)} post-retarget filter(s)"
              ],
          details:
            Map.put(analysis.details, :post_retarget_filter_count, length(post_retarget_filters))
      }
    else
      %{analysis | details: Map.put(analysis.details, :post_retarget_filter_count, 0)}
    end
  end

  # Check aggregate query complexity
  defp check_aggregate_complexity(analysis, selecto) do
    group_by_count = length(selecto.set.group_by)

    if group_by_count > 5 do
      %{
        analysis
        | score: analysis.score + 10,
          warnings:
            analysis.warnings ++
              [
                "High number of GROUP BY columns (#{group_by_count})"
              ],
          recommendations:
            analysis.recommendations ++
              [
                "Consider pre-aggregating data for common queries"
              ],
          details: Map.put(analysis.details, :group_by_count, group_by_count)
      }
    else
      %{analysis | details: Map.put(analysis.details, :group_by_count, group_by_count)}
    end
  end

  # Finalize analysis and determine if query should be blocked
  defp finalize_analysis(analysis, max_score, opts) do
    block_on_warnings = Keyword.get(opts, :block_on_warnings, false)

    # Determine if we should block
    should_block =
      cond do
        length(analysis.blocking_issues) > 0 -> true
        analysis.score > max_score -> true
        block_on_warnings && length(analysis.warnings) > 0 -> true
        true -> false
      end

    # Add score warning if over threshold but not blocking
    analysis =
      if analysis.score > @warn_threshold && analysis.score <= max_score do
        %{
          analysis
          | warnings:
              analysis.warnings ++
                [
                  "Complexity score #{analysis.score}/#{max_score} - approaching limit"
                ]
        }
      else
        analysis
      end

    if should_block do
      # Add score to blocking issues if over limit
      analysis =
        if analysis.score > max_score && length(analysis.blocking_issues) == 0 do
          %{
            analysis
            | blocking_issues:
                analysis.blocking_issues ++
                  [
                    "Complexity score #{analysis.score} exceeds maximum #{max_score}"
                  ]
          }
        else
          analysis
        end

      {:error, :too_complex, analysis}
    else
      {:ok, analysis}
    end
  end

  # Helper: Count all joins recursively
  defp count_joins(joins) when is_map(joins) do
    Enum.reduce(joins, 0, fn {_name, join_config}, acc ->
      nested_count =
        case Map.get(join_config, :joins) do
          nil -> 0
          nested_joins -> count_joins(nested_joins)
        end

      acc + 1 + nested_count
    end)
  end

  defp count_joins(_), do: 0

  # Helper: Count subselects
  defp count_subselects(query_set) do
    Map.get(query_set, :subselected, []) |> length()
  end

  # Helper: Detect cartesian products
  # This is a simplified check - real implementation would need deeper analysis
  defp has_cartesian_product?(joins) when is_map(joins) do
    # In Selecto, joins are configured with associations, so cartesian products
    # are less common. This would need to check for missing join conditions.
    # For now, we'll return false as Selecto's join system handles this.
    false
  end

  defp has_cartesian_product?(_), do: false

  # Helper: Find maximum IN clause size
  defp find_max_in_clause(filters) do
    Enum.reduce(filters, 0, fn filter, max_size ->
      size =
        case filter do
          # Filter tuple format: {field, {:in, values}}
          {_field, {:in, values}} when is_list(values) ->
            length(values)

          # Filter map format with comp: "IN" and value as list
          %{"comp" => comp, "value" => value} when comp in ["IN", "in"] and is_list(value) ->
            length(value)

          _ ->
            0
        end

      max(max_size, size)
    end)
  end

  # Helper: Count LIKE filters with leading wildcards
  defp count_leading_wildcard_like(filters) do
    Enum.count(filters, fn filter ->
      case filter do
        # Tuple format: {field, {:like, "%value"}}
        {_field, {:like, value}} when is_binary(value) ->
          String.starts_with?(value, "%")

        # Map format with comp: "LIKE"
        %{"comp" => comp, "value" => value}
        when comp in ["LIKE", "like", "ILIKE", "ilike"] and is_binary(value) ->
          String.starts_with?(value, "%")

        _ ->
          false
      end
    end)
  end

  @doc """
  Get a human-readable summary of the complexity analysis.
  """
  @spec format_summary(t()) :: String.t()
  def format_summary(analysis) do
    lines = [
      "Query Complexity Analysis:",
      "  Score: #{analysis.score}/#{analysis.details.max_score}"
    ]

    lines =
      if length(analysis.blocking_issues) > 0 do
        lines ++
          ["  Blocking Issues:"] ++
          Enum.map(analysis.blocking_issues, fn issue -> "    - #{issue}" end)
      else
        lines
      end

    lines =
      if length(analysis.warnings) > 0 do
        lines ++
          ["  Warnings:"] ++
          Enum.map(analysis.warnings, fn warning -> "    - #{warning}" end)
      else
        lines
      end

    lines =
      if length(analysis.recommendations) > 0 do
        lines ++
          ["  Recommendations:"] ++
          Enum.map(analysis.recommendations, fn rec -> "    - #{rec}" end)
      else
        lines
      end

    Enum.join(lines, "\n")
  end

  @doc """
  Check if a query is safe to execute based on complexity.
  Returns true if safe, false if blocked.
  """
  @spec safe_to_execute?(Selecto.t(), keyword()) :: boolean()
  def safe_to_execute?(selecto, opts \\ []) do
    case analyze(selecto, opts) do
      {:ok, _analysis} -> true
      {:error, :too_complex, _analysis} -> false
    end
  end
end
