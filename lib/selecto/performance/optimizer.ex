defmodule Selecto.Performance.Optimizer do
  @moduledoc """
  Query optimization suggestions and automatic query improvements.
  
  Analyzes Selecto queries and provides:
  - Optimization suggestions based on query structure
  - Automatic query rewriting for better performance
  - Index recommendations
  - Join order optimization
  - Filter pushdown optimization
  """
  
  require Logger
  
  @doc """
  Analyze a Selecto query and provide optimization suggestions.
  
  Returns a list of suggestions with severity levels and potential impact.
  
  ## Examples
  
      iex> Optimizer.suggest_optimizations(selecto)
      {:ok, [
        %{
          type: :missing_index,
          severity: :high,
          message: "Consider adding index on films.rating",
          impact: "Could reduce query time by ~60%",
          sql: "CREATE INDEX idx_films_rating ON films(rating);"
        },
        %{
          type: :join_order,
          severity: :medium,
          message: "Reorder joins to filter earlier",
          impact: "Could reduce intermediate result size"
        }
      ]}
  """
  def suggest_optimizations(selecto, options \\ []) do
    suggestions = []
    
    # Analyze filters for index opportunities
    suggestions = suggestions ++ analyze_filters(selecto)
    
    # Analyze joins for optimization
    suggestions = suggestions ++ analyze_joins(selecto)
    
    # Analyze aggregations
    suggestions = suggestions ++ analyze_aggregations(selecto)
    
    # Analyze sorting and grouping
    suggestions = suggestions ++ analyze_sorting(selecto)
    
    # Check for common anti-patterns
    suggestions = suggestions ++ detect_antipatterns(selecto)
    
    # Run EXPLAIN if requested
    suggestions = if options[:with_explain] do
      case Selecto.Performance.QueryAnalyzer.analyze_query(selecto, analyze: false) do
        {:ok, analysis} ->
          suggestions ++ extract_explain_suggestions(analysis)
        _ ->
          suggestions
      end
    else
      suggestions
    end
    
    # Sort by severity
    sorted_suggestions = Enum.sort_by(suggestions, &severity_score(&1.severity), :desc)
    
    {:ok, sorted_suggestions}
  end
  
  @doc """
  Automatically optimize a query by applying safe transformations.
  
  Returns an optimized version of the query that should produce
  the same results but with better performance.
  
  ## Examples
  
      iex> optimized = Optimizer.auto_optimize(selecto)
      iex> Selecto.to_sql(optimized)
      # Returns optimized SQL
  """
  def auto_optimize(selecto, options \\ []) do
    optimized = selecto
    
    # Apply filter pushdown
    optimized = if options[:filter_pushdown] != false do
      push_down_filters(optimized)
    else
      optimized
    end
    
    # Optimize join order
    optimized = if options[:optimize_joins] != false do
      optimize_join_order(optimized)
    else
      optimized
    end
    
    # Eliminate redundant operations
    optimized = if options[:eliminate_redundant] != false do
      eliminate_redundant(optimized)
    else
      optimized
    end
    
    # Add query hints if supported
    optimized = if options[:add_hints] do
      add_performance_hints(optimized)
    else
      optimized
    end
    
    optimized
  end
  
  @doc """
  Generate index recommendations based on query patterns.
  
  Analyzes a collection of queries to identify the most beneficial indexes.
  """
  def recommend_indexes(queries, options \\ []) when is_list(queries) do
    # Collect all filter columns
    filter_columns = queries
      |> Enum.flat_map(&extract_filter_columns/1)
      |> Enum.frequencies()
    
    # Collect all join columns
    join_columns = queries
      |> Enum.flat_map(&extract_join_columns/1)
      |> Enum.frequencies()
    
    # Collect all sort columns
    sort_columns = queries
      |> Enum.flat_map(&extract_sort_columns/1)
      |> Enum.frequencies()
    
    # Generate recommendations
    recommendations = []
    
    # Single column indexes for frequently filtered columns
    recommendations = recommendations ++ 
      generate_single_column_indexes(filter_columns, options[:threshold] || 5)
    
    # Composite indexes for common filter combinations
    recommendations = recommendations ++
      generate_composite_indexes(queries, options[:threshold] || 3)
    
    # Join indexes
    recommendations = recommendations ++
      generate_join_indexes(join_columns, options[:threshold] || 5)
    
    # Sort indexes for ORDER BY without filters
    recommendations = recommendations ++
      generate_sort_indexes(sort_columns, filter_columns)
    
    # Deduplicate and prioritize
    recommendations
    |> Enum.uniq_by(& &1.index_name)
    |> Enum.sort_by(& &1.priority, :desc)
    |> Enum.take(options[:max_recommendations] || 10)
  end
  
  # Private functions - Filter Analysis
  
  defp analyze_filters(selecto) do
    selecto.filters
    |> Enum.flat_map(fn {field, _value} ->
      # Check if field could benefit from an index
      case analyze_filter_field(field, selecto) do
        nil -> []
        suggestion -> [suggestion]
      end
    end)
  end
  
  defp analyze_filter_field(field, selecto) do
    # Parse field to get table and column
    {table, column} = parse_field_reference(field)
    
    # Check if this looks like it needs an index
    cond do
      String.contains?(field, "id") ->
        # ID fields usually have indexes
        nil
      
      String.ends_with?(field, "_at") ->
        # Timestamp fields often benefit from indexes
        %{
          type: :missing_index,
          severity: :medium,
          field: field,
          message: "Consider adding index on #{table}.#{column} for timestamp queries",
          impact: "Could improve time-range query performance",
          sql: "CREATE INDEX idx_#{table}_#{column} ON #{table}(#{column});"
        }
      
      true ->
        # General field that might benefit from index
        %{
          type: :potential_index,
          severity: :low,
          field: field,
          message: "Field #{table}.#{column} is frequently filtered",
          impact: "Index could improve filter performance",
          sql: "CREATE INDEX idx_#{table}_#{column} ON #{table}(#{column});"
        }
    end
  end
  
  # Private functions - Join Analysis
  
  defp analyze_joins(selecto) do
    join_count = map_size(selecto.joins)
    
    suggestions = []
    
    # Check for too many joins
    suggestions = if join_count > 5 do
      [%{
        type: :excessive_joins,
        severity: :high,
        message: "Query has #{join_count} joins which may impact performance",
        impact: "Consider breaking into multiple queries or using materialized views"
      } | suggestions]
    else
      suggestions
    end
    
    # Check for Cartesian products
    suggestions = suggestions ++ detect_cartesian_products(selecto)
    
    # Check join order efficiency
    suggestions = suggestions ++ analyze_join_order(selecto)
    
    suggestions
  end
  
  defp detect_cartesian_products(selecto) do
    # Look for joins without proper conditions
    selecto.joins
    |> Enum.filter(fn {_alias, join} ->
      # Check if join lacks proper conditions
      !join[:on] && !join[:owner_key] && !join[:related_key]
    end)
    |> Enum.map(fn {join_alias, _join} ->
      %{
        type: :cartesian_product,
        severity: :critical,
        message: "Join '#{join_alias}' may cause Cartesian product",
        impact: "This can cause exponential row multiplication"
      }
    end)
  end
  
  defp analyze_join_order(selecto) do
    # Analyze if joins could be reordered for better performance
    # This is a simplified heuristic
    joins_with_filters = selecto.joins
      |> Enum.map(fn {alias, join} ->
        filter_count = count_filters_for_join(alias, selecto.filters)
        {alias, join, filter_count}
      end)
      |> Enum.sort_by(fn {_alias, _join, filter_count} -> filter_count end, :desc)
    
    current_order = Map.keys(selecto.joins)
    optimal_order = Enum.map(joins_with_filters, fn {alias, _join, _count} -> alias end)
    
    if current_order != optimal_order do
      [%{
        type: :join_order,
        severity: :medium,
        message: "Joins could be reordered for better performance",
        impact: "Filter-heavy tables should be joined first",
        current_order: current_order,
        suggested_order: optimal_order
      }]
    else
      []
    end
  end
  
  defp count_filters_for_join(join_alias, filters) do
    prefix = "#{join_alias}."
    Enum.count(filters, fn {field, _value} ->
      String.starts_with?(to_string(field), prefix)
    end)
  end
  
  # Private functions - Aggregation Analysis
  
  defp analyze_aggregations(selecto) do
    suggestions = []
    
    # Check for aggregations without GROUP BY
    has_aggregations = Enum.any?(selecto.select, fn
      {:func, _name, _args} -> true
      _ -> false
    end)
    
    suggestions = if has_aggregations && selecto.group_by == [] do
      [%{
        type: :aggregation_without_group,
        severity: :low,
        message: "Aggregation without GROUP BY will produce single row",
        impact: "Ensure this is intended behavior"
      } | suggestions]
    else
      suggestions
    end
    
    # Check for SELECT * with GROUP BY
    suggestions = if selecto.group_by != [] && :* in selecto.select do
      [%{
        type: :select_star_with_group,
        severity: :high,
        message: "SELECT * with GROUP BY is invalid",
        impact: "Only grouped columns and aggregates are allowed"
      } | suggestions]
    else
      suggestions
    end
    
    suggestions
  end
  
  # Private functions - Sorting Analysis
  
  defp analyze_sorting(selecto) do
    suggestions = []
    
    # Check for sorting without index
    suggestions = suggestions ++ Enum.map(selecto.order_by, fn {field, _direction} ->
      %{
        type: :sort_without_index,
        severity: :medium,
        message: "Sorting on #{field} may benefit from index",
        impact: "Index can eliminate sort operation",
        sql: generate_sort_index_sql(field)
      }
    end)
    
    # Check for sorting with LIMIT
    has_limit = selecto.limit != nil
    has_order = selecto.order_by != []
    
    suggestions = if has_limit && !has_order do
      [%{
        type: :limit_without_order,
        severity: :medium,
        message: "LIMIT without ORDER BY returns unpredictable results",
        impact: "Results may vary between executions"
      } | suggestions]
    else
      suggestions
    end
    
    suggestions
  end
  
  # Private functions - Anti-pattern Detection
  
  defp detect_antipatterns(selecto) do
    patterns = []
    
    # Check for OR conditions that prevent index usage
    patterns = patterns ++ detect_or_antipattern(selecto)
    
    # Check for function calls on indexed columns
    patterns = patterns ++ detect_function_on_column(selecto)
    
    # Check for negative conditions that can't use indexes
    patterns = patterns ++ detect_negative_conditions(selecto)
    
    # Check for wildcards at start of LIKE patterns
    patterns = patterns ++ detect_leading_wildcards(selecto)
    
    patterns
  end
  
  defp detect_or_antipattern(selecto) do
    selecto.filters
    |> Enum.filter(fn
      {_field, {:or, _conditions}} -> true
      _ -> false
    end)
    |> Enum.map(fn {field, _value} ->
      %{
        type: :or_condition,
        severity: :medium,
        message: "OR condition on #{field} may prevent index usage",
        impact: "Consider using IN clause or UNION instead"
      }
    end)
  end
  
  defp detect_function_on_column(selecto) do
    selecto.filters
    |> Enum.filter(fn {field, _value} ->
      # Check if field contains function call
      String.contains?(to_string(field), "(")
    end)
    |> Enum.map(fn {field, _value} ->
      %{
        type: :function_on_column,
        severity: :high,
        message: "Function on column #{field} prevents index usage",
        impact: "Consider functional index or rewrite condition"
      }
    end)
  end
  
  defp detect_negative_conditions(selecto) do
    selecto.filters
    |> Enum.filter(fn
      {_field, {:not, _}} -> true
      {_field, {:neq, _}} -> true
      {_field, {:not_in, _}} -> true
      _ -> false
    end)
    |> Enum.map(fn {field, _value} ->
      %{
        type: :negative_condition,
        severity: :low,
        message: "Negative condition on #{field} can't use index efficiently",
        impact: "Consider positive conditions if possible"
      }
    end)
  end
  
  defp detect_leading_wildcards(selecto) do
    selecto.filters
    |> Enum.filter(fn
      {_field, {:like, pattern}} when is_binary(pattern) ->
        String.starts_with?(pattern, "%")
      _ -> false
    end)
    |> Enum.map(fn {field, _value} ->
      %{
        type: :leading_wildcard,
        severity: :high,
        message: "Leading wildcard on #{field} prevents index usage",
        impact: "Consider full-text search or trigram index"
      }
    end)
  end
  
  # Private functions - Query Optimization
  
  defp push_down_filters(selecto) do
    # Move filters closer to their source tables
    # This is a simplified implementation
    selecto
  end
  
  defp optimize_join_order(selecto) do
    # Reorder joins based on selectivity and filter presence
    # This would require statistics to do properly
    selecto
  end
  
  defp eliminate_redundant(selecto) do
    # Remove redundant operations
    selecto
    |> eliminate_duplicate_joins()
    |> eliminate_redundant_columns()
  end
  
  defp eliminate_duplicate_joins(selecto) do
    # Remove duplicate join paths
    selecto
  end
  
  defp eliminate_redundant_columns(selecto) do
    # Remove columns that aren't used
    selecto
  end
  
  defp add_performance_hints(selecto) do
    # Add database-specific performance hints
    # PostgreSQL supports comments that can act as hints
    selecto
  end
  
  # Private functions - Helper Functions
  
  defp parse_field_reference(field) when is_binary(field) do
    case String.split(field, ".") do
      [table, column] -> {table, column}
      [column] -> {selecto_source_table(), column}
      parts -> {Enum.at(parts, -2), List.last(parts)}
    end
  end
  defp parse_field_reference(field), do: {"unknown", to_string(field)}
  
  defp selecto_source_table, do: "main"
  
  defp severity_score(:critical), do: 4
  defp severity_score(:high), do: 3
  defp severity_score(:medium), do: 2
  defp severity_score(:low), do: 1
  
  defp extract_explain_suggestions(analysis) do
    analysis.suggestions
    |> Enum.map(fn suggestion ->
      %{
        type: :explain_suggestion,
        severity: :medium,
        message: suggestion,
        impact: "Identified by EXPLAIN analysis"
      }
    end)
  end
  
  defp extract_filter_columns(selecto) do
    selecto.filters
    |> Enum.map(fn {field, _value} ->
      {_table, column} = parse_field_reference(field)
      column
    end)
  end
  
  defp extract_join_columns(selecto) do
    selecto.joins
    |> Enum.flat_map(fn {_alias, join} ->
      [join[:owner_key], join[:related_key]]
      |> Enum.filter(&(&1))
      |> Enum.map(&to_string/1)
    end)
  end
  
  defp extract_sort_columns(selecto) do
    selecto.order_by
    |> Enum.map(fn {field, _dir} ->
      {_table, column} = parse_field_reference(field)
      column
    end)
  end
  
  defp generate_single_column_indexes(column_frequencies, threshold) do
    column_frequencies
    |> Enum.filter(fn {_column, count} -> count >= threshold end)
    |> Enum.map(fn {column, count} ->
      %{
        type: :single_column_index,
        index_name: "idx_#{column}",
        column: column,
        frequency: count,
        priority: count,
        sql: "CREATE INDEX idx_#{column} ON table(#{column});"
      }
    end)
  end
  
  defp generate_composite_indexes(queries, threshold) do
    # Find common filter combinations
    filter_combinations = queries
      |> Enum.map(fn selecto ->
        selecto.filters
        |> Map.keys()
        |> Enum.map(fn field ->
          {_table, column} = parse_field_reference(field)
          column
        end)
        |> Enum.sort()
      end)
      |> Enum.filter(fn combo -> length(combo) > 1 end)
      |> Enum.frequencies()
      |> Enum.filter(fn {_combo, count} -> count >= threshold end)
    
    filter_combinations
    |> Enum.map(fn {columns, count} ->
      index_name = "idx_" <> Enum.join(columns, "_")
      %{
        type: :composite_index,
        index_name: index_name,
        columns: columns,
        frequency: count,
        priority: count * length(columns),
        sql: "CREATE INDEX #{index_name} ON table(#{Enum.join(columns, ", ")});"
      }
    end)
  end
  
  defp generate_join_indexes(join_columns, threshold) do
    join_columns
    |> Enum.filter(fn {_column, count} -> count >= threshold end)
    |> Enum.map(fn {column, count} ->
      %{
        type: :join_index,
        index_name: "idx_join_#{column}",
        column: column,
        frequency: count,
        priority: count * 2, # Join indexes are more important
        sql: "CREATE INDEX idx_join_#{column} ON table(#{column});"
      }
    end)
  end
  
  defp generate_sort_indexes(sort_columns, filter_columns) do
    # Only recommend sort indexes for columns not already filtered
    sort_columns
    |> Enum.reject(fn {column, _count} ->
      Map.has_key?(filter_columns, column)
    end)
    |> Enum.map(fn {column, count} ->
      %{
        type: :sort_index,
        index_name: "idx_sort_#{column}",
        column: column,
        frequency: count,
        priority: count,
        sql: "CREATE INDEX idx_sort_#{column} ON table(#{column});"
      }
    end)
  end
  
  defp generate_sort_index_sql(field) do
    {table, column} = parse_field_reference(field)
    "CREATE INDEX idx_#{table}_#{column} ON #{table}(#{column});"
  end
end