defmodule Selecto.Subfilter.SQL.AggregationBuilder do
  @moduledoc """
  Builds subqueries with aggregations (COUNT, SUM, AVG, etc.).
  
  This strategy is used for subfilters that perform an aggregation and
  compare the result to a value, such as checking if a film has more
  than 5 actors.
  
  ## Example SQL
  
      (
        SELECT COUNT(fa.actor_id)
        FROM film_actor fa
        WHERE fa.film_id = film.film_id
      ) > 5
  """
  
  alias Selecto.Subfilter.{Spec, Error}
  alias Selecto.Subfilter.JoinPathResolver.JoinResolution

  @doc """
  Generate aggregation subquery SQL for a given subfilter.
  """
  @spec generate(Spec.t(), JoinResolution.t(), any()) :: 
    {:ok, String.t(), [any()]} | {:error, Error.t()}
  def generate(%Spec{filter_spec: filter_spec} = spec, %JoinResolution{} = join_resolution, _registry) do
    with {:ok, joins_sql, params1} <- build_joins_sql(join_resolution),
         {:ok, where_sql, params2} <- build_where_sql(spec, join_resolution) do
      
      subquery_sql = """
      (
        SELECT #{build_aggregation_select(filter_spec, join_resolution)}
        FROM #{build_from_clause(join_resolution)}
        #{joins_sql}
        WHERE #{where_sql}
      )
      """
      
      final_sql = "#{subquery_sql} #{filter_spec.operator} ?"
      params = params1 ++ params2 ++ [filter_spec.value]
      
      {:ok, final_sql, params}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp build_aggregation_select(%{agg_function: agg_func}, %JoinResolution{target_table: target_table}) do
    agg_field = 
      case agg_func do
        :count -> "*" # Or a specific field for COUNT(DISTINCT ...)
        _ -> "#{target_table}.#{agg_func}" # Simplification
      end
      
    "#{String.upcase(to_string(agg_func))}(#{agg_field})"
  end

  defp build_from_clause(%JoinResolution{joins: [first_join | _]}) do
    "#{first_join.from}"
  end

  defp build_joins_sql(%JoinResolution{joins: joins}) do
    join_clauses = Enum.map(joins, fn join ->
      "#{join_type_to_sql(join.type)} JOIN #{join.to} ON #{join.on}"
    end)
    
    {:ok, Enum.join(join_clauses, "\n"), []}
  end

  defp join_type_to_sql(:inner), do: "INNER"
  defp join_type_to_sql(:left), do: "LEFT"
  defp join_type_to_sql(:right), do: "RIGHT"
  defp join_type_to_sql(:full), do: "FULL"
  defp join_type_to_sql(:self), do: ""

  defp build_where_sql(%Spec{filter_spec: filter_spec} = _spec, %JoinResolution{joins: [first_join | _], target_table: target_table, target_field: target_field}) do
    # Correlate the subquery with the main query
    correlation_sql = "#{first_join.from}.film_id = film.film_id" # Simplification
    
    # Add temporal or range conditions if applicable
    case filter_spec.type do
      :temporal when not is_nil(filter_spec.temporal_type) ->
        with {:ok, temporal_sql, params} <- build_temporal_condition(filter_spec, target_table, target_field) do
          {:ok, "#{correlation_sql} AND #{temporal_sql}", params}
        end
        
      :range when not is_nil(filter_spec.min_value) and not is_nil(filter_spec.max_value) ->
        with {:ok, range_sql, params} <- build_range_condition(filter_spec, target_table, target_field) do
          {:ok, "#{correlation_sql} AND #{range_sql}", params}
        end
        
      _ ->
        {:ok, correlation_sql, []}
    end
  end

  # Build temporal condition for aggregation subqueries
  defp build_temporal_condition(filter_spec, target_table, target_field) do
    qualified_field = "#{target_table}.#{target_field}"
    
    case filter_spec.temporal_type do
      :recent_years ->
        sql = "#{qualified_field} > (CURRENT_DATE - INTERVAL '#{filter_spec.value} years')"
        {:ok, sql, []}
        
      :within_days ->
        sql = "#{qualified_field} > (CURRENT_DATE - INTERVAL '#{filter_spec.value} days')"
        {:ok, sql, []}
        
      :within_hours ->
        sql = "#{qualified_field} > (NOW() - INTERVAL '#{filter_spec.value} hours')"
        {:ok, sql, []}
        
      :since_date ->
        sql = "#{qualified_field} > ?"
        {:ok, sql, [filter_spec.value]}
        
      _ ->
        {:error, %Error{
          type: :unsupported_temporal_type,
          message: "Unsupported temporal type: #{filter_spec.temporal_type}",
          details: %{temporal_type: filter_spec.temporal_type}
        }}
    end
  end

  # Build range condition for aggregation subqueries
  defp build_range_condition(filter_spec, target_table, target_field) do
    qualified_field = "#{target_table}.#{target_field}"
    sql = "#{qualified_field} BETWEEN ? AND ?"
    params = [filter_spec.min_value, filter_spec.max_value]
    {:ok, sql, params}
  end
end