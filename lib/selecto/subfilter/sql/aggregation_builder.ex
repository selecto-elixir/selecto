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

  defp build_where_sql(_spec, %JoinResolution{joins: [first_join | _]}) do
    # Correlate the subquery with the main query
    correlation_sql = "#{first_join.from}.film_id = film.film_id" # Simplification
    
    {:ok, correlation_sql, []}
  end
end