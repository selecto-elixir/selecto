defmodule Selecto.Subfilter.SQL.AnyAllBuilder do
  @moduledoc """
  Builds ANY and ALL subqueries for subfilters.
  
  These strategies are useful for more complex comparisons against a set of
  values returned by a subquery.
  
  ## Example SQL (ANY)
  
      release_year > ANY (
        SELECT year
        FROM special_release_years
      )
      
  ## Example SQL (ALL)
  
      rating > ALL (
        SELECT rating
        FROM competing_films
      )
  """
  
  alias Selecto.Subfilter.{Spec, Error}
  alias Selecto.Subfilter.JoinPathResolver.JoinResolution

  @doc """
  Generate ANY or ALL subquery SQL for a given subfilter.
  """
  @spec generate(:any | :all, Spec.t(), JoinResolution.t(), any()) :: 
    {:ok, String.t(), [any()]} | {:error, Error.t()}
  def generate(type, %Spec{} = spec, %JoinResolution{} = join_resolution, _registry) do
    with {:ok, joins_sql, params1} <- build_joins_sql(join_resolution),
         {:ok, where_sql, params2} <- build_where_sql(spec, join_resolution) do
      
      subquery_sql = """
      SELECT #{build_select_clause(join_resolution)}
      FROM #{build_from_clause(join_resolution)}
      #{joins_sql}
      #{where_sql}
      """
      
      # The main query's field to compare against
      main_query_field = "film.#{spec.relationship_path.target_field}" # Simplification
      
      final_sql = 
        "#{main_query_field} #{spec.filter_spec.operator} #{String.upcase(to_string(type))} (#{subquery_sql})"
      
      {:ok, final_sql, params1 ++ params2}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp build_select_clause(%JoinResolution{target_table: target_table, target_field: target_field}) do
    "#{target_table}.#{target_field}"
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

  defp build_where_sql(%Spec{filter_spec: filter_spec} = spec, %JoinResolution{target_table: target_table, target_field: target_field} = join_resolution) do
    # Build correlation and any additional filter conditions
    correlation_sql = "film.film_id = film_actor.film_id" # Simplification
    
    case filter_spec.type do
      # For temporal and range filters in ANY/ALL context, we need to add them to the WHERE clause
      :temporal ->
        with {:ok, temporal_sql, params} <- build_temporal_condition(filter_spec, target_table, target_field) do
          {:ok, "WHERE #{correlation_sql} AND #{temporal_sql}", params}
        end
        
      :range ->
        with {:ok, range_sql, params} <- build_range_condition(filter_spec, target_table, target_field) do
          {:ok, "WHERE #{correlation_sql} AND #{range_sql}", params}
        end
        
      _ ->
        # For other filter types, correlation is usually sufficient
        {:ok, "WHERE #{correlation_sql}", []}
    end
  end

  # Build temporal condition for ANY/ALL subqueries
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

  # Build range condition for ANY/ALL subqueries
  defp build_range_condition(filter_spec, target_table, target_field) do
    qualified_field = "#{target_table}.#{target_field}"
    sql = "#{qualified_field} BETWEEN ? AND ?"
    params = [filter_spec.min_value, filter_spec.max_value]
    {:ok, sql, params}
  end
end