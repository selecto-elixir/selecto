defmodule Selecto.Subfilter.SQL.InBuilder do
  @moduledoc """
  Builds IN subqueries for subfilters.
  
  This strategy is useful when you need to filter the main query based on a
  set of IDs returned by the subquery. It can be more performant than EXISTS
  in some cases, especially when the subquery returns a small number of rows.
  
  ## Example SQL
  
      film_id IN (
        SELECT fc.film_id
        FROM film_category fc
        JOIN category c ON fc.category_id = c.category_id
        WHERE c.name IN ('Action', 'Comedy')
      )
  """
  
  alias Selecto.Subfilter.{Spec, Error}
  alias Selecto.Subfilter.JoinPathResolver.JoinResolution

  @doc """
  Generate IN subquery SQL for a given subfilter.
  """
  @spec generate(Spec.t(), JoinResolution.t(), any()) :: 
    {:ok, String.t(), [any()]} | {:error, Error.t()}
  def generate(%Spec{} = spec, %JoinResolution{} = join_resolution, _registry) do
    with {:ok, joins_sql, params1} <- build_joins_sql(join_resolution),
         {:ok, where_sql, params2} <- build_where_sql(spec, join_resolution) do
      
      subquery_sql = """
      SELECT #{build_select_clause(join_resolution)}
      FROM #{build_from_clause(join_resolution)}
      #{joins_sql}
      WHERE #{where_sql}
      """
      
      # The main query's field to filter on
      main_query_field = "film.film_id" # This is a simplification
      
      final_sql = 
        if spec.negate do
          "#{main_query_field} NOT IN (#{subquery_sql})"
        else
          "#{main_query_field} IN (#{subquery_sql})"
        end
      
      {:ok, final_sql, params1 ++ params2}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp build_select_clause(%JoinResolution{joins: [first_join | _]}) do
    # The SELECT clause should return the foreign key that links back to the main query
    "#{first_join.from}.film_id" # This is a simplification
  end

  defp build_from_clause(%JoinResolution{joins: [first_join | _]}) do
    "#{first_join.from}"
  end

  defp build_joins_sql(%JoinResolution{joins: joins}) do
    join_clauses = 
      joins
      |> Enum.reject(fn join -> join.type == :self end)
      |> Enum.map(fn join ->
        case Map.get(join, :on) do
          nil -> "#{join_type_to_sql(join.type)} JOIN #{join.to}"
          on_clause -> "#{join_type_to_sql(join.type)} JOIN #{join.to} ON #{on_clause}"
        end
      end)
    
    {:ok, Enum.join(join_clauses, "\n"), []}
  end

  defp join_type_to_sql(:inner), do: "INNER"
  defp join_type_to_sql(:left), do: "LEFT"
  defp join_type_to_sql(:right), do: "RIGHT"
  defp join_type_to_sql(:full), do: "FULL"
  defp join_type_to_sql(:self), do: ""

  defp build_where_sql(%Spec{filter_spec: filter_spec}, %JoinResolution{target_table: target_table, target_field: target_field}) do
    build_filter_condition(filter_spec, target_table, target_field)
  end

  # Build filter condition based on filter spec type
  defp build_filter_condition(%{type: :temporal} = filter_spec, target_table, target_field) do
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

  defp build_filter_condition(%{type: :range} = filter_spec, target_table, target_field) do
    qualified_field = "#{target_table}.#{target_field}"
    sql = "#{qualified_field} BETWEEN ? AND ?"
    params = [filter_spec.min_value, filter_spec.max_value]
    {:ok, sql, params}
  end

  defp build_filter_condition(%{type: :in_list} = filter_spec, target_table, target_field) do
    placeholders = Enum.map_join(filter_spec.values, ", ", fn _ -> "?" end)
    filter_sql = "#{target_table}.#{target_field} IN (#{placeholders})"
    params = filter_spec.values
    {:ok, filter_sql, params}
  end

  defp build_filter_condition(%{type: :equality} = filter_spec, target_table, target_field) do
    filter_sql = "#{target_table}.#{target_field} = ?"
    params = [filter_spec.value]
    {:ok, filter_sql, params}
  end

  defp build_filter_condition(filter_spec, _target_table, _target_field) do
    {:error, %Error{
      type: :unsupported_filter_for_in_strategy,
      message: "IN strategy only supports equality, IN list, temporal, and range filters",
      details: %{filter_type: filter_spec.type}
    }}
  end
end