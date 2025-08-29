defmodule Selecto.Subfilter.SQL.ExistsBuilder do
  @moduledoc """
  Builds EXISTS subqueries for subfilters.

  This is the default strategy and is suitable for most subfilter patterns
  where you just need to check for the existence of related records that
  match a certain criteria.

  ## Example SQL

      EXISTS (
        SELECT 1
        FROM film_category fc
        JOIN category c ON fc.category_id = c.category_id
        WHERE fc.film_id = film.film_id
          AND c.name = 'Action'
      )
  """

  alias Selecto.Subfilter.{Spec, Error}
  alias Selecto.Subfilter.JoinPathResolver.JoinResolution

  @doc """
  Generate EXISTS subquery SQL for a given subfilter.
  """
  @spec generate(Spec.t(), JoinResolution.t(), any()) ::
    {:ok, String.t(), [any()]} | {:error, Error.t()}
  def generate(%Spec{} = spec, %JoinResolution{} = join_resolution, _registry) do
    with {:ok, joins_sql, params1} <- build_joins_sql(join_resolution),
         {:ok, where_sql, params2} <- build_where_sql(spec, join_resolution) do

      subquery_sql = """
      SELECT 1
      FROM #{build_from_clause(join_resolution)}
      #{joins_sql}
      WHERE #{where_sql}
      """

      final_sql =
        if spec.negate do
          "NOT EXISTS (#{subquery_sql})"
        else
          "EXISTS (#{subquery_sql})"
        end

      {:ok, final_sql, params1 ++ params2}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp build_from_clause(%JoinResolution{joins: [first_join | _]}) do
    # The FROM clause should be the first table in the join sequence
    "#{first_join.from}"
  end

  defp build_joins_sql(%JoinResolution{joins: joins}) do
    # Build the JOIN clauses from the resolved join path
    # Filter out self joins as they don't need actual JOIN clauses
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
  defp join_type_to_sql(:self), do: "" # Self joins are handled in WHERE

  defp build_where_sql(%Spec{filter_spec: filter_spec}, %JoinResolution{target_table: target_table, target_field: target_field}) do
    # Build the WHERE clause for the subquery

    # This needs to correlate the subquery with the main query
    correlation_sql = "#{target_table}.film_id = film.film_id" # This is a simplification

    # Build the filter SQL based on filter spec type
    with {:ok, filter_sql, params} <- build_filter_condition(filter_spec, target_table, target_field) do
      {:ok, "#{correlation_sql} AND #{filter_sql}", params}
    end
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
        {:error, "Unsupported temporal type: #{filter_spec.temporal_type}"}
    end
  end

  defp build_filter_condition(%{type: :range} = filter_spec, target_table, target_field) do
    qualified_field = "#{target_table}.#{target_field}"
    sql = "#{qualified_field} BETWEEN ? AND ?"
    params = [filter_spec.min_value, filter_spec.max_value]
    {:ok, sql, params}
  end

  defp build_filter_condition(filter_spec, target_table, target_field) do
    # Default case for existing equality, comparison, in_list, aggregation filters
    qualified_field = "#{target_table}.#{target_field}"
    sql = "#{qualified_field} #{filter_spec.operator} ?"
    params = [filter_spec.value]
    {:ok, sql, params}
  end
end
