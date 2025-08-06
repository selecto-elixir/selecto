defmodule Selecto.Builder.Sql do

  import Selecto.Builder.Sql.Helpers

  def build(selecto, _opts) do
    {aliases, sel_joins, select_clause, select_params} = build_select(selecto)
    {filter_joins, where_clause, where_params} = build_where(selecto)
    {group_by_joins, group_by_clause, group_params} = build_group_by(selecto)
    {order_by_joins, order_by_clause, order_params} = build_order_by(selecto)

    joins_in_order =
      Selecto.Builder.Join.get_join_order(
        Selecto.joins(selecto),
        List.flatten(sel_joins ++ filter_joins ++ group_by_joins ++ order_by_joins)
      )

    {from_clause, from_params} = build_from(selecto, joins_in_order)

    sql = "
        select #{select_clause}
        from #{from_clause}
    "

    sql =
      case where_clause do
        "()" -> sql
        _ -> sql <> "
        where #{where_clause}
      "
      end

    sql =
      case group_by_clause do
        "" -> sql
        _ -> sql <> "
        group by #{group_by_clause}
      "
      end

    sql =
      if String.contains?(group_by_clause, "rollup") do
        case order_by_clause do
          "" -> sql
          _ -> "select * from (" <> sql <> ") as rollupfix
        order by #{order_by_clause}
      "
        end
      else
        case order_by_clause do
          "" -> sql
          _ -> sql <> "
        order by #{order_by_clause}
      "
        end
      end

    params = select_params ++ from_params ++ where_params ++ group_params ++ order_params

    params_num = Enum.with_index(params) |> Enum.map(fn {_, index} -> "$#{index + 1}" end)

    ## replace ^SelectoParam^ with $1 etc. There has to be a better way???? TODO use 1.. params length
    sql =
      String.split(sql, "^SelectoParam^")
      |> Enum.zip(params_num ++ [""])
      |> Enum.map(fn {a, b} -> [a, b] end)
      |> List.flatten()
      |> Enum.join("")

    params = select_params ++ from_params ++ where_params ++ group_params ++ order_params

    {sql, aliases, params}
  end

  #rework to allow parameterized joins, CTEs etc TODO
  defp build_from(selecto, joins) do
    Enum.reduce(joins, {[], []}, fn
      :selecto_root, {fc, p} ->
        {fc ++ [~s[#{Selecto.source_table(selecto)} #{build_join_string(selecto, "selecto_root")}]], p}

      join, {fc, p} ->
        config = Selecto.joins(selecto)[join]
        
        case Map.get(config, :join_type) do
          :many_to_many ->
            build_many_to_many_join(selecto, join, config, fc, p)
            
          :hierarchical_adjacency ->
            build_hierarchical_adjacency_join(selecto, join, config, fc, p)
            
          :hierarchical_materialized_path ->
            build_hierarchical_materialized_path_join(selecto, join, config, fc, p)
            
          :hierarchical_closure_table ->
            build_hierarchical_closure_table_join(selecto, join, config, fc, p)
            
          :star_dimension ->
            build_star_dimension_join(selecto, join, config, fc, p)
            
          :snowflake_dimension ->
            build_snowflake_dimension_join(selecto, join, config, fc, p)
            
          _ ->
            # Standard join
            {fc ++
               [
                 ~s[ left join #{config.source} #{build_join_string(selecto, join)} on #{build_selector_string(selecto, join, config.my_key)} = #{build_selector_string(selecto, config.requires_join, config.owner_key)}]
               ], p}
        end
    end)
  end

  defp build_many_to_many_join(selecto, join, config, fc, p) do
    # Many-to-many joins typically require going through a join table
    # This assumes the association is properly configured as has_many :through
    {fc ++
       [
         ~s[ left join #{config.source} #{build_join_string(selecto, join)} on #{build_selector_string(selecto, join, config.my_key)} = #{build_selector_string(selecto, config.requires_join, config.owner_key)}]
       ], p}
  end

  defp build_hierarchical_adjacency_join(selecto, join, config, fc, p) do
    # Self-referencing join for adjacency list pattern
    # Standard left join but with special CTE handling for recursive queries
    {fc ++
       [
         ~s[ left join #{config.source} #{build_join_string(selecto, join)} on #{build_selector_string(selecto, join, config.my_key)} = #{build_selector_string(selecto, config.requires_join, config.owner_key)}]
       ], p}
  end

  defp build_hierarchical_materialized_path_join(selecto, join, config, fc, p) do
    # Standard join for materialized path - path operations handled in WHERE/SELECT clauses
    {fc ++
       [
         ~s[ left join #{config.source} #{build_join_string(selecto, join)} on #{build_selector_string(selecto, join, config.my_key)} = #{build_selector_string(selecto, config.requires_join, config.owner_key)}]
       ], p}
  end

  defp build_hierarchical_closure_table_join(selecto, join, config, fc, p) do
    # Closure table requires additional join to the closure table
    closure_table = Map.get(config, :closure_table)
    ancestor_field = Map.get(config, :ancestor_field, :ancestor_id)
    descendant_field = Map.get(config, :descendant_field, :descendant_id)
    
    {fc ++
       [
         ~s[ left join #{closure_table} #{build_join_string(selecto, "#{join}_closure")} on #{build_selector_string(selecto, config.requires_join, config.owner_key)} = #{build_join_string(selecto, "#{join}_closure")}.#{ancestor_field}],
         ~s[ left join #{config.source} #{build_join_string(selecto, join)} on #{build_join_string(selecto, "#{join}_closure")}.#{descendant_field} = #{build_selector_string(selecto, join, config.my_key)}]
       ], p}
  end

  defp build_star_dimension_join(selecto, join, config, fc, p) do
    # Star dimension joins are typically optimized for aggregation
    # Use standard join but may have different indexing hints in real implementation
    {fc ++
       [
         ~s[ left join #{config.source} #{build_join_string(selecto, join)} on #{build_selector_string(selecto, join, config.my_key)} = #{build_selector_string(selecto, config.requires_join, config.owner_key)}]
       ], p}
  end

  defp build_snowflake_dimension_join(selecto, join, config, fc, p) do
    # Snowflake dimensions may require additional normalization joins
    normalization_joins = Map.get(config, :normalization_joins, [])
    
    base_join = ~s[ left join #{config.source} #{build_join_string(selecto, join)} on #{build_selector_string(selecto, join, config.my_key)} = #{build_selector_string(selecto, config.requires_join, config.owner_key)}]
    
    # Add any required normalization joins
    additional_joins = Enum.map(normalization_joins, fn norm_join ->
      ~s[ left join #{norm_join.table} #{build_join_string(selecto, norm_join.alias)} on #{build_selector_string(selecto, join, norm_join.local_key)} = #{build_selector_string(selecto, norm_join.alias, norm_join.remote_key)}]
    end)
    
    {fc ++ [base_join] ++ additional_joins, p}
  end

  defp build_select(selecto) do
    {aliases, joins, selects, params} =
      selecto.set.selected
      |> Enum.map(fn s -> Selecto.Builder.Sql.Select.build(selecto, s) end)
      |> Enum.reduce(
        {[], [], [], []},
        fn {f, j, p, as}, {aliases, joins, selects, params} ->
          {aliases ++ [as], joins ++ [j], selects ++ [f], params ++ p}
        end
      )

    {aliases, joins, Enum.join(selects, ", "), params}
  end

  defp build_where(selecto) do
    Selecto.Builder.Sql.Where.build(
      selecto,
      {:and, Map.get(Selecto.domain(selecto), :required_filters, []) ++ selecto.set.filtered}
    )
  end

  defp build_group_by(selecto) do
    Selecto.Builder.Sql.Group.build(selecto)
  end

  defp build_order_by(selecto) do
    Selecto.Builder.Sql.Order.build(selecto)
  end
end
