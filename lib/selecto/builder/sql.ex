defmodule Selecto.Builder.Sql do
  import Selecto.Builder.Sql.Helpers
  alias Selecto.SQL.Params
  alias Selecto.Builder.Cte
  alias Selecto.Builder.Sql.Hierarchy

  def build(selecto, _opts) do
    # Phase 4: All SQL builders now use iodata parameterization (no legacy functions remain)
    {aliases, sel_joins, select_iodata, select_params} = build_select(selecto)
    {filter_joins, where_iolist, _where_params} = build_where(selecto)
    {group_by_joins, group_by_iodata, _group_by_params} = build_group_by(selecto)
    {order_by_joins, order_by_iodata, _order_by_params} = build_order_by(selecto)

    joins_in_order =
      Selecto.Builder.Join.get_join_order(
        Selecto.joins(selecto),
        List.flatten(sel_joins ++ filter_joins ++ group_by_joins ++ order_by_joins)
      )

    # Phase 1: Enhanced FROM builder with CTE detection  
    {from_iodata, from_params, required_ctes} = build_from_with_ctes(selecto, joins_in_order)

    {where_section, where_finalized_params} =
      cond do
        where_iolist in [[], ["()"], "()"] -> {"", []}
        true ->
          {where_sql, where_sql_params} = Params.finalize(where_iolist)
          {"\n        where #{where_sql}\n      ", where_sql_params}
      end

    {group_by_section, group_by_finalized_params} =
      cond do
        group_by_iodata in [[], [""]] -> {"", []}
        true ->
          {group_by_sql, group_by_sql_params} = Params.finalize(group_by_iodata)
          {"\n        group by #{group_by_sql}\n      ", group_by_sql_params}
      end

    {order_by_section, order_by_finalized_params} =
      cond do
        order_by_iodata in [[], [""]] -> {"", []}
        true ->
          {order_by_sql, order_by_sql_params} = Params.finalize(order_by_iodata)
          {"\n        order by #{order_by_sql}\n      ", order_by_sql_params}
      end

    # Phase 4: Build complete iodata structure - all SQL clauses converted
    base_iodata = [
      "\n        select ", select_iodata,
      "\n        from ", from_iodata
    ]

    # Convert sections to iodata
    where_iodata_section = if where_section == "", do: [], else: ["\n        where ", where_iolist, "\n      "]
    group_by_iodata_section = if group_by_section == "", do: [], else: ["\n        group by ", group_by_iodata, "\n      "]
    order_by_iodata_section = if order_by_section == "", do: [], else: ["\n        order by ", order_by_iodata, "\n      "]

    # Build base query iodata
    base_query_iodata =
      if group_by_section != "" and String.contains?(group_by_section, "rollup") and order_by_section != "" do
        # Rollup case: wrap in subquery
        ["select * from (", base_iodata, where_iodata_section, group_by_iodata_section, ") as rollupfix", order_by_iodata_section]
      else
        # Normal case: combine all sections
        base_iodata ++ where_iodata_section ++ group_by_iodata_section ++ order_by_iodata_section
      end

    # Phase 1: Integrate CTEs with main query
    all_base_params = select_params ++ from_params ++ where_finalized_params ++ group_by_finalized_params ++ order_by_finalized_params
    {final_query_iodata, cte_integrated_params} = 
      Cte.integrate_ctes_with_query(required_ctes, base_query_iodata, all_base_params)
    
    # Phase 4: All parameters are now properly handled through iodata - no sentinel patterns remain
    {sql, final_params} = Params.finalize(final_query_iodata)

    # Combine parameters in correct order - CTE params already integrated
    final_all_params = cte_integrated_params ++ final_params

    {sql, aliases, final_all_params}
  end

  # Phase 4: All legacy string-based functions removed - only iodata functions remain

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

  # Phase 4: SELECT now uses iodata by default
  defp build_select(selecto) do
    {aliases, joins, selects_iodata, params} =
      selecto.set.selected
      |> Enum.map(fn s -> Selecto.Builder.Sql.Select.build(selecto, s) end)
      |> Enum.reduce(
        {[], [], [], []},
        fn {select_iodata, j, p, as}, {aliases, joins, selects, params} ->
          {aliases ++ [as], joins ++ [j], selects ++ [select_iodata], params ++ p}
        end
      )

    # SELECT clauses are now native iodata, just intersperse with commas
    final_select_iodata = Enum.intersperse(selects_iodata, ", ")

    {aliases, joins, final_select_iodata, List.flatten(params)}
  end

  # Phase 1: Enhanced FROM builder with CTE detection and hierarchy support
  defp build_from_with_ctes(selecto, joins) do
    Enum.reduce(joins, {[], [], []}, fn 
      :selecto_root, {fc, p, ctes} ->
        root_table = Selecto.source_table(selecto)
        root_alias = build_join_string(selecto, "selecto_root")
        {fc ++ [[root_table, " ", root_alias]], p, ctes}
      
      join, {fc, p, ctes} ->
        config = Selecto.joins(selecto)[join]
        
        case detect_advanced_join_pattern(config) do
          {:hierarchy, pattern} -> 
            Hierarchy.build_hierarchy_join_with_cte(selecto, join, config, pattern, fc, p, ctes)
          
          {:tagging, _} -> 
            build_tagging_join(selecto, join, config, fc, p, ctes)
          
          {:olap, type} ->
            build_olap_join(selecto, join, config, type, fc, p, ctes)
            
          :basic ->
            # Existing basic join logic
            join_iodata = [
              " left join ", config.source, " ", build_join_string(selecto, join),
              " on ", build_selector_string(selecto, join, config.my_key),
              " = ", build_selector_string(selecto, config.requires_join, config.owner_key)
            ]
            {fc ++ [join_iodata], p, ctes}
        end
    end)
  end

  # Phase 1: Join pattern detection for advanced join types
  defp detect_advanced_join_pattern(config) do
    case Map.get(config, :join_type) do
      :hierarchical_adjacency -> {:hierarchy, :adjacency_list}
      :hierarchical_materialized_path -> {:hierarchy, :materialized_path}  
      :hierarchical_closure_table -> {:hierarchy, :closure_table}
      :many_to_many -> {:tagging, nil}
      :star_dimension -> {:olap, :star}
      :snowflake_dimension -> {:olap, :snowflake}
      _ -> :basic
    end
  end

  # Phase 3: Full many-to-many tagging implementation
  defp build_tagging_join(selecto, join, config, fc, p, ctes) do
    # Use the dedicated tagging builder for proper many-to-many handling
    alias Selecto.Builder.Sql.Tagging
    Tagging.build_tagging_join_with_aggregation(selecto, join, config, fc, p, ctes)
  end

  # Phase 4: Full OLAP dimension optimization implementation
  defp build_olap_join(selecto, join, config, olap_type, fc, p, ctes) do
    # Use the dedicated OLAP builder for star/snowflake schema optimization
    alias Selecto.Builder.Sql.Olap
    Olap.build_olap_join_with_optimization(selecto, join, config, olap_type, fc, p, ctes)
  end

  # Phase 1: Legacy join builders removed - replaced with CTE-enhanced versions above
  # Phase 2+: Full advanced join functionality will be implemented in specialized modules
end
