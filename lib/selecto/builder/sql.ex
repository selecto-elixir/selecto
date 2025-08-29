defmodule Selecto.Builder.Sql do
  import Selecto.Builder.Sql.Helpers
  # import Selecto.Types - removed to avoid circular dependency

  alias Selecto.SQL.Params
  alias Selecto.Builder.CTE, as: Cte
  alias Selecto.Builder.Sql.Hierarchy
  alias Selecto.Builder.LateralJoin
  alias Selecto.Builder.ValuesClause

  @spec build(Selecto.Types.t(), Selecto.Types.sql_generation_options()) :: {String.t(), [%{String.t() => String.t()}], [any()]}
  def build(selecto, _opts) do
    # Check for Set Operations first as they completely override query structure
    cond do
      Selecto.Builder.SetOperations.has_set_operations?(selecto) ->
        build_set_operation_query(selecto, _opts)
      
      Selecto.Pivot.has_pivot?(selecto) ->
        build_pivot_query(selecto, _opts)
        
      true ->
        build_standard_query(selecto, _opts)
    end
  end

  defp build_standard_query(selecto, _opts) do
    # Phase 4: All SQL builders now use iodata parameterization (no legacy functions remain)
    {aliases, sel_joins, select_iodata, select_params} = build_select_with_subselects(selecto)
    {window_joins, window_iodata, window_params} = Selecto.Builder.Window.build_window_functions(selecto)
    {filter_joins, where_iolist, _where_params} = build_where(selecto)
    {group_by_joins, group_by_iodata, _group_by_params} = build_group_by(selecto)
    {order_by_joins, order_by_iodata, _order_by_params} = build_order_by(selecto)

    joins_in_order =
      Selecto.Builder.Join.get_join_order(
        Selecto.joins(selecto),
        List.flatten(sel_joins ++ window_joins ++ filter_joins ++ group_by_joins ++ order_by_joins)
      )

    # Phase 1: Enhanced FROM builder with CTE detection
    {from_iodata, from_params, required_ctes} = build_from_with_ctes(selecto, joins_in_order)
    
    # Add VALUES clauses as CTEs
    values_ctes = build_values_clauses_as_ctes(selecto)
    
    # Add user-defined CTEs
    user_ctes = case Map.get(selecto.set, :ctes) do
      nil -> []
      ctes when is_list(ctes) -> ctes
    end
    
    all_required_ctes = required_ctes ++ values_ctes ++ user_ctes
    
    # Add LATERAL joins to FROM clause
    {lateral_join_iodata, lateral_join_params} = build_lateral_joins(selecto)
    combined_from_iodata = combine_from_with_lateral_joins(from_iodata, lateral_join_iodata)

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
    # Combine regular select fields with window functions
    combined_select_iodata = 
      case window_iodata do
        [] -> select_iodata
        _ -> [select_iodata, ", ", window_iodata]
      end
    
    base_iodata = [
      "\n        select ", combined_select_iodata,
      "\n        from ", combined_from_iodata
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
    all_base_params = select_params ++ window_params ++ from_params ++ lateral_join_params ++ where_finalized_params ++ group_by_finalized_params ++ order_by_finalized_params
    {final_query_iodata, _cte_integrated_params} =
      Cte.integrate_ctes_with_query(all_required_ctes, base_query_iodata, all_base_params)

    # Phase 4: All parameters are now properly handled through iodata - no sentinel patterns remain
    {sql, final_params} = Params.finalize(final_query_iodata)

    # CTE params are already integrated into the iodata, so final_params contains everything
    # Don't double-count parameters
    {sql, aliases, final_params}
  end

  defp build_pivot_query(selecto, _opts) do
    # Use Pivot builder to construct the entire query
    pivot_config = Selecto.Pivot.get_pivot_config(selecto)

    # Build pivot-specific SELECT with subselects if needed
    # Pass pivot alias information to SELECT builder
    pivot_aliases = get_pivot_aliases(pivot_config)
    {aliases, _sel_joins, select_iodata, select_params} = build_select_with_subselects(selecto, pivot_aliases)

    # Build pivot FROM clause and WHERE conditions
    {from_iodata, pivot_where_iodata, from_params, _join_deps} = Selecto.Builder.Pivot.build_pivot_query(selecto, [])

    # Assemble final query
    base_iodata = [
      "\n        select ", select_iodata,
      "\n        from ", from_iodata
    ]

    final_iodata = if pivot_where_iodata != [] do
      base_iodata ++ ["\n        where ", pivot_where_iodata]
    else
      base_iodata
    end

    all_params = select_params ++ from_params
    {sql, final_params} = Params.finalize(final_iodata)

    {sql, aliases, final_params}
  end

  defp build_set_operation_query(selecto, _opts) do
    # Build set operations using the dedicated builder
    {set_op_iodata, set_op_params} = Selecto.Builder.SetOperations.build_set_operations(selecto)
    
    # Check if we need to add ORDER BY to the entire set operation result
    order_by_iodata = []
    order_by_params = []
    
    if Selecto.Builder.SetOperations.should_apply_outer_order_by?(selecto) do
      {_order_by_joins, order_by_iodata_result, order_by_params_result} = build_order_by(selecto)
      order_by_iodata = if order_by_iodata_result != [], do: ["\nORDER BY ", order_by_iodata_result], else: []
      order_by_params = order_by_params_result
    end
    
    # Combine set operations with any outer ORDER BY
    final_iodata = [set_op_iodata] ++ order_by_iodata
    all_params = set_op_params ++ order_by_params
    
    # Finalize the SQL
    {sql, final_params} = Selecto.SQL.Params.finalize(final_iodata)
    
    # For set operations, we don't return field aliases since the result schema
    # depends on the left query's structure
    aliases = %{}
    
    {sql, aliases, final_params}
  end

  # Enhanced SELECT builder that includes subselects
  defp build_select_with_subselects(selecto) do
    build_select_with_subselects(selecto, %{})
  end

  defp build_select_with_subselects(selecto, pivot_aliases) do
    # Determine the source alias to use for subselect correlation
    source_alias = get_source_alias_for_subselects(pivot_aliases)

    # Build regular SELECT fields
    {aliases, sel_joins, select_iodata, select_params} = build_select(selecto, pivot_aliases)

    # Build JSON operations SELECT fields if they exist
    json_select_clauses = []
    json_select_params = []
    
    {json_select_clauses, json_select_params} = 
      case Map.get(selecto.set, :json_selects) do
        nil -> {[], []}
        json_specs when is_list(json_specs) ->
          json_specs
          |> Enum.map(&Selecto.Builder.JsonOperations.build_json_select/1)
          |> Enum.unzip()
          |> case do
            {[], []} -> {[], []}
            {clauses, params} -> {clauses, List.flatten(params)}
          end
      end

    # Add subselect fields if they exist
    if Selecto.Subselect.has_subselects?(selecto) do
      {subselect_clauses, subselect_params} = Selecto.Builder.Subselect.build_subselect_clauses(selecto, source_alias)

      # Combine regular, JSON, and subselect fields
      all_select_parts = [select_iodata, json_select_clauses, subselect_clauses] 
                        |> Enum.reject(&(&1 == []))
      
      combined_select = if length(all_select_parts) > 1 do
        Enum.intersperse(all_select_parts, ", ") |> List.flatten()
      else
        List.flatten(all_select_parts)
      end

      {aliases, sel_joins, combined_select, select_params ++ json_select_params ++ subselect_params}
    else
      # No subselects, but might have JSON operations
      if json_select_clauses != [] do
        combined_select = if select_iodata != [] do
          [select_iodata, ", "] ++ Enum.intersperse(json_select_clauses, ", ")
        else
          json_select_clauses
        end
        {aliases, sel_joins, combined_select, select_params ++ json_select_params}
      else
        {aliases, sel_joins, select_iodata, select_params}
      end
    end
  end

  defp build_pivot_where(selecto) do
    # Post-pivot filters are now handled within the pivot FROM clause
    # This function is kept for backward compatibility but returns empty
    # since filters are already incorporated into the pivot subquery
    {[], []}
  end

  defp get_target_alias, do: "t"

  defp escape_identifier(identifier) do
    # Escape SQL identifiers - simplified implementation
    "\"#{identifier}\""
  end

  defp get_pivot_aliases(pivot_config) do
    # Extract table aliases from pivot configuration
    # The pivot builder uses "t" for target table and "s" for source table
    if pivot_config do
      target_schema = Map.get(pivot_config, :target_schema)
      %{
        target_schema => "t",  # Target table alias
        :source => "s"         # Source table alias (if needed)
      }
    else
      %{}
    end
  end

  defp get_source_alias_for_subselects(pivot_aliases) do
    # If we have pivot aliases, use the target alias for correlation, otherwise use default
    if pivot_aliases != %{} do
      # In pivot context, correlate with the main query's target table
      target_schema = Map.keys(pivot_aliases) |> Enum.find(fn k -> k != :source end)
      Map.get(pivot_aliases, target_schema, "selecto_root")
    else
      "selecto_root"
    end
  end

  # Phase 4: All legacy string-based functions removed - only iodata functions remain

  @spec build_where(Selecto.Types.t()) :: {Selecto.Types.join_dependencies(), Selecto.Types.iodata_with_markers(), Selecto.Types.sql_params()}
  defp build_where(selecto) do
    # Combine regular filters with JSON filters
    regular_filters = Map.get(Selecto.domain(selecto), :required_filters, []) ++ selecto.set.filtered
    
    # Add JSON filters if they exist
    json_filters = case Map.get(selecto.set, :json_filters) do
      nil -> []
      json_specs when is_list(json_specs) ->
        Enum.map(json_specs, &Selecto.Builder.JsonOperations.build_json_filter/1)
    end
    
    all_filters = regular_filters ++ json_filters
    
    Selecto.Builder.Sql.Where.build(selecto, {:and, all_filters})
  end

  @spec build_group_by(Selecto.Types.t()) :: {Selecto.Types.join_dependencies(), Selecto.Types.iodata_with_markers(), Selecto.Types.sql_params()}
  defp build_group_by(selecto) do
    Selecto.Builder.Sql.Group.build(selecto)
  end

  @spec build_order_by(Selecto.Types.t()) :: {Selecto.Types.join_dependencies(), Selecto.Types.iodata_with_markers(), Selecto.Types.sql_params()}
  defp build_order_by(selecto) do
    # Build regular ORDER BY clauses
    {order_joins, order_iodata, order_params} = Selecto.Builder.Sql.Order.build(selecto)
    
    # Build JSON ORDER BY clauses if they exist
    {json_order_joins, json_order_iodata, json_order_params} = 
      case Map.get(selecto.set, :json_order_by) do
        nil -> {[], [], []}
        json_sorts when is_list(json_sorts) ->
          json_sorts
          |> Enum.map(fn {spec, direction} ->
            json_sql = Selecto.Builder.JsonOperations.build_json_select(spec)
            dir_str = case direction do
              :desc -> " desc"
              _ -> " asc"
            end
            {[], [json_sql, dir_str], []}
          end)
          |> Enum.reduce({[], [], []}, fn {j, c, p}, {acc_j, acc_c, acc_p} ->
            {acc_j ++ j, acc_c ++ [c], acc_p ++ p}
          end)
      end
    
    # Combine regular and JSON ORDER BY clauses
    all_joins = order_joins ++ json_order_joins
    all_iodata = if order_iodata != [] and json_order_iodata != [] do
      order_iodata ++ [", "] ++ Enum.intersperse(json_order_iodata, ", ")
    else
      order_iodata ++ json_order_iodata
    end
    all_params = order_params ++ json_order_params
    
    {all_joins, all_iodata, all_params}
  end

  # Phase 4: SELECT now uses iodata by default
  @spec build_select(Selecto.Types.t()) :: {[%{String.t() => String.t()}], Selecto.Types.join_dependencies(), Selecto.Types.iodata_with_markers(), Selecto.Types.sql_params()}
  defp build_select(selecto) do
    build_select(selecto, %{})
  end

  defp build_select(selecto, pivot_aliases) do
    {aliases, joins, selects_iodata, params} =
      selecto.set.selected
      |> Enum.map(fn s -> Selecto.Builder.Sql.Select.build(selecto, s, pivot_aliases) end)
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

          {:enhanced, join_type} ->
            build_enhanced_join(selecto, join, config, join_type, fc, p, ctes)

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
      join_type when join_type in [:self_join, :lateral_join, :cross_join, :full_outer_join, :conditional_join] -> {:enhanced, join_type}
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

  # Phase 3: Enhanced join builder implementation
  defp build_enhanced_join(selecto, join, config, _join_type, fc, p, ctes) do
    # Use the enhanced joins module to build SQL for new join types
    case Selecto.EnhancedJoins.build_enhanced_join_sql(config, selecto) do
      nil ->
        # Fallback to basic join if enhanced join fails
        join_iodata = [
          " left join ", config.source, " ", build_join_string(selecto, join),
          " on ", build_selector_string(selecto, join, config.my_key),
          " = ", build_selector_string(selecto, config.requires_join, config.owner_key)
        ]
        {fc ++ [join_iodata], p, ctes}

      enhanced_join_iodata ->
        # Use the enhanced join SQL
        {fc ++ [enhanced_join_iodata], p, ctes}
    end
  end

  # Note: Using existing helper functions from Selecto.Builder.Sql.Helpers
  # build_join_string/2 and build_selector_string/3 are imported at the top of the module

  # Phase 4: LATERAL join integration functions
  defp build_lateral_joins(selecto) do
    lateral_specs = Map.get(selecto.set, :lateral_joins, [])
    
    case lateral_specs do
      [] -> {[], []}
      specs -> LateralJoin.build_lateral_joins(specs)
    end
  end
  
  defp combine_from_with_lateral_joins(from_iodata, lateral_join_iodata) do
    case lateral_join_iodata do
      [] -> from_iodata
      lateral_joins -> from_iodata ++ [" "] ++ Enum.intersperse(lateral_joins, " ")
    end
  end

  # Phase 4.2: VALUES clause integration as CTEs
  defp build_values_clauses_as_ctes(selecto) do
    values_specs = Map.get(selecto.set, :values_clauses, [])
    
    Enum.map(values_specs, fn spec ->
      values_cte_sql = ValuesClause.build_values_cte(spec)
      # VALUES clauses don't have parameters in our simple implementation
      # but this structure is consistent with other CTE builders
      {values_cte_sql, []}
    end)
  end

  # Phase 1: Legacy join builders removed - replaced with CTE-enhanced versions above
  # Phase 2+: Full advanced join functionality will be implemented in specialized modules
end
