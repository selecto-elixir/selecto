defmodule Selecto.Builder.Sql do
  import Selecto.Builder.Sql.Helpers
  alias Selecto.SQL.Params

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

    {from_iodata, from_params} = build_from(selecto, joins_in_order)

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

    # Handle rollup special case in iodata
    final_iodata =
      if group_by_section != "" and String.contains?(group_by_section, "rollup") and order_by_section != "" do
        # Rollup case: wrap in subquery
        ["select * from (", base_iodata, where_iodata_section, group_by_iodata_section, ") as rollupfix", order_by_iodata_section]
      else
        # Normal case: combine all sections
        base_iodata ++ where_iodata_section ++ group_by_iodata_section ++ order_by_iodata_section
      end

    # Phase 4: All parameters are now properly handled through iodata - no sentinel patterns remain
    all_params = select_params ++ from_params ++ where_finalized_params ++ group_by_finalized_params ++ order_by_finalized_params
    {sql, final_params} = Params.finalize(final_iodata)

    # Combine parameters in correct order
    final_all_params = all_params ++ final_params

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

  # Phase 4: FROM builder using iodata (now the main and only implementation)
  defp build_from(selecto, joins) do
    Enum.reduce(joins, {[], []}, fn
      :selecto_root, {fc, p} ->
        root_table = Selecto.source_table(selecto)
        root_alias = build_join_string(selecto, "selecto_root")
        {fc ++ [[root_table, " ", root_alias]], p}

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
            join_iodata = [
              " left join ", config.source, " ", build_join_string(selecto, join),
              " on ", build_selector_string(selecto, join, config.my_key),
              " = ", build_selector_string(selecto, config.requires_join, config.owner_key)
            ]
            {fc ++ [join_iodata], p}
        end
    end)
  end


  # Phase 4: iodata join builders (legacy functions removed)
  defp build_many_to_many_join(selecto, join, config, fc, p) do
    join_iodata = [
      " left join ", config.source, " ", build_join_string(selecto, join),
      " on ", build_selector_string(selecto, join, config.my_key),
      " = ", build_selector_string(selecto, config.requires_join, config.owner_key)
    ]
    {fc ++ [join_iodata], p}
  end

  defp build_hierarchical_adjacency_join(selecto, join, config, fc, p) do
    join_iodata = [
      " left join ", config.source, " ", build_join_string(selecto, join),
      " on ", build_selector_string(selecto, join, config.my_key),
      " = ", build_selector_string(selecto, config.requires_join, config.owner_key)
    ]
    {fc ++ [join_iodata], p}
  end

  defp build_hierarchical_materialized_path_join(selecto, join, config, fc, p) do
    join_iodata = [
      " left join ", config.source, " ", build_join_string(selecto, join),
      " on ", build_selector_string(selecto, join, config.my_key),
      " = ", build_selector_string(selecto, config.requires_join, config.owner_key)
    ]
    {fc ++ [join_iodata], p}
  end

  defp build_hierarchical_closure_table_join(selecto, join, config, fc, p) do
    closure_table = Map.get(config, :closure_table)
    ancestor_field = Map.get(config, :ancestor_field, :ancestor_id)
    descendant_field = Map.get(config, :descendant_field, :descendant_id)

    closure_alias = build_join_string(selecto, "#{join}_closure")
    join_alias = build_join_string(selecto, join)

    join_iodata = [
      [" left join ", closure_table, " ", closure_alias,
       " on ", build_selector_string(selecto, config.requires_join, config.owner_key),
       " = ", closure_alias, ".", to_string(ancestor_field)],
      [" left join ", config.source, " ", join_alias,
       " on ", closure_alias, ".", to_string(descendant_field),
       " = ", build_selector_string(selecto, join, config.my_key)]
    ]
    {fc ++ join_iodata, p}
  end

  defp build_star_dimension_join(selecto, join, config, fc, p) do
    join_iodata = [
      " left join ", config.source, " ", build_join_string(selecto, join),
      " on ", build_selector_string(selecto, join, config.my_key),
      " = ", build_selector_string(selecto, config.requires_join, config.owner_key)
    ]
    {fc ++ [join_iodata], p}
  end

  defp build_snowflake_dimension_join(selecto, join, config, fc, p) do
    normalization_joins = Map.get(config, :normalization_joins, [])

    base_join = [
      " left join ", config.source, " ", build_join_string(selecto, join),
      " on ", build_selector_string(selecto, join, config.my_key),
      " = ", build_selector_string(selecto, config.requires_join, config.owner_key)
    ]

    additional_joins = Enum.map(normalization_joins, fn norm_join ->
      [" left join ", norm_join.table, " ", build_join_string(selecto, norm_join.alias),
       " on ", build_selector_string(selecto, join, norm_join.local_key),
       " = ", build_selector_string(selecto, norm_join.alias, norm_join.remote_key)]
    end)

    {fc ++ [base_join] ++ additional_joins, p}
  end
end
