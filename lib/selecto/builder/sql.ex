defmodule Selecto.Builder.Sql do
  @moduledoc """
  Main SQL compilation entry point for a configured `Selecto` query.

  This module orchestrates the full query pipeline by composing SELECT, FROM,
  JOIN, WHERE, GROUP BY, ORDER BY, LIMIT/OFFSET, and optional advanced features
  such as CTEs, retarget queries, set operations, lateral joins, and UNNEST clauses.

  The public `build/2` function returns `{sql, aliases, params}` where `sql` is
  the finalized query string and `params` is the ordered bind parameter list.
  """

  import Selecto.Builder.Sql.Helpers
  # import Selecto.Types - removed to avoid circular dependency

  alias Selecto.AdapterSQL
  alias Selecto.SQL.Params
  alias Selecto.Builder.CteSql, as: Cte
  alias Selecto.Builder.Sql.Hierarchy
  alias Selecto.Builder.LateralJoin
  alias Selecto.Builder.ValuesClause

  @spec build(Selecto.Types.t(), Selecto.Types.sql_generation_options()) ::
          {String.t(), [%{String.t() => String.t()}], [any()]}
  def build(selecto, opts) do
    # Check for Set Operations first as they completely override query structure
    cond do
      Selecto.Builder.SetOperations.has_set_operations?(selecto) ->
        build_set_operation_query(selecto, opts)

      Selecto.Retarget.has_retarget?(selecto) ->
        build_retarget_query(selecto, opts)

      true ->
        build_standard_query(selecto, opts)
    end
  end

  @doc false
  def benchmark_components(selecto) do
    {aliases, sel_joins, select_iodata, select_params} = build_select_with_subselects(selecto)

    {window_joins, window_iodata, window_params} =
      Selecto.Builder.Window.build_window_functions(selecto)

    {filter_joins, where_iolist, _where_params} = build_where(selecto)
    {group_by_joins, group_by_iodata, _group_by_params} = build_group_by(selecto)
    {order_by_joins, order_by_iodata, _order_by_params} = build_order_by(selecto)

    requested_joins =
      List.flatten(sel_joins ++ window_joins ++ filter_joins ++ group_by_joins ++ order_by_joins)

    joins_in_order = Selecto.Builder.Join.get_join_order(Selecto.joins(selecto), requested_joins)
    {from_iodata, from_params, required_ctes} = build_from_with_ctes(selecto, joins_in_order)

    values_ctes = build_values_clauses_as_ctes(selecto)

    user_ctes =
      case Map.get(selecto.set, :ctes) do
        nil -> []
        ctes when is_list(ctes) -> Enum.map(ctes, &convert_user_cte_spec/1)
      end

    all_required_ctes = required_ctes ++ values_ctes ++ user_ctes
    {lateral_join_iodata, lateral_join_params} = build_lateral_joins(selecto)
    {unnest_iodata, unnest_params} = build_unnest_operations(selecto)

    combined_from_iodata =
      combine_from_with_lateral_and_unnest(from_iodata, lateral_join_iodata, unnest_iodata)

    adapter = Map.get(selecto, :adapter, Selecto.AdapterSupport.default_adapter())

    {where_section, where_finalized_params} = finalize_section(where_iolist, adapter, :where)

    {group_by_section, group_by_finalized_params} =
      finalize_section(group_by_iodata, adapter, :group_by)

    is_rollup_query = group_by_section != "" and String.contains?(group_by_section, "rollup")

    {order_by_section, order_by_finalized_params} =
      finalize_order_section(selecto, order_by_iodata, adapter, is_rollup_query)

    combined_select_iodata =
      case window_iodata do
        [] -> select_iodata
        _ -> [select_iodata, ", ", window_iodata]
      end

    base_iodata = [
      "\n        select ",
      combined_select_iodata,
      "\n        from ",
      combined_from_iodata
    ]

    requires_order_for_pagination? = AdapterSQL.requires_order_for_pagination?(selecto)

    where_iodata_section =
      if where_section == "", do: [], else: ["\n        where ", where_iolist, "\n      "]

    group_by_iodata_section =
      if group_by_section == "",
        do: [],
        else: ["\n        group by ", group_by_iodata, "\n      "]

    order_by_iodata_section =
      cond do
        order_by_section != "" ->
          ["\n        order by ", order_by_iodata, "\n      "]

        requires_order_for_pagination? and
            (not is_nil(Map.get(selecto.set, :limit)) or not is_nil(Map.get(selecto.set, :offset))) ->
          ["\n        order by (select 1)\n      "]

        true ->
          []
      end

    {limit_iodata_section, offset_iodata_section} = pagination_sections(selecto, adapter)
    rollup_sort_fix_enabled? = rollup_sort_fix_enabled?(selecto)

    base_query_iodata =
      if group_by_section != "" and String.contains?(group_by_section, "rollup") and
           order_by_section != "" and rollup_sort_fix_enabled? do
        [
          "select * from (",
          base_iodata,
          where_iodata_section,
          group_by_iodata_section,
          ") as rollupfix",
          order_by_section,
          limit_iodata_section,
          offset_iodata_section
        ]
      else
        base_iodata ++
          where_iodata_section ++
          group_by_iodata_section ++
          order_by_iodata_section ++ limit_iodata_section ++ offset_iodata_section
      end

    all_base_params =
      select_params ++
        window_params ++
        from_params ++
        lateral_join_params ++
        unnest_params ++
        where_finalized_params ++ group_by_finalized_params ++ order_by_finalized_params

    {final_query_iodata, _cte_integrated_params} =
      Cte.integrate_ctes_with_query(
        all_required_ctes,
        base_query_iodata,
        all_base_params,
        adapter
      )

    %{
      aliases: aliases,
      requested_joins: requested_joins,
      joins_in_order: joins_in_order,
      from_iodata: from_iodata,
      combined_from_iodata: combined_from_iodata,
      final_query_iodata: final_query_iodata,
      adapter: adapter
    }
  end

  @doc false
  def benchmark_join_order(selecto, requested_joins) do
    Selecto.Builder.Join.get_join_order(Selecto.joins(selecto), requested_joins)
  end

  @doc false
  def benchmark_build_from(selecto, joins_in_order) do
    build_from_with_ctes(selecto, joins_in_order)
  end

  defp build_standard_query(selecto, opts) do
    # Phase 4: All SQL builders now use iodata parameterization.
    {aliases, sel_joins, select_iodata, select_params} = build_select_with_subselects(selecto)

    {window_joins, window_iodata, window_params} =
      Selecto.Builder.Window.build_window_functions(selecto)

    {filter_joins, where_iolist, _where_params} = build_where(selecto)
    {group_by_joins, group_by_iodata, _group_by_params} = build_group_by(selecto)
    {order_by_joins, order_by_iodata, _order_by_params} = build_order_by(selecto)

    joins_in_order =
      Selecto.Builder.Join.get_join_order(
        Selecto.joins(selecto),
        List.flatten(
          sel_joins ++ window_joins ++ filter_joins ++ group_by_joins ++ order_by_joins
        )
      )

    # Phase 1: Enhanced FROM builder with CTE detection
    {from_iodata, from_params, required_ctes} = build_from_with_ctes(selecto, joins_in_order)

    # Add VALUES clauses as CTEs
    values_ctes = build_values_clauses_as_ctes(selecto)

    # Add user-defined CTEs
    user_ctes =
      if Keyword.get(opts, :emit_user_ctes, true) do
        case Map.get(selecto.set, :ctes) do
          nil ->
            []

          ctes when is_list(ctes) ->
            Enum.map(ctes, &convert_user_cte_spec/1)
        end
      else
        []
      end

    all_required_ctes = required_ctes ++ values_ctes ++ user_ctes

    # Add LATERAL joins to FROM clause
    {lateral_join_iodata, lateral_join_params} = build_lateral_joins(selecto)

    # Add UNNEST operations to FROM clause
    {unnest_iodata, unnest_params} = build_unnest_operations(selecto)

    combined_from_iodata =
      combine_from_with_lateral_and_unnest(from_iodata, lateral_join_iodata, unnest_iodata)

    {where_section, where_finalized_params} =
      cond do
        where_iolist in [[], ["()"], "()"] ->
          {"", []}

        true ->
          {where_sql, where_sql_params} =
            Params.finalize(where_iolist,
              adapter: Map.get(selecto, :adapter, Selecto.AdapterSupport.default_adapter())
            )

          {"\n        where #{where_sql}\n      ", where_sql_params}
      end

    {group_by_section, group_by_finalized_params} =
      cond do
        group_by_iodata in [[], [""]] ->
          {"", []}

        true ->
          {group_by_sql, group_by_sql_params} =
            Params.finalize(group_by_iodata,
              adapter: Map.get(selecto, :adapter, Selecto.AdapterSupport.default_adapter())
            )

          {"\n        group by #{group_by_sql}\n      ", group_by_sql_params}
      end

    # Check if this is a rollup query to determine ORDER BY format
    is_rollup_query = group_by_section != "" and String.contains?(group_by_section, "rollup")

    {order_by_section, order_by_finalized_params} =
      cond do
        order_by_iodata in [[], [""]] ->
          {"", []}

        is_rollup_query ->
          # For rollup queries, check if we're already using literal positions
          # If so, the order_by_iodata should already be correct
          # Otherwise convert to use positions
          case selecto.set.order_by do
            [{:literal_position, _} | _] ->
              # Already using literal positions, finalize normally
              {order_by_sql, order_by_sql_params} =
                Params.finalize(order_by_iodata,
                  adapter: Map.get(selecto, :adapter, Selecto.AdapterSupport.default_adapter())
                )

              {"\n        order by #{order_by_sql}\n      ", order_by_sql_params}

            _ ->
              # Need to convert to literal positions for rollup
              # This shouldn't happen if aggregate view is properly configured
              # But as a fallback, use column positions 1, 2, 3...
              rollup_order_parts =
                selecto.set.order_by
                |> Enum.with_index(1)
                |> Enum.map(fn {_order_spec, index} ->
                  rollup_literal_order_sql(selecto, index)
                end)
                |> Enum.join(", ")

              if rollup_order_parts == "" do
                {"", []}
              else
                {"\n        order by #{rollup_order_parts}\n      ", []}
              end
          end

        true ->
          {order_by_sql, order_by_sql_params} =
            Params.finalize(order_by_iodata,
              adapter: Map.get(selecto, :adapter, Selecto.AdapterSupport.default_adapter())
            )

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
      "\n        select ",
      combined_select_iodata,
      "\n        from ",
      combined_from_iodata
    ]

    adapter = Map.get(selecto, :adapter, Selecto.AdapterSupport.default_adapter())
    requires_order_for_pagination? = AdapterSQL.requires_order_for_pagination?(selecto)

    # Convert sections to iodata
    where_iodata_section =
      if where_section == "", do: [], else: ["\n        where ", where_iolist, "\n      "]

    group_by_iodata_section =
      if group_by_section == "",
        do: [],
        else: ["\n        group by ", group_by_iodata, "\n      "]

    order_by_iodata_section =
      cond do
        order_by_section != "" ->
          ["\n        order by ", order_by_iodata, "\n      "]

        requires_order_for_pagination? and
            (not is_nil(Map.get(selecto.set, :limit)) or
               not is_nil(Map.get(selecto.set, :offset))) ->
          ["\n        order by (select 1)\n      "]

        true ->
          []
      end

    {limit_iodata_section, offset_iodata_section} =
      pagination_sections(selecto, adapter)

    # Build base query iodata
    rollup_sort_fix_enabled? = rollup_sort_fix_enabled?(selecto)

    base_query_iodata =
      if group_by_section != "" and String.contains?(group_by_section, "rollup") and
           order_by_section != "" and rollup_sort_fix_enabled? do
        # Rollup case: wrap in subquery
        [
          "select * from (",
          base_iodata,
          where_iodata_section,
          group_by_iodata_section,
          ") as rollupfix",
          order_by_section,
          limit_iodata_section,
          offset_iodata_section
        ]
      else
        # Normal case: combine all sections
        base_iodata ++
          where_iodata_section ++
          group_by_iodata_section ++
          order_by_iodata_section ++ limit_iodata_section ++ offset_iodata_section
      end

    # Phase 1: Integrate CTEs with main query
    all_base_params =
      select_params ++
        window_params ++
        from_params ++
        lateral_join_params ++
        unnest_params ++
        where_finalized_params ++ group_by_finalized_params ++ order_by_finalized_params

    {final_query_iodata, _cte_integrated_params} =
      Cte.integrate_ctes_with_query(
        all_required_ctes,
        base_query_iodata,
        all_base_params,
        adapter
      )

    # Phase 4: All parameters are now properly handled through iodata - no sentinel patterns remain
    {sql, final_params} =
      Params.finalize(final_query_iodata,
        adapter: adapter
      )

    # CTE params are already integrated into the iodata, so final_params contains everything
    # Don't double-count parameters
    {sql, aliases, final_params}
  end

  defp finalize_section(iodata, _adapter, _type) when iodata in [[], [""], ["()"], "", "()"],
    do: {"", []}

  defp finalize_section(iodata, adapter, _type) do
    Params.finalize(iodata, adapter: adapter)
  end

  defp finalize_order_section(selecto, order_by_iodata, adapter, is_rollup_query) do
    cond do
      order_by_iodata in [[], [""]] ->
        {"", []}

      is_rollup_query ->
        case selecto.set.order_by do
          [{:literal_position, _} | _] ->
            Params.finalize(order_by_iodata, adapter: adapter)

          _ ->
            rollup_order_parts =
              selecto.set.order_by
              |> Enum.with_index(1)
              |> Enum.map(fn {_order_spec, index} -> rollup_literal_order_sql(selecto, index) end)
              |> Enum.join(", ")

            if rollup_order_parts == "", do: {"", []}, else: {rollup_order_parts, []}
        end

      true ->
        Params.finalize(order_by_iodata, adapter: adapter)
    end
  end

  defp rollup_sort_fix_enabled?(%{config: config}) when is_map(config) do
    Map.get(config, :rollup_sort_fix, true)
  end

  defp rollup_sort_fix_enabled?(_), do: true

  defp rollup_literal_order_sql(selecto, index) do
    selecto
    |> AdapterSQL.rollup_literal_order(index)
    |> IO.iodata_to_binary()
  end

  defp json_builder_opts(selecto) do
    adapter = Map.get(selecto, :adapter, Selecto.AdapterSupport.default_adapter())

    if Selecto.AdapterSupport.adapter_name(adapter) in [:mssql, :mysql, :mariadb, :sqlite] do
      [adapter: adapter, table_alias: "selecto_root"]
    else
      [adapter: adapter]
    end
  end

  defp build_retarget_query(selecto, _opts) do
    # Use Retarget builder to construct the entire query
    retarget_config = Selecto.Retarget.get_retarget_config(selecto)

    # Build retarget-specific SELECT with subselects if needed
    # Pass retarget alias information to SELECT builder
    retarget_aliases = get_retarget_aliases(retarget_config)

    {aliases, sel_joins, select_iodata, select_params} =
      build_select_with_subselects(selecto, retarget_aliases)

    # Build retarget FROM clause and WHERE conditions
    {from_iodata, retarget_where_iodata, from_params, join_deps} =
      Selecto.Builder.Retarget.build_retarget_query(selecto, [])

    # Check if we have a CTE spec
    {cte_clause, cte_params} =
      case join_deps do
        [{:cte, cte_spec}] ->
          # Build the WITH clause
          cte_iodata = [
            "WITH ",
            cte_spec.name,
            " AS (\n        ",
            cte_spec.query,
            "\n        )\n        "
          ]

          {cte_iodata, cte_spec.params}

        _ ->
          {[], []}
      end

    # Build joins from the retarget target to other tables needed for selected columns.
    {join_iodata, join_params} = build_retarget_joins(selecto, sel_joins, retarget_config)

    # Assemble final query
    base_iodata =
      if cte_clause != [] do
        [cte_clause, "select ", select_iodata, "\n        from ", from_iodata]
      else
        ["\n        select ", select_iodata, "\n        from ", from_iodata]
      end

    # Add joins if needed
    base_iodata =
      if join_iodata != [] do
        base_iodata ++ [join_iodata]
      else
        base_iodata
      end

    final_iodata =
      if retarget_where_iodata != [] do
        base_iodata ++ ["\n        where ", retarget_where_iodata]
      else
        base_iodata
      end

    _all_params = select_params ++ cte_params ++ from_params ++ join_params

    {sql, final_params} =
      Params.finalize(final_iodata,
        adapter: Map.get(selecto, :adapter, Selecto.AdapterSupport.default_adapter())
      )

    {sql, aliases, final_params}
  end

  defp build_retarget_joins(selecto, sel_joins, retarget_config) do
    # After retargeting, add joins from the retarget target to any other tables
    # that are referenced in the selected columns

    # Filter out joins that are part of the retarget path (already in subquery)
    needed_joins =
      Enum.reject(sel_joins, fn join ->
        # Don't add joins that are part of the path to the retarget target
        join == :selecto_root || join == retarget_config.target_schema
      end)

    if needed_joins == [] do
      {[], []}
    else
      # Build JOIN clauses for the needed tables
      join_clauses =
        Enum.map(needed_joins, fn join_name ->
          build_retarget_join_clause(selecto, retarget_config.target_schema, join_name)
        end)

      {Enum.join(join_clauses, "\n        "), []}
    end
  end

  defp build_retarget_join_clause(selecto, from_schema, to_schema) do
    # Build a JOIN clause from the retarget target to another schema
    # Look up the association/join configuration

    # For film -> language, we need: LEFT JOIN language ON t.language_id = language.language_id
    case find_join_config_between(selecto, from_schema, to_schema) do
      {:ok, join_config} ->
        to_table = get_table_name(selecto, to_schema)
        owner_key = Map.get(join_config, :owner_key, "#{to_schema}_id")
        related_key = Map.get(join_config, :related_key, "#{to_schema}_id")

        [
          "\n        LEFT JOIN ",
          to_table,
          " ",
          to_string(to_schema),
          " ON t.",
          to_string(owner_key),
          " = ",
          to_string(to_schema),
          ".",
          to_string(related_key)
        ]

      _ ->
        # Fallback - try standard foreign key pattern
        to_table = get_table_name(selecto, to_schema)

        [
          "\n        LEFT JOIN ",
          to_table,
          " ",
          to_string(to_schema),
          " ON t.",
          to_string(to_schema),
          "_id = ",
          to_string(to_schema),
          ".",
          to_string(to_schema),
          "_id"
        ]
    end
  end

  defp find_join_config_between(selecto, from_schema, to_schema) do
    # Find the join configuration from from_schema to to_schema
    # First check if from_schema has a direct association to to_schema

    from_config =
      case from_schema do
        :source -> selecto.domain.source
        schema_name -> Map.get(selecto.domain.schemas, schema_name)
      end

    if from_config do
      associations = Map.get(from_config, :associations, %{})

      case Map.get(associations, to_schema) do
        nil ->
          # Also check in the joins structure
          joins = Map.get(selecto.domain, :joins, %{})
          find_join_in_structure(joins, from_schema, to_schema)

        config ->
          {:ok, config}
      end
    else
      {:error, :not_found}
    end
  end

  defp find_join_in_structure(joins, _from, to) when is_map(joins) do
    # Look for the target in the joins structure
    case Map.get(joins, to) do
      nil -> {:error, :not_found}
      config -> {:ok, config}
    end
  end

  defp find_join_in_structure(_, _, _), do: {:error, :not_found}

  defp get_table_name(selecto, schema_name) do
    # Get the actual table name for a schema
    case Map.get(selecto.domain.schemas, schema_name) do
      %{source_table: table} -> table
      _ -> to_string(schema_name)
    end
  end

  defp build_set_operation_query(selecto, _opts) do
    # Build set operations using the dedicated builder
    {set_op_iodata, _set_op_params} = Selecto.Builder.SetOperations.build_set_operations(selecto)

    # Check if we need to add ORDER BY to the entire set operation result
    {order_by_iodata, order_by_params} =
      if Selecto.Builder.SetOperations.should_apply_outer_order_by?(selecto) do
        {_order_by_joins, order_by_iodata_result, order_by_params_result} =
          build_order_by(selecto)

        order_iodata =
          if order_by_iodata_result != [], do: ["\nORDER BY ", order_by_iodata_result], else: []

        {order_iodata, order_by_params_result}
      else
        {[], []}
      end

    adapter = Map.get(selecto, :adapter, Selecto.AdapterSupport.default_adapter())
    {limit_iodata, offset_iodata} = pagination_sections(selecto, adapter, "\n")

    # Combine set operations with any outer ORDER BY/LIMIT/OFFSET
    final_iodata = [set_op_iodata] ++ order_by_iodata ++ limit_iodata ++ offset_iodata
    _all_params = order_by_params

    # Finalize the SQL
    {sql, final_params} =
      Selecto.SQL.Params.finalize(final_iodata,
        adapter: adapter
      )

    # For set operations, we don't return field aliases since the result schema
    # depends on the left query's structure
    aliases = %{}

    {sql, aliases, final_params}
  end

  defp pagination_sections(selecto, adapter, prefix \\ "\n        ") do
    limit_value = Map.get(selecto.set, :limit)
    offset_value = Map.get(selecto.set, :offset)

    if AdapterSQL.offset_fetch_pagination?(adapter) do
      pagination_iodata =
        cond do
          is_nil(limit_value) and is_nil(offset_value) ->
            []

          is_nil(limit_value) ->
            [prefix, "offset ", Integer.to_string(offset_value), " rows"]

          true ->
            [
              prefix,
              "offset ",
              Integer.to_string(offset_value || 0),
              " rows fetch next ",
              Integer.to_string(limit_value),
              " rows only"
            ]
        end

      {[], pagination_iodata}
    else
      limit_iodata =
        case limit_value do
          nil -> []
          value -> [prefix, "limit ", Integer.to_string(value)]
        end

      offset_iodata =
        case offset_value do
          nil -> []
          value -> [prefix, "offset ", Integer.to_string(value)]
        end

      {limit_iodata, offset_iodata}
    end
  end

  # Enhanced SELECT builder that includes subselects
  defp build_select_with_subselects(selecto) do
    build_select_with_subselects(selecto, %{})
  end

  defp build_select_with_subselects(selecto, retarget_aliases) do
    # Determine the source alias to use for subselect correlation
    source_alias = get_source_alias_for_subselects(retarget_aliases)

    # Build regular SELECT fields
    {aliases, sel_joins, select_iodata, select_params} = build_select(selecto, retarget_aliases)

    # Build JSON operations SELECT fields if they exist
    {json_select_clauses, json_select_params} =
      case Map.get(selecto.set, :json_selects) do
        nil ->
          {[], []}

        json_specs when is_list(json_specs) ->
          json_builder_opts = json_builder_opts(selecto)

          clauses =
            Enum.map(json_specs, fn spec ->
              Selecto.Builder.JsonOperations.build_json_select(spec, json_builder_opts)
            end)

          if clauses == [] do
            {[], []}
          else
            # JSON select builders currently return iodata expressions directly.
            {clauses, []}
          end
      end

    # Build Array operations SELECT fields if they exist
    {array_select_clauses, array_select_params} =
      case Map.get(selecto.set, :array_operations) do
        nil ->
          {[], []}

        array_specs when is_list(array_specs) ->
          array_specs
          |> Enum.filter(fn spec -> not Selecto.Advanced.ArrayOperations.is_unnest?(spec) end)
          |> Enum.map(fn spec ->
            Selecto.Advanced.ArrayOperations.to_sql(spec, [], selecto)
          end)
          |> Enum.unzip()
          |> case do
            {[], []} -> {[], []}
            {clauses, params} -> {clauses, List.flatten(params)}
          end
      end

    # Add subselect fields if they exist
    if Selecto.Subselect.has_subselects?(selecto) do
      {subselect_clauses, subselect_params} =
        Selecto.Builder.Subselect.build_subselect_clauses(selecto, source_alias)

      # Combine regular, JSON, array, and subselect fields
      all_select_parts =
        [select_iodata, json_select_clauses, array_select_clauses, subselect_clauses]
        |> Enum.reject(&(&1 == []))

      combined_select =
        if length(all_select_parts) > 1 do
          Enum.intersperse(all_select_parts, ", ") |> List.flatten()
        else
          List.flatten(all_select_parts)
        end

      {aliases, sel_joins, combined_select,
       select_params ++ json_select_params ++ array_select_params ++ subselect_params}
    else
      # No subselects, but might have JSON or array operations
      extra_clauses = json_select_clauses ++ array_select_clauses

      if extra_clauses != [] do
        combined_select =
          if select_iodata != [] do
            [select_iodata, ", "] ++ Enum.intersperse(extra_clauses, ", ")
          else
            extra_clauses
          end

        {aliases, sel_joins, combined_select,
         select_params ++ json_select_params ++ array_select_params}
      else
        {aliases, sel_joins, select_iodata, select_params}
      end
    end
  end

  defp get_retarget_aliases(retarget_config) do
    # Extract table aliases from retarget configuration.
    # The retarget builder uses "t" for target table and "s" for source table.
    if retarget_config do
      target_schema = Map.get(retarget_config, :target_schema)

      %{
        # Target table alias
        target_schema => "t",
        # Source table alias (if needed)
        :source => "s"
      }
    else
      %{}
    end
  end

  defp get_source_alias_for_subselects(retarget_aliases) do
    # If we have retarget aliases, use the target alias for correlation; otherwise use default.
    if retarget_aliases != %{} do
      # In retarget context, correlate with the main query's target table.
      target_schema = Map.keys(retarget_aliases) |> Enum.find(fn k -> k != :source end)
      Map.get(retarget_aliases, target_schema, "selecto_root")
    else
      "selecto_root"
    end
  end

  # Phase 4: SELECT now uses iodata by default.

  @spec build_where(Selecto.Types.t()) ::
          {Selecto.Types.join_dependencies(), Selecto.Types.iodata_with_markers(),
           Selecto.Types.sql_params()}
  defp build_where(selecto) do
    # Combine regular filters with JSON and array filters
    # Filter out any bucket_ranges strings that shouldn't be filters
    set_filters =
      selecto.set.filtered
      |> Enum.reject(fn
        filter when is_binary(filter) ->
          # Check if this looks like a bucket range string
          String.match?(filter, ~r/^\d+-\d+,\d+\+$|^\d+,\d+-\d+|\d+\+/)

        _ ->
          false
      end)

    domain_required_filters = Map.get(Selecto.domain(selecto), :required_filters, [])
    set_required_filters = Map.get(selecto.set, :required_filters, [])

    regular_filters =
      (domain_required_filters ++ set_required_filters ++ set_filters)
      |> Enum.uniq()

    # Add JSON filters if they exist
    json_filters =
      case Map.get(selecto.set, :json_filters) do
        nil ->
          []

        json_specs when is_list(json_specs) ->
          json_builder_opts = json_builder_opts(selecto)

          Enum.map(json_specs, fn spec ->
            {:raw_sql_filter,
             Selecto.Builder.JsonOperations.build_json_filter(spec, json_builder_opts)}
          end)
      end

    # Add Array filters if they exist
    array_filters =
      case Map.get(selecto.set, :array_filters) do
        nil ->
          []

        array_specs when is_list(array_specs) ->
          # Wrap array specs in a tuple to identify them in WHERE builder
          Enum.map(array_specs, fn spec ->
            {:array_filter, spec}
          end)
      end

    all_filters = regular_filters ++ json_filters ++ array_filters

    Selecto.Builder.Sql.Where.build(selecto, {:and, all_filters})
  end

  @spec build_group_by(Selecto.Types.t()) ::
          {Selecto.Types.join_dependencies(), Selecto.Types.iodata_with_markers(),
           Selecto.Types.sql_params()}
  defp build_group_by(selecto) do
    Selecto.Builder.Sql.Group.build(selecto)
  end

  @spec build_order_by(Selecto.Types.t()) ::
          {Selecto.Types.join_dependencies(), Selecto.Types.iodata_with_markers(),
           Selecto.Types.sql_params()}
  defp build_order_by(selecto) do
    # Build regular ORDER BY clauses
    {order_joins, order_iodata, order_params} = Selecto.Builder.Sql.Order.build(selecto)

    # Build JSON ORDER BY clauses if they exist
    {json_order_joins, json_order_iodata, json_order_params} =
      case Map.get(selecto.set, :json_order_by) do
        nil ->
          {[], [], []}

        json_sorts when is_list(json_sorts) ->
          json_builder_opts = json_builder_opts(selecto)

          json_sorts
          |> Enum.map(fn {spec, direction} ->
            json_sql = Selecto.Builder.JsonOperations.build_json_select(spec, json_builder_opts)

            dir_str =
              case direction do
                :desc -> " desc"
                _ -> " asc"
              end

            {[], [json_sql, dir_str], []}
          end)
          |> Enum.reduce({[], [], []}, fn {j, c, p}, {acc_j, acc_c, acc_p} ->
            {Enum.reverse(j, acc_j), [c | acc_c], Enum.reverse(p, acc_p)}
          end)
          |> then(fn {joins, clauses, params} ->
            {Enum.reverse(joins), Enum.reverse(clauses), Enum.reverse(params)}
          end)
      end

    # Combine regular and JSON ORDER BY clauses
    all_joins = order_joins ++ json_order_joins

    all_iodata =
      if order_iodata != [] and json_order_iodata != [] do
        order_iodata ++ [", "] ++ Enum.intersperse(json_order_iodata, ", ")
      else
        order_iodata ++ json_order_iodata
      end

    all_params = order_params ++ json_order_params

    {all_joins, all_iodata, all_params}
  end

  # Phase 4: SELECT now uses iodata by default
  defp build_select(selecto, retarget_aliases) do
    {aliases, joins, selects_iodata, params} =
      selecto.set.selected
      |> Enum.map(fn s -> Selecto.Builder.Sql.Select.build(selecto, s, retarget_aliases) end)
      |> Enum.reduce(
        {[], [], [], []},
        fn {select_iodata, j, p, as}, {aliases, joins, selects, params} ->
          {[as | aliases], [j | joins], [select_iodata | selects], Enum.reverse(p, params)}
        end
      )

    aliases = Enum.reverse(aliases)
    joins = Enum.reverse(joins)
    selects_iodata = Enum.reverse(selects_iodata)
    params = Enum.reverse(params)

    # SELECT clauses are now native iodata, just intersperse with commas
    final_select_iodata = Enum.intersperse(selects_iodata, ", ")

    {aliases, joins, final_select_iodata, params}
  end

  # Phase 1: Enhanced FROM builder with CTE detection and hierarchy support
  defp build_from_with_ctes(selecto, joins) do
    joins_config = Selecto.joins(selecto)

    case build_basic_from_with_ctes(selecto, joins, joins_config) do
      {:ok, result} -> result
      :generic -> build_from_with_ctes_generic(selecto, joins, joins_config)
    end
  end

  defp build_from_with_ctes_generic(selecto, joins, joins_config) do
    Enum.reduce(joins, {[], [], []}, fn
      :selecto_root, {fc, p, ctes} ->
        root_table = Selecto.source_table(selecto)
        # Quote both the table name and alias for the adapter
        quoted_table = quote_identifier(selecto, root_table)
        root_alias = build_join_string(selecto, "selecto_root")
        {fc ++ [[quoted_table, " ", root_alias]], p, ctes}

      join, {fc, p, ctes} ->
        config = joins_config[join]

        # Skip if join doesn't exist in config
        if config == nil do
          {fc, p, ctes}
        else
          if Map.get(config, :join_type) == :subquery do
            build_subquery_join(selecto, join, config, fc, p, ctes)
          else
            case detect_advanced_join_pattern(config) do
              {:hierarchy, pattern} ->
                Hierarchy.build_hierarchy_join_with_cte(
                  selecto,
                  join,
                  config,
                  pattern,
                  fc,
                  p,
                  ctes
                )

              {:tagging, _} ->
                build_tagging_join(selecto, join, config, fc, p, ctes)

              {:olap, type} ->
                build_olap_join(selecto, join, config, type, fc, p, ctes)

              {:enhanced, join_type} ->
                build_enhanced_join(selecto, join, config, join_type, fc, p, ctes)

              :basic ->
                # Existing basic join logic
                join_iodata = [
                  sql_join_keyword(config),
                  quote_identifier(selecto, config.source),
                  " ",
                  build_join_string(selecto, join),
                  " on ",
                  build_join_on_clause(selecto, join, config)
                ]

                {fc ++ [join_iodata], p, ctes}
            end
          end
        end
    end)
  end

  defp build_basic_from_with_ctes(selecto, joins, joins_config) do
    joins
    |> Enum.reduce_while([], fn
      :selecto_root, acc ->
        root_table = Selecto.source_table(selecto)
        quoted_table = quote_identifier(selecto, root_table)
        root_alias = build_join_string(selecto, "selecto_root")
        {:cont, [[quoted_table, " ", root_alias] | acc]}

      join, acc ->
        case joins_config[join] do
          nil ->
            {:cont, acc}

          %{join_type: :subquery} ->
            {:halt, :generic}

          config ->
            if detect_advanced_join_pattern(config) == :basic do
              join_iodata = [
                sql_join_keyword(config),
                quote_identifier(selecto, config.source),
                " ",
                build_join_string(selecto, join),
                " on ",
                build_join_on_clause(selecto, join, config)
              ]

              {:cont, [join_iodata | acc]}
            else
              {:halt, :generic}
            end
        end
    end)
    |> case do
      :generic -> :generic
      from_clauses -> {:ok, {Enum.reverse(from_clauses), [], []}}
    end
  end

  # Dynamic subquery joins registered by Selecto.DynamicJoin.join_subquery/4.
  # `config.subquery` is SQL text with $n placeholders and `config.subquery_params`
  # carries values; convert placeholders to iodata params so numbering is coordinated.
  defp build_subquery_join(selecto, join, config, fc, p, ctes) do
    subquery_sql =
      Map.get(config, :subquery) ||
        raise ArgumentError, "Subquery join #{inspect(join)} is missing :subquery SQL"

    subquery_params = Map.get(config, :subquery_params, [])
    subquery_iodata = convert_sql_placeholders_to_iodata(subquery_sql, subquery_params)

    on_conditions = Map.get(config, :on, [])
    requires_join = Map.get(config, :requires_join, :selecto_root)

    on_iodata = build_on_conditions(selecto, join, requires_join, on_conditions)

    join_iodata = [
      sql_join_keyword(config),
      "(",
      subquery_iodata,
      ") ",
      build_join_string(selecto, join),
      " on ",
      on_iodata
    ]

    {fc ++ [join_iodata], p ++ subquery_params, ctes}
  end

  defp build_on_conditions(_selecto, _join, _requires_join, []),
    do: raise(ArgumentError, "Subquery join requires at least one :on condition")

  defp build_on_conditions(selecto, join, requires_join, on_conditions)
       when is_list(on_conditions) do
    on_conditions
    |> Enum.map(fn
      %{left: left, right: right, operator: operator} ->
        [
          resolve_on_side(selecto, requires_join, left),
          " ",
          to_string(operator),
          " ",
          resolve_on_side(selecto, join, right)
        ]

      %{left: left, right: right} ->
        [
          resolve_on_side(selecto, requires_join, left),
          " = ",
          resolve_on_side(selecto, join, right)
        ]

      other ->
        raise ArgumentError, "Invalid :on condition for join: #{inspect(other)}"
    end)
    |> Enum.intersperse(" and ")
  end

  defp convert_sql_placeholders_to_iodata(sql, params) do
    values_by_index =
      params
      |> Enum.with_index(1)
      |> Map.new(fn {value, idx} -> {idx, value} end)

    Regex.split(~r/(\$\d+)/, sql, include_captures: true, trim: false)
    |> Enum.map(fn part ->
      case Regex.run(~r/^\$(\d+)$/, part, capture: :all_but_first) do
        [idx] ->
          case Map.fetch(values_by_index, String.to_integer(idx)) do
            {:ok, value} -> {:param, value}
            :error -> part
          end

        _ ->
          part
      end
    end)
  end

  # Phase 1: Join pattern detection for advanced join types
  defp detect_advanced_join_pattern(config) do
    case Map.get(config, :join_type) do
      :hierarchical_adjacency ->
        {:hierarchy, :adjacency_list}

      :hierarchical_materialized_path ->
        {:hierarchy, :materialized_path}

      :hierarchical_closure_table ->
        {:hierarchy, :closure_table}

      :many_to_many ->
        {:tagging, nil}

      :star_dimension ->
        {:olap, :star}

      :snowflake_dimension ->
        {:olap, :snowflake}

      join_type
      when join_type in [
             :self_join,
             :lateral_join,
             :cross_join,
             :full_outer_join,
             :conditional_join
           ] ->
        {:enhanced, join_type}

      _ ->
        :basic
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
          sql_join_keyword(config),
          quote_identifier(selecto, config.source),
          " ",
          build_join_string(selecto, join),
          " on ",
          build_join_on_clause(selecto, join, config)
        ]

        {fc ++ [join_iodata], p, ctes}

      enhanced_join_iodata ->
        # Use the enhanced join SQL
        {fc ++ [enhanced_join_iodata], p, ctes}
    end
  end

  # Note: Using existing helper functions from Selecto.Builder.Sql.Helpers
  # build_join_string/2 and build_selector_string/3 are imported at the top of the module

  defp sql_join_keyword(config) do
    case Map.get(config, :type, :left) do
      :inner -> " inner join "
      :right -> " right join "
      :full -> " full join "
      :cross -> " cross join "
      "inner" -> " inner join "
      "right" -> " right join "
      "full" -> " full join "
      "cross" -> " cross join "
      _ -> " left join "
    end
  end

  defp build_join_on_clause(selecto, join, config) do
    base_on =
      case Map.get(config, :on, []) do
        on_conditions when is_list(on_conditions) and on_conditions != [] ->
          requires_join = Map.get(config, :requires_join, :selecto_root)
          build_on_conditions(selecto, join, requires_join, on_conditions)

        _ ->
          [
            build_selector_string(selecto, join, config.my_key),
            " = ",
            build_selector_string(selecto, config.requires_join, config.owner_key)
          ]
      end

    append_param_filters_to_on_clause(
      selecto,
      join,
      base_on,
      Map.get(config, :param_filters, %{})
    )
  end

  defp append_param_filters_to_on_clause(_selecto, _join, base_on, nil), do: base_on

  defp append_param_filters_to_on_clause(_selecto, _join, base_on, param_filters)
       when map_size(param_filters) == 0,
       do: base_on

  defp append_param_filters_to_on_clause(selecto, join, base_on, param_filters)
       when is_map(param_filters) do
    filter_clauses =
      param_filters
      |> Enum.sort_by(fn {field, _value} -> to_string(field) end)
      |> Enum.map(fn {field, value} -> build_param_filter_clause(selecto, join, field, value) end)

    [base_on, " and ", Enum.intersperse(filter_clauses, " and ")]
  end

  defp build_param_filter_clause(selecto, join, field, nil) do
    [build_selector_string(selecto, join, field), " is null"]
  end

  defp build_param_filter_clause(selecto, join, field, {:not_eq, value}) do
    [build_selector_string(selecto, join, field), " != ", {:param, value}]
  end

  defp build_param_filter_clause(selecto, join, field, {:gt, value}) do
    [build_selector_string(selecto, join, field), " > ", {:param, value}]
  end

  defp build_param_filter_clause(selecto, join, field, {:gte, value}) do
    [build_selector_string(selecto, join, field), " >= ", {:param, value}]
  end

  defp build_param_filter_clause(selecto, join, field, {:lt, value}) do
    [build_selector_string(selecto, join, field), " < ", {:param, value}]
  end

  defp build_param_filter_clause(selecto, join, field, {:lte, value}) do
    [build_selector_string(selecto, join, field), " <= ", {:param, value}]
  end

  defp build_param_filter_clause(selecto, join, field, {:between, min_value, max_value}) do
    [
      build_selector_string(selecto, join, field),
      " between ",
      {:param, min_value},
      " and ",
      {:param, max_value}
    ]
  end

  defp build_param_filter_clause(selecto, join, field, {:in, values}) when is_list(values) do
    if values == [] do
      "1 = 0"
    else
      [
        build_selector_string(selecto, join, field),
        " in (",
        Enum.intersperse(Enum.map(values, fn value -> {:param, value} end), ", "),
        ")"
      ]
    end
  end

  defp build_param_filter_clause(selecto, join, field, value) do
    [build_selector_string(selecto, join, field), " = ", {:param, value}]
  end

  defp resolve_on_side(selecto, default_join, field_ref) when is_binary(field_ref) do
    if String.contains?(field_ref, ".") do
      field_ref
    else
      build_selector_string(selecto, default_join, field_ref)
    end
  end

  defp resolve_on_side(selecto, default_join, field_ref) when is_atom(field_ref) do
    build_selector_string(selecto, default_join, field_ref)
  end

  defp resolve_on_side(_selecto, _default_join, field_ref), do: to_string(field_ref)

  # Phase 4: LATERAL join integration functions
  defp build_lateral_joins(selecto) do
    lateral_specs = Map.get(selecto.set, :lateral_joins, [])
    adapter = Map.get(selecto, :adapter, Selecto.AdapterSupport.default_adapter())

    case lateral_specs do
      [] -> {[], []}
      specs -> LateralJoin.build_lateral_joins(specs, adapter: adapter, selecto: selecto)
    end
  end

  defp combine_from_with_lateral_and_unnest(from_iodata, lateral_join_iodata, unnest_iodata) do
    all_joins = lateral_join_iodata ++ unnest_iodata

    case all_joins do
      [] -> from_iodata
      joins -> from_iodata ++ [" "] ++ Enum.intersperse(joins, " ")
    end
  end

  defp build_unnest_operations(selecto) do
    case Map.get(selecto.set, :unnest, []) do
      [] ->
        {[], []}

      specs ->
        {unnest_clauses, params} =
          specs
          |> Enum.map(&build_single_unnest(selecto, &1))
          |> Enum.reduce({[], []}, fn {clause, p}, {clauses, params} ->
            {[clause | clauses], Enum.reverse(p, params)}
          end)

        unnest_clauses = Enum.reverse(unnest_clauses)
        params = Enum.reverse(params)

        # Format as comma-separated FROM clause additions  
        {unnest_clauses, params}
    end
  end

  defp build_single_unnest(selecto, %{field: field, alias: alias_name, ordinality: ordinality}) do
    {field_iodata, field_params} =
      case field do
        f when is_binary(f) ->
          # Simple field reference - use adapter-aware quoting
          {
            [
              force_quote_identifier(selecto, "selecto_root"),
              ".",
              force_quote_identifier(selecto, field)
            ],
            []
          }

        {:array, _} = array_expr ->
          # Array construction expression
          Selecto.Builder.Sql.Select.prep_selector(selecto, array_expr)
      end

    unnest_clause =
      case ordinality do
        nil ->
          ["CROSS JOIN LATERAL UNNEST(", field_iodata, ") AS ", alias_name]

        ord_alias ->
          [
            "CROSS JOIN LATERAL UNNEST(",
            field_iodata,
            ") WITH ORDINALITY AS ",
            alias_name,
            "(value, ",
            ord_alias,
            ")"
          ]
      end

    {unnest_clause, field_params}
  end

  # Convert user CTE spec to builder format
  defp convert_user_cte_spec(%{type: :recursive} = spec) do
    %Selecto.Advanced.CTE.Spec{
      name: spec.name,
      type: :recursive,
      base_query: spec.base_query,
      recursive_query: spec.recursive_query,
      validated: true,
      dependencies: Map.get(spec, :dependencies, []),
      columns: Map.get(spec, :columns)
    }
  end

  defp convert_user_cte_spec(%{type: type} = spec) when type in [:normal, nil] do
    %Selecto.Advanced.CTE.Spec{
      name: spec.name,
      type: :normal,
      query_builder: spec.query_builder || spec.query,
      validated: true,
      columns: Map.get(spec, :columns),
      dependencies: Map.get(spec, :dependencies, [])
    }
  end

  # Phase 4.2: VALUES clause integration as CTEs
  defp build_values_clauses_as_ctes(selecto) do
    values_specs = Map.get(selecto.set, :values_clauses, [])
    adapter = Map.get(selecto, :adapter, Selecto.AdapterSupport.default_adapter())

    Enum.map(values_specs, fn spec ->
      values_cte_sql = ValuesClause.build_values_cte(spec, adapter)
      # Raw CTE entry handled directly by Selecto.Builder.CteSql.
      {:raw_cte, values_cte_sql, []}
    end)
  end

  # Phase 1: Legacy join builders removed - replaced with CTE-enhanced versions above
  # Phase 2+: Full advanced join functionality will be implemented in specialized modules
end
