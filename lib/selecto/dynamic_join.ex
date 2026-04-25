defmodule Selecto.DynamicJoin do
  @moduledoc """
  Handles dynamic join additions at runtime.

  This module enables adding joins to a Selecto query that aren't defined
  in the domain configuration. This is useful for:

  - Ad-hoc queries that need joins beyond the preconfigured domain
  - Programmatic query building where joins depend on runtime conditions
  - Parameterized joins where the same association needs different filters

  ## Examples

      # Enable a predefined join from the domain
      selecto |> Selecto.join(:category)

      # Enable a join with specific type override
      selecto |> Selecto.join(:orders, type: :inner)

      # Add a parameterized instance of a join
      selecto |> Selecto.join_parameterize(:products, "electronics", category: "Electronics")

      # Add a custom join not in the domain
      selecto |> Selecto.join(:audit_log,
        source: "audit_logs",
        on: [%{left: "id", right: "record_id"}],
        type: :left
      )
  """

  @doc """
  Enable a join from the domain configuration or add a custom join.

  ## Parameters

  - `selecto` - The Selecto struct
  - `join_id` - The join identifier (atom)
  - `options` - Optional configuration overrides

  ## Options

  - `:type` - Join type (:left, :inner, :right, :full). Default: :left
  - `:source` - Source table name (required for custom joins)
  - `:on` - Join conditions as list of maps with :left and :right keys
  - `:owner_key` - The key on the parent table
  - `:related_key` - The key on the joined table
  - `:fields` - Map of field configurations to expose from the joined table

  ## Examples

      # Enable domain-configured join
      selecto |> Selecto.DynamicJoin.join(:category)

      # Custom join with explicit configuration
      selecto |> Selecto.DynamicJoin.join(:audit_log,
        source: "audit_logs",
        on: [%{left: "id", right: "record_id"}],
        type: :left,
        fields: %{
          action: %{type: :string},
          created_at: %{type: :naive_datetime}
        }
      )
  """
  @spec join(Selecto.t(), atom(), keyword()) :: Selecto.t()
  def join(selecto, join_id, options \\ []) do
    # Check if this join already exists in the domain
    existing_join = get_in(selecto.config, [:joins, join_id])

    join_config =
      if existing_join do
        # Merge options into existing join config
        merge_join_options(existing_join, options)
      else
        # Create a new custom join config
        build_custom_join(selecto, join_id, options)
      end

    validate_cte_source!(selecto, join_id, join_config)
    join_config = maybe_enrich_cte_join(join_config, selecto)

    # Add to active joins in the set
    current_joins = Map.get(selecto.set, :active_joins, [])
    updated_joins = Enum.uniq(current_joins ++ [join_id])

    # Store the join config in dynamic_joins for SQL builder
    current_dynamic = Map.get(selecto.set, :dynamic_joins, %{})
    updated_dynamic = Map.put(current_dynamic, join_id, join_config)

    # Update the selecto struct
    selecto
    |> put_in([Access.key(:set), :active_joins], updated_joins)
    |> put_in([Access.key(:set), :dynamic_joins], updated_dynamic)
    |> register_dynamic_join_fields(join_id, join_config)
  end

  @doc """
  Create a parameterized instance of an existing join.

  Parameterized joins allow the same association to be joined multiple times
  with different filter conditions, useful for queries like:

  - "Products in Electronics category" vs "Products in Clothing category"
  - "Active orders" vs "Cancelled orders"
  - "Primary contacts" vs "Billing contacts"

  ## Parameters

  - `selecto` - The Selecto struct
  - `join_id` - Base join identifier to parameterize
  - `parameter` - Unique parameter value to identify this instance
  - `options` - Filter conditions and options

  ## Options

  - All options from `join/3` plus:
  - Any key that matches a filter in the join's filter config will be applied

  ## Examples

      # Create parameterized join for electronics products
      selecto
      |> Selecto.DynamicJoin.join_parameterize(:products, "electronics",
          category_id: 1,
          active: true
        )
      |> Selecto.select(["products:electronics.product_name"])

      # Multiple parameterized instances
      selecto
      |> Selecto.DynamicJoin.join_parameterize(:orders, "active", status: "active")
      |> Selecto.DynamicJoin.join_parameterize(:orders, "completed", status: "completed")
      |> Selecto.select([
          "orders:active.total as active_total",
          "orders:completed.total as completed_total"
        ])
  """
  @spec join_parameterize(Selecto.t(), atom(), String.t() | atom(), keyword()) :: Selecto.t()
  def join_parameterize(selecto, join_id, parameter, options \\ []) do
    # Create a unique identifier for this parameterized join
    param_join_id = :"#{join_id}:#{parameter}"

    # Get the base join configuration
    base_join = get_in(selecto.config, [:joins, join_id])

    unless base_join do
      raise ArgumentError,
            "Cannot parameterize join #{inspect(join_id)} - not found in domain configuration"
    end

    # Extract filter options vs regular options
    {filters, join_opts} = Keyword.split(options, extract_filter_keys(base_join))

    # Build parameterized join config
    param_config =
      base_join
      |> Map.put(:parameter, parameter)
      |> Map.put(:base_join, join_id)
      |> Map.put(:param_filters, Map.new(filters))
      |> merge_join_options(join_opts)

    # Update field names to use parameterized prefix
    param_config = update_field_names_for_param(param_config, param_join_id)

    # Add to active joins
    current_joins = Map.get(selecto.set, :active_joins, [])
    updated_joins = Enum.uniq(current_joins ++ [param_join_id])

    current_dynamic = Map.get(selecto.set, :dynamic_joins, %{})
    updated_dynamic = Map.put(current_dynamic, param_join_id, param_config)

    selecto
    |> put_in([Access.key(:set), :active_joins], updated_joins)
    |> put_in([Access.key(:set), :dynamic_joins], updated_dynamic)
    |> register_dynamic_join_fields(param_join_id, param_config)
  end

  @doc """
  Join with another Selecto query as a subquery.

  This creates a join using a separate Selecto query as the right side,
  enabling complex subquery joins.

  ## Parameters

  - `selecto` - The main Selecto struct
  - `join_id` - Identifier for this join
  - `join_selecto` - The Selecto struct to use as subquery
  - `options` - Join configuration

  ## Options

  - `:type` - Join type (:left, :inner, :right, :full). Default: :left
  - `:on` - Join conditions referencing the subquery alias

  ## Examples

      # Create a subquery for aggregated data
      order_totals = Selecto.configure(order_domain, connection)
      |> Selecto.select(["customer_id", {:sum, "total", as: "total_spent"}])
      |> Selecto.group_by(["customer_id"])

      # Join to main query
      selecto
      |> Selecto.DynamicJoin.join_subquery(:customer_totals, order_totals,
          on: [%{left: "customer_id", right: "customer_id"}]
        )
      |> Selecto.select(["name", "customer_totals.total_spent"])
  """
  @spec join_subquery(Selecto.t(), atom(), Selecto.t(), keyword()) :: Selecto.t()
  def join_subquery(selecto, join_id, join_selecto, options \\ []) do
    join_type = Keyword.get(options, :type, :left)
    on_conditions = Keyword.get(options, :on, [])

    # Generate SQL for the subquery
    {subquery_sql, _aliases, subquery_params} = Selecto.gen_sql(join_selecto, [])

    subquery_root_alias = build_subquery_root_alias(selecto, join_id, join_selecto)
    rewritten_subquery_sql = rewrite_subquery_root_alias(subquery_sql, subquery_root_alias)

    # Build subquery join config
    subquery_config = %{
      id: join_id,
      name: to_string(join_id),
      join_type: :subquery,
      subquery: rewritten_subquery_sql,
      subquery_params: subquery_params,
      subquery_root_alias: subquery_root_alias,
      on: on_conditions,
      type: join_type,
      requires_join: :selecto_root,
      # Extract field info from the subquery's selected aliases
      fields: extract_subquery_fields(join_selecto, join_id)
    }

    # Add to dynamic joins
    current_joins = Map.get(selecto.set, :active_joins, [])
    updated_joins = Enum.uniq(current_joins ++ [join_id])

    current_dynamic = Map.get(selecto.set, :dynamic_joins, %{})
    updated_dynamic = Map.put(current_dynamic, join_id, subquery_config)

    selecto
    |> put_in([Access.key(:set), :active_joins], updated_joins)
    |> put_in([Access.key(:set), :dynamic_joins], updated_dynamic)
    |> register_dynamic_join_fields(join_id, subquery_config)
  end

  # Private helpers

  defp merge_join_options(join_config, options) do
    Enum.reduce(options, join_config, fn
      {:type, type}, acc ->
        Map.put(acc, :type, type)

      {:source, source}, acc ->
        Map.put(acc, :source, source)

      {:source_kind, source_kind}, acc ->
        Map.put(acc, :source_kind, normalize_source_kind(source_kind))

      {:on, on}, acc ->
        Map.put(acc, :on, on)

      {:owner_key, key}, acc ->
        Map.put(acc, :owner_key, key)

      {:related_key, key}, acc ->
        Map.put(acc, :my_key, key)

      {:fields, fields}, acc ->
        Map.put(acc, :fields, fields)

      _, acc ->
        acc
    end)
  end

  defp build_custom_join(selecto, join_id, options) do
    source =
      Keyword.get(options, :source) || raise ArgumentError, "Custom joins require :source option"

    on = Keyword.get(options, :on, [])
    type = Keyword.get(options, :type, :left)
    owner_key = Keyword.get(options, :owner_key, :id)
    related_key = Keyword.get(options, :related_key, :"#{join_id}_id")
    fields = Keyword.get(options, :fields, %{})
    source_kind = options |> Keyword.get(:source_kind) |> normalize_source_kind()

    %{
      id: join_id,
      name: to_string(join_id),
      source: source,
      on: on,
      type: type,
      owner_key: owner_key,
      my_key: related_key,
      requires_join: :selecto_root,
      join_type: :custom,
      is_custom: true,
      fields: build_field_configs(join_id, fields),
      filters: %{}
    }
    |> maybe_put_source_kind(source_kind)
    |> maybe_enrich_cte_join(selecto)
  end

  defp validate_cte_source!(selecto, join_id, %{source_kind: :cte, source: source}) do
    case get_cte_spec_by_name(selecto, source) do
      nil ->
        raise ArgumentError,
              "Join '#{join_id}' references CTE source '#{source}' but no such CTE is registered"

      _cte_spec ->
        :ok
    end
  end

  defp validate_cte_source!(_selecto, _join_id, _join_config), do: :ok

  defp maybe_enrich_cte_join(join_config, selecto) do
    source = Map.get(join_config, :source)

    case get_cte_spec_by_name(selecto, source) do
      nil ->
        join_config

      cte_spec ->
        cte_columns = normalize_cte_columns(Map.get(cte_spec, :columns))

        join_config
        |> Map.put(:source_kind, :cte)
        |> Map.put(:cte_name, Map.get(cte_spec, :name))
        |> Map.put(:cte_columns, cte_columns)
        |> maybe_infer_cte_fields(cte_columns)
    end
  end

  defp maybe_infer_cte_fields(join_config, []), do: join_config

  defp maybe_infer_cte_fields(join_config, cte_columns) do
    case Map.get(join_config, :fields) do
      fields when is_map(fields) and map_size(fields) > 0 ->
        join_config

      _ ->
        join_id = Map.fetch!(join_config, :id)

        inferred_fields =
          Enum.into(cte_columns, %{}, fn column ->
            field_key = "#{join_id}.#{column}"

            {field_key,
             %{
               name: column,
               field: column,
               requires_join: join_id,
               type: :any
             }}
          end)

        Map.put(join_config, :fields, inferred_fields)
    end
  end

  defp get_cte_spec_by_name(selecto, cte_name) when is_atom(cte_name) do
    get_cte_spec_by_name(selecto, Atom.to_string(cte_name))
  end

  defp get_cte_spec_by_name(selecto, cte_name) when is_binary(cte_name) do
    selecto
    |> Map.get(:set, %{})
    |> Map.get(:ctes, [])
    |> Enum.find(&(Map.get(&1, :name) == cte_name))
  end

  defp get_cte_spec_by_name(_selecto, _cte_name), do: nil

  defp maybe_put_source_kind(join_config, nil), do: join_config

  defp maybe_put_source_kind(join_config, source_kind),
    do: Map.put(join_config, :source_kind, source_kind)

  defp normalize_source_kind(nil), do: nil
  defp normalize_source_kind(:cte), do: :cte
  defp normalize_source_kind(:table), do: :table
  defp normalize_source_kind("cte"), do: :cte
  defp normalize_source_kind("table"), do: :table
  defp normalize_source_kind(other), do: other

  defp normalize_cte_columns(columns) when is_list(columns), do: Enum.map(columns, &to_string/1)
  defp normalize_cte_columns(_columns), do: []

  defp build_field_configs(join_id, fields) do
    Enum.reduce(fields, %{}, fn {field_name, config}, acc ->
      field_key = "#{join_id}.#{field_name}"

      field_config =
        Map.merge(
          %{
            name: to_string(field_name),
            field: to_string(field_name),
            requires_join: join_id,
            type: :string
          },
          config
        )

      Map.put(acc, field_key, field_config)
    end)
  end

  defp extract_filter_keys(join_config) do
    filters = Map.get(join_config, :filters, %{})
    Map.keys(filters) |> Enum.map(&String.to_atom/1)
  end

  defp update_field_names_for_param(config, param_join_id) do
    updated_fields =
      config.fields
      |> Enum.map(fn {_key, field_config} ->
        dot_key = "#{param_join_id}.#{field_config.field}"
        updated_config = Map.put(field_config, :requires_join, param_join_id)
        {dot_key, updated_config}
      end)
      |> Enum.into(%{})

    Map.put(config, :fields, updated_fields)
  end

  defp extract_subquery_fields(join_selecto, join_id) do
    # Extract selected fields from the subquery to make them available
    join_selecto.set.selected
    |> Enum.flat_map(&subquery_output_fields/1)
    |> Enum.uniq()
    |> Enum.reduce(%{}, fn field, acc ->
      field_key = "#{join_id}.#{field}"

      Map.put(acc, field_key, %{
        name: field,
        field: field,
        requires_join: join_id,
        type: :unknown
      })
    end)
  end

  defp subquery_output_fields({:field, _selector, alias_name}) do
    [to_string(alias_name)]
  end

  defp subquery_output_fields({:field, selector}) do
    selector
    |> subquery_output_field_name()
    |> List.wrap()
  end

  defp subquery_output_fields({:row, _fields, alias_name}) do
    [to_string(alias_name)]
  end

  defp subquery_output_fields({_function, _args, opts}) when is_list(opts) do
    case Keyword.get(opts, :as) do
      nil -> []
      alias_name -> [to_string(alias_name)]
    end
  end

  defp subquery_output_fields(field) when is_binary(field) or is_atom(field) do
    [subquery_output_field_name(field)]
  end

  defp subquery_output_fields(_field), do: []

  defp subquery_output_field_name({:field, _selector, alias_name}) do
    to_string(alias_name)
  end

  defp subquery_output_field_name({:field, selector}) do
    subquery_output_field_name(selector)
  end

  defp subquery_output_field_name(field) when is_binary(field) or is_atom(field) do
    field
    |> to_string()
    |> String.split(".")
    |> List.last()
  end

  defp register_dynamic_join_fields(selecto, join_id, join_config) do
    # Add the join's fields to the selecto config columns
    new_fields = Map.get(join_config, :fields, %{})

    current_columns = Map.get(selecto.config, :columns, %{})
    updated_columns = Map.merge(current_columns, new_fields)

    # Also register the join in config.joins for SQL builder
    current_joins = Map.get(selecto.config, :joins, %{})
    updated_joins = Map.put(current_joins, join_id, join_config)

    selecto
    |> put_in([Access.key(:config), :columns], updated_columns)
    |> put_in([Access.key(:config), :joins], updated_joins)
  end

  defp build_subquery_root_alias(selecto, join_id, join_selecto) do
    table_segment =
      join_selecto
      |> Selecto.source_table()
      |> normalize_alias_segment("source")

    join_segment = normalize_alias_segment(join_id, "join")
    base_alias = "subq_root_#{table_segment}_#{join_segment}"

    used_aliases =
      selecto
      |> existing_subquery_root_aliases(join_id)
      |> MapSet.new()

    ensure_unique_alias(base_alias, used_aliases)
  end

  defp existing_subquery_root_aliases(selecto, current_join_id) do
    selecto
    |> Map.get(:set, %{})
    |> Map.get(:dynamic_joins, %{})
    |> Enum.reject(fn {join_id, _config} -> join_id == current_join_id end)
    |> Enum.map(fn {_join_id, config} -> Map.get(config, :subquery_root_alias) end)
    |> Enum.filter(&is_binary/1)
  end

  defp ensure_unique_alias(base_alias, used_aliases) do
    if MapSet.member?(used_aliases, base_alias) do
      Stream.iterate(2, &(&1 + 1))
      |> Enum.find_value(fn suffix ->
        alias_name = "#{base_alias}_#{suffix}"

        if MapSet.member?(used_aliases, alias_name) do
          nil
        else
          alias_name
        end
      end)
    else
      base_alias
    end
  end

  defp normalize_alias_segment(value, fallback) do
    normalized =
      value
      |> to_string()
      |> String.downcase()
      |> String.replace(~r/[^a-z0-9]+/u, "_")
      |> String.trim("_")

    if normalized == "", do: fallback, else: normalized
  end

  defp rewrite_subquery_root_alias(subquery_sql, alias_name) when is_binary(subquery_sql) do
    Regex.replace(~r/\bselecto_root\b/u, subquery_sql, alias_name)
  end
end
