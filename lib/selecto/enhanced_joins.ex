defmodule Selecto.EnhancedJoins do
  @moduledoc """
  Enhanced join types and patterns for Selecto.
  
  This module extends the base join functionality with additional join types
  and enhanced field resolution capabilities.
  
  ## New Join Types
  
  ### Self-Joins
  Join a table to itself with different aliases for comparison or hierarchical relationships.
  
  ### Lateral Joins
  Correlated subqueries that can reference columns from preceding tables in the FROM clause.
  
  ### Cross Joins
  Cartesian product between tables (use with caution for performance).
  
  ### Full Outer Joins
  Complete outer join that returns all rows from both tables.
  
  ### Conditional Joins
  Dynamic join conditions based on field values or runtime parameters.
  
  ## Enhanced Join Configuration
  
  ```elixir
  joins: %{
    # Self-join for manager relationships
    manager: %{
      type: :self_join,
      self_key: :manager_id,
      target_key: :id,
      alias: "mgr",
      condition_type: :left
    },
    
    # Lateral join for complex correlated queries
    recent_orders: %{
      type: :lateral_join,
      lateral_query: "SELECT * FROM orders o WHERE o.customer_id = customers.id ORDER BY o.created_at DESC LIMIT 5",
      alias: "recent"
    },
    
    # Cross join for combinations (use carefully)
    product_variants: %{
      type: :cross_join,
      source: "product_options",
      alias: "variants"
    },
    
    # Full outer join
    all_transactions: %{
      type: :full_outer_join,
      source: "transactions", 
      left_key: :account_id,
      right_key: :account_id,
      alias: "trans"
    },
    
    # Conditional join with runtime conditions
    applicable_discounts: %{
      type: :conditional_join,
      source: "discounts",
      conditions: [
        {:field_comparison, "orders.total", :gte, "discounts.minimum_amount"},
        {:date_range, "orders.created_at", "discounts.valid_from", "discounts.valid_to"}
      ],
      condition_type: :left
    }
  }
  ```
  """
  
  @type join_type :: :self_join | :lateral_join | :cross_join | :full_outer_join | :conditional_join
  @type condition_type :: :inner | :left | :right | :full
  @type join_condition :: {:field_comparison, String.t(), atom(), String.t()} |
                          {:date_range, String.t(), String.t(), String.t()} |
                          {:custom_sql, String.t()}
  
  @doc """
  Configure an enhanced join based on its type.
  """
  def configure_enhanced_join(id, association, %{type: join_type} = config, parent, from_source, queryable) 
      when join_type in [:self_join, :lateral_join, :cross_join, :full_outer_join, :conditional_join] do
    
    case join_type do
      :self_join ->
        configure_self_join(id, association, config, parent, from_source, queryable)
      :lateral_join ->
        configure_lateral_join(id, association, config, parent, from_source, queryable)
      :cross_join ->
        configure_cross_join(id, association, config, parent, from_source, queryable)
      :full_outer_join ->
        configure_full_outer_join(id, association, config, parent, from_source, queryable)
      :conditional_join ->
        configure_conditional_join(id, association, config, parent, from_source, queryable)
    end
  end
  
  @doc """
  Generate SQL for enhanced join types.
  """
  def build_enhanced_join_sql(join_config, selecto) do
    case join_config.join_type do
      :self_join ->
        build_self_join_sql(join_config, selecto)
      :lateral_join ->
        build_lateral_join_sql(join_config, selecto)
      :cross_join ->
        build_cross_join_sql(join_config, selecto)
      :full_outer_join ->
        build_full_outer_join_sql(join_config, selecto)
      :conditional_join ->
        build_conditional_join_sql(join_config, selecto)
      _ ->
        # Fallback to standard join building
        nil
    end
  end
  
  # Self-Join Configuration
  defp configure_self_join(id, association, config, parent, from_source, queryable) do
    name = Map.get(config, :name, id)
    self_key = Map.get(config, :self_key, :parent_id)
    target_key = Map.get(config, :target_key, :id)
    join_alias = Map.get(config, :alias, "#{id}_self")
    condition_type = Map.get(config, :condition_type, :left)
    
    # Configure custom columns for self-reference access
    config = Map.put(config, :custom_columns, Map.get(config, :custom_columns, %{}) |> Map.put(
        "#{id}_reference", %{
          name: "#{name} Reference",
          select: "#{join_alias}.name",  # Use the alias for field references
          group_by_format: fn {a, _id}, _def -> a end,
          filterable: true
        }
      )
    )
    
    base_join_config(id, config, parent, from_source, queryable, name)
    |> Map.merge(%{
      join_type: :self_join,
      self_key: self_key,
      target_key: target_key,
      join_alias: join_alias,
      condition_type: condition_type,
      source_table: queryable.source_table  # Self-join uses same table
    })
  end
  
  # Lateral Join Configuration
  defp configure_lateral_join(id, _association, config, parent, from_source, queryable) do
    name = Map.get(config, :name, id)
    lateral_query = Map.get(config, :lateral_query, "")
    join_alias = Map.get(config, :alias, "#{id}_lateral")
    
    if lateral_query == "" do
      raise ArgumentError, "Lateral join requires :lateral_query configuration"
    end
    
    # Configure custom columns for lateral query results
    config = Map.put(config, :custom_columns, Map.get(config, :custom_columns, %{}) |> Map.put(
        "#{id}_result", %{
          name: "#{name} Result",
          select: "#{join_alias}.*",  # Select all from lateral query
          group_by_format: fn {a, _id}, _def -> a end,
          filterable: false  # Lateral results typically aren't directly filterable
        }
      )
    )
    
    base_join_config(id, config, parent, from_source, queryable, name)
    |> Map.merge(%{
      join_type: :lateral_join,
      lateral_query: lateral_query,
      join_alias: join_alias
    })
  end
  
  # Cross Join Configuration
  defp configure_cross_join(id, _association, config, parent, from_source, queryable) do
    name = Map.get(config, :name, id)
    join_alias = Map.get(config, :alias, "#{id}_cross")
    
    # Add warning for performance considerations
    config = Map.put(config, :custom_columns, Map.get(config, :custom_columns, %{}) |> Map.put(
        "#{id}_combination", %{
          name: "#{name} Combination",
          select: "#{join_alias}.id",
          group_by_format: fn {a, _id}, _def -> a end,
          filterable: true,
          performance_warning: "Cross joins can produce large result sets"
        }
      )
    )
    
    base_join_config(id, config, parent, from_source, queryable, name)
    |> Map.merge(%{
      join_type: :cross_join,
      join_alias: join_alias
    })
  end
  
  # Full Outer Join Configuration
  defp configure_full_outer_join(id, association, config, parent, from_source, queryable) do
    name = Map.get(config, :name, id)
    left_key = Map.get(config, :left_key, association.owner_key)
    right_key = Map.get(config, :right_key, association.related_key)
    join_alias = Map.get(config, :alias, "#{id}_full")
    
    # Configure columns with null handling for outer join
    config = Map.put(config, :custom_columns, Map.get(config, :custom_columns, %{}) |> Map.put(
        "#{id}_coalesced", %{
          name: "#{name} (with nulls)",
          select: "COALESCE(#{join_alias}.name, 'No match')",
          group_by_format: fn {a, _id}, _def -> a end,
          filterable: true,
          handles_nulls: true
        }
      )
    )
    
    base_join_config(id, config, parent, from_source, queryable, name)
    |> Map.merge(%{
      join_type: :full_outer_join,
      left_key: left_key,
      right_key: right_key,
      join_alias: join_alias
    })
  end
  
  # Conditional Join Configuration
  defp configure_conditional_join(id, _association, config, parent, from_source, queryable) do
    name = Map.get(config, :name, id)
    conditions = Map.get(config, :conditions, [])
    condition_type = Map.get(config, :condition_type, :left)
    join_alias = Map.get(config, :alias, "#{id}_cond")
    
    if conditions == [] do
      raise ArgumentError, "Conditional join requires :conditions configuration"
    end
    
    # Configure columns for conditional results
    config = Map.put(config, :custom_columns, Map.get(config, :custom_columns, %{}) |> Map.put(
        "#{id}_conditional", %{
          name: "#{name} (conditional)",
          select: "#{join_alias}.name",
          group_by_format: fn {a, _id}, _def -> a end,
          filterable: true,
          is_conditional: true
        }
      )
    )
    
    base_join_config(id, config, parent, from_source, queryable, name)
    |> Map.merge(%{
      join_type: :conditional_join,
      conditions: conditions,
      condition_type: condition_type,
      join_alias: join_alias
    })
  end
  
  # SQL Generation for Enhanced Joins
  
  defp build_self_join_sql(join_config, _selecto) do
    table = join_config.source
    alias_name = join_config.join_alias
    self_key = join_config.self_key
    target_key = join_config.target_key
    condition_type = join_config.condition_type
    
    join_type_sql = case condition_type do
      :inner -> "INNER JOIN"
      :left -> "LEFT JOIN"
      :right -> "RIGHT JOIN"
      :full -> "FULL OUTER JOIN"
    end
    
    [
      " ", join_type_sql, " ", table, " ", alias_name,
      " ON ", "selecto_root.", Atom.to_string(self_key),
      " = ", alias_name, ".", Atom.to_string(target_key)
    ]
  end
  
  defp build_lateral_join_sql(join_config, _selecto) do
    lateral_query = join_config.lateral_query
    alias_name = join_config.join_alias
    
    [
      " LEFT JOIN LATERAL (",
      lateral_query,
      ") ", alias_name, " ON true"
    ]
  end
  
  defp build_cross_join_sql(join_config, _selecto) do
    table = join_config.source
    alias_name = join_config.join_alias
    
    [
      " CROSS JOIN ", table, " ", alias_name
    ]
  end
  
  defp build_full_outer_join_sql(join_config, _selecto) do
    table = join_config.source
    alias_name = join_config.join_alias
    left_key = join_config.left_key
    right_key = join_config.right_key
    
    [
      " FULL OUTER JOIN ", table, " ", alias_name,
      " ON ", "selecto_root.", Atom.to_string(left_key),
      " = ", alias_name, ".", Atom.to_string(right_key)
    ]
  end
  
  defp build_conditional_join_sql(join_config, _selecto) do
    table = join_config.source
    alias_name = join_config.join_alias
    conditions = join_config.conditions
    condition_type = join_config.condition_type
    
    join_type_sql = case condition_type do
      :inner -> "INNER JOIN"
      :left -> "LEFT JOIN"
      :right -> "RIGHT JOIN"
      :full -> "FULL OUTER JOIN"
    end
    
    condition_sql = build_condition_sql(conditions, alias_name)
    
    [
      " ", join_type_sql, " ", table, " ", alias_name,
      " ON ", condition_sql
    ]
  end
  
  defp build_condition_sql(conditions, _alias_name) do
    conditions
    |> Enum.map(fn condition ->
      case condition do
        {:field_comparison, left_field, operator, right_field} ->
          op_sql = case operator do
            :eq -> "="
            :ne -> "!="
            :gt -> ">"
            :gte -> ">="
            :lt -> "<"
            :lte -> "<="
          end
          "#{left_field} #{op_sql} #{right_field}"
          
        {:date_range, date_field, from_field, to_field} ->
          "#{date_field} BETWEEN #{from_field} AND #{to_field}"
          
        {:custom_sql, sql} ->
          sql
      end
    end)
    |> Enum.join(" AND ")
  end
  
  # Helper for common join configuration
  defp base_join_config(id, config, parent, from_source, queryable, name) do
    # Create a domain-like config for column generation
    enhanced_config = Map.merge(config, %{name: name})
    
    %{
      config: enhanced_config,
      from_source: from_source,
      source: queryable.source_table,
      id: id,
      name: name,
      requires_join: parent,
      filters: Map.get(config, :filters, %{}),
      fields: get_enhanced_join_fields(queryable, enhanced_config)
    }
  end
  
  defp get_enhanced_join_fields(queryable, config) do
    # Get base fields from queryable
    base_fields = queryable.fields -- queryable.redact_fields
    
    # Add any custom fields from join configuration
    custom_fields = Map.get(config, :additional_fields, [])
    
    # Ensure alias is an atom
    alias_atom = case Map.get(config, :alias, :enhanced_join) do
      alias_name when is_binary(alias_name) -> String.to_atom(alias_name)
      alias_name when is_atom(alias_name) -> alias_name
    end
    
    Selecto.Schema.Column.configure_columns(
      alias_atom,
      base_fields ++ custom_fields,
      queryable,
      config
    )
  end
end