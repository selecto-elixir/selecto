defmodule Selecto.Schema.Join do
  # selecto meta join can edit, add, alter this join!

  @moduledoc """
  # Join Configuration and Patterns

  This module handles join configuration for various database join patterns,
  providing simplified configuration helpers for common scenarios.

  ## Available Join Types

  ### :dimension (Existing)
  Basic dimension table join with ID filtering and name display.
  - `dimension`: Field to use for display (required)
  - `name`: Display name for the join (optional)

  ### :tagging (New)
  Many-to-many relationship through a join table (e.g., posts <-> tags).
  - `tag_field`: Field to display from tag table (default: :name)
  - `name`: Display name for the join (optional)
  Creates custom columns for tag aggregation and faceted filtering.

  ### :hierarchical (New) 
  Self-referencing hierarchical relationships with multiple implementation patterns:
  - `hierarchy_type`: :adjacency_list, :materialized_path, or :closure_table
  - `depth_limit`: Maximum recursion depth for adjacency lists (default: 5)
  - `path_field`: Field containing path for materialized path pattern (default: :path)
  - `path_separator`: Separator used in path field (default: "/")
  - `closure_table`: Name of closure table for closure table pattern
  - `ancestor_field`, `descendant_field`, `depth_field`: Closure table field names

  ### :star_dimension (New)
  Optimized for OLAP star schema dimensions.
  - `display_field`: Field to use for dimension display (default: :name)
  - `name`: Display name for the dimension (optional)
  Creates faceted filters optimized for aggregation queries.

  ### :snowflake_dimension (New)
  Normalized dimension tables requiring additional joins.
  - `display_field`: Field to use for dimension display (default: :name)
  - `normalization_joins`: List of additional joins needed for full context
  - `name`: Display name for the dimension (optional)

  ## Standard Join Types (Default behavior)
  - one_to_one - Standard lookup with all columns available
  - one_to_many - Treated like one-to-one
  - belongs_to - Treated like one-to-one

  ## Configuration Examples

  ```elixir
  joins: %{
    # Many-to-many tagging
    tags: %{type: :tagging, tag_field: :name},
    
    # Hierarchical adjacency list
    manager: %{type: :hierarchical, hierarchy_type: :adjacency_list, depth_limit: 5},
    
    # Star schema dimension
    customer: %{type: :star_dimension, display_field: :full_name},
    
    # Snowflake dimension with normalization
    category: %{
      type: :snowflake_dimension,
      display_field: :name,
      normalization_joins: [%{table: "category_groups", alias: "cg"}]
    }
  }
  ```
  """

  ### source - a schema name such as SelectoTest.Store.Film
  ### joins - the joins map from this join structure

  # we consume the join tree (atom/list) to a flat map of joins then into a map
  def recurse_joins(source, domain) do
    normalize_joins(source, domain.joins, :selecto_root, domain)
    |> List.flatten()
    |> Enum.reduce(%{}, fn j, acc -> Map.put(acc, j.id, j) end)
  end


  ### source, the schema module name
  ### joins, the joins map from parent,
  ### parent, the 'parent' join

  defp normalize_joins(source, joins, parent, domain) do

    Enum.reduce(joins, [], fn
      ### Non assoc, need to add tests to verify TODO
      # {id, %{non_assoc: true} = config}, acc ->
      #   acc = acc ++ [configure(id, config, parent, source)]
      #   case Map.get(config, :joins) do
      #     nil -> acc
      #     _ -> acc ++ normalize_joins(config.source, config.joins, id)
      #   end

      {id, config}, acc ->
        association = source.associations[id]
        queryable = domain.schemas[association.queryable]
        acc = acc ++ [configure(id, association, config, parent, source, queryable)]
        case Map.get(config, :joins) do
          nil -> acc
          _ -> acc ++ normalize_joins(queryable, config.joins, id, domain)
        end
    end)
  end


  # #### Non-assoc joins
  # defp configure(_id, _config, _dep, _from_source) do
  # end

  ## TODO this does not work yet!
  # defp configure(_id, %{through: _through} = _association, _config, _dep, _from_source) do
  #   ### we are going to expand the through but only add the

  #   ##??????
  # end

  ### Custom TODO
  # defp configure(id, %{type: :custom} = config, dep) do
  #   ### this join does not have an association

  # end

  ### id, the id of this join in the joins map on parent
  ### association, the struct form ecto.schema that has instructions on how to join
  ### config, the map that contains config details for this join
  ### parent - atom that references the parent
  ### from_source - the association from the parent or the domain if parent is root

  ### Dimension table join
  defp configure(id, association, %{type: :dimension} = config, parent, from_source, queryable) do
    #dimension table, has one 'name-ish' value to display, and then the Local reference would provide ID filtering.
    # So create a field for group-by that displays NAME and filters by ID

    name = Map.get(config, :name, id)

    from_field = case parent do
      :selecto_root -> "#{association.owner_key}"
      _ -> "#{parent}[#{association.owner_key}]"
    end

    config = Map.put(config, :custom_columns, Map.get(config, :custom_columns, %{}) |> Map.put(
        "#{id}", %{ ## we will use the nane of the join's association!
          name: name,
          ### concat_ws?
          select: "#{association.field}[#{config.dimension}]",
          ### we will always get a tuple of select + group_by_filter_select here
          group_by_format: fn {a, _id}, _def -> a end,
          group_by_filter: from_field,
          group_by_filter_select: ["#{association.field}[#{config.dimension}]", from_field ]
        }
      )
    )

    ### Add custom filter that has a select (or set of checkboxes or radios... filter_form_type: :checks eg) list of names, IF not existing by name of the remote ID filter and:
    ### - the join provides a list of tuples {filterable, viewable}
    ### - the join provides a function returning same
    ### - the join has 'facet: true' in which case we will determine given all other filters, what are the available items & how many matches for those items

    %{
      config: config,
      from_source: from_source,
      owner_key: association.owner_key,
      my_key: association.related_key,
      source: queryable.source_table,
      id: id,
      name: name,
      ## probably don't need 'where'
      requires_join: parent,
      filters: Map.get(config, :filters, %{}),
      fields:
        Selecto.Schema.Column.configure_columns(
          association.field,
          [config.dimension],
          queryable,
          config
        )
    } |> parameterize()
  end

  ### Many-to-many tagging join (through join table)
  defp configure(id, association, %{type: :tagging} = config, parent, from_source, queryable) do
    # For tagging relationships, we need to handle the intermediate join table
    # This assumes the association is a has_many :through relationship
    # Example: Post has_many :tags, through: :post_tags

    name = Map.get(config, :name, id)
    tag_field = Map.get(config, :tag_field, :name)  # Field to display from tag table
    
    # Configure custom columns for tag aggregation
    config = Map.put(config, :custom_columns, Map.get(config, :custom_columns, %{}) |> Map.put(
        "#{id}_list", %{
          name: "#{name} List",
          select: "string_agg(#{association.field}[#{tag_field}], ', ')",
          group_by_format: fn {a, _id}, _def -> a end,
          filterable: false  # Aggregate fields typically aren't filterable
        }
      )
    )

    # Add faceted filter for individual tags
    config = Map.put(config, :custom_filters, Map.get(config, :custom_filters, %{}) |> Map.put(
        "#{id}_filter", %{
          name: "#{name}",
          filter_type: :multi_select,
          facet: true,  # Enable faceted filtering
          source_table: queryable.source_table,
          source_field: tag_field
        }
      )
    )

    %{
      config: config,
      from_source: from_source,
      owner_key: association.owner_key,
      my_key: association.related_key,
      source: queryable.source_table,
      id: id,
      name: name,
      requires_join: parent,
      join_type: :many_to_many,  # Special marker for SQL builder
      filters: Map.get(config, :filters, %{}),
      fields:
        Selecto.Schema.Column.configure_columns(
          association.field,
          queryable.fields -- queryable.redact_fields,
          queryable,
          config
        )
    } |> parameterize()
  end

  ### Hierarchical self-join (adjacency list pattern)
  defp configure(id, association, %{type: :hierarchical} = config, parent, from_source, queryable) do
    # Self-referencing table with parent_id pointing to same table
    # Supports adjacency list hierarchical pattern
    
    name = Map.get(config, :name, id)
    hierarchy_type = Map.get(config, :hierarchy_type, :adjacency_list)
    depth_limit = Map.get(config, :depth_limit, 5)  # Prevent infinite recursion

    case hierarchy_type do
      :adjacency_list ->
        configure_adjacency_list(id, association, config, parent, from_source, queryable, name, depth_limit)
      
      :materialized_path ->
        configure_materialized_path(id, association, config, parent, from_source, queryable, name)
      
      :closure_table ->
        configure_closure_table(id, association, config, parent, from_source, queryable, name)
    end
  end

  defp configure_adjacency_list(id, association, config, parent, from_source, queryable, name, _depth_limit) do
    # Add custom columns for hierarchy navigation
    # These reference fields from the CTE built in the SQL builder
    cte_alias = "#{id}_hierarchy"  # This matches the CTE name generated by hierarchy builder
    
    config = Map.put(config, :custom_columns, Map.get(config, :custom_columns, %{}) |> Map.put(
        "#{id}_path", %{
          name: "#{name} Path",
          select: "#{cte_alias}.path",  # Reference CTE field directly
          group_by_format: fn {a, _id}, _def -> a end,
          filterable: false
        }
      ) |> Map.put(
        "#{id}_level", %{
          name: "#{name} Level",
          select: "#{cte_alias}.level",  # Reference CTE field directly
          group_by_format: fn {a, _id}, _def -> a end,
          filterable: true
        }
      ) |> Map.put(
        "#{id}_path_array", %{
          name: "#{name} Path Array",
          select: "#{cte_alias}.path_array",  # Reference CTE path array
          group_by_format: fn {a, _id}, _def -> a end,
          filterable: false
        }
      )
    )

    # Get the depth limit from config for the join struct
    depth_limit = Map.get(config, :depth_limit, 5)
    
    %{
      config: config,
      from_source: from_source,
      owner_key: association.owner_key,
      my_key: association.related_key,
      source: queryable.source_table,
      id: id,
      name: name,
      requires_join: parent,
      join_type: :hierarchical_adjacency,
      hierarchy_depth: depth_limit,
      filters: Map.get(config, :filters, %{}),
      fields:
        Selecto.Schema.Column.configure_columns(
          association.field,
          queryable.fields -- queryable.redact_fields,
          queryable,
          config
        )
    } |> parameterize()
  end

  defp configure_materialized_path(id, association, config, parent, from_source, queryable, name) do
    path_field = Map.get(config, :path_field, :path)
    path_separator = Map.get(config, :path_separator, "/")

    # Add custom columns for path-based operations
    # These reference fields from the CTE built in the SQL builder
    cte_alias = "#{id}_materialized_path"  # This matches the CTE name
    
    config = Map.put(config, :custom_columns, Map.get(config, :custom_columns, %{}) |> Map.put(
        "#{id}_depth", %{
          name: "#{name} Depth",
          select: "#{cte_alias}.depth",  # Reference CTE calculated depth
          group_by_format: fn {a, _id}, _def -> a end,
          filterable: true
        }
      ) |> Map.put(
        "#{id}_path_array", %{
          name: "#{name} Path Array",
          select: "#{cte_alias}.path_array",  # Reference CTE path array
          group_by_format: fn {a, _id}, _def -> a end,
          filterable: false
        }
      )
    )

    %{
      config: config,
      from_source: from_source,
      owner_key: association.owner_key,
      my_key: association.related_key,
      source: queryable.source_table,
      id: id,
      name: name,
      requires_join: parent,
      join_type: :hierarchical_materialized_path,
      path_field: path_field,
      path_separator: path_separator,
      filters: Map.get(config, :filters, %{}),
      fields:
        Selecto.Schema.Column.configure_columns(
          association.field,
          queryable.fields -- queryable.redact_fields,
          queryable,
          config
        )
    } |> parameterize()
  end

  defp configure_closure_table(id, association, config, parent, from_source, queryable, name) do
    # Assumes a separate closure table exists for storing ancestor-descendant relationships
    closure_table = Map.get(config, :closure_table, "#{queryable.source_table}_closure")
    ancestor_field = Map.get(config, :ancestor_field, :ancestor_id)
    descendant_field = Map.get(config, :descendant_field, :descendant_id)
    depth_field = Map.get(config, :depth_field, :depth)

    # Add custom columns leveraging closure table CTE
    # These reference fields from the CTE built in the SQL builder
    cte_alias = "#{id}_closure"  # This matches the CTE name
    
    config = Map.put(config, :custom_columns, Map.get(config, :custom_columns, %{}) |> Map.put(
        "#{id}_depth", %{
          name: "#{name} Depth",
          select: "#{cte_alias}.depth",  # Reference CTE depth field
          group_by_format: fn {a, _id}, _def -> a end,
          filterable: true
        }
      ) |> Map.put(
        "#{id}_descendant_count", %{
          name: "#{name} Descendant Count",
          select: "#{cte_alias}.descendant_count",  # Reference CTE calculated count
          group_by_format: fn {a, _id}, _def -> a end,
          filterable: true
        }
      )
    )

    %{
      config: config,
      from_source: from_source,
      owner_key: association.owner_key,
      my_key: association.related_key,
      source: queryable.source_table,
      id: id,
      name: name,
      requires_join: parent,
      join_type: :hierarchical_closure_table,
      closure_table: closure_table,
      ancestor_field: ancestor_field,
      descendant_field: descendant_field,
      depth_field: depth_field,
      filters: Map.get(config, :filters, %{}),
      fields:
        Selecto.Schema.Column.configure_columns(
          association.field,
          queryable.fields -- queryable.redact_fields,
          queryable,
          config
        )
    } |> parameterize()
  end

  ### Star schema dimension join (optimized for OLAP)
  defp configure(id, association, %{type: :star_dimension} = config, parent, from_source, queryable) do
    # Star schema dimensions are optimized for aggregation and analysis
    # They typically contain descriptive attributes and are joined to fact tables
    
    name = Map.get(config, :name, id)
    display_field = Map.get(config, :display_field, :name)
    
    # Configure dimension-specific features
    config = Map.put(config, :custom_columns, Map.get(config, :custom_columns, %{}) |> Map.put(
        "#{id}_display", %{
          name: "#{name}",
          select: "#{association.field}[#{display_field}]",
          group_by_format: fn {a, _id}, _def -> a end,
          filterable: true,
          is_dimension: true  # Mark as dimension for special handling
        }
      )
    )
    
    # Add aggregation-friendly filters
    config = Map.put(config, :custom_filters, Map.get(config, :custom_filters, %{}) |> Map.put(
        "#{id}_facet", %{
          name: "#{name} Filter",
          filter_type: :select_facet,
          facet: true,  # Enable faceted filtering for dimensions
          source_table: queryable.source_table,
          source_field: display_field,
          is_dimension: true
        }
      )
    )

    %{
      config: config,
      from_source: from_source,
      owner_key: association.owner_key,
      my_key: association.related_key,
      source: queryable.source_table,
      id: id,
      name: name,
      requires_join: parent,
      join_type: :star_dimension,  # Optimized for OLAP queries
      display_field: display_field,
      filters: Map.get(config, :filters, %{}),
      fields:
        Selecto.Schema.Column.configure_columns(
          association.field,
          queryable.fields -- queryable.redact_fields,
          queryable,
          config
        )
    } |> parameterize()
  end

  ### Snowflake schema normalized dimension join
  defp configure(id, association, %{type: :snowflake_dimension} = config, parent, from_source, queryable) do
    # Snowflake dimensions are normalized and may require multiple joins to get full context
    # They maintain referential integrity but require more complex queries
    
    name = Map.get(config, :name, id)
    display_field = Map.get(config, :display_field, :name)
    normalization_joins = Map.get(config, :normalization_joins, [])  # Additional joins needed
    
    # Configure for normalized dimension access
    config = Map.put(config, :custom_columns, Map.get(config, :custom_columns, %{}) |> Map.put(
        "#{id}_normalized", %{
          name: "#{name}",
          select: build_snowflake_select(association.field, display_field, normalization_joins),
          group_by_format: fn {a, _id}, _def -> a end,
          filterable: true,
          requires_normalization_joins: normalization_joins
        }
      )
    )

    %{
      config: config,
      from_source: from_source,
      owner_key: association.owner_key,
      my_key: association.related_key,
      source: queryable.source_table,
      id: id,
      name: name,
      requires_join: parent,
      join_type: :snowflake_dimension,
      display_field: display_field,
      normalization_joins: normalization_joins,  # Track additional joins needed
      filters: Map.get(config, :filters, %{}),
      fields:
        Selecto.Schema.Column.configure_columns(
          association.field,
          queryable.fields -- queryable.redact_fields,
          queryable,
          config
        )
    } |> parameterize()
  end

  ### Regular (catch-all clause)
  defp configure(id, association, config, parent, from_source, queryable) do
    std_config(id, association, config, parent, from_source, queryable)
  end

  # Helper functions for building hierarchy SQL
  # Note: Previously broken CTE functions have been removed and replaced with 
  # proper CTE integration in the SQL builder. Custom columns now reference
  # CTE fields directly instead of embedding invalid subqueries.

  defp build_snowflake_select(field, display_field, normalization_joins) do
    # Build select clause that may require additional joins for normalized data
    # This is a simplified version - real implementation would coordinate with SQL builder
    case normalization_joins do
      [] -> "#{field}[#{display_field}]"
      [%{alias: alias_name} | _] -> "#{alias_name}.#{display_field}"  # Use the normalized table's field
      [join | _] when is_binary(join) -> "#{join}.#{display_field}"  # Fallback for string joins
    end
  end

  defp std_config(id, association, config, parent, from_source, queryable) do
    %{
      config: config,
      from_source: from_source,
      owner_key: association.owner_key,
      my_key: association.related_key,
      source: queryable.source_table,
      id: id,
      name: Map.get(config, :name, id),
      ## probably don't need 'where'
      requires_join: parent,
      filters: make_filters(config),
      fields:
        Selecto.Schema.Column.configure_columns(
          association.field,
          queryable.fields -- queryable.redact_fields,
          queryable,
          config
        )
    } |> parameterize()
  end


  defp parameterize(join) do
    join
  end


  defp make_filters(config) do
    Map.get(config, :filters, %{})
  end

end
