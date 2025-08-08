defmodule Selecto.Builder.Sql.Tagging do
  @moduledoc """
  Many-to-many tagging SQL pattern generation for join table relationships.
  
  Supports tagging patterns through intermediate join tables with aggregation
  and faceted filtering capabilities. Handles the complexity of many-to-many
  relationships while maintaining proper parameterization.
  
  Phase 3: Full many-to-many implementation with tag aggregation and faceted filtering
  
  ## Supported Patterns
  
  - **Basic many-to-many joins**: LEFT JOIN through intermediate table
  - **Tag aggregation**: string_agg for comma-separated tag lists  
  - **Faceted filtering**: EXISTS subqueries for tag filtering
  - **Tag counting**: COUNT-based filtering for minimum tag requirements
  
  ## Examples
  
      # Basic tagging join: posts ↔ post_tags ↔ tags
      config = %{
        source: "tags",
        join_table: "post_tags",  
        tag_field: "name",
        main_foreign_key: "post_id",
        tag_foreign_key: "tag_id"
      }
      
      # Generates SQL like:
      # LEFT JOIN post_tags pt ON main.id = pt.post_id  
      # LEFT JOIN tags t ON pt.tag_id = t.id
      # With string_agg(t.name, ', ') for aggregation
  """
  
  import Selecto.Builder.Sql.Helpers

  @doc """
  Build many-to-many tagging join with intermediate table.
  
  Generates the double-JOIN pattern required for many-to-many relationships:
  1. Main table → intermediate join table  
  2. Join table → tag table
  
  Includes support for tag aggregation and proper GROUP BY handling.
  
  ## Parameters
  - `selecto`: Main selecto struct
  - `join`: Join identifier (atom)
  - `config`: Join configuration with tagging options
  - `fc`: Current from clause iodata  
  - `p`: Current parameters list
  - `ctes`: Current CTEs list
  
  Returns: `{updated_from_clause, updated_params, updated_ctes}`
  """
  def build_tagging_join_with_aggregation(selecto, join, config, fc, p, ctes) do
    # Extract tagging configuration
    _tag_field = Map.get(config, :tag_field, "name")  # Will be used for aggregation columns
    join_table = get_join_table_name(config, join)
    main_foreign_key = Map.get(config, :main_foreign_key, "#{extract_main_table(selecto)}_id")
    tag_foreign_key = Map.get(config, :tag_foreign_key, "tag_id")
    
    # Build intermediate table alias
    join_table_alias = "#{join}_jt"
    tag_table_alias = build_join_string(selecto, join)
    
    # Build the double JOIN pattern
    # 1. Main table → intermediate join table
    intermediate_join_iodata = [
      " LEFT JOIN ", join_table, " ", join_table_alias,
      " ON ", build_main_table_reference(selecto), ".id = ",
      join_table_alias, ".", main_foreign_key
    ]
    
    # 2. Intermediate table → tag table  
    tag_join_iodata = [
      " LEFT JOIN ", config.source, " ", tag_table_alias,
      " ON ", join_table_alias, ".", tag_foreign_key, " = ",
      tag_table_alias, ".id"
    ]
    
    # Combine both joins
    combined_join_iodata = [intermediate_join_iodata, tag_join_iodata]
    
    # Add to from clause
    {fc ++ combined_join_iodata, p, ctes}
  end
  
  @doc """
  Build tag aggregation column SQL.
  
  Generates string_agg expressions for displaying comma-separated tag lists.
  Handles NULL values and provides proper GROUP BY compatibility.
  
  ## Examples
  
      build_tag_aggregation_column("tags", "name", "tag_list")
      #=> "string_agg(tags.name, ', ') as tag_list"
      
      build_tag_aggregation_column("categories", "title", "category_names")  
      #=> "string_agg(categories.title, ', ') as category_names"
  
  Returns: iodata for SELECT clause
  """
  def build_tag_aggregation_column(tag_table_alias, tag_field, column_alias) do
    [
      "string_agg(", tag_table_alias, ".", tag_field, ", ', ') as ", column_alias
    ]
  end
  
  @doc """
  Build tag count column SQL.
  
  Generates COUNT expressions for counting distinct tags per record.
  Useful for filtering by minimum tag requirements.
  
  ## Examples
  
      build_tag_count_column("tags", "tag_count")
      #=> "COUNT(DISTINCT tags.id) as tag_count"
  
  Returns: iodata for SELECT clause  
  """
  def build_tag_count_column(tag_table_alias, column_alias) do
    [
      "COUNT(DISTINCT ", tag_table_alias, ".id) as ", column_alias
    ]
  end
  
  @doc """
  Build faceted tag filter using EXISTS subquery.
  
  Generates EXISTS subqueries for filtering records that have specific tags.
  Supports both single tag and array-based tag filtering.
  
  ## Examples
  
      # Single tag filter
      build_faceted_tag_filter(config, "programming", :single)
      
      # Multiple tags filter (ANY match)
      build_faceted_tag_filter(config, ["elixir", "phoenix"], :any)
      
      # Multiple tags filter (ALL required)  
      build_faceted_tag_filter(config, ["web", "backend"], :all)
  
  Returns: `{where_clause_iodata, params}`
  """
  def build_faceted_tag_filter(config, tag_values, match_type \\ :any) do
    join_table = get_join_table_name(config, :filter)
    tag_table = config.source
    tag_field = Map.get(config, :tag_field, "name")
    main_foreign_key = Map.get(config, :main_foreign_key, "post_id") # Will be dynamically determined
    tag_foreign_key = Map.get(config, :tag_foreign_key, "tag_id")
    
    case match_type do
      :single when is_binary(tag_values) ->
        # Single tag EXISTS filter
        where_iodata = [
          "EXISTS (",
          "SELECT 1 FROM ", join_table, " jt ",
          "JOIN ", tag_table, " t ON jt.", tag_foreign_key, " = t.id ",
          "WHERE jt.", main_foreign_key, " = main.id ",
          "AND t.", tag_field, " = $1",
          ")"
        ]
        {where_iodata, [tag_values]}
        
      :any when is_list(tag_values) ->
        # Multiple tags with ANY match (tag1 OR tag2 OR tag3)
        where_iodata = [
          "EXISTS (",
          "SELECT 1 FROM ", join_table, " jt ",
          "JOIN ", tag_table, " t ON jt.", tag_foreign_key, " = t.id ",
          "WHERE jt.", main_foreign_key, " = main.id ",
          "AND t.", tag_field, " = ANY($1)",
          ")"
        ]
        {where_iodata, [tag_values]}
        
      :all when is_list(tag_values) ->
        # Multiple tags with ALL required (tag1 AND tag2 AND tag3)
        # Uses COUNT to ensure all tags are present
        tag_count = length(tag_values)
        where_iodata = [
          "(",
          "SELECT COUNT(DISTINCT t.", tag_field, ") FROM ", join_table, " jt ",
          "JOIN ", tag_table, " t ON jt.", tag_foreign_key, " = t.id ",
          "WHERE jt.", main_foreign_key, " = main.id ",
          "AND t.", tag_field, " = ANY($1)",
          ") = $2"
        ]
        {where_iodata, [tag_values, tag_count]}
    end
  end
  
  @doc """
  Build tag count filter for minimum tag requirements.
  
  Generates WHERE conditions that filter records based on the number of tags
  they have. Useful for finding "well-tagged" content.
  
  ## Examples
  
      build_tag_count_filter(config, {:gte, 3})  # At least 3 tags
      build_tag_count_filter(config, {:eq, 1})   # Exactly 1 tag  
      build_tag_count_filter(config, {:between, 2, 5})  # Between 2-5 tags
  
  Returns: `{where_clause_iodata, params}`
  """
  def build_tag_count_filter(config, {operator, count}) when operator in [:gte, :gt, :lte, :lt, :eq] do
    join_table = get_join_table_name(config, :filter)
    main_foreign_key = Map.get(config, :main_foreign_key, "post_id")
    
    # Map operators to SQL
    sql_op = case operator do
      :gte -> ">="
      :gt -> ">"
      :lte -> "<="
      :lt -> "<"
      :eq -> "="
    end
    
    where_iodata = [
      "(",
      "SELECT COUNT(*) FROM ", join_table, " jt ",
      "WHERE jt.", main_foreign_key, " = main.id",
      ") ", sql_op, " $1"
    ]
    
    {where_iodata, [count]}
  end
  
  def build_tag_count_filter(config, {:between, min_count, max_count}) do
    join_table = get_join_table_name(config, :filter)
    main_foreign_key = Map.get(config, :main_foreign_key, "post_id")
    
    where_iodata = [
      "(",
      "SELECT COUNT(*) FROM ", join_table, " jt ",
      "WHERE jt.", main_foreign_key, " = main.id",
      ") BETWEEN $1 AND $2"
    ]
    
    {where_iodata, [min_count, max_count]}
  end
  
  # Helper functions
  
  defp get_join_table_name(config, _context) do
    # Try to extract join table name from configuration
    case Map.get(config, :join_table) do
      nil -> 
        # Fallback: try to infer from association or use default pattern
        source = config.source
        "#{String.trim_trailing(source, "s")}_#{source}" # e.g., "tag" + "tags" = "tag_tags"
      table_name -> table_name
    end
  end
  
  defp extract_main_table(selecto) do
    # Extract main table name from selecto struct
    # This is a simplified extraction - real implementation would need to handle
    # the domain configuration properly
    case selecto do
      %{domain: %{source: %{source_table: table}}} -> table
      _ -> "main"  # Fallback
    end
  end
  
  defp build_main_table_reference(selecto) do
    # In a real query, this would be the main table alias or name
    # For now, using "main" as a placeholder
    case extract_main_table(selecto) do
      "main" -> "main"
      table -> table
    end
  end
end