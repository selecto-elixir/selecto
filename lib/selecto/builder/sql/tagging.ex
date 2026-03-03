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
    _tag_field = Map.get(config, :tag_field, "name")

    join_table = get_join_table_name(config)
    main_foreign_key = get_main_foreign_key(config, selecto)
    tag_foreign_key = get_tag_foreign_key(config, selecto)

    requires_join = Map.get(config, :requires_join, :selecto_root)
    owner_key = Map.get(config, :owner_key, :id)
    related_key = Map.get(config, :my_key, :id)

    join_table_alias = "#{join}_jt"
    tag_table_alias = build_join_string(selecto, join)

    intermediate_join_iodata = [
      " LEFT JOIN ",
      join_table,
      " ",
      join_table_alias,
      " ON ",
      build_selector_string(selecto, requires_join, owner_key),
      " = ",
      join_table_alias,
      ".",
      main_foreign_key
    ]

    tag_join_iodata = [
      " LEFT JOIN ",
      config.source,
      " ",
      tag_table_alias,
      " ON ",
      join_table_alias,
      ".",
      tag_foreign_key,
      " = ",
      build_selector_string(selecto, join, related_key)
    ]

    combined_join_iodata = [intermediate_join_iodata, tag_join_iodata]
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
      "string_agg(",
      tag_table_alias,
      ".",
      tag_field,
      ", ', ') as ",
      column_alias
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
      "COUNT(DISTINCT ",
      tag_table_alias,
      ".id) as ",
      column_alias
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
    join_table = get_join_table_name(config)
    tag_table = config.source
    tag_field = Map.get(config, :tag_field, "name")
    # Will be dynamically determined
    main_foreign_key = Map.get(config, :main_foreign_key, "post_id")
    tag_foreign_key = Map.get(config, :tag_foreign_key, "tag_id")

    case match_type do
      :single when is_binary(tag_values) ->
        # Single tag EXISTS filter
        where_iodata = [
          "EXISTS (",
          "SELECT 1 FROM ",
          join_table,
          " jt ",
          "JOIN ",
          tag_table,
          " t ON jt.",
          tag_foreign_key,
          " = t.id ",
          "WHERE jt.",
          main_foreign_key,
          " = main.id ",
          "AND t.",
          tag_field,
          " = $1",
          ")"
        ]

        {where_iodata, [tag_values]}

      :any when is_list(tag_values) ->
        # Multiple tags with ANY match (tag1 OR tag2 OR tag3)
        where_iodata = [
          "EXISTS (",
          "SELECT 1 FROM ",
          join_table,
          " jt ",
          "JOIN ",
          tag_table,
          " t ON jt.",
          tag_foreign_key,
          " = t.id ",
          "WHERE jt.",
          main_foreign_key,
          " = main.id ",
          "AND t.",
          tag_field,
          " = ANY($1)",
          ")"
        ]

        {where_iodata, [tag_values]}

      :all when is_list(tag_values) ->
        # Multiple tags with ALL required (tag1 AND tag2 AND tag3)
        # Uses COUNT to ensure all tags are present
        tag_count = length(tag_values)

        where_iodata = [
          "(",
          "SELECT COUNT(DISTINCT t.",
          tag_field,
          ") FROM ",
          join_table,
          " jt ",
          "JOIN ",
          tag_table,
          " t ON jt.",
          tag_foreign_key,
          " = t.id ",
          "WHERE jt.",
          main_foreign_key,
          " = main.id ",
          "AND t.",
          tag_field,
          " = ANY($1)",
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
  def build_tag_count_filter(config, {operator, count})
      when operator in [:gte, :gt, :lte, :lt, :eq] do
    join_table = get_join_table_name(config)
    main_foreign_key = Map.get(config, :main_foreign_key, "post_id")

    # Map operators to SQL
    sql_op =
      case operator do
        :gte -> ">="
        :gt -> ">"
        :lte -> "<="
        :lt -> "<"
        :eq -> "="
      end

    where_iodata = [
      "(",
      "SELECT COUNT(*) FROM ",
      join_table,
      " jt ",
      "WHERE jt.",
      main_foreign_key,
      " = main.id",
      ") ",
      sql_op,
      " $1"
    ]

    {where_iodata, [count]}
  end

  def build_tag_count_filter(config, {:between, min_count, max_count}) do
    join_table = get_join_table_name(config)
    main_foreign_key = Map.get(config, :main_foreign_key, "post_id")

    where_iodata = [
      "(",
      "SELECT COUNT(*) FROM ",
      join_table,
      " jt ",
      "WHERE jt.",
      main_foreign_key,
      " = main.id",
      ") BETWEEN $1 AND $2"
    ]

    {where_iodata, [min_count, max_count]}
  end

  # Helper functions

  defp get_join_table_name(config) do
    case Map.get(config, :join_table) || Map.get(config, :join_through) do
      nil ->
        source = to_string(config.source)
        "#{String.trim_trailing(source, "s")}_#{source}"

      table_name ->
        table_name
    end
  end

  defp get_main_foreign_key(config, selecto) do
    Map.get(config, :main_foreign_key) || infer_main_foreign_key(config, selecto)
  end

  defp get_tag_foreign_key(config, selecto) do
    Map.get(config, :tag_foreign_key) || infer_tag_foreign_key(config, selecto)
  end

  defp infer_main_foreign_key(config, selecto) do
    join_keys = extract_join_key_names(config)

    parent_reference =
      case Map.get(config, :requires_join, :selecto_root) do
        :selecto_root -> extract_main_table(selecto)
        parent -> to_string(parent)
      end

    parent_prefix = singularize(parent_reference)

    find_join_key(join_keys, parent_prefix) || "#{parent_prefix}_id"
  end

  defp infer_tag_foreign_key(config, _selecto) do
    join_keys = extract_join_key_names(config)

    source_prefix =
      config
      |> Map.get(:source, "tag")
      |> to_string()
      |> singularize()

    find_join_key(join_keys, source_prefix) || "#{source_prefix}_id"
  end

  defp extract_join_key_names(config) do
    case Map.get(config, :join_keys, []) do
      join_keys when is_list(join_keys) ->
        Enum.map(join_keys, fn
          {key, _value} -> to_string(key)
          key when is_atom(key) -> to_string(key)
          key when is_binary(key) -> key
          other -> to_string(other)
        end)

      join_keys when is_map(join_keys) ->
        join_keys
        |> Map.keys()
        |> Enum.map(&to_string/1)

      _ ->
        []
    end
  end

  defp find_join_key(join_keys, prefix) do
    exact = Enum.find(join_keys, &(&1 == "#{prefix}_id"))

    exact ||
      Enum.find(join_keys, fn key ->
        String.starts_with?(key, "#{prefix}_")
      end)
  end

  defp singularize(value) when is_binary(value) do
    cond do
      String.ends_with?(value, "ies") ->
        String.replace_suffix(value, "ies", "y")

      String.ends_with?(value, "sses") ->
        String.replace_suffix(value, "es", "")

      String.ends_with?(value, "s") and String.length(value) > 1 ->
        String.trim_trailing(value, "s")

      true ->
        value
    end
  end

  defp extract_main_table(selecto) do
    # Extract main table name from selecto struct
    # This is a simplified extraction - real implementation would need to handle
    # the domain configuration properly
    case selecto do
      %{domain: %{source: %{source_table: table}}} -> table
      # Fallback
      _ -> "main"
    end
  end
end
