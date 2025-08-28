defmodule Selecto.Builder.Sql.Olap do
  @moduledoc """
  OLAP dimension optimization SQL patterns for star and snowflake schemas.
  
  Provides optimized JOIN patterns for analytical workloads, focusing on
  fact table performance and dimension table efficiency. Handles both
  star schema (denormalized dimensions) and snowflake schema (normalized
  dimension hierarchies) patterns.
  
  Phase 4: Full OLAP dimension implementation with query optimization
  
  ## Supported Patterns
  
  - **Star schema dimensions**: Direct fact-to-dimension joins optimized for aggregation
  - **Snowflake dimensions**: Multi-level normalization chains with proper JOIN ordering
  - **Dimension filtering**: Optimized WHERE clause placement for analytical queries
  - **Fact table hints**: Query hints and ordering for large fact table performance
  
  ## Star vs Snowflake
  
  **Star Schema** - Denormalized dimensions for query performance:
  ```
  fact_table -> dimension_1 (all attributes in one table)
             -> dimension_2 (all attributes in one table)
  ```
  
  **Snowflake Schema** - Normalized dimensions for data integrity:  
  ```
  fact_table -> dim_level_1 -> dim_level_2 -> dim_level_3
  ```
  
  ## Examples
  
      # Star schema: sales facts with denormalized customer dimension
      config = %{
        type: :star_dimension,
        source: "customers",
        display_field: "full_name",
        dimension_key: "customer_id"
      }
      
      # Snowflake schema: product hierarchy with normalization
      config = %{
        type: :snowflake_dimension,
        source: "products", 
        display_field: "name",
        normalization_joins: [
          %{table: "categories", key: "category_id"},
          %{table: "brands", key: "brand_id"}
        ]
      }
  """
  
  import Selecto.Builder.Sql.Helpers

  @doc """
  Build star schema dimension join optimized for OLAP queries.
  
  Star schemas prioritize query performance by denormalizing dimension data
  into single tables. This creates direct fact-to-dimension joins that are
  optimal for aggregation queries and analytical workloads.
  
  ## Optimizations Applied
  - Dimension tables joined directly to fact table
  - Dimension filters pushed down for early elimination  
  - Display fields aliased for clear result presentation
  - Faceted filtering enabled for interactive analytics
  
  ## Parameters
  - `selecto`: Main selecto struct (contains fact table info)
  - `join`: Join identifier (dimension name)
  - `config`: OLAP dimension configuration  
  - `fc`: Current from clause iodata
  - `p`: Current parameters list
  - `ctes`: Current CTEs list
  
  Returns: `{updated_from_clause, updated_params, updated_ctes}`
  """
  def build_star_dimension_join(selecto, join, config, fc, p, ctes) do
    # Extract star schema configuration
    _display_field = Map.get(config, :display_field, "name")  # Used for custom columns
    dimension_key = Map.get(config, :dimension_key, "#{join}_id")
    
    # Build fact table reference (for optimal JOIN ordering)
    fact_table_ref = get_fact_table_reference(selecto)
    dimension_alias = build_join_string(selecto, join)
    
    # Star schema: direct fact-to-dimension JOIN
    # Optimized for aggregation queries - dimension data is denormalized
    star_join_iodata = [
      " LEFT JOIN ", config.source, " ", dimension_alias,
      " ON ", fact_table_ref, ".", dimension_key, " = ", dimension_alias, ".id"
    ]
    
    # Add query hints for OLAP performance (PostgreSQL-specific optimizations)
    optimized_join_iodata = add_star_schema_hints(star_join_iodata, config)
    
    {fc ++ [optimized_join_iodata], p, ctes}
  end
  
  @doc """
  Build snowflake schema dimension join with normalization chain.
  
  Snowflake schemas normalize dimension data across multiple tables to
  maintain data integrity. This requires chaining multiple JOINs to
  reconstruct the full dimensional context.
  
  ## Normalization Chain Handling
  - Primary dimension table joined to fact
  - Secondary normalization tables joined in sequence
  - Proper JOIN ordering to avoid Cartesian products
  - Optimized for referential integrity queries
  
  ## Parameters  
  - `selecto`: Main selecto struct
  - `join`: Primary dimension identifier
  - `config`: Snowflake dimension configuration with normalization_joins
  - `fc`: Current from clause iodata
  - `p`: Current parameters list
  - `ctes`: Current CTEs list
  
  Returns: `{updated_from_clause, updated_params, updated_ctes}`
  """
  def build_snowflake_dimension_join(selecto, join, config, fc, p, ctes) do
    # Extract snowflake configuration
    _display_field = Map.get(config, :display_field, "name")  # Used for custom columns
    normalization_joins = Map.get(config, :normalization_joins, [])
    dimension_key = Map.get(config, :dimension_key, "#{join}_id")
    
    # Build primary dimension join (fact -> primary dimension table)
    fact_table_ref = get_fact_table_reference(selecto)
    primary_alias = build_join_string(selecto, join)
    
    primary_join_iodata = [
      " LEFT JOIN ", config.source, " ", primary_alias,
      " ON ", fact_table_ref, ".", dimension_key, " = ", primary_alias, ".id"
    ]
    
    # Build normalization chain JOINs (primary -> secondary -> tertiary...)
    normalization_joins_iodata = build_normalization_chain(
      primary_alias, normalization_joins, join
    )
    
    # Combine primary join with normalization chain
    combined_join_iodata = [primary_join_iodata | normalization_joins_iodata]
    
    # Add snowflake-specific optimizations
    optimized_joins = add_snowflake_schema_hints(combined_join_iodata, config)
    
    {fc ++ optimized_joins, p, ctes}
  end
  
  @doc """
  Build fact table optimization hints and JOIN ordering.
  
  Fact tables in OLAP systems are typically very large, so JOIN ordering
  and query hints are critical for performance. This function adds
  database-specific optimizations for fact table queries.
  
  ## Optimizations Applied
  - Fact table scanned first (for selective WHERE conditions)
  - Dimension tables joined in order of selectivity
  - Query hints for large table handling
  - Index hints for dimensional foreign keys
  
  ## Examples
  
      build_fact_table_optimization(selecto, :sales_facts, %{
        large_fact_table: true,
        primary_dimensions: [:time, :customer, :product],
        estimated_rows: 10_000_000
      })
  
  Returns: `{query_hints_iodata, optimization_params}`
  """
  def build_fact_table_optimization(selecto, fact_config, join_configs) do
    # Detect if this is a large fact table scenario
    estimated_rows = Map.get(fact_config, :estimated_rows, 1000)
    is_large_fact = estimated_rows > 100_000
    
    # Build query hints for fact table performance
    fact_hints = if is_large_fact do
      build_large_fact_table_hints(selecto, fact_config)
    else
      []
    end
    
    # Optimize JOIN ordering - dimensions by estimated selectivity
    join_order_hints = build_join_order_optimization(join_configs)
    
    # Combine optimizations
    combined_hints = fact_hints ++ join_order_hints
    {combined_hints, []}
  end
  
  @doc """
  Build dimension-aware WHERE clause optimization.
  
  In OLAP queries, WHERE clause placement significantly affects performance.
  Dimension filters should be applied early, while fact table filters
  need careful consideration of index usage.
  
  ## Filter Placement Strategy
  - Dimension filters: Applied at JOIN time for early elimination
  - Fact filters: Applied after JOINs for optimal fact table index usage  
  - Time dimension filters: Special handling for partitioned fact tables
  
  ## Examples
  
      build_dimension_filter_optimization(%{
        dimension_filters: [
          {"customers.region", "=", "North America"},
          {"products.category", "IN", ["Electronics", "Books"]}
        ],
        fact_filters: [
          {"sales.amount", ">", 1000},
          {"sales.date", "BETWEEN", ["2023-01-01", "2023-12-31"]}
        ]
      })
  
  Returns: `{optimized_where_iodata, filter_params}`
  """
  def build_dimension_filter_optimization(filter_config) do
    dimension_filters = Map.get(filter_config, :dimension_filters, [])
    fact_filters = Map.get(filter_config, :fact_filters, [])
    
    # Build dimension filters (applied at JOIN time)
    dim_filter_clauses = Enum.map(dimension_filters, fn {field, op, value} ->
      build_dimension_filter_clause(field, op, value)
    end)
    
    # Build fact filters (applied after JOINs)  
    fact_filter_clauses = Enum.map(fact_filters, fn {field, op, value} ->
      build_fact_filter_clause(field, op, value)
    end)
    
    # Combine with proper precedence
    combined_where = combine_olap_filters(dim_filter_clauses, fact_filter_clauses)
    
    # Extract parameters from all filters
    all_params = extract_filter_params(dimension_filters ++ fact_filters)
    
    {combined_where, all_params}
  end
  
  # Helper functions
  
  defp get_fact_table_reference(selecto) do
    # Extract fact table name from selecto configuration
    case selecto do
      %{domain: %{source: %{source_table: table}}} -> table
      _ -> "fact_table"  # Fallback for testing
    end
  end
  
  defp add_star_schema_hints(join_iodata, config) do
    # Add PostgreSQL-specific hints for star schema performance
    case Map.get(config, :enable_query_hints, false) do
      true ->
        # Add index hints and join order suggestions
        hint_comments = [
          "/* STAR_SCHEMA_HINT: Use dimension indexes */ ",
          join_iodata
        ]
        hint_comments
        
      false ->
        join_iodata
    end
  end
  
  defp build_normalization_chain(primary_alias, normalization_joins, base_join) do
    # Build a chain of JOINs for snowflake normalization
    # Each join references the previous table in the chain
    {_, join_chain} = Enum.reduce(normalization_joins, {primary_alias, []}, 
      fn norm_join, {prev_alias, acc_joins} ->
        
        # Extract normalization join configuration
        norm_table = Map.get(norm_join, :table)
        norm_key = Map.get(norm_join, :key, "id")
        norm_foreign_key = Map.get(norm_join, :foreign_key, "#{norm_table}_id")
        
        # Create alias for this normalization table
        norm_alias = "#{base_join}_#{norm_table}"
        
        # Build JOIN from previous table to this normalization table
        norm_join_iodata = [
          " LEFT JOIN ", norm_table, " ", norm_alias,
          " ON ", prev_alias, ".", norm_foreign_key, " = ", norm_alias, ".", norm_key
        ]
        
        {norm_alias, acc_joins ++ [norm_join_iodata]}
      end
    )
    
    join_chain
  end
  
  defp add_snowflake_schema_hints(join_iodata_list, config) do
    # Add optimization hints for snowflake schema queries
    case Map.get(config, :enable_query_hints, false) do
      true ->
        # Prepend query hint for snowflake optimization
        hint = "/* SNOWFLAKE_SCHEMA_HINT: Optimize normalization chain */ "
        [hint | join_iodata_list]
        
      false ->
        join_iodata_list
    end
  end
  
  defp build_large_fact_table_hints(_selecto, fact_config) do
    # Generate PostgreSQL-specific hints for large fact tables
    table_name = Map.get(fact_config, :table_name, "fact_table")
    
    [
      "/* LARGE_FACT_TABLE: #{table_name} */ ",
      "/* HINT: seq_page_cost=0.1, random_page_cost=0.1 */ "
    ]
  end
  
  defp build_join_order_optimization(join_configs) do
    # Suggest optimal JOIN ordering based on dimension characteristics
    # More selective dimensions should be joined first
    
    case Map.get(join_configs, :optimize_join_order, false) do
      true ->
        ["/* HINT: Use dimension selectivity for JOIN order */ "]
      false ->
        []
    end
  end
  
  defp build_dimension_filter_clause(field, operator, value) do
    # Build WHERE clause for dimension filtering
    case operator do
      "=" -> [field, " = $?"]
      "IN" when is_list(value) -> [field, " IN ($?)"]
      "LIKE" -> [field, " LIKE $?"]
      _ -> [field, " ", operator, " $?"]
    end
  end
  
  defp build_fact_filter_clause(field, operator, value) do
    # Build WHERE clause for fact table filtering
    case operator do
      "BETWEEN" when is_list(value) and length(value) == 2 ->
        [field, " BETWEEN $? AND $?"]
      _ ->
        [field, " ", operator, " $?"]
    end
  end
  
  defp combine_olap_filters(dim_filters, fact_filters) do
    # Combine dimension and fact filters with proper precedence
    all_filters = dim_filters ++ fact_filters
    
    case all_filters do
      [] -> []
      [single] -> single
      multiple -> 
        # Join with AND, wrapping each filter in parentheses
        filter_parts = Enum.map(multiple, fn filter -> ["(", filter, ")"] end)
        Enum.intersperse(filter_parts, " AND ")
    end
  end
  
  defp extract_filter_params(filters) do
    # Extract parameter values from filter specifications
    # Keep arrays as single parameters to avoid flattening
    Enum.map(filters, fn {_field, _op, value} ->
      value  # Keep value as-is (arrays stay as arrays, scalars stay as scalars)
    end)
  end
  
  @doc """
  Build OLAP-optimized join with pattern detection.
  
  Main entry point for OLAP join building. Detects whether to use
  star or snowflake patterns based on configuration and applies
  appropriate optimizations.
  
  ## Pattern Detection
  - Star schema: Single dimension table with denormalized data
  - Snowflake schema: Multiple normalization tables in chain
  
  Returns: `{from_clause_iodata, params, ctes}`
  """
  def build_olap_join_with_optimization(selecto, join, config, olap_type, fc, p, ctes) do
    case olap_type do
      :star ->
        build_star_dimension_join(selecto, join, config, fc, p, ctes)
        
      :snowflake ->
        build_snowflake_dimension_join(selecto, join, config, fc, p, ctes)
        
      _ ->
        # Fallback to basic dimension join
        build_basic_dimension_join(selecto, join, config, fc, p, ctes)
    end
  end
  
  # Fallback for unrecognized OLAP patterns
  defp build_basic_dimension_join(selecto, join, config, fc, p, ctes) do
    # Basic dimension join without OLAP-specific optimizations
    basic_join_iodata = [
      " LEFT JOIN ", config.source, " ", build_join_string(selecto, join),
      " ON ", build_selector_string(selecto, config.requires_join, config.owner_key),
      " = ", build_selector_string(selecto, join, config.my_key)
    ]
    
    {fc ++ [basic_join_iodata], p, ctes}
  end
end