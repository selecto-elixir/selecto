defmodule Selecto.Builder.Sql.Hierarchy do
  @moduledoc """
  Hierarchical SQL pattern generation for self-referencing relationships.
  
  Supports adjacency lists, materialized paths, and closure table patterns
  using recursive CTEs and specialized SQL constructs.
  
  Phase 1: Foundation stubs that maintain backward compatibility
  Phase 2: Full CTE implementation for all hierarchy patterns
  """
  
  import Selecto.Builder.Sql.Helpers
  # alias Selecto.Builder.Cte  # Phase 2: Will be used for CTE generation
  
  @doc """
  Build hierarchical join with appropriate CTE pattern.
  
  Phase 1: Returns basic LEFT JOIN to maintain existing functionality
  Phase 2: Will implement full recursive CTE generation
  
  Returns: {from_clause_iodata, params, [ctes]}
  """
  def build_hierarchy_join_with_cte(selecto, join, config, pattern, fc, p, ctes) do
    case pattern do
      :adjacency_list ->
        build_adjacency_list_join(selecto, join, config, fc, p, ctes)
      
      :materialized_path ->
        build_materialized_path_join(selecto, join, config, fc, p, ctes)
      
      :closure_table ->
        build_closure_table_join(selecto, join, config, fc, p, ctes)
        
      _ ->
        # Fallback for unrecognized patterns
        build_basic_hierarchy_fallback(selecto, join, config, fc, p, ctes)
    end
  end
  
  # Phase 2: Adjacency List - Full Implementation
  defp build_adjacency_list_join(selecto, join, config, fc, p, ctes) do
    # Build recursive CTE for adjacency list pattern
    {hierarchy_cte, cte_params} = build_adjacency_list_cte(selecto, join, config)
    
    # Create CTE name for this hierarchy
    cte_name = hierarchy_cte_name(join)
    
    # Build JOIN to reference the CTE in the main query
    # Join main table to the CTE results
    cte_join_iodata = [
      " LEFT JOIN ", cte_name, " ", build_join_string(selecto, join),
      " ON ", build_selector_string(selecto, config.requires_join, config.owner_key),
      " = ", build_join_string(selecto, join), ".id"
    ]
    
    # Add CTE to the list and return JOIN clause with updated parameters
    new_ctes = ctes ++ [{hierarchy_cte, cte_params}]
    {fc ++ [cte_join_iodata], p ++ cte_params, new_ctes}
  end
  
  # Phase 2: Materialized Path - Full Implementation
  defp build_materialized_path_join(selecto, join, config, fc, p, ctes) do
    # Build materialized path query CTE
    {path_cte, cte_params} = build_materialized_path_query(selecto, join, config)
    
    # Create CTE name for this path query
    cte_name = "#{join}_materialized_path"
    
    # Build JOIN to reference the materialized path CTE
    cte_join_iodata = [
      " LEFT JOIN ", cte_name, " ", build_join_string(selecto, join),
      " ON ", build_selector_string(selecto, config.requires_join, config.owner_key),
      " = ", build_join_string(selecto, join), ".id"
    ]
    
    # Add CTE to the list and return JOIN clause
    new_ctes = ctes ++ [{path_cte, cte_params}]
    {fc ++ [cte_join_iodata], p ++ cte_params, new_ctes}
  end
  
  # Phase 2: Closure Table - Full Implementation
  defp build_closure_table_join(selecto, join, config, fc, p, ctes) do
    # Build closure table query CTE
    {closure_cte, cte_params} = build_closure_table_query(selecto, join, config)
    
    # Create CTE name for this closure query
    cte_name = "#{join}_closure"
    
    # Build JOIN to reference the closure table CTE
    cte_join_iodata = [
      " LEFT JOIN ", cte_name, " ", build_join_string(selecto, join),
      " ON ", build_selector_string(selecto, config.requires_join, config.owner_key),
      " = ", build_join_string(selecto, join), ".id"
    ]
    
    # Add CTE to the list and return JOIN clause
    new_ctes = ctes ++ [{closure_cte, cte_params}]
    {fc ++ [cte_join_iodata], p ++ cte_params, new_ctes}
  end
  
  # Fallback for basic hierarchy joins or unrecognized patterns
  defp build_basic_hierarchy_fallback(selecto, join, config, fc, p, ctes) do
    basic_join_iodata = build_basic_join_clause(selecto, join, config)
    {fc ++ [basic_join_iodata], p, ctes}
  end
  
  # Helper: Build basic LEFT JOIN clause for Phase 1 compatibility
  defp build_basic_join_clause(selecto, join, config) do
    [
      " left join ", config.source, " ", build_join_string(selecto, join),
      " on ", build_selector_string(selecto, join, config.my_key),
      " = ", build_selector_string(selecto, config.requires_join, config.owner_key)
    ]
  end
  
  # Phase 2 Helper Stubs - Will be implemented in Phase 2
  
  @doc """
  Build recursive CTE for adjacency list pattern.
  
  Generates a recursive CTE that traverses parent-child relationships to build
  hierarchical paths and levels. Uses Selecto-powered CTE generation for safety.
  
  ## Examples
  
      # Build CTE for category hierarchy
      {hierarchy_cte, params} = build_adjacency_list_cte(selecto, :categories, config)
      
      # Generates CTE like:
      # WITH RECURSIVE category_hierarchy AS (
      #   -- Base case: root nodes
      #   SELECT id, name, parent_id, 0 as level, CAST(id AS TEXT) as path
      #   FROM categories WHERE parent_id IS NULL
      #   UNION ALL
      #   -- Recursive case: child nodes  
      #   SELECT c.id, c.name, c.parent_id, h.level + 1, h.path || '/' || c.id
      #   FROM categories c JOIN category_hierarchy h ON c.parent_id = h.id
      #   WHERE h.level < 5
      # )
  
  Returns: {cte_iodata, params}
  """
  def build_adjacency_list_cte(_selecto, join, config) do
    # Get configuration values
    source_table = config.source
    depth_limit = Map.get(config, :hierarchy_depth, 5)
    id_field = Map.get(config, :id_field, "id")
    name_field = Map.get(config, :name_field, "name") 
    parent_field = Map.get(config, :parent_field, "parent_id")
    
    # For now, create a simplified CTE without full Selecto integration
    # This is Phase 2.0 - will be enhanced in Phase 2.5
    cte_name = hierarchy_cte_name(join)
    
    # Build raw SQL iodata for recursive CTE with parameter placeholder
    # Base case: SELECT id, name, parent_id, 0 as level, CAST(id AS TEXT) as path FROM table WHERE parent_id IS NULL
    base_case_iodata = [
      "SELECT #{id_field}, #{name_field}, #{parent_field}, 0 as level, ",
      "CAST(#{id_field} AS TEXT) as path, ARRAY[#{id_field}] as path_array ",
      "FROM #{source_table} WHERE #{parent_field} IS NULL"
    ]
    
    # Recursive case: JOIN with CTE (use raw placeholder for now)
    recursive_case_iodata = [
      "SELECT c.#{id_field}, c.#{name_field}, c.#{parent_field}, h.level + 1, ",
      "h.path || '/' || CAST(c.#{id_field} AS TEXT), h.path_array || c.#{id_field} ",
      "FROM #{source_table} c JOIN #{cte_name} h ON c.#{parent_field} = h.#{id_field} ",
      "WHERE h.level < $1"
    ]
    
    # Build complete CTE
    cte_iodata = [
      "WITH RECURSIVE ", cte_name, " AS (",
      base_case_iodata,
      " UNION ALL ",
      recursive_case_iodata,
      ")"
    ]
    
    # Return CTE with parameters
    {cte_iodata, [depth_limit]}
  end
  
  @doc """
  Build materialized path query patterns.
  
  Generates SQL for path-based hierarchy queries using LIKE patterns and depth calculations.
  
  ## Example
  
      # For a path like "root/electronics/computers"
      config = %{
        source: "categories", 
        path_field: "path",
        path_separator: "/",
        root_path: "root"
      }
      
      # Generates SQL that calculates depth and filters by path patterns
  
  Returns: {query_iodata, params}
  """
  def build_materialized_path_query(_selecto, join, config) do
    source_table = config.source
    path_field = Map.get(config, :path_field, "path")
    path_separator = Map.get(config, :path_separator, "/")
    root_path = Map.get(config, :root_path, "")
    
    # Build path pattern for filtering descendants
    # If root_path is provided, filter for paths starting with it
    path_pattern = case root_path do
      "" -> "%"  # Match any path
      path -> "#{path}#{path_separator}%"  # Match descendants of root_path
    end
    
    # Build query that includes depth calculation
    query_name = "#{join}_materialized_path"
    query_iodata = [
      "WITH ", query_name, " AS (",
      "SELECT *, ",
      "(length(#{path_field}) - length(replace(#{path_field}, '#{path_separator}', ''))) as depth, ",
      "string_to_array(#{path_field}, '#{path_separator}') as path_array ",
      "FROM #{source_table} ",
      "WHERE #{path_field} LIKE $1",
      ")"
    ]
    
    {query_iodata, [path_pattern]}
  end
  
  @doc """
  Build closure table join patterns.
  
  Generates queries for closure table pattern with ancestor-descendant relationships.
  Closure tables maintain all ancestor-descendant relationships explicitly.
  
  ## Example
  
      # For a closure table with ancestor_id, descendant_id, depth columns
      config = %{
        source: "categories",
        closure_table: "category_closure", 
        ancestor_field: "ancestor_id",
        descendant_field: "descendant_id",
        depth_field: "depth"
      }
      
  Returns: {query_iodata, params}
  """
  def build_closure_table_query(_selecto, join, config) do
    source_table = config.source
    closure_table = Map.get(config, :closure_table, "#{source_table}_closure")
    ancestor_field = Map.get(config, :ancestor_field, "ancestor_id")
    descendant_field = Map.get(config, :descendant_field, "descendant_id")
    depth_field = Map.get(config, :depth_field, "depth")
    
    # Build closure table query that includes descendant count
    query_name = "#{join}_closure"
    query_iodata = [
      "WITH ", query_name, " AS (",
      "SELECT c.*, cl.#{depth_field}, ",
      "(SELECT COUNT(*) FROM #{closure_table} cl2 ",
      "WHERE cl2.#{ancestor_field} = c.id) as descendant_count ",
      "FROM #{source_table} c ",
      "JOIN #{closure_table} cl ON c.id = cl.#{descendant_field}",
      ")"
    ]
    
    # No parameters needed for basic closure table query
    {query_iodata, []}
  end
  
  # Phase 2: Additional helper functions will be added here
  # - cycle detection for adjacency lists
  # - path parsing and validation
  # - closure table optimization
  # - CTE parameter coordination

  @doc """
  Example of how Phase 2 hierarchical joins would use Selecto-powered CTEs.
  
  This demonstrates the integration pattern that will be implemented in Phase 2,
  showing how hierarchical joins will leverage the new Selecto CTE API.
  
  ## Phase 2 Implementation Preview
  
      # Instead of raw SQL generation, we'll use Selecto queries:
      
      def build_adjacency_cte_with_selecto(selecto, join, config) do
        domain = build_hierarchy_domain(selecto, join, config)
        connection = selecto.postgrex_opts
        
        # Base case using Selecto
        base_case = Selecto.configure(domain, connection)
          |> Selecto.select([
            "id", 
            "name", 
            "parent_id",
            {:literal, 0, "level"},
            {:literal, "id", "path"}
          ])
          |> Selecto.filter([{"parent_id", nil}])
        
        # Recursive case using Selecto  
        recursive_case = Selecto.configure(domain, connection)
          |> Selecto.select([
            "c.id",
            "c.name",
            "c.parent_id", 
            "h.level + 1",
            {:func, "concat", ["h.path", {:literal, "/"}, "c.id"]}
          ])
          |> Selecto.filter([{"h.level", {:lt, 5}}])
          # Special handling for CTE JOIN would be added here
        
        # Use Selecto-powered CTE generation
        Selecto.Builder.Cte.build_recursive_cte_from_selecto(
          hierarchy_name(join), base_case, recursive_case
        )
      end
  """
  def example_selecto_hierarchy_usage do
    """
    This function demonstrates the intended usage pattern for Phase 2:
    
    # Users will build hierarchical CTEs using familiar Selecto syntax
    hierarchy_domain = configure_hierarchy_domain(categories_table)
    
    {hierarchy_cte, params} = Cte.build_hierarchy_cte_from_selecto(
      "category_tree",
      hierarchy_domain,
      connection,
      %{
        id_field: "id",
        name_field: "name",
        parent_field: "parent_id",
        depth_limit: 5,
        root_condition: [{"parent_id", nil}],
        additional_fields: ["description", "sort_order"]
      }
    )
    
    # Main query can then reference the CTE
    main_selecto = Selecto.configure(main_domain, connection)
      |> Selecto.select(["main.*", "h.level", "h.path"])
      |> Selecto.filter([{"main.active", true}])
    
    # CTE integration happens automatically in the SQL builder
    {final_sql, final_params} = build_query_with_ctes(main_selecto, [hierarchy_cte])
    """
  end

  # Phase 2: Helper functions for Selecto CTE integration
  
  def hierarchy_cte_name(join), do: "#{join}_hierarchy"
  
  defp build_hierarchy_domain_from_config(_selecto, config) do
    # Build a simplified domain for hierarchy CTE generation
    # Extract the source table structure from the main selecto configuration
    source_table = config.source
    id_field = Map.get(config, :id_field, "id")
    name_field = Map.get(config, :name_field, "name") 
    parent_field = Map.get(config, :parent_field, "parent_id")
    
    # Create field atoms for consistency
    id_atom = String.to_atom(id_field)
    name_atom = String.to_atom(name_field)
    parent_atom = String.to_atom(parent_field)
    
    # Create a minimal domain for this table with dynamic field names
    %{
      name: "Hierarchy Domain",
      source: %{
        source_table: source_table,
        primary_key: id_atom,
        fields: [id_atom, name_atom, parent_atom],  # Use configured field names
        redact_fields: [],
        columns: %{
          id_atom => %{type: :integer},
          name_atom => %{type: :string},
          parent_atom => %{type: :integer}
        },
        associations: %{}
      },
      schemas: %{},
      default_selected: [id_field, name_field],
      joins: %{},
      filters: %{}
    }
  end
  
  defp configure_hierarchy_domain(table) do
    # Configure domain specifically for hierarchy operations
    %{
      name: "Hierarchy Domain",
      source: %{
        source_table: table,
        primary_key: :id,
        fields: [:id, :name, :parent_id],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string}, 
          parent_id: %{type: :integer}
        },
        associations: %{}
      },
      schemas: %{},
      default_selected: ["id", "name"],
      joins: %{},
      filters: %{}
    }
  end
  
  defp build_query_with_ctes(main_selecto, ctes) do
    # Integrate CTEs with main Selecto query
    # This coordinates CTE parameters with main query parameters
    {main_sql, _aliases, main_params} = Selecto.gen_sql(main_selecto, [])
    main_iodata = [main_sql]
    
    # Use our CTE integration
    Selecto.Builder.Cte.integrate_ctes_with_query(ctes, main_iodata, main_params)
  end
end