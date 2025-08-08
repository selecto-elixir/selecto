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
  
  # Phase 1: Adjacency List Stub
  # Phase 2: Will implement recursive CTE like this:
  # WITH RECURSIVE hierarchy AS (
  #   SELECT id, name, parent_id, 0 as level, CAST(name AS TEXT) as path
  #   FROM categories WHERE parent_id IS NULL
  #   UNION ALL
  #   SELECT c.id, c.name, c.parent_id, h.level + 1, h.path || ' > ' || c.name
  #   FROM categories c JOIN hierarchy h ON c.parent_id = h.id
  #   WHERE h.level < 5
  # )
  defp build_adjacency_list_join(selecto, join, config, fc, p, ctes) do
    # Phase 1: Return basic LEFT JOIN to maintain functionality
    basic_join_iodata = build_basic_join_clause(selecto, join, config)
    
    # TODO Phase 2: Replace with recursive CTE
    # {hierarchy_cte, cte_params} = build_adjacency_cte(selecto, join, config)
    # new_ctes = ctes ++ [{hierarchy_cte, cte_params}]
    # {fc ++ [cte_join_reference], p ++ cte_params, new_ctes}
    
    {fc ++ [basic_join_iodata], p, ctes}
  end
  
  # Phase 1: Materialized Path Stub  
  # Phase 2: Will implement path-based queries like this:
  # SELECT *, (length(path) - length(replace(path, '/', ''))) as depth
  # FROM categories WHERE path LIKE 'root/electronics%'
  defp build_materialized_path_join(selecto, join, config, fc, p, ctes) do
    # Phase 1: Return basic LEFT JOIN
    basic_join_iodata = build_basic_join_clause(selecto, join, config)
    
    # TODO Phase 2: Implement path-based SQL generation
    # path_field = Map.get(config, :path_field, :path)
    # path_condition = build_path_condition(selecto, join, config, path_field)
    
    {fc ++ [basic_join_iodata], p, ctes}
  end
  
  # Phase 1: Closure Table Stub
  # Phase 2: Will implement closure table patterns like this:
  # SELECT c.*, cl.depth, 
  #   (SELECT COUNT(*) FROM category_closure cl2 WHERE cl2.ancestor_id = c.id) as descendant_count
  # FROM categories c
  # JOIN category_closure cl ON c.id = cl.descendant_id
  # WHERE cl.ancestor_id = ?
  defp build_closure_table_join(selecto, join, config, fc, p, ctes) do
    # Phase 1: Return basic LEFT JOIN
    basic_join_iodata = build_basic_join_clause(selecto, join, config)
    
    # TODO Phase 2: Implement closure table SQL with intermediate joins
    # closure_table = Map.get(config, :closure_table)
    # ancestor_field = Map.get(config, :ancestor_field, :ancestor_id)
    # descendant_field = Map.get(config, :descendant_field, :descendant_id)
    
    {fc ++ [basic_join_iodata], p, ctes}
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
  
  Phase 2 Implementation: Generate WITH RECURSIVE for parent-child traversal
  """
  def build_adjacency_cte(_selecto, _join, _config) do
    # Phase 2: Full implementation
    # Will generate recursive CTE with proper parameterization
    # Will handle depth limits and cycle detection
    raise "Phase 2: Not yet implemented - use basic joins for now"
  end
  
  @doc """
  Build materialized path query patterns.
  
  Phase 2 Implementation: Generate path-based WHERE conditions  
  """
  def build_materialized_path_query(_selecto, _join, _config) do
    # Phase 2: Full implementation
    # Will generate LIKE patterns for path traversal
    # Will handle path depth calculations
    raise "Phase 2: Not yet implemented - use basic joins for now"
  end
  
  @doc """
  Build closure table join patterns.
  
  Phase 2 Implementation: Generate ancestor-descendant relationship queries
  """
  def build_closure_table_query(_selecto, _join, _config) do
    # Phase 2: Full implementation  
    # Will generate multi-table closure joins
    # Will handle descendant counting and depth queries
    raise "Phase 2: Not yet implemented - use basic joins for now"
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

  # Phase 2 Preview: Helper functions for Selecto CTE integration
  
  defp hierarchy_name(join), do: "#{join}_hierarchy"
  
  defp build_hierarchy_domain(_selecto, _join, _config) do
    # Phase 2: Build appropriate domain for hierarchy CTE generation
    # This would extract the relevant table schema and create a domain
    # suitable for building the base and recursive cases
    raise "Phase 2: Not yet implemented"
  end
  
  defp configure_hierarchy_domain(_table) do
    # Phase 2: Configure domain specifically for hierarchy operations
    raise "Phase 2: Not yet implemented"
  end
  
  defp build_query_with_ctes(_main_selecto, _ctes) do
    # Phase 2: Integrate CTEs with main Selecto query
    # This would coordinate CTE parameters with main query parameters
    raise "Phase 2: Not yet implemented"
  end
end