defmodule Selecto.Advanced.CTE do
  @moduledoc """
  Common Table Expression (CTE) support for PostgreSQL WITH clauses.
  
  Provides comprehensive support for non-recursive and recursive CTEs, enabling
  hierarchical queries, query modularity, and complex data processing patterns.
  
  ## Examples
  
      # Non-recursive CTE
      selecto
      |> Selecto.with_cte("high_value_customers", fn ->
          Selecto.configure(customer_domain, connection)
          |> Selecto.select(["customer_id", "first_name", "last_name"])
          |> Selecto.aggregate([{"payment.amount", :sum, as: "total_spent"}])
          |> Selecto.join(:inner, "payment", on: "customer.customer_id = payment.customer_id")
          |> Selecto.group_by(["customer.customer_id", "customer.first_name", "customer.last_name"])
          |> Selecto.having([{"total_spent", {:>, 100}}])
        end)
      |> Selecto.select(["film.title", "high_value_customers.first_name"])
      |> Selecto.join(:inner, "high_value_customers", 
          on: "rental.customer_id = high_value_customers.customer_id")
      
      # Recursive CTE for hierarchical data
      selecto
      |> Selecto.with_recursive_cte("org_hierarchy",
          base_query: fn ->
            # Anchor: top-level managers
            Selecto.configure(employee_domain, connection)
            |> Selecto.select(["employee_id", "name", "manager_id", {:literal, 0, as: "level"}])
            |> Selecto.filter([{"manager_id", nil}])
          end,
          recursive_query: fn cte ->
            # Recursive: employees under each manager
            Selecto.configure(employee_domain, connection)
            |> Selecto.select(["employee.employee_id", "employee.name", "employee.manager_id", 
                              {:func, "org_hierarchy.level + 1", as: "level"}])
            |> Selecto.join(:inner, cte, on: "employee.manager_id = org_hierarchy.employee_id")
          end
        )
  """
  
  defmodule Spec do
    @moduledoc """
    Specification for Common Table Expression definitions.
    """
    defstruct [
      :id,                    # Unique identifier for the CTE
      :name,                  # CTE name (used in WITH clause)
      :query_builder,         # Function that builds the CTE query
      :columns,              # Optional column list
      :type,                 # :normal or :recursive
      :base_query,           # For recursive CTEs - the anchor query
      :recursive_query,      # For recursive CTEs - the recursive part
      :dependencies,         # List of other CTEs this depends on
      :validated             # Boolean indicating if CTE has been validated
    ]
    
    @type cte_type :: :normal | :recursive
    
    @type t :: %__MODULE__{
      id: String.t(),
      name: String.t(),
      query_builder: (-> struct()) | nil,
      columns: [String.t()] | nil,
      type: cte_type(),
      base_query: (-> struct()) | nil,
      recursive_query: (struct() -> struct()) | nil,
      dependencies: [String.t()],
      validated: boolean()
    }
  end
  
  defmodule ValidationError do
    @moduledoc """
    Error raised when CTE specification is invalid.
    """
    defexception [:type, :message, :details]
    
    @type t :: %__MODULE__{
      type: :invalid_name | :invalid_query | :circular_dependency | :missing_recursive_parts,
      message: String.t(),
      details: map()
    }
  end
  
  @doc """
  Create a non-recursive CTE specification.
  
  ## Parameters
  
  - `name` - CTE name for the WITH clause
  - `query_builder` - Function that returns a Selecto query
  - `opts` - Options including :columns, :dependencies
  
  ## Examples
  
      # Simple CTE
      CTE.create_cte("active_customers", fn ->
        Selecto.configure(customer_domain, connection)
        |> Selecto.filter([{"active", true}])
      end)
      
      # CTE with explicit columns
      CTE.create_cte("customer_stats", 
        fn ->
          Selecto.configure(customer_domain, connection)
          |> Selecto.select(["customer_id", {:func, "COUNT", ["rental_id"], as: "rental_count"}])
          |> Selecto.join(:left, "rental", on: "customer.customer_id = rental.customer_id")
          |> Selecto.group_by(["customer_id"])
        end,
        columns: ["customer_id", "rental_count"]
      )
  """
  def create_cte(name, query_builder, opts \\ []) do
    spec = %Spec{
      id: generate_cte_id(name),
      name: name,
      query_builder: query_builder,
      columns: Keyword.get(opts, :columns),
      type: :normal,
      base_query: nil,
      recursive_query: nil,
      dependencies: Keyword.get(opts, :dependencies, []),
      validated: false
    }
    
    case validate_cte(spec) do
      {:ok, validated_spec} -> validated_spec
      {:error, validation_error} -> raise validation_error
    end
  end
  
  @doc """
  Create a recursive CTE specification.
  
  ## Parameters
  
  - `name` - CTE name for the WITH clause
  - `base_query` - Function that returns the anchor query
  - `recursive_query` - Function that takes the CTE reference and returns recursive query
  - `opts` - Options including :columns, :dependencies
  
  ## Examples
  
      # Hierarchical employee structure
      CTE.create_recursive_cte("employee_hierarchy",
        base_query: fn ->
          # Anchor: top-level managers
          Selecto.configure(employee_domain, connection)
          |> Selecto.select(["employee_id", "name", "manager_id", {:literal, 0, as: "level"}])
          |> Selecto.filter([{"manager_id", nil}])
        end,
        recursive_query: fn cte_ref ->
          # Recursive: subordinates
          Selecto.configure(employee_domain, connection)
          |> Selecto.select(["employee.employee_id", "employee.name", "employee.manager_id",
                            {:func, "employee_hierarchy.level + 1", as: "level"}])
          |> Selecto.join(:inner, cte_ref, on: "employee.manager_id = employee_hierarchy.employee_id")
        end
      )
  """
  def create_recursive_cte(name, opts) do
    base_query = Keyword.get(opts, :base_query)
    recursive_query = Keyword.get(opts, :recursive_query)
    
    unless is_function(base_query, 0) do
      raise ArgumentError, "base_query must be a function with arity 0"
    end
    
    unless is_function(recursive_query, 1) do
      raise ArgumentError, "recursive_query must be a function with arity 1"
    end
    
    spec = %Spec{
      id: generate_cte_id(name),
      name: name,
      query_builder: nil,
      columns: Keyword.get(opts, :columns),
      type: :recursive,
      base_query: base_query,
      recursive_query: recursive_query,
      dependencies: Keyword.get(opts, :dependencies, []),
      validated: false
    }
    
    case validate_cte(spec) do
      {:ok, validated_spec} -> validated_spec
      {:error, validation_error} -> raise validation_error
    end
  end
  
  @doc """
  Validate a CTE specification.
  
  Ensures the CTE name is valid, queries are properly formed,
  and dependencies don't create circular references.
  """
  def validate_cte(%Spec{} = spec) do
    with :ok <- validate_cte_name(spec.name),
         :ok <- validate_cte_queries(spec),
         :ok <- validate_cte_dependencies(spec) do
      
      validated_spec = %{spec | validated: true}
      {:ok, validated_spec}
    else
      {:error, reason} -> {:error, reason}
    end
  end
  
  # Validate CTE name follows SQL identifier rules
  defp validate_cte_name(name) when is_binary(name) do
    cond do
      String.length(name) == 0 ->
        {:error, %ValidationError{
          type: :invalid_name,
          message: "CTE name cannot be empty",
          details: %{name: name}
        }}
        
      not String.match?(name, ~r/^[a-zA-Z_][a-zA-Z0-9_]*$/) ->
        {:error, %ValidationError{
          type: :invalid_name,
          message: "CTE name must be a valid SQL identifier",
          details: %{name: name, expected: "Valid SQL identifier (letters, numbers, underscore)"}
        }}
        
      String.length(name) > 63 ->
        {:error, %ValidationError{
          type: :invalid_name,
          message: "CTE name too long (max 63 characters)",
          details: %{name: name, length: String.length(name)}
        }}
        
      true -> :ok
    end
  end
  
  defp validate_cte_name(name) do
    {:error, %ValidationError{
      type: :invalid_name,
      message: "CTE name must be a string",
      details: %{name: name}
    }}
  end
  
  # Validate CTE queries are properly formed
  defp validate_cte_queries(%Spec{type: :normal, query_builder: query_builder}) do
    if is_function(query_builder, 0) do
      :ok
    else
      {:error, %ValidationError{
        type: :invalid_query,
        message: "Normal CTE must have a query_builder function with arity 0",
        details: %{}
      }}
    end
  end
  
  defp validate_cte_queries(%Spec{type: :recursive, base_query: base_query, recursive_query: recursive_query}) do
    cond do
      not is_function(base_query, 0) ->
        {:error, %ValidationError{
          type: :missing_recursive_parts,
          message: "Recursive CTE must have a base_query function with arity 0",
          details: %{}
        }}
        
      not is_function(recursive_query, 1) ->
        {:error, %ValidationError{
          type: :missing_recursive_parts,
          message: "Recursive CTE must have a recursive_query function with arity 1",
          details: %{}
        }}
        
      true -> :ok
    end
  end
  
  # Validate CTE dependencies (placeholder for circular dependency detection)
  defp validate_cte_dependencies(%Spec{dependencies: dependencies}) do
    # For now, just validate that dependencies are strings
    if Enum.all?(dependencies, &is_binary/1) do
      :ok
    else
      {:error, %ValidationError{
        type: :circular_dependency,
        message: "CTE dependencies must be strings",
        details: %{dependencies: dependencies}
      }}
    end
  end
  
  # Generate unique ID for CTE
  defp generate_cte_id(name) do
    unique = :erlang.unique_integer([:positive])
    "cte_#{name}_#{unique}"
  end
  
  @doc """
  Detect circular dependencies in a list of CTEs.
  
  Returns {:error, cycle} if a circular dependency is found,
  {:ok, ordered_ctes} if CTEs can be ordered for execution.
  """
  def detect_circular_dependencies(ctes) when is_list(ctes) do
    # Build dependency graph
    graph = build_dependency_graph(ctes)
    
    # Perform topological sort to detect cycles
    case topological_sort(graph) do
      {:ok, ordered_names} ->
        # Return CTEs in dependency order
        ordered_ctes = Enum.map(ordered_names, fn name ->
          Enum.find(ctes, &(&1.name == name))
        end)
        {:ok, ordered_ctes}
        
      {:error, cycle} ->
        {:error, %ValidationError{
          type: :circular_dependency,
          message: "Circular dependency detected in CTEs",
          details: %{cycle: cycle}
        }}
    end
  end
  
  # Build dependency graph from CTE list
  defp build_dependency_graph(ctes) do
    Enum.reduce(ctes, %{}, fn cte, graph ->
      Map.put(graph, cte.name, cte.dependencies)
    end)
  end
  
  # Simple topological sort implementation
  defp topological_sort(graph) do
    # Find nodes with no incoming edges
    all_nodes = Map.keys(graph)
    nodes_with_incoming = graph |> Map.values() |> List.flatten() |> MapSet.new()
    start_nodes = Enum.reject(all_nodes, &MapSet.member?(nodes_with_incoming, &1))
    
    topological_sort_helper(graph, start_nodes, [], MapSet.new())
  end
  
  defp topological_sort_helper(graph, [], result, visited) do
    if MapSet.size(visited) == map_size(graph) do
      {:ok, Enum.reverse(result)}
    else
      # There are remaining nodes, which means there's a cycle
      remaining = Map.keys(graph) |> Enum.reject(&MapSet.member?(visited, &1))
      {:error, remaining}
    end
  end
  
  defp topological_sort_helper(graph, [node | rest_nodes], result, visited) do
    if MapSet.member?(visited, node) do
      topological_sort_helper(graph, rest_nodes, result, visited)
    else
      new_visited = MapSet.put(visited, node)
      new_result = [node | result]
      
      # Add nodes that depend on this node to the queue
      dependencies = Map.get(graph, node, [])
      ready_nodes = Enum.filter(dependencies, fn dep ->
        # Check if all dependencies of dep are already visited
        dep_deps = Map.get(graph, dep, [])
        Enum.all?(dep_deps, &MapSet.member?(new_visited, &1))
      end)
      
      topological_sort_helper(graph, rest_nodes ++ ready_nodes, new_result, new_visited)
    end
  end
end