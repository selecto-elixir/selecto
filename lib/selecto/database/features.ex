defmodule Selecto.Database.Features do
  @moduledoc """
  Feature detection and compatibility system for database adapters.
  
  Provides a standardized way to check for database feature support
  and handle capability differences across database systems.
  """

  @type feature :: atom()
  @type feature_support :: boolean() | {:version, String.t(), boolean()} | map()

  # Core SQL features that all databases must support
  @required_features [
    :select,
    :insert,
    :update,
    :delete,
    :where,
    :joins,
    :subqueries,
    :group_by,
    :order_by
  ]

  # Optional advanced features
  @optional_features [
    # Join types
    :inner_join,
    :left_join,
    :right_join,
    :full_outer_join,
    :cross_join,
    :lateral_join,
    :natural_join,
    
    # Advanced SQL
    :cte,                    # Common Table Expressions
    :recursive_cte,          # Recursive CTEs
    :window_functions,       # Window/analytical functions
    :pivot,                  # PIVOT/UNPIVOT operations
    :merge,                  # MERGE/UPSERT operations
    :returning,              # RETURNING clause
    
    # Grouping extensions
    :rollup,                 # GROUP BY ROLLUP
    :cube,                   # GROUP BY CUBE
    :grouping_sets,          # GROUP BY GROUPING SETS
    
    # Data types
    :arrays,                 # Native array types
    :json,                   # JSON/JSONB support
    :xml,                    # XML data type
    :uuid,                   # UUID data type
    :inet,                   # Network address types
    :range_types,            # Range data types
    :composite_types,        # User-defined composite types
    
    # Text search
    :fulltext_search,        # Full-text search capabilities
    :regex,                  # Regular expression support
    :similarity_search,      # Similarity/fuzzy matching
    
    # Constraints & Indexes
    :check_constraints,      # CHECK constraints
    :exclusion_constraints,  # EXCLUDE constraints
    :partial_indexes,        # Partial/filtered indexes
    :expression_indexes,     # Indexes on expressions
    :gin_indexes,           # Generalized inverted indexes
    :gist_indexes,          # Generalized search tree indexes
    
    # Performance
    :explain,               # EXPLAIN query plans
    :explain_analyze,       # EXPLAIN ANALYZE
    :hints,                 # Query hints/optimizer hints
    :parallel_query,        # Parallel query execution
    :partitioning,          # Table partitioning
    
    # Transactions
    :savepoints,            # Transaction savepoints
    :two_phase_commit,      # 2PC support
    :advisory_locks,        # Advisory locking
    :row_level_locks,       # Row-level locking
    :skip_locked,           # SKIP LOCKED option
    
    # Special features
    :materialized_views,    # Materialized views
    :triggers,              # Database triggers
    :stored_procedures,     # Stored procedures/functions
    :events,                # Scheduled events
    :sequences,             # Sequence objects
    :identity_columns,      # IDENTITY/AUTO_INCREMENT
    :generated_columns,     # Generated/computed columns
    :temporal_tables,       # System-versioned tables
    
    # Extensions
    :listen_notify,         # LISTEN/NOTIFY (PostgreSQL)
    :copy,                  # COPY command for bulk loading
    :merge_statement,       # MERGE statement
    :connect_by,           # CONNECT BY (hierarchical queries)
    
    # Compatibility
    :schemas,              # Schema/namespace support
    :catalogs,             # Catalog support
    :collations,           # Custom collations
    :extensions,           # Extension system
  ]

  @doc """
  Returns all required features that every adapter must support.
  """
  @spec required_features() :: [feature()]
  def required_features, do: @required_features

  @doc """
  Returns all optional features that adapters may support.
  """
  @spec optional_features() :: [feature()]
  def optional_features, do: @optional_features

  @doc """
  Checks if an adapter supports a specific feature.
  """
  @spec supports?(module() | map(), feature()) :: boolean()
  def supports?(adapter, feature) when is_atom(adapter) do
    if function_exported?(adapter, :supports?, 1) do
      adapter.supports?(feature)
    else
      false
    end
  end

  def supports?(capabilities, feature) when is_map(capabilities) do
    case Map.get(capabilities, feature) do
      true -> true
      false -> false
      nil -> feature in @required_features
      {:version, _min_version, supported} -> supported
      _ -> false
    end
  end

  @doc """
  Validates that an adapter supports all required features.
  """
  @spec validate_required_features(module() | map()) :: :ok | {:error, [feature()]}
  def validate_required_features(adapter) do
    missing = Enum.reject(@required_features, &supports?(adapter, &1))
    
    if missing == [] do
      :ok
    else
      {:error, missing}
    end
  end

  @doc """
  Returns a capability matrix for an adapter.
  """
  @spec capability_matrix(module() | map()) :: map()
  def capability_matrix(adapter) when is_atom(adapter) do
    if function_exported?(adapter, :capabilities, 0) do
      adapter.capabilities()
    else
      default_capabilities()
    end
  end

  def capability_matrix(capabilities) when is_map(capabilities) do
    Map.merge(default_capabilities(), capabilities)
  end

  @doc """
  Checks if a feature requires a specific database version.
  """
  @spec version_requirement(map(), feature()) :: String.t() | nil
  def version_requirement(capabilities, feature) do
    case Map.get(capabilities, feature) do
      {:version, min_version, _} -> min_version
      _ -> nil
    end
  end

  @doc """
  Returns features that can be emulated if not natively supported.
  """
  @spec emulatable_features() :: map()
  def emulatable_features do
    %{
      full_outer_join: :union_all,     # Can emulate with LEFT JOIN + RIGHT JOIN + UNION
      right_join: :left_join_reverse,   # Can reverse table order and use LEFT JOIN
      rollup: :union_all,              # Can emulate with multiple GROUP BY + UNION
      pivot: :case_when,                # Can emulate with CASE statements
      merge: :insert_update,            # Can emulate with INSERT + UPDATE
      row_number: :subquery_count       # Can emulate with subqueries (inefficient)
    }
  end

  @doc """
  Suggests an emulation strategy for an unsupported feature.
  """
  @spec emulation_strategy(feature()) :: {:ok, atom()} | {:error, :no_emulation}
  def emulation_strategy(feature) do
    case Map.get(emulatable_features(), feature) do
      nil -> {:error, :no_emulation}
      strategy -> {:ok, strategy}
    end
  end

  @doc """
  Groups features by category for documentation/UI purposes.
  """
  @spec feature_categories() :: map()
  def feature_categories do
    %{
      joins: [
        :inner_join, :left_join, :right_join, :full_outer_join,
        :cross_join, :lateral_join, :natural_join
      ],
      advanced_sql: [
        :cte, :recursive_cte, :window_functions, :pivot, :merge, :returning
      ],
      grouping: [
        :rollup, :cube, :grouping_sets
      ],
      data_types: [
        :arrays, :json, :xml, :uuid, :inet, :range_types, :composite_types
      ],
      text_search: [
        :fulltext_search, :regex, :similarity_search
      ],
      constraints: [
        :check_constraints, :exclusion_constraints
      ],
      indexes: [
        :partial_indexes, :expression_indexes, :gin_indexes, :gist_indexes
      ],
      performance: [
        :explain, :explain_analyze, :hints, :parallel_query, :partitioning
      ],
      transactions: [
        :savepoints, :two_phase_commit, :advisory_locks, :row_level_locks, :skip_locked
      ],
      database_objects: [
        :materialized_views, :triggers, :stored_procedures, :events, 
        :sequences, :identity_columns, :generated_columns, :temporal_tables
      ]
    }
  end

  @doc """
  Returns a human-readable description of a feature.
  """
  @spec feature_description(feature()) :: String.t()
  def feature_description(feature) do
    descriptions = %{
      cte: "Common Table Expressions (WITH clause)",
      recursive_cte: "Recursive Common Table Expressions",
      window_functions: "Window/Analytical Functions (ROW_NUMBER, RANK, etc.)",
      lateral_join: "LATERAL JOIN (correlated subquery in FROM)",
      full_outer_join: "FULL OUTER JOIN",
      rollup: "GROUP BY ROLLUP",
      cube: "GROUP BY CUBE",
      arrays: "Native array data types",
      json: "JSON/JSONB data types and operations",
      fulltext_search: "Full-text search capabilities",
      materialized_views: "Materialized views",
      returning: "RETURNING clause for DML statements",
      merge: "MERGE/UPSERT operations",
      savepoints: "Transaction savepoints",
      explain_analyze: "EXPLAIN ANALYZE for query performance analysis",
      partitioning: "Table partitioning",
      generated_columns: "Generated/computed columns"
    }
    
    Map.get(descriptions, feature, to_string(feature))
  end

  # Private functions

  defp default_capabilities do
    @required_features
    |> Enum.map(&{&1, true})
    |> Map.new()
  end
end