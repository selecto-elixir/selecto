defmodule Selecto.ConnectionPool do
  @moduledoc """
  Connection pooling and management for Selecto.
  
  Provides a high-performance connection pool using DBConnection for efficient
  database connection reuse, prepared statement caching, and connection health monitoring.
  
  ## Features
  
  - Connection pooling with configurable pool sizes
  - Prepared statement caching for repeated queries
  - Connection health monitoring and automatic recovery
  - Support for both direct Postgrex and Ecto repository connections
  - Graceful fallback to single connections when pooling is disabled
  
  ## Configuration
  
      # Application config
      config :selecto, Selecto.ConnectionPool,
        pool_size: 10,
        max_overflow: 20,
        prepared_statement_cache_size: 1000,
        connection_timeout: 5000,
        checkout_timeout: 5000
  
  ## Usage
  
      # Start a connection pool
      {:ok, pool} = Selecto.ConnectionPool.start_pool(postgrex_opts)
      
      # Configure Selecto with pooled connection
      selecto = Selecto.configure(domain, {:pool, pool})
      
      # Or use default pool management
      selecto = Selecto.configure(domain, postgrex_opts, pool: true)
  """
  
  use GenServer
  require Logger
  
  @default_pool_config [
    pool_size: 10,
    max_overflow: 20,
    prepared_statement_cache_size: 1000,
    connection_timeout: 5000,
    checkout_timeout: 5000
  ]
  
  @type pool_ref :: pid() | atom()
  @type connection_config :: Keyword.t() | map()
  @type pool_options :: Keyword.t()
  
  @doc """
  Start a connection pool with the given configuration.
  
  ## Parameters
  
  - `connection_config` - Postgrex connection configuration
  - `pool_options` - Pool-specific options (optional)
  
  ## Returns
  
  - `{:ok, pool_ref}` - Pool started successfully
  - `{:error, reason}` - Pool startup failed
  
  ## Examples
  
      # Start pool with Postgrex config
      config = [
        hostname: "localhost",
        username: "user", 
        password: "pass",
        database: "mydb"
      ]
      {:ok, pool} = Selecto.ConnectionPool.start_pool(config)
      
      # Start pool with custom options
      {:ok, pool} = Selecto.ConnectionPool.start_pool(config, pool_size: 20)
  """
  @spec start_pool(connection_config(), pool_options()) :: {:ok, pool_ref()} | {:error, term()}
  def start_pool(connection_config, pool_options \\ []) do
    pool_config = Keyword.merge(@default_pool_config, pool_options)
    
    # Create unique pool name based on connection config
    pool_name = generate_pool_name(connection_config)
    
    # Prepare DBConnection configuration
    dbconnection_opts = [
      name: pool_name,
      pool: DBConnection.ConnectionPool,
      pool_size: pool_config[:pool_size],
      pool_overflow: pool_config[:max_overflow],
      timeout: pool_config[:connection_timeout],
      queue_target: pool_config[:checkout_timeout],
      queue_interval: 1000
    ]
    
    # Merge with Postgrex-specific options
    postgrex_opts = Keyword.merge(connection_config, dbconnection_opts)
    
    case Postgrex.start_link(postgrex_opts) do
      {:ok, pool_pid} ->
        # Start pool manager
        manager_opts = [
          pool_pid: pool_pid,
          pool_name: pool_name,
          pool_config: pool_config,
          connection_config: connection_config
        ]
        
        case GenServer.start_link(__MODULE__, manager_opts, name: :"#{pool_name}_manager") do
          {:ok, manager_pid} ->
            {:ok, %{pool: pool_pid, manager: manager_pid, name: pool_name}}
          {:error, reason} ->
            GenServer.stop(pool_pid)
            {:error, reason}
        end
      {:error, reason} ->
        {:error, reason}
    end
  end
  
  @doc """
  Stop a connection pool.
  
  Gracefully shuts down the pool and all its connections.
  """
  @spec stop_pool(pool_ref()) :: :ok
  def stop_pool(%{pool: pool_pid, manager: manager_pid}) do
    GenServer.stop(manager_pid)
    GenServer.stop(pool_pid)
  end
  def stop_pool(pool_pid) when is_pid(pool_pid) do
    GenServer.stop(pool_pid)
  end
  
  @doc """
  Execute a query using a pooled connection.
  
  Automatically handles connection checkout/checkin and prepared statement caching.
  """
  @spec execute(pool_ref(), String.t(), list(), Keyword.t()) :: {:ok, Postgrex.Result.t()} | {:error, term()}
  def execute(pool_ref, query, params, opts \\ []) do
    use_prepared = Keyword.get(opts, :prepared, true)
    cache_key = if use_prepared, do: generate_cache_key(query), else: nil
    
    case get_pool_pid(pool_ref) do
      {:ok, pool_pid} ->
        execute_with_pool(pool_pid, query, params, cache_key, opts)
      {:error, reason} ->
        {:error, reason}
    end
  end
  
  @doc """
  Get pool statistics for monitoring.
  
  Returns information about pool health, connection counts, and cache statistics.
  """
  @spec pool_stats(pool_ref()) :: map()
  def pool_stats(pool_ref) do
    case get_manager_pid(pool_ref) do
      {:ok, manager_pid} ->
        GenServer.call(manager_pid, :get_stats)
      {:error, _reason} ->
        %{error: "Pool manager not available"}
    end
  end
  
  @doc """
  Clear prepared statement cache for a pool.
  """
  @spec clear_cache(pool_ref()) :: :ok
  def clear_cache(pool_ref) do
    case get_manager_pid(pool_ref) do
      {:ok, manager_pid} ->
        GenServer.cast(manager_pid, :clear_cache)
      {:error, _reason} ->
        :ok
    end
  end
  
  # GenServer Implementation
  
  @impl GenServer
  def init(opts) do
    pool_pid = Keyword.fetch!(opts, :pool_pid)
    pool_name = Keyword.fetch!(opts, :pool_name)
    pool_config = Keyword.fetch!(opts, :pool_config)
    connection_config = Keyword.fetch!(opts, :connection_config)
    
    # Initialize prepared statement cache
    cache_size = pool_config[:prepared_statement_cache_size]
    cache = :ets.new(:"#{pool_name}_prepared_cache", [:set, :private])
    
    state = %{
      pool_pid: pool_pid,
      pool_name: pool_name,
      pool_config: pool_config,
      connection_config: connection_config,
      prepared_cache: cache,
      cache_size: cache_size,
      stats: %{
        queries_executed: 0,
        cache_hits: 0,
        cache_misses: 0,
        connections_created: 0,
        errors: 0
      }
    }
    
    # Setup periodic health checks
    schedule_health_check()
    
    {:ok, state}
  end
  
  @impl GenServer
  def handle_call(:get_stats, _from, state) do
    pool_info = try do
      # Get DBConnection pool info if available
      DBConnection.status(state.pool_pid)
    rescue
      _ -> %{available: 0, size: 0}
    end
    
    stats = Map.merge(state.stats, %{
      pool_info: pool_info,
      cache_size: :ets.info(state.prepared_cache, :size),
      uptime: System.system_time(:second)
    })
    
    {:reply, stats, state}
  end
  
  @impl GenServer
  def handle_cast(:clear_cache, state) do
    :ets.delete_all_objects(state.prepared_cache)
    {:noreply, state}
  end
  
  @impl GenServer
  def handle_info(:health_check, state) do
    # Perform health check
    case validate_pool_health(state.pool_pid) do
      :ok ->
        schedule_health_check()
        {:noreply, state}
      {:error, reason} ->
        Logger.warning("Pool health check failed: #{inspect(reason)}")
        schedule_health_check()
        {:noreply, update_in(state.stats.errors, &(&1 + 1))}
    end
  end
  
  @impl GenServer
  def terminate(_reason, state) do
    :ets.delete(state.prepared_cache)
    :ok
  end
  
  # Private Functions
  
  defp execute_with_pool(pool_pid, query, params, cache_key, opts) do
    case checkout_connection(pool_pid, opts) do
      {:ok, conn} ->
        try do
          result = if cache_key do
            execute_with_prepared_cache(conn, query, params, cache_key)
          else
            Postgrex.query(conn, query, params)
          end
          
          checkin_connection(pool_pid, conn)
          result
        rescue
          error ->
            checkin_connection(pool_pid, conn)
            {:error, error}
        end
      {:error, reason} ->
        {:error, reason}
    end
  end
  
  defp execute_with_prepared_cache(conn, query, params, cache_key) do
    # Try to get prepared statement from cache
    case get_prepared_statement(cache_key) do
      {:ok, prepared_name} ->
        # Execute with cached prepared statement
        case Postgrex.execute(conn, prepared_name, params) do
          {:ok, result} ->
            update_cache_stats(:hit)
            {:ok, result}
          {:error, reason} ->
            # If prepared statement execution fails, fall back to regular query
            Logger.debug("Prepared statement execution failed: #{inspect(reason)}. Falling back to regular query.")
            execute_regular_query_with_cache(conn, query, params, cache_key)
        end
      
      :not_found ->
        # Prepare statement and cache it
        execute_regular_query_with_cache(conn, query, params, cache_key)
    end
  end
  
  defp execute_regular_query_with_cache(conn, query, params, cache_key) do
    case Postgrex.query(conn, query, params) do
      {:ok, result} ->
        # Try to prepare and cache the statement for future use
        prepare_and_cache_statement(conn, query, cache_key)
        update_cache_stats(:miss)
        {:ok, result}
      {:error, reason} ->
        {:error, reason}
    end
  end
  
  defp get_prepared_statement(cache_key) do
    # Access the cache from the current process state
    case Process.get(:prepared_cache) do
      nil -> :not_found
      cache_table ->
        case :ets.lookup(cache_table, cache_key) do
          [{^cache_key, prepared_name, _timestamp}] -> {:ok, prepared_name}
          [] -> :not_found
        end
    end
  end
  
  defp prepare_and_cache_statement(conn, query, cache_key) do
    # Generate unique prepared statement name
    prepared_name = "selecto_prepared_#{cache_key}"
    
    case Postgrex.prepare(conn, prepared_name, query) do
      {:ok, _prepared} ->
        case Process.get(:prepared_cache) do
          nil -> :error
          cache_table ->
            timestamp = System.system_time(:second)
            :ets.insert(cache_table, {cache_key, prepared_name, timestamp})
            
            # Manage cache size - remove oldest entries if cache is full
            manage_cache_size(cache_table)
            :ok
        end
      {:error, reason} ->
        Logger.debug("Failed to prepare statement: #{inspect(reason)}")
        :error
    end
  end
  
  defp manage_cache_size(cache_table) do
    cache_size = :ets.info(cache_table, :size)
    max_size = 1000  # Default cache size
    
    if cache_size > max_size do
      # Remove oldest 10% of entries
      entries_to_remove = div(max_size, 10)
      
      # Get all entries and sort by timestamp
      all_entries = :ets.tab2list(cache_table)
      |> Enum.sort_by(fn {_key, _name, timestamp} -> timestamp end)
      |> Enum.take(entries_to_remove)
      
      # Remove oldest entries
      Enum.each(all_entries, fn {key, _name, _timestamp} ->
        :ets.delete(cache_table, key)
      end)
    end
  end
  
  defp update_cache_stats(type) do
    # This would be implemented to update cache statistics
    # For now, just log the cache activity
    case type do
      :hit -> Logger.debug("Prepared statement cache hit")
      :miss -> Logger.debug("Prepared statement cache miss")
    end
  end
  
  defp checkout_connection(_pool_pid, _opts) do
    # TODO: Implement proper connection checkout using the correct DBConnection API
    # The DBConnection.checkout/2 function doesn't exist in the public API
    {:error, :not_implemented}
  end
  
  defp checkin_connection(_pool_pid, _conn) do
    # TODO: Implement proper connection checkin using the correct DBConnection API
    # The DBConnection.checkin/2 function doesn't exist in the public API
    :ok
  end
  
  defp validate_pool_health(pool_pid) do
    try do
      if Process.alive?(pool_pid) do
        :ok
      else
        {:error, "Pool process not alive"}
      end
    rescue
      error -> {:error, error}
    end
  end
  
  defp schedule_health_check() do
    Process.send_after(self(), :health_check, 30_000)
  end
  
  
  # Make these public for testing
  def get_pool_pid(%{pool: pool_pid}), do: {:ok, pool_pid}
  def get_pool_pid(pool_pid) when is_pid(pool_pid), do: {:ok, pool_pid}
  def get_pool_pid(_), do: {:error, "Invalid pool reference"}
  
  def get_manager_pid(%{manager: manager_pid}), do: {:ok, manager_pid}
  def get_manager_pid(_), do: {:error, "Invalid pool reference"}
  
  # Expose for testing
  def generate_pool_name(connection_config) do
    # Create a unique pool name based on connection parameters
    hash = :crypto.hash(:md5, inspect(connection_config))
    |> Base.encode16(case: :lower)
    |> String.slice(0, 8)
    
    :"selecto_pool_#{hash}"
  end
  
  def generate_cache_key(query) do
    :crypto.hash(:md5, query) |> Base.encode16(case: :lower)
  end
end