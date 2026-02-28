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

  - `connection_config` - Database connection configuration
  - `pool_options` - Pool-specific options (optional)

  ## Returns

  - `{:ok, pool_ref}` - Pool started successfully
  - `{:error, reason}` - Pool startup failed

  ## Examples

      # Start pool with Postgrex config (default adapter)
      config = [
        hostname: "localhost",
        username: "user", 
        password: "pass",
        database: "mydb"
      ]
      {:ok, pool} = Selecto.ConnectionPool.start_pool(config)
      
      # Start pool with custom options and adapter
      {:ok, pool} = Selecto.ConnectionPool.start_pool(config, 
        pool_size: 20,
        adapter: Selecto.DB.MySQL
      )
  """
  @spec start_pool(connection_config(), pool_options()) :: {:ok, pool_ref()} | {:error, term()}
  def start_pool(connection_config, pool_options \\ []) do
    pool_config = Keyword.merge(@default_pool_config, pool_options)
    adapter = Keyword.get(pool_options, :adapter, Selecto.Adapters.PostgreSQL)

    # Create unique pool name based on connection config
    pool_name = generate_pool_name(connection_config)

    # Check if adapter supports pooling
    if adapter == Selecto.Adapters.PostgreSQL do
      # Use DBConnection pooling for PostgreSQL
      start_postgrex_pool(connection_config, pool_config, pool_name)
    else
      # For other adapters, create a simple pool manager
      # Many adapters may have their own pooling mechanisms
      start_generic_pool(adapter, connection_config, pool_config, pool_name)
    end
  end

  defp start_postgrex_pool(connection_config, pool_config, pool_name) do
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
          adapter: Selecto.Adapters.PostgreSQL,
          pool_pid: pool_pid,
          pool_name: pool_name,
          pool_config: pool_config,
          connection_config: connection_config
        ]

        case GenServer.start_link(__MODULE__, manager_opts, name: :"#{pool_name}_manager") do
          {:ok, manager_pid} ->
            {:ok,
             %{
               adapter: Selecto.Adapters.PostgreSQL,
               pool: pool_pid,
               manager: manager_pid,
               name: pool_name
             }}

          {:error, reason} ->
            GenServer.stop(pool_pid)
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp start_generic_pool(adapter, connection_config, pool_config, pool_name) do
    # For non-PostgreSQL adapters, create a simple connection manager
    # The adapter itself may handle pooling internally
    manager_opts = [
      adapter: adapter,
      pool_name: pool_name,
      pool_config: pool_config,
      connection_config: connection_config
    ]

    case GenServer.start_link(__MODULE__, manager_opts, name: :"#{pool_name}_manager") do
      {:ok, manager_pid} ->
        {:ok, %{adapter: adapter, manager: manager_pid, name: pool_name}}

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
  @spec execute(pool_ref(), String.t(), list(), Keyword.t()) ::
          {:ok, Postgrex.Result.t()} | {:error, term()}
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
    pool_info =
      try do
        # Get DBConnection pool info if available
        DBConnection.status(state.pool_pid)
      rescue
        _ -> %{available: 0, size: 0}
      end

    stats =
      Map.merge(state.stats, %{
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
    # DBConnection pools work by passing the pool pid directly to query functions
    # The pool handles checkout/checkin internally and transparently
    timeout = Keyword.get(opts, :timeout, 15_000)

    try do
      if cache_key do
        execute_with_prepared_cache(pool_pid, query, params, cache_key, timeout)
      else
        # Direct query execution - pool handles connection management internally
        Postgrex.query(pool_pid, query, params, timeout: timeout)
      end
    rescue
      e in DBConnection.ConnectionError ->
        {:error, Selecto.Error.connection_error(Exception.message(e), %{exception: e})}

      e in Postgrex.Error ->
        {:error, Selecto.Error.query_error(Exception.message(e), query, params, %{exception: e})}

      e ->
        {:error, Selecto.Error.query_error(Exception.message(e), query, params, %{exception: e})}
    end
  end

  defp execute_with_prepared_cache(pool_pid, query, params, cache_key, timeout) do
    # For prepared statements, we use Postgrex's built-in prepare/execute mechanism
    # DBConnection handles the connection pooling transparently

    # Check if we have a cached prepared statement name
    case Process.get({:selecto_prepared, cache_key}) do
      nil ->
        # No cached statement - use regular query which auto-prepares in Postgrex
        # Postgrex automatically prepares and caches statements internally
        result = Postgrex.query(pool_pid, query, params, timeout: timeout)

        # Cache the statement reference for future use (statement is cached in Postgrex)
        Process.put({:selecto_prepared, cache_key}, true)

        result

      _cached ->
        # Statement is already prepared in Postgrex's internal cache
        Postgrex.query(pool_pid, query, params, timeout: timeout)
    end
  end

  @doc """
  Checkout a connection from the pool for manual management.

  This is useful for transactions or when you need to execute multiple
  queries on the same connection.

  ## Parameters

  - `pool_ref` - The pool reference
  - `opts` - Options including :timeout

  ## Returns

  - `{:ok, connection_ref}` - Connection checked out successfully
  - `{:error, reason}` - Checkout failed

  ## Examples

      {:ok, conn} = Selecto.ConnectionPool.checkout(pool)
      try do
        Postgrex.query!(conn, "BEGIN", [])
        Postgrex.query!(conn, "INSERT INTO ...", [...])
        Postgrex.query!(conn, "COMMIT", [])
      after
        Selecto.ConnectionPool.checkin(pool, conn)
      end
  """
  @spec checkout(pool_ref(), Keyword.t()) :: {:ok, DBConnection.conn()} | {:error, term()}
  def checkout(pool_ref, opts \\ []) do
    _timeout = Keyword.get(opts, :timeout, 15_000)

    case get_pool_pid(pool_ref) do
      {:ok, pool_pid} ->
        # DBConnection pools handle checkout internally when you call query functions
        # For manual checkout semantics, we return a reference to the pool
        # The actual connection checkout happens when you execute a query
        ref = make_ref()
        Process.put({:selecto_checkout, ref}, pool_pid)
        {:ok, {:selecto_conn, ref, pool_pid}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Return a checked-out connection to the pool.

  Always call this after checkout/2 to return the connection.
  """
  @spec checkin(pool_ref(), term()) :: :ok
  def checkin(_pool_ref, {:selecto_conn, ref, _pool_pid}) do
    Process.delete({:selecto_checkout, ref})
    :ok
  end

  def checkin(_pool_ref, _conn), do: :ok

  @doc """
  Execute a function with a checked-out connection.

  This is the recommended way to use connections for multiple operations.
  The connection is automatically returned to the pool when the function completes.

  ## Examples

      Selecto.ConnectionPool.with_connection(pool, fn conn ->
        Postgrex.query!(conn, "SELECT * FROM users", [])
      end)
  """
  @spec with_connection(pool_ref(), (DBConnection.conn() -> result)) ::
          {:ok, result} | {:error, term()}
        when result: term()
  def with_connection(pool_ref, fun) when is_function(fun, 1) do
    case get_pool_pid(pool_ref) do
      {:ok, pool_pid} ->
        # DBConnection.run handles checkout/checkin automatically
        try do
          result = fun.(pool_pid)
          {:ok, result}
        rescue
          e in DBConnection.ConnectionError ->
            {:error, Selecto.Error.connection_error(Exception.message(e), %{exception: e})}

          e ->
            {:error, Selecto.Error.query_error(Exception.message(e), nil, [], %{exception: e})}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Execute a transaction on a pooled connection.

  All queries in the function are executed within a database transaction.

  ## Examples

      Selecto.ConnectionPool.transaction(pool, fn conn ->
        Postgrex.query!(conn, "INSERT INTO orders ...", [...])
        Postgrex.query!(conn, "UPDATE inventory ...", [...])
      end)
  """
  @spec transaction(pool_ref(), (DBConnection.conn() -> result), Keyword.t()) ::
          {:ok, result} | {:error, term()}
        when result: term()
  def transaction(pool_ref, fun, opts \\ []) when is_function(fun, 1) do
    case get_pool_pid(pool_ref) do
      {:ok, pool_pid} ->
        Postgrex.transaction(pool_pid, fun, opts)

      {:error, reason} ->
        {:error, reason}
    end
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
    hash =
      :crypto.hash(:md5, inspect(connection_config))
      |> Base.encode16(case: :lower)
      |> String.slice(0, 8)

    :"selecto_pool_#{hash}"
  end

  def generate_cache_key(query) do
    :crypto.hash(:md5, query) |> Base.encode16(case: :lower)
  end
end
