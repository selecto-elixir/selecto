defmodule Selecto.ConnectionPool do
  @moduledoc """
  Connection pooling and management for Selecto.

  Provides a high-performance connection pool using DBConnection for efficient
  database connection reuse, prepared statement caching, and connection health monitoring.

  ## Features

  - Connection pooling with configurable pool sizes
  - Prepared statement caching for repeated queries
  - Connection health monitoring and automatic recovery
  - Adapter-owned pool startup and pooled execution
  - Graceful fallback to direct connections when pooling is disabled

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
      {:ok, pool} = Selecto.ConnectionPool.start_pool(connection_input)
      
      # Configure Selecto with pooled connection
      selecto = Selecto.configure(domain, {:pool, pool})
      
      # Or use default pool management
      selecto = Selecto.configure(domain, connection_input, pool: true)
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

  @runtime Selecto.ConnectionPool.Runtime
  @registry Selecto.ConnectionPool.Registry
  @manager_supervisor Selecto.ConnectionPool.ManagerSupervisor
  @prepared_statements_table :selecto_prepared_statements
  @checkout_refs_table :selecto_checkout_refs

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    pool_name = Keyword.fetch!(opts, :pool_name)
    GenServer.start_link(__MODULE__, opts, name: via_manager(pool_name))
  end

  def child_spec(opts) do
    pool_name = Keyword.fetch!(opts, :pool_name)

    %{
      id: {__MODULE__, pool_name},
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      shutdown: 5000,
      type: :worker
    }
  end

  @doc """
  Start a connection pool with the given configuration.

  ## Parameters

  - `connection_config` - Database connection configuration
  - `pool_options` - Pool-specific options (optional)

  ## Returns

  - `{:ok, pool_ref}` - Pool started successfully
  - `{:error, reason}` - Pool startup failed

  ## Examples

      # Start pool with default adapter config
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
        adapter: SelectoDBMySQL.Adapter
      )
  """
  @spec start_pool(connection_config(), pool_options()) :: {:ok, pool_ref()} | {:error, term()}
  def start_pool(connection_config, pool_options \\ []) do
    pool_config = Keyword.merge(@default_pool_config, pool_options)
    adapter = Keyword.get(pool_options, :adapter, Selecto.AdapterSupport.default_adapter())

    # Create unique pool name based on connection config and adapter
    pool_name = generate_pool_name(%{adapter: adapter, connection_config: connection_config})

    with :ok <- ensure_runtime_started() do
      cond do
        function_exported?(adapter, :start_pool, 3) ->
          adapter.start_pool(connection_config, pool_config, pool_name)

        Selecto.AdapterSupport.postgresql_adapter?(adapter) ->
          {:error, {:adapter_pool_start_unavailable, adapter}}

        true ->
          start_generic_pool(adapter, connection_config, pool_config, pool_name)
      end
    end
  end

  defp start_generic_pool(adapter, connection_config, pool_config, pool_name) do
    case get_manager_pid_by_name(pool_name) do
      {:ok, manager_pid} ->
        build_pool_ref_from_manager(manager_pid)

      :error ->
        cond do
          not is_atom(adapter) ->
            {:error, {:invalid_adapter, adapter}}

          not function_exported?(adapter, :connect, 1) ->
            {:error, {:unsupported_adapter, adapter}}

          true ->
            case adapter.connect(connection_config) do
              {:ok, connection} ->
                manager_opts = [
                  adapter: adapter,
                  connection: connection,
                  pool_name: pool_name,
                  pool_config: pool_config,
                  connection_config: connection_config
                ]

                case start_manager(manager_opts) do
                  {:ok, manager_pid, :started} ->
                    build_pool_ref_from_manager(manager_pid)

                  {:ok, manager_pid, :existing} ->
                    maybe_stop_connection(connection)
                    build_pool_ref_from_manager(manager_pid)

                  {:error, reason} ->
                    maybe_stop_connection(connection)
                    {:error, reason}
                end

              {:error, reason} ->
                {:error, reason}
            end
        end
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

  def stop_pool(%{manager: manager_pid, connection: connection}) do
    GenServer.stop(manager_pid)
    maybe_stop_connection(connection)
    :ok
  end

  def stop_pool(pool_pid) when is_pid(pool_pid) do
    GenServer.stop(pool_pid)
  end

  @doc """
  Execute a query using a pooled connection.

  Automatically handles connection checkout/checkin and prepared statement caching.
  """
  @spec execute(pool_ref(), String.t(), list(), Keyword.t()) ::
          {:ok, term()} | {:error, term()}
  def execute(pool_ref, query, params, opts \\ [])

  def execute({:pool, pool_ref}, query, params, opts) do
    execute(pool_ref, query, params, opts)
  end

  def execute(%{adapter: adapter, connection: connection}, query, params, opts) do
    if Selecto.AdapterSupport.postgresql_adapter?(adapter) do
      execute(%{adapter: adapter}, query, params, opts)
    else
      if function_exported?(adapter, :execute, 4) do
        adapter.execute(connection, query, params, opts)
      else
        {:error, {:unsupported_adapter, adapter}}
      end
    end
  end

  def execute(pool_ref, query, params, opts) do
    adapter = pool_adapter(pool_ref)

    cond do
      function_exported?(adapter, :execute_pool, 4) ->
        adapter.execute_pool(pool_ref, query, params, opts)

      true ->
        {:error, {:unsupported_adapter, adapter}}
    end
  end

  @doc """
  Get pool statistics for monitoring.

  Returns information about pool health, connection counts, and cache statistics.
  """
  @spec pool_stats(pool_ref()) :: map()
  def pool_stats({:pool, pool_ref}) do
    pool_stats(pool_ref)
  end

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
  def clear_cache({:pool, pool_ref}) do
    clear_cache(pool_ref)
  end

  def clear_cache(pool_ref) do
    maybe_clear_prepared_flags(pool_ref)

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
    pool_pid = Keyword.get(opts, :pool_pid)
    connection = Keyword.get(opts, :connection)
    adapter = Keyword.get(opts, :adapter, Selecto.AdapterSupport.default_adapter())
    pool_name = Keyword.fetch!(opts, :pool_name)
    pool_config = Keyword.fetch!(opts, :pool_config)
    connection_config = Keyword.fetch!(opts, :connection_config)

    state = %{
      adapter: adapter,
      pool_pid: pool_pid,
      connection: connection,
      pool_name: pool_name,
      pool_config: pool_config,
      connection_config: connection_config,
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
  def handle_call(:pool_reference, _from, state) do
    reference =
      if Selecto.AdapterSupport.postgresql_adapter?(state.adapter) do
        %{
          adapter: state.adapter,
          pool: state.pool_pid,
          manager: self(),
          name: state.pool_name
        }
      else
        %{
          adapter: state.adapter,
          connection: state.connection,
          manager: self(),
          name: state.pool_name
        }
      end

    {:reply, reference, state}
  end

  @impl GenServer
  def handle_call(:get_stats, _from, state) do
    pool_info =
      if is_pid(state.pool_pid) do
        try do
          DBConnection.status(state.pool_pid)
        rescue
          _ -> %{available: 0, size: 0}
        end
      else
        %{available: nil, size: nil, mode: :adapter_managed}
      end

    stats =
      Map.merge(state.stats, %{
        pool_info: pool_info,
        cache_size: prepared_cache_size(state.pool_pid),
        uptime: System.system_time(:second)
      })

    {:reply, stats, state}
  end

  @impl GenServer
  def handle_cast(:clear_cache, state) do
    clear_prepared_flags(state.pool_pid)
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:health_check, state) do
    # Perform health check
    case validate_pool_health(state) do
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
  def terminate(_reason, _state), do: :ok

  # Private Functions

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
        run_checkout_queries(conn)
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
        put_checkout_ref(ref, pool_pid, self())
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
    delete_checkout_ref(ref)
    :ok
  end

  def checkin(_pool_ref, _conn), do: :ok

  @doc false
  def checkout_lookup(ref) do
    case lookup_checkout_ref(ref) do
      {pool_pid, _owner_pid} -> {:ok, pool_pid}
      :error -> :error
    end
  end

  @doc """
  Execute a function with a checked-out connection.

  This is the recommended way to use connections for multiple operations.
  The connection is automatically returned to the pool when the function completes.

  ## Examples

      Selecto.ConnectionPool.with_connection(pool, fn conn ->
        run_queries_with_adapter_connection(conn)
      end)
  """
  @spec with_connection(pool_ref(), (DBConnection.conn() -> result)) ::
          {:ok, result} | {:error, term()}
        when result: term()
  def with_connection(pool_ref, fun) when is_function(fun, 1) do
    adapter = pool_adapter(pool_ref)

    cond do
      function_exported?(adapter, :with_connection, 2) ->
        adapter.with_connection(pool_ref, fun)

      true ->
        {:error, {:unsupported_adapter, adapter}}
    end
  end

  @doc """
  Execute a transaction on a pooled connection.

  All queries in the function are executed within a database transaction.

  ## Examples

      Selecto.ConnectionPool.transaction(pool, fn conn ->
        run_transaction_steps(conn)
      end)
  """
  @spec transaction(pool_ref(), (DBConnection.conn() -> result), Keyword.t()) ::
          {:ok, result} | {:error, term()}
        when result: term()
  def transaction(pool_ref, fun, opts \\ []) when is_function(fun, 1) do
    adapter = pool_adapter(pool_ref)

    cond do
      function_exported?(adapter, :transaction, 3) ->
        adapter.transaction(pool_ref, fun, opts)

      true ->
        {:error, {:unsupported_adapter, adapter}}
    end
  end

  defp pool_adapter(%{adapter: adapter}) when is_atom(adapter), do: adapter
  defp pool_adapter(_pool_ref), do: Selecto.AdapterSupport.default_adapter()

  defp validate_pool_health(%{pool_pid: pool_pid}) when is_pid(pool_pid) do
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

  defp validate_pool_health(%{adapter: adapter, connection: connection}) do
    cond do
      is_nil(connection) ->
        {:error, "Adapter connection not available"}

      function_exported?(adapter, :supports?, 1) ->
        :ok

      true ->
        {:error, "Adapter does not implement health capabilities"}
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

  def get_manager_pid(%{name: pool_name}) when is_atom(pool_name) do
    case get_manager_pid_by_name(pool_name) do
      {:ok, manager_pid} -> {:ok, manager_pid}
      :error -> {:error, "Invalid pool reference"}
    end
  end

  def get_manager_pid(pool_name) when is_atom(pool_name) do
    case get_manager_pid_by_name(pool_name) do
      {:ok, manager_pid} -> {:ok, manager_pid}
      :error -> {:error, "Invalid pool reference"}
    end
  end

  def get_manager_pid(_), do: {:error, "Invalid pool reference"}

  defp ensure_runtime_started do
    case @runtime.ensure_started() do
      {:ok, _pid} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp via_manager(pool_name) when is_atom(pool_name) do
    {:via, Registry, {@registry, {:manager, pool_name}}}
  end

  @doc false
  def get_manager_pid_by_name(pool_name) when is_atom(pool_name) do
    case Process.whereis(@registry) do
      nil ->
        :error

      _pid ->
        case Registry.lookup(@registry, {:manager, pool_name}) do
          [{pid, _value}] -> {:ok, pid}
          [] -> :error
        end
    end
  end

  @doc false
  def start_manager(manager_opts) when is_list(manager_opts) do
    pool_name = Keyword.fetch!(manager_opts, :pool_name)

    case DynamicSupervisor.start_child(@manager_supervisor, {__MODULE__, manager_opts}) do
      {:ok, manager_pid} ->
        {:ok, manager_pid, :started}

      {:error, {:already_started, manager_pid}} ->
        {:ok, manager_pid, :existing}

      {:error, {:already_present, _child_id}} ->
        case get_manager_pid_by_name(pool_name) do
          {:ok, manager_pid} -> {:ok, manager_pid, :existing}
          :error -> {:error, :manager_start_conflict}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc false
  def build_pool_ref_from_manager(manager_pid) when is_pid(manager_pid) do
    case GenServer.call(manager_pid, :pool_reference) do
      %{} = reference -> {:ok, reference}
      other -> {:error, {:invalid_pool_reference, other}}
    end
  catch
    :exit, reason -> {:error, {:manager_unavailable, reason}}
  end

  defp maybe_stop_connection(connection) when is_pid(connection) do
    if Process.alive?(connection) do
      Process.exit(connection, :normal)
    else
      :ok
    end
  end

  defp maybe_stop_connection(_), do: :ok

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

  @doc false
  def prepared_statement_cached?(pool_pid, cache_key)
      when is_pid(pool_pid) and is_binary(cache_key) do
    table = ensure_prepared_statements_table()
    :ets.member(table, {pool_pid, cache_key})
  end

  @doc false
  def mark_prepared_statement(pool_pid, cache_key)
      when is_pid(pool_pid) and is_binary(cache_key) do
    table = ensure_prepared_statements_table()
    :ets.insert(table, {{pool_pid, cache_key}, true})
    :ok
  end

  defp maybe_clear_prepared_flags(pool_ref) do
    case get_pool_pid(pool_ref) do
      {:ok, pool_pid} -> clear_prepared_flags(pool_pid)
      {:error, _reason} -> :ok
    end
  end

  defp prepared_cache_size(pool_pid) when is_pid(pool_pid) do
    table = ensure_prepared_statements_table()

    :ets.select_count(table, [
      {{{pool_pid, :_}, :_}, [], [true]}
    ])
  end

  defp prepared_cache_size(_), do: 0

  defp clear_prepared_flags(pool_pid) when is_pid(pool_pid) do
    table = ensure_prepared_statements_table()

    :ets.select_delete(table, [
      {{{pool_pid, :_}, :_}, [], [true]}
    ])

    :ok
  end

  defp clear_prepared_flags(_), do: :ok

  defp put_checkout_ref(ref, pool_pid, owner_pid)
       when is_reference(ref) and is_pid(pool_pid) and is_pid(owner_pid) do
    table = ensure_checkout_refs_table()
    :ets.insert(table, {ref, pool_pid, owner_pid})
    :ok
  end

  defp delete_checkout_ref(ref) when is_reference(ref) do
    table = ensure_checkout_refs_table()
    :ets.delete(table, ref)
    :ok
  end

  defp lookup_checkout_ref(ref) when is_reference(ref) do
    table = ensure_checkout_refs_table()

    case :ets.lookup(table, ref) do
      [{^ref, pool_pid, owner_pid}] -> {pool_pid, owner_pid}
      [] -> :error
    end
  end

  defp ensure_prepared_statements_table do
    ensure_named_table(@prepared_statements_table)
  end

  defp ensure_checkout_refs_table do
    ensure_named_table(@checkout_refs_table)
  end

  defp ensure_named_table(name) when is_atom(name) do
    case :ets.whereis(name) do
      :undefined ->
        try do
          :ets.new(name, [
            :set,
            :public,
            :named_table,
            read_concurrency: true,
            write_concurrency: true
          ])
        rescue
          ArgumentError -> :ets.whereis(name)
        end

      table ->
        table
    end
  end
end
