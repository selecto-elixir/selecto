defmodule Selecto.Connection do
  @moduledoc """
  Unified connection interface for database adapters.
  
  This module provides a consistent interface for managing database connections
  across different adapters, including connection establishment, pooling,
  and transaction management.
  """

  alias Selecto.Database.Registry
  
  @doc """
  Establishes a connection using the specified adapter.
  
  ## Parameters
  
  - `adapter` - The adapter module to use
  - `opts` - Connection options (adapter-specific)
  
  ## Returns
  
  - `{:ok, connection}` - Successfully established connection
  - `{:error, reason}` - Connection failed
  
  ## Examples
  
      # PostgreSQL connection
      {:ok, conn} = Selecto.Connection.connect(Selecto.Adapters.PostgreSQL, 
        hostname: "localhost",
        database: "myapp_dev"
      )
      
      # SQLite connection
      {:ok, conn} = Selecto.Connection.connect(Selecto.DB.SQLite,
        database: ":memory:"
      )
  """
  def connect(adapter, opts) when is_atom(adapter) do
    if function_exported?(adapter, :connect, 1) do
      adapter.connect(opts)
    else
      {:error, {:adapter_error, "Adapter #{inspect(adapter)} does not implement connect/1"}}
    end
  end

  @doc """
  Disconnects from the database.
  """
  def disconnect(adapter, connection) when is_atom(adapter) do
    if function_exported?(adapter, :disconnect, 1) do
      adapter.disconnect(connection)
    else
      :ok  # Default to success if not implemented
    end
  end

  @doc """
  Executes a query using the appropriate adapter.
  
  ## Parameters
  
  - `adapter` - The adapter module
  - `connection` - The database connection
  - `query` - SQL query string
  - `params` - Query parameters
  - `opts` - Execution options
  
  ## Returns
  
  - `{:ok, result}` - Query executed successfully
  - `{:error, reason}` - Query execution failed
  """
  def execute(adapter, connection, query, params, opts \\ []) do
    if function_exported?(adapter, :execute, 4) do
      adapter.execute(connection, query, params, opts)
    else
      {:error, {:adapter_error, "Adapter #{inspect(adapter)} does not implement execute/4"}}
    end
  end

  @doc """
  Begins a transaction.
  """
  def transaction(adapter, connection, fun, opts \\ []) when is_function(fun) do
    if function_exported?(adapter, :transaction, 3) do
      adapter.transaction(connection, fun, opts)
    else
      # Fallback to manual transaction management
      with {:ok, conn} <- begin_transaction(adapter, connection, opts),
           {:ok, result} <- safe_execute(fun),
           :ok <- commit(adapter, conn) do
        {:ok, result}
      else
        {:error, _} = error ->
          rollback(adapter, connection)
          error
      end
    end
  end

  @doc """
  Checks out a connection from a pool.
  """
  def checkout(adapter, pool) do
    if function_exported?(adapter, :checkout, 1) do
      adapter.checkout(pool)
    else
      {:error, :not_supported}
    end
  end

  @doc """
  Returns a connection to the pool.
  """
  def checkin(adapter, pool, connection) do
    if function_exported?(adapter, :checkin, 2) do
      adapter.checkin(pool, connection)
    else
      :ok
    end
  end

  @doc """
  Discovers available adapters in the system.
  
  Returns a list of adapter modules that are available and properly implement
  the adapter behavior.
  """
  def discover_adapters do
    # Built-in adapters
    builtin = [
      Selecto.Adapters.PostgreSQL
    ]
    
    # Optional external adapters
    external = [
      Selecto.DB.SQLite,
      Selecto.DB.MySQL
    ]
    
    # Check which ones are actually available
    (builtin ++ external)
    |> Enum.filter(&adapter_available?/1)
    |> Enum.map(fn adapter ->
      %{
        module: adapter,
        name: adapter_name(adapter),
        dialect: adapter_dialect(adapter)
      }
    end)
  end

  @doc """
  Returns the default adapter to use when none is specified.
  """
  def default_adapter do
    Selecto.Adapters.PostgreSQL
  end

  @doc """
  Checks if an adapter is available and properly loaded.
  """
  def adapter_available?(adapter) when is_atom(adapter) do
    Code.ensure_loaded?(adapter) and
      function_exported?(adapter, :connect, 1) and
      function_exported?(adapter, :execute, 4)
  end

  @doc """
  Gets human-readable name for an adapter.
  """
  def adapter_name(adapter) when is_atom(adapter) do
    case adapter do
      Selecto.Adapters.PostgreSQL -> "PostgreSQL"
      Selecto.DB.SQLite -> "SQLite"
      Selecto.DB.MySQL -> "MySQL"
      _ -> 
        adapter
        |> Module.split()
        |> List.last()
    end
  end

  @doc """
  Gets the SQL dialect for an adapter.
  """
  def adapter_dialect(adapter) when is_atom(adapter) do
    cond do
      function_exported?(adapter, :dialect, 0) ->
        adapter.dialect()
      
      function_exported?(adapter, :capabilities, 0) ->
        case adapter.capabilities() do
          %{dialect: dialect} -> dialect
          _ -> "unknown"
        end
      
      true ->
        case adapter do
          Selecto.Adapters.PostgreSQL -> "postgresql"
          Selecto.DB.SQLite -> "sqlite"
          Selecto.DB.MySQL -> "mysql"
          _ -> "unknown"
        end
    end
  end

  # Private functions

  defp begin_transaction(adapter, connection, opts) do
    if function_exported?(adapter, :begin, 2) do
      adapter.begin(connection, opts)
    else
      {:ok, connection}
    end
  end

  defp commit(adapter, connection) do
    if function_exported?(adapter, :commit, 1) do
      adapter.commit(connection)
    else
      :ok
    end
  end

  defp rollback(adapter, connection) do
    if function_exported?(adapter, :rollback, 1) do
      adapter.rollback(connection)
    else
      :ok
    end
  end

  defp safe_execute(fun) do
    try do
      {:ok, fun.()}
    rescue
      e -> {:error, e}
    end
  end
end