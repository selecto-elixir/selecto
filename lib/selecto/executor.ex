defmodule Selecto.Executor do
  @moduledoc """
  Query execution engine for Selecto.

  Handles the execution of generated SQL queries against Postgrex connections
  or Ecto repositories, with proper error handling and connection management.
  """

  require Logger

  @doc """
  Execute a query and return results with standardized error handling.

  ## Parameters

  - `selecto` - The Selecto struct containing connection and query info
  - `opts` - Execution options (currently unused but reserved for future use)

  ## Returns

  - `{:ok, {rows, columns, aliases}}` - Successful execution with results
  - `{:error, %Selecto.Error{}}` - Execution failure with detailed error

  ## Examples

      case Selecto.Executor.execute(selecto) do
        {:ok, {rows, columns, aliases}} ->
          # Process successful results
          handle_results(rows, columns, aliases)
        {:error, error} ->
          # Handle database error
          Logger.error("Query failed: \#{inspect(error)}")
      end
  """
  @spec execute(Selecto.Types.t(), Selecto.Types.execute_options()) :: Selecto.Types.safe_execute_result()
  def execute(selecto, opts \\ []) do
    start_time = System.monotonic_time(:millisecond)
    query_id = :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)

    try do
      # Check query complexity before execution (unless disabled)
      if opts[:analyze_complexity] != false do
        case Selecto.Performance.ComplexityAnalyzer.analyze(selecto, opts) do
          {:ok, analysis} ->
            # Log warnings but proceed
            Enum.each(analysis.warnings, fn warning ->
              Logger.warning("[Selecto] Query complexity: #{warning}")
            end)

            # Emit telemetry for complexity analysis
            :telemetry.execute(
              [:selecto, :query, :complexity_analyzed],
              %{complexity_score: analysis.score},
              %{
                query_id: query_id,
                warnings: analysis.warnings,
                details: analysis.details
              }
            )

          {:error, :too_complex, analysis} ->
            Logger.error("[Selecto] Query rejected due to high complexity",
              score: analysis.score,
              issues: analysis.blocking_issues
            )

            # Emit telemetry for rejected query
            :telemetry.execute(
              [:selecto, :query, :complexity_rejected],
              %{complexity_score: analysis.score},
              %{
                query_id: query_id,
                blocking_issues: analysis.blocking_issues,
                recommendations: analysis.recommendations
              }
            )

            {:error, Selecto.Error.validation_error(
              "Query too complex to execute safely",
              %{
                complexity_score: analysis.score,
                max_score: analysis.details.max_score,
                issues: analysis.blocking_issues,
                recommendations: analysis.recommendations,
                details: analysis.details
              }
            )}
        end
      end

      # Execute with timeout protection
      result = execute_with_timeout_protection(selecto, opts, query_id, start_time)

      # Apply output format transformation if specified
      case result do
        {:ok, {rows, columns, aliases}} ->
          format = Keyword.get(opts, :format, :raw)
          format_options = Keyword.get(opts, :format_options, [])

          case Selecto.Output.Formats.transform({rows, columns, aliases}, format, format_options) do
            {:ok, transformed_result} -> {:ok, transformed_result}
            {:error, transform_error} -> {:error, Selecto.Error.transformation_error("Output format transformation failed", %{
              format: format,
              options: format_options,
              error: transform_error
            })}
          end
        error_result -> error_result
      end
    rescue
      error ->
        duration = System.monotonic_time(:millisecond) - start_time
        error_result = {:error, Selecto.Error.from_reason(error)}

        # Emit telemetry event for query error
        :telemetry.execute(
          [:selecto, :query, :error],
          %{count: 1},
          %{
            query_id: query_id,
            error: error,
            duration: duration
          }
        )

        track_query_execution("Query compilation failed", duration, error_result)
        error_result
    catch
      :exit, reason ->
        duration = System.monotonic_time(:millisecond) - start_time
        error_result = {:error, Selecto.Error.connection_error("Database connection failed", %{exit_reason: reason})}

        # Emit telemetry event for connection error
        :telemetry.execute(
          [:selecto, :query, :error],
          %{count: 1},
          %{
            query_id: query_id,
            error: reason,
            duration: duration
          }
        )

        track_query_execution("Database connection failed", duration, error_result)
        error_result
    end
  end

  # Execute query with timeout protection
  defp execute_with_timeout_protection(selecto, opts, query_id, start_time) do
    # Get timeout from options or default
    timeout = opts[:timeout] || 30_000  # Default 30 seconds
    max_timeout = 300_000  # 5 minutes absolute maximum
    timeout = min(timeout, max_timeout)

    # Wrap execution in Task.async for timeout enforcement
    task = Task.async(fn ->
      try do
        {query, aliases, params} = Selecto.gen_sql(selecto, opts)

        # Emit telemetry event for query start
        :telemetry.execute(
          [:selecto, :query, :start],
          %{system_time: System.system_time()},
          %{query_id: query_id, query: query}
        )

        # Handle different execution contexts: adapters, Ecto repos, or direct Postgrex connections
        result = cond do
        # If we have a database adapter (non-PostgreSQL or new style), use adapter execution
        selecto.adapter && selecto.adapter != Selecto.DB.PostgreSQL ->
          execute_with_adapter(selecto.adapter, selecto.connection, query, params, aliases)

        # If it's an Ecto repo (module), try to use Ecto.Adapters.SQL.query
        is_atom(selecto.postgrex_opts) && not is_nil(selecto.postgrex_opts) ->
          execute_with_ecto_repo(selecto.postgrex_opts, query, params, aliases)

        # If it's a Postgrex connection, use Postgrex.query directly (PostgreSQL backward compatibility)
        true ->
          execute_with_postgrex(selecto.postgrex_opts, query, params, aliases)
      end

      # Track query execution for monitoring (if SelectoDev.QueryMonitor is available)
      duration = System.monotonic_time(:millisecond) - start_time

      # Emit telemetry event for successful query completion
      :telemetry.execute(
        [:selecto, :query, :complete],
        %{
          duration: duration,
          execution_time: duration
        },
        %{
          query_id: query_id,
          query: query,
          row_count: case result do
            {:ok, {rows, _, _}} -> length(rows)
            _ -> 0
          end
        }
      )

      track_query_execution(query, duration, result)

      # Apply output format transformation if specified
      case result do
        {:ok, {rows, columns, aliases}} ->
          format = Keyword.get(opts, :format, :raw)
          format_options = Keyword.get(opts, :format_options, [])

          case Selecto.Output.Formats.transform({rows, columns, aliases}, format, format_options) do
            {:ok, transformed_result} -> {:ok, transformed_result}
            {:error, transform_error} -> {:error, Selecto.Error.transformation_error("Output format transformation failed", %{
              format: format,
              options: format_options,
              error: transform_error
            })}
          end
        error_result -> error_result
      end
    rescue
      error ->
        duration = System.monotonic_time(:millisecond) - start_time
        error_result = {:error, Selecto.Error.from_reason(error)}

        # Emit telemetry event for query error
        :telemetry.execute(
          [:selecto, :query, :error],
          %{count: 1},
          %{
            query_id: query_id,
            error: error,
            duration: duration
          }
        )

        track_query_execution("Query compilation failed", duration, error_result)
        error_result
    catch
      :exit, reason ->
        duration = System.monotonic_time(:millisecond) - start_time
        error_result = {:error, Selecto.Error.connection_error("Database connection failed", %{exit_reason: reason})}

        # Emit telemetry event for connection error
        :telemetry.execute(
          [:selecto, :query, :error],
          %{count: 1},
          %{
            query_id: query_id,
            error: reason,
            duration: duration
          }
        )

        track_query_execution("Database connection failed", duration, error_result)
        error_result
      end
    end)

    # Wait for task with timeout
    case Task.yield(task, timeout) || Task.shutdown(task) do
      {:ok, result} ->
        result

      nil ->
        # Task was killed due to timeout
        duration = System.monotonic_time(:millisecond) - start_time

        Logger.error("[Selecto] Query timeout after #{timeout}ms")

        # Emit timeout telemetry
        :telemetry.execute(
          [:selecto, :query, :timeout],
          %{duration: duration, timeout: timeout},
          %{query_id: query_id}
        )

        {:error, Selecto.Error.timeout_error(
          "Query exceeded timeout of #{timeout}ms",
          %{timeout: timeout, duration: duration}
        )}
    end
  end

  @doc """
  Execute a query and return results with metadata including SQL, params, and execution time.

  ## Parameters

  - `selecto` - The Selecto struct containing connection and query info
  - `opts` - Execution options

  ## Returns

  - `{:ok, result, metadata}` - Successful execution with results and metadata
  - `{:error, error}` - Execution failure with detailed error

  The metadata map includes:
  - `:sql` - The generated SQL query string
  - `:params` - The query parameters
  - `:execution_time` - Query execution time in milliseconds

  ## Examples

      case Selecto.Executor.execute_with_metadata(selecto) do
        {:ok, {rows, columns, aliases}, _meta} ->
          # Process successful results with metadata
          handle_results(rows, columns, aliases)
        {:error, error} ->
          # Handle database error
          Logger.error("Query failed: \#{inspect(error)}")
      end
  """
  @spec execute_with_metadata(Selecto.Types.t(), Selecto.Types.execute_options()) :: 
    {:ok, Selecto.Types.execute_result(), map()} | {:error, Selecto.Error.t()}
  def execute_with_metadata(selecto, opts \\ []) do
    start_time = System.monotonic_time(:millisecond)

    try do
      {query, aliases, params} = Selecto.gen_sql(selecto, opts)

      # Track the SQL and params for metadata
      sql_metadata = %{
        sql: query,
        params: params
      }

      require Logger
      Logger.debug("=== EXECUTE_WITH_METADATA ROUTING ===")
      Logger.debug("selecto.postgrex_opts: #{inspect(selecto.postgrex_opts)}")
      Logger.debug("is_atom: #{is_atom(selecto.postgrex_opts)}")
      Logger.debug("selecto.adapter: #{inspect(selecto.adapter)}")

      # Handle different execution contexts: adapters, Ecto repos, or direct Postgrex connections
      result = cond do
        # If we have a database adapter (non-PostgreSQL or new style), use adapter execution
        selecto.adapter && selecto.adapter != Selecto.DB.PostgreSQL ->
          Logger.debug("Routing to: execute_with_adapter")
          execute_with_adapter(selecto.adapter, selecto.connection, query, params, aliases)

        # If it's an Ecto repo (module that has __adapter__ function), try to use Ecto.Adapters.SQL.query
        is_atom(selecto.postgrex_opts) && not is_nil(selecto.postgrex_opts) && is_ecto_repo?(selecto.postgrex_opts) ->
          Logger.debug("Routing to: execute_with_ecto_repo")
          execute_with_ecto_repo(selecto.postgrex_opts, query, params, aliases)

        # If it's a Postgrex connection or registered name, use Postgrex.query directly (PostgreSQL backward compatibility)
        true ->
          Logger.debug("Routing to: execute_with_postgrex")
          execute_with_postgrex(selecto.postgrex_opts, query, params, aliases)
      end

      # Calculate execution time
      duration = System.monotonic_time(:millisecond) - start_time
      
      # Track query execution for monitoring (if SelectoDev.QueryMonitor is available)
      track_query_execution(query, duration, result)

      # Apply output format transformation if specified and add metadata
      case result do
        {:ok, {rows, columns, aliases}} ->
          format = Keyword.get(opts, :format, :raw)
          format_options = Keyword.get(opts, :format_options, [])

          transformed_result = case Selecto.Output.Formats.transform({rows, columns, aliases}, format, format_options) do
            {:ok, transformed} -> transformed
            {:error, _transform_error} -> {rows, columns, aliases}
          end
          
          metadata = Map.put(sql_metadata, :execution_time, duration)
          {:ok, transformed_result, metadata}
          
        error_result -> 
          error_result
      end
    rescue
      error ->
        duration = System.monotonic_time(:millisecond) - start_time
        error_result = {:error, Selecto.Error.from_reason(error)}
        track_query_execution("Query compilation failed", duration, error_result)
        error_result
    catch
      :exit, reason ->
        duration = System.monotonic_time(:millisecond) - start_time
        error_result = {:error, Selecto.Error.connection_error("Database connection failed", %{exit_reason: reason})}
        track_query_execution("Database connection failed", duration, error_result)
        error_result
    end
  end

  @doc """
  Execute a query expecting exactly one row, returning {:ok, row} or {:error, reason}.

  Useful for queries that should return a single record (e.g., with LIMIT 1 or aggregate functions).
  Returns an error if zero rows or multiple rows are returned.

  ## Examples

      case Selecto.Executor.execute_one(selecto) do
        {:ok, row} ->
          # Handle single row result
          process_single_result(row)
        {:error, :no_results} ->
          # Handle case where no rows were found
        {:error, :multiple_results} ->
          # Handle case where multiple rows were found
        {:error, error} ->
          # Handle database or other errors
      end
  """
  @spec execute_one(Selecto.Types.t(), Selecto.Types.execute_options()) :: Selecto.Types.safe_execute_one_result()
  def execute_one(selecto, opts \\ []) do
    case execute(selecto, opts) do
      {:ok, {[], _columns, _aliases}} ->
        {:error, Selecto.Error.no_results_error()}
      {:ok, {[single_row], _columns, aliases}} ->
        {:ok, {single_row, aliases}}
      {:ok, {_multiple_rows, _columns, _aliases}} ->
        {:error, Selecto.Error.multiple_results_error()}
      {:error, %Selecto.Error{} = error} ->
        {:error, error}
    end
  end

  @doc """
  Execute query using a database adapter.
  
  This function delegates to the adapter's execute/4 function, allowing
  for different database types like SQLite, MySQL, etc.
  """
  def execute_with_adapter(adapter, connection, query, params, aliases) do
    try do
      case adapter.execute(connection, query, params, []) do
        {:ok, result} -> 
          # Ensure consistent result format across adapters
          rows = Map.get(result, :rows, [])
          columns = Map.get(result, :columns, [])
          {:ok, {rows, columns, aliases}}
        {:error, reason} -> 
          {:error, Selecto.Error.from_reason(reason)}
      end
    rescue
      error ->
        {:error, Selecto.Error.connection_error("Adapter execution failed", %{
          adapter: adapter,
          connection: inspect(connection),
          error: inspect(error)
        })}
    catch
      :exit, reason ->
        {:error, Selecto.Error.connection_error("Adapter connection failed", %{
          adapter: adapter, 
          exit_reason: reason
        })}
    end
  end

  @doc """
  Execute query using an Ecto repository.

  Attempts to use Ecto.Adapters.SQL.query first, falling back to direct
  Postgrex connection if Ecto is not available.
  """
  def execute_with_ecto_repo(repo, query, params, aliases) do
    try do
      # Use apply to avoid compile-time dependency on Ecto.Adapters.SQL
      case apply(Ecto.Adapters.SQL, :query, [repo, query, params]) do
        {:ok, result} -> {:ok, {result.rows, result.columns, aliases}}
        {:error, reason} -> {:error, Selecto.Error.from_reason(reason)}
      end
    rescue
      UndefinedFunctionError ->
        # Ecto.Adapters.SQL not available, fall back to temporary connection
        execute_with_ecto_fallback(repo, query, params, aliases)
      error ->
        {:error, Selecto.Error.from_reason(error)}
    end
  end

  @doc """
  Execute query using direct Postgrex connection or connection pool.
  """
  def execute_with_postgrex(conn, query, params, aliases) do
    require Logger
    Logger.debug("=== EXECUTE_WITH_POSTGREX CALLED ===")
    Logger.debug("Connection type: #{inspect(conn)}")
    Logger.debug("Is PID?: #{inspect(is_pid(conn))}")
    Logger.debug("Is atom (registered name)?: #{inspect(is_atom(conn))}")
    Logger.debug("Query: #{inspect(query)}")

    case conn do
      # Handle pooled connections
      {:pool, pool_ref} ->
        execute_with_connection_pool(pool_ref, query, params, aliases)

      # Handle direct Postgrex connections (PID or registered name)
      conn when is_pid(conn) or is_atom(conn) ->
        require Logger
        Logger.debug("=== POSTGREX QUERY DEBUG ===")
        Logger.debug("Query: #{inspect(query)}")
        Logger.debug("Params: #{inspect(params)}")
        Logger.debug("Aliases: #{inspect(aliases)}")

        case Postgrex.query(conn, query, params) do
          {:ok, result} ->
            Logger.debug("Postgrex result columns: #{inspect(result.columns)}")
            Logger.debug("Postgrex result row count: #{length(result.rows)}")
            {:ok, {result.rows, result.columns, aliases}}
          {:error, reason} ->
            Logger.error("=== POSTGREX QUERY ERROR ===")
            Logger.error("Error reason: #{inspect(reason)}")
            Logger.error("Query: #{query}")
            Logger.error("Params: #{inspect(params)}")
            {:error, Selecto.Error.query_error("Query execution failed", query, params, %{reason: reason})}
        end

      # Handle invalid connection types
      _ ->
        {:error, Selecto.Error.connection_error("Invalid connection type", %{connection: inspect(conn)})}
    end
  end

  @doc """
  Execute query using connection pool.
  """
  def execute_with_connection_pool(pool_ref, query, params, aliases) do
    case Selecto.ConnectionPool.execute(pool_ref, query, params, prepared: true) do
      {:ok, result} -> {:ok, {result.rows, result.columns, aliases}}
      {:error, reason} -> {:error, Selecto.Error.query_error("Pooled query execution failed", query, params, %{reason: reason})}
    end
  end

  @doc """
  Fallback execution when Ecto.Adapters.SQL is not available.

  Creates a temporary Postgrex connection using Ecto repo configuration.
  """
  def execute_with_ecto_fallback(repo, query, params, aliases) do
    config = apply(repo, :config, [])
    postgrex_opts = [
      username: config[:username],
      password: config[:password],
      hostname: config[:hostname] || "localhost",
      database: config[:database],
      port: config[:port] || 5432,
      supervisor: false
    ]

    case Postgrex.start_link(postgrex_opts) do
      {:ok, conn} ->
        result = case Postgrex.query(conn, query, params) do
          {:ok, result} -> {:ok, {result.rows, result.columns, aliases}}
          {:error, reason} -> {:error, Selecto.Error.query_error("Query execution failed", query, params, %{reason: reason})}
        end
        GenServer.stop(conn)
        result
      {:error, reason} ->
        {:error, Selecto.Error.connection_error("Failed to connect to database", %{reason: reason})}
    end
  end

  @doc """
  Validate connection before executing query.

  Returns `:ok` if connection is valid, `{:error, reason}` otherwise.
  """
  def validate_connection(selecto) do
    case selecto.postgrex_opts do
      repo when is_atom(repo) and not is_nil(repo) ->
        # For Ecto repos, we assume they're properly configured
        # Could be enhanced to ping the database
        :ok
      {:pool, pool_ref} ->
        # For pooled connections, validate pool health
        try do
          case Selecto.ConnectionPool.pool_stats(pool_ref) do
            %{error: _} -> {:error, "Connection pool is not available"}
            stats when is_map(stats) -> :ok
          end
        catch
          :exit, _ -> {:error, "Connection pool is not available"}
        end
      conn when is_pid(conn) ->
        # For Postgrex connections, check if process is alive
        if Process.alive?(conn) do
          :ok
        else
          {:error, "Postgrex connection process is not alive"}
        end
      _ ->
        {:error, "Invalid connection configuration"}
    end
  end

  @doc """
  Get connection statistics for monitoring.

  Returns information about the current connection state.
  """
  def connection_info(selecto) do
    case selecto.postgrex_opts do
      repo when is_atom(repo) and not is_nil(repo) ->
        %{
          type: :ecto_repo,
          repo: repo,
          status: :connected
        }
      {:pool, pool_ref} ->
        stats = try do
          Selecto.ConnectionPool.pool_stats(pool_ref)
        catch
          :exit, _ -> %{error: "Pool manager not available"}
        end
        %{
          type: :connection_pool,
          pool_ref: pool_ref,
          status: :connected,
          pool_stats: stats
        }
      conn when is_pid(conn) ->
        %{
          type: :postgrex,
          pid: conn,
          status: if(Process.alive?(conn), do: :connected, else: :disconnected)
        }
      other ->
        %{
          type: :unknown,
          value: other,
          status: :invalid
        }
    end
  end

  # Track query execution for monitoring if SelectoDev.QueryMonitor is available.
  defp track_query_execution(_query, _duration, result) do
    try do
      # Only attempt to track if the QueryMonitor module exists and is running
      if Code.ensure_loaded?(SelectoDev.QueryMonitor) do
        case result do
          {:ok, _} ->
            # SelectoDev.QueryMonitor.track_query(query, duration)
            :ok
          {:error, error} ->
            _error_message = case error do
              %{message: msg} -> msg
              error when is_binary(error) -> error
              error -> inspect(error)
            end
            # SelectoDev.QueryMonitor.track_query_error(query, error_message, duration)
            :ok
        end
      end
    rescue
      # Ignore any errors in tracking - we don't want monitoring to break queries
      _ -> :ok
    catch
      # Also catch any exits from GenServer calls
      :exit, _ -> :ok
    end
  end

  # Helper function to check if a module is an Ecto repo
  defp is_ecto_repo?(module) when is_atom(module) do
    # Check if the module has the __adapter__ function, which all Ecto repos have
    try do
      function_exported?(module, :__adapter__, 0)
    rescue
      _ -> false
    end
  end
  defp is_ecto_repo?(_), do: false
end
