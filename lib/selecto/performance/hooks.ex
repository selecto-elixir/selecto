defmodule Selecto.Performance.Hooks do
  @moduledoc """
  Performance monitoring hooks for Selecto query execution.
  
  Provides configurable hooks that can be inserted at various points
  in the query execution pipeline to collect performance metrics,
  trigger optimizations, and enable monitoring.
  """
  
  @behaviour Selecto.Performance.HookBehaviour
  
  @hook_points ~w(
    before_query_build
    after_query_build
    before_execution
    after_execution
    on_error
    on_cache_hit
    on_cache_miss
  )a
  
  @doc """
  Register a hook function for a specific hook point.
  
  ## Hook Points
  
  - `:before_query_build` - Called before SQL generation starts
  - `:after_query_build` - Called after SQL is generated
  - `:before_execution` - Called before query execution
  - `:after_execution` - Called after successful execution
  - `:on_error` - Called when an error occurs
  - `:on_cache_hit` - Called when query result is found in cache
  - `:on_cache_miss` - Called when query result is not in cache
  
  ## Examples
  
      # Register a timing hook
      Hooks.register(:before_execution, fn context ->
        Map.put(context, :start_time, System.monotonic_time(:millisecond))
      end)
      
      # Register a logging hook
      Hooks.register(:after_execution, fn ctx ->
        Logger.info("Query executed in \#{ctx.execution_time}ms")
        ctx
      end)
  """
  def register(hook_point, hook_fn) when hook_point in @hook_points and is_function(hook_fn, 1) do
    hooks = get_hooks(hook_point)
    put_hooks(hook_point, [hook_fn | hooks])
    :ok
  end
  
  @doc """
  Unregister all hooks for a specific hook point.
  """
  def unregister(hook_point) when hook_point in @hook_points do
    put_hooks(hook_point, [])
    :ok
  end
  
  @doc """
  Execute all registered hooks for a hook point.
  
  Hooks are executed in reverse order of registration (LIFO).
  Each hook receives the context and should return an updated context.
  """
  def run_hooks(hook_point, context) when hook_point in @hook_points do
    hooks = get_hooks(hook_point)
    
    Enum.reduce(hooks, context, fn hook_fn, ctx ->
      try do
        hook_fn.(ctx)
      rescue
        error ->
          # Log hook errors but don't break execution
          require Logger
          Logger.error("Hook error at #{hook_point}: #{inspect(error)}")
          ctx
      end
    end)
  end
  
  @doc """
  Wrap a query execution with performance hooks.
  
  This is the main entry point for integrating hooks into query execution.
  """
  def with_hooks(selecto, execution_fn, options \\ []) do
    context = %{
      selecto: selecto,
      options: options,
      query_id: generate_query_id(),
      started_at: System.monotonic_time(:millisecond)
    }
    
    # Run pre-execution hooks
    context = run_hooks(:before_query_build, context)
    
    # Generate SQL
    {sql, params} = Selecto.to_sql(selecto)
    
    context = Map.merge(context, %{
      sql: sql,
      params: params,
      query_built_at: System.monotonic_time(:millisecond)
    })
    
    context = run_hooks(:after_query_build, context)
    
    # Check cache if enabled
    context = if options[:cache] do
      cache_key = generate_cache_key(sql, params)
      
      case Selecto.Performance.QueryCache.get(cache_key) do
        {:ok, cached_result} ->
          context
          |> Map.put(:cache_hit, true)
          |> Map.put(:cached_result, cached_result)
          |> run_hooks(:on_cache_hit)
        
        :miss ->
          context
          |> Map.put(:cache_hit, false)
          |> Map.put(:cache_key, cache_key)
          |> run_hooks(:on_cache_miss)
      end
    else
      context
    end
    
    # Execute or return cached result
    result = if context[:cache_hit] do
      {:ok, context.cached_result}
    else
      context = run_hooks(:before_execution, context)
      
      execution_result = try do
        execution_fn.(selecto, sql, params)
      rescue
        error ->
          error_context = Map.put(context, :error, error)
          run_hooks(:on_error, error_context)
          reraise error, __STACKTRACE__
      end
      
      completed_at = System.monotonic_time(:millisecond)
      
      context = Map.merge(context, %{
        result: execution_result,
        completed_at: completed_at,
        execution_time: completed_at - context.query_built_at,
        total_time: completed_at - context.started_at
      })
      
      context = run_hooks(:after_execution, context)
      
      # Cache result if caching is enabled and execution was successful
      if options[:cache] && context[:cache_key] && match?({:ok, _}, execution_result) do
        Selecto.Performance.QueryCache.put(context.cache_key, elem(execution_result, 1), options[:cache_ttl])
      end
      
      # Record metrics
      record_metrics(context)
      
      execution_result
    end
    
    result
  end
  
  @doc """
  Install default performance monitoring hooks.
  
  This sets up a standard set of hooks for:
  - Query timing
  - Metrics collection
  - Slow query logging
  - EXPLAIN ANALYZE for slow queries
  """
  def install_default_hooks(options \\ []) do
    slow_query_threshold = Keyword.get(options, :slow_query_threshold, 100)
    auto_explain_threshold = Keyword.get(options, :auto_explain_threshold, 500)
    
    # Timing hook
    register(:before_execution, fn context ->
      Map.put(context, :exec_start_time, System.monotonic_time(:millisecond))
    end)
    
    # Metrics collection hook
    register(:after_execution, fn context ->
      if Process.whereis(Selecto.Performance.MetricsCollector) do
        metrics = extract_metrics(context)
        Selecto.Performance.MetricsCollector.record_query(context.query_id, metrics)
      end
      context
    end)
    
    # Slow query logging
    register(:after_execution, fn context ->
      if context.execution_time > slow_query_threshold do
        require Logger
        Logger.warning("Slow query detected (#{context.execution_time}ms): #{String.slice(context.sql, 0, 200)}...")
        
        # Auto-explain very slow queries
        if context.execution_time > auto_explain_threshold && !context[:explained] do
          spawn(fn ->
            case Selecto.Performance.QueryAnalyzer.analyze_query(context.selecto) do
              {:ok, analysis} ->
                Logger.info("Auto-EXPLAIN for slow query #{context.query_id}:\n#{format_analysis(analysis)}")
              _ ->
                :ok
            end
          end)
        end
      end
      context
    end)
    
    # Cache statistics
    register(:on_cache_hit, fn context ->
      :telemetry.execute([:selecto, :cache, :hit], %{count: 1}, %{query_id: context.query_id})
      context
    end)
    
    register(:on_cache_miss, fn context ->
      :telemetry.execute([:selecto, :cache, :miss], %{count: 1}, %{query_id: context.query_id})
      context
    end)
    
    # Error tracking
    register(:on_error, fn context ->
      require Logger
      Logger.error("Query execution failed: #{inspect(context.error)}")
      :telemetry.execute([:selecto, :query, :error], %{count: 1}, %{
        query_id: context.query_id,
        error: context.error
      })
      context
    end)
    
    :ok
  end
  
  @doc """
  Create a custom hook for specific monitoring needs.
  
  ## Examples
  
      # Hook to track queries by user
      hook = Hooks.create_hook(:user_tracking, fn context ->
        user_id = context.options[:user_id]
        :telemetry.execute([:app, :query, :by_user], %{count: 1}, %{user_id: user_id})
        context
      end)
      
      Hooks.register(:before_execution, hook)
  """
  def create_hook(name, hook_fn) when is_atom(name) and is_function(hook_fn, 1) do
    fn context ->
      context
      |> Map.put(:hook_name, name)
      |> hook_fn.()
    end
  end
  
  @doc """
  Create a conditional hook that only runs when condition is met.
  
  ## Examples
  
      # Only log queries that touch certain tables
      hook = Hooks.conditional_hook(
        fn ctx -> String.contains?(ctx.sql, "users") end,
        fn ctx -> 
          Logger.info("User table query: \#{ctx.sql}")
          ctx
        end
      )
  """
  def conditional_hook(condition_fn, hook_fn) 
      when is_function(condition_fn, 1) and is_function(hook_fn, 1) do
    fn context ->
      if condition_fn.(context) do
        hook_fn.(context)
      else
        context
      end
    end
  end
  
  @doc """
  Chain multiple hooks together.
  
  ## Examples
  
      combined = Hooks.chain_hooks([
        timing_hook,
        logging_hook,
        metrics_hook
      ])
      
      Hooks.register(:after_execution, combined)
  """
  def chain_hooks(hooks) when is_list(hooks) do
    fn context ->
      Enum.reduce(hooks, context, fn hook, ctx ->
        hook.(ctx)
      end)
    end
  end
  
  # Private functions
  
  defp get_hooks(hook_point) do
    key = hook_key(hook_point)
    Process.get(key, [])
  end
  
  defp put_hooks(hook_point, hooks) do
    key = hook_key(hook_point)
    Process.put(key, hooks)
  end
  
  defp hook_key(hook_point) do
    :"selecto_hooks_#{hook_point}"
  end
  
  defp generate_query_id do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
  end
  
  defp generate_cache_key(sql, params) do
    :crypto.hash(:sha256, :erlang.term_to_binary({sql, params}))
    |> Base.encode16(case: :lower)
  end
  
  defp extract_metrics(context) do
    %{
      query_id: context.query_id,
      execution_time: context.execution_time,
      total_time: context.total_time,
      query_build_time: context.query_built_at - context.started_at,
      sql_length: String.length(context.sql),
      param_count: length(context.params),
      cache_hit: context[:cache_hit] || false,
      timestamp: System.system_time(:second)
    }
  end
  
  defp record_metrics(context) do
    :telemetry.execute(
      [:selecto, :query, :complete],
      %{
        duration: context.total_time,
        execution_time: context.execution_time
      },
      %{
        query_id: context.query_id,
        cache_hit: context[:cache_hit] || false
      }
    )
  end
  
  defp format_analysis(analysis) do
    """
    Execution Time: #{analysis.execution_time}ms
    Planning Time: #{analysis.planning_time}ms
    Total Cost: #{analysis.total_cost}
    Rows: #{analysis.actual_rows} (estimated: #{analysis.plan_rows})
    
    Suggestions:
    #{Enum.map_join(analysis.suggestions, "\n", fn s -> "  - #{s}" end)}
    
    Slow Operations:
    #{Enum.map_join(analysis.slow_nodes, "\n", fn node ->
      "  - #{node.type} on #{node.relation || "unknown"} (#{node.time}ms)"
    end)}
    """
  end
end