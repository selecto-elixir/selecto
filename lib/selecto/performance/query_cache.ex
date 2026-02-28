defmodule Selecto.Performance.QueryCache do
  @moduledoc """
  High-performance query result caching for Selecto.

  Provides intelligent caching of query results with:
  - Automatic cache key generation from SQL and parameters
  - TTL-based expiration
  - Size-based eviction (LRU)
  - Cache invalidation strategies
  - Cache statistics and monitoring
  - Multi-level caching (memory + optional Redis/ETS)
  """

  use GenServer
  require Logger

  @default_config %{
    max_size: 1000,
    default_ttl: :timer.minutes(5),
    eviction_policy: :lru,
    cache_backend: :ets,
    track_stats: true,
    compression: false,
    # bytes
    compression_threshold: 1024
  }

  @cache_table :selecto_query_cache
  @stats_table :selecto_cache_stats

  defstruct [
    :config,
    :cache_table,
    :stats_table,
    :lru_list,
    :size_bytes,
    :item_count
  ]

  # Client API

  @doc """
  Start the query cache process.

  ## Options

  - `:max_size` - Maximum number of cached queries (default: 1000)
  - `:default_ttl` - Default TTL in milliseconds (default: 5 minutes)
  - `:eviction_policy` - :lru, :lfu, or :ttl (default: :lru)
  - `:cache_backend` - :ets, :persistent_term, or :redis (default: :ets)
  - `:track_stats` - Enable cache statistics (default: true)
  - `:compression` - Enable result compression (default: false)
  """
  def start_link(opts \\ []) do
    if Process.whereis(__MODULE__) do
      GenServer.stop(__MODULE__)
    end

    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Get a cached query result.

  Returns {:ok, result} if found, :miss if not in cache.
  """
  def get(cache_key) do
    GenServer.call(__MODULE__, {:get, cache_key})
  end

  @doc """
  Store a query result in cache.

  ## Options

  - `:ttl` - Override default TTL for this entry
  - `:tags` - Tags for cache invalidation
  - `:compress` - Force compression for this entry
  """
  def put(cache_key, result, options \\ []) do
    GenServer.cast(__MODULE__, {:put, cache_key, result, options})
  end

  @doc """
  Generate a cache key from Selecto query.
  """
  def generate_key(selecto) do
    try do
      {sql, params} = Selecto.to_sql(selecto)
      generate_key_from_sql(sql, params)
    rescue
      _ ->
        :crypto.hash(:sha256, :erlang.term_to_binary(selecto))
        |> Base.encode16(case: :lower)
    end
  end

  @doc """
  Generate a cache key from SQL and parameters.
  """
  def generate_key_from_sql(sql, params) do
    :crypto.hash(:sha256, :erlang.term_to_binary({sql, params}))
    |> Base.encode16(case: :lower)
  end

  @doc """
  Invalidate cache entries by key or pattern.
  """
  def invalidate(key_or_pattern) do
    GenServer.call(__MODULE__, {:invalidate, key_or_pattern})
  end

  @doc """
  Invalidate cache entries by tags.
  """
  def invalidate_by_tags(tags) when is_list(tags) do
    GenServer.call(__MODULE__, {:invalidate_by_tags, tags})
  end

  @doc """
  Clear entire cache.
  """
  def clear do
    GenServer.call(__MODULE__, :clear)
  end

  @doc """
  Get cache statistics.
  """
  def stats do
    GenServer.call(__MODULE__, :stats)
  end

  @doc """
  Warm up cache with frequently used queries.
  """
  def warmup(queries) when is_list(queries) do
    GenServer.call(__MODULE__, {:warmup, queries})
  end

  @doc """
  Execute a query with caching.

  This is a convenience function that checks cache, executes if needed,
  and caches the result.
  """
  def cached_execute(selecto, execute_fn, options \\ []) do
    cache_key = generate_key(selecto)

    case get(cache_key) do
      {:ok, cached_result} ->
        # Record cache hit
        record_hit(cache_key)
        {:ok, cached_result}

      :miss ->
        # Execute query
        case execute_fn.(selecto) do
          {:ok, result} = success ->
            # Cache the result
            put(cache_key, result, options)
            record_miss(cache_key)
            success

          error ->
            error
        end
    end
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    config = Map.merge(@default_config, Map.new(opts))

    # Create cache table
    cache_table = create_cache_table(config.cache_backend)

    # Create stats table if tracking enabled
    stats_table =
      if config.track_stats do
        :ets.new(@stats_table, [:set, :public, :named_table])
      else
        nil
      end

    # Initialize stats
    if stats_table do
      :ets.insert(stats_table, [
        {:hits, 0},
        {:misses, 0},
        {:evictions, 0},
        {:invalidations, 0},
        {:size_bytes, 0},
        {:item_count, 0}
      ])
    end

    # Schedule TTL cleanup
    if config.eviction_policy == :ttl do
      schedule_ttl_cleanup()
    end

    {:ok,
     %__MODULE__{
       config: config,
       cache_table: cache_table,
       stats_table: stats_table,
       lru_list: [],
       size_bytes: 0,
       item_count: 0
     }}
  end

  @impl true
  def handle_call({:get, cache_key}, _from, state) do
    case lookup_cache(state.cache_table, cache_key) do
      {:ok, entry} ->
        # Check TTL
        if expired?(entry) do
          delete_from_cache(state.cache_table, cache_key)
          update_stats(state, :misses)
          {:reply, :miss, state}
        else
          # Update LRU if needed
          new_state =
            if state.config.eviction_policy == :lru do
              update_lru(state, cache_key)
            else
              state
            end

          update_stats(state, :hits)

          # Decompress if needed
          result =
            if entry.compressed do
              decompress_result(entry.result)
            else
              entry.result
            end

          {:reply, {:ok, result}, new_state}
        end

      :miss ->
        update_stats(state, :misses)
        {:reply, :miss, state}
    end
  end

  @impl true
  def handle_call({:invalidate, pattern}, _from, state) do
    count = invalidate_pattern(state.cache_table, pattern)

    new_state = %{state | item_count: max(0, state.item_count - count)}

    update_stats(new_state, :invalidations, count)

    {:reply, {:ok, count}, new_state}
  end

  @impl true
  def handle_call({:invalidate_by_tags, tags}, _from, state) do
    count = invalidate_by_tags_impl(state.cache_table, tags)

    new_state = %{state | item_count: max(0, state.item_count - count)}

    update_stats(new_state, :invalidations, count)

    {:reply, {:ok, count}, new_state}
  end

  @impl true
  def handle_call(:clear, _from, state) do
    clear_cache(state.cache_table)

    new_state = %{state | item_count: 0, size_bytes: 0, lru_list: []}

    if state.stats_table do
      :ets.insert(state.stats_table, [{:item_count, 0}, {:size_bytes, 0}])
    end

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:stats, _from, state) do
    stats =
      if state.stats_table do
        :ets.tab2list(state.stats_table)
        |> Map.new()
        |> Map.merge(%{
          hit_rate: calculate_hit_rate(state.stats_table),
          avg_entry_size:
            if(state.item_count > 0, do: state.size_bytes / state.item_count, else: 0),
          memory_usage: state.size_bytes,
          cache_size: state.item_count,
          max_size: state.config.max_size
        })
      else
        %{stats_disabled: true}
      end

    {:reply, stats, state}
  end

  @impl true
  def handle_call({:warmup, queries}, _from, state) do
    # Warm up cache with pre-computed queries
    warmed =
      Enum.reduce(queries, 0, fn {selecto, result}, count ->
        cache_key = generate_key(selecto)
        handle_cast({:put, cache_key, result, []}, state)
        count + 1
      end)

    {:reply, {:ok, warmed}, state}
  end

  @impl true
  def handle_cast({:put, cache_key, result, options}, state) do
    ttl = Keyword.get(options, :ttl, state.config.default_ttl)
    tags = Keyword.get(options, :tags, [])

    # Calculate size
    result_size = :erlang.external_size(result)

    # Compress if needed
    {final_result, compressed} =
      if should_compress?(result_size, options, state.config) do
        {compress_result(result), true}
      else
        {result, false}
      end

    entry = %{
      result: final_result,
      inserted_at: System.monotonic_time(:millisecond),
      ttl: ttl,
      tags: tags,
      size: result_size,
      compressed: compressed,
      access_count: 0,
      last_accessed: System.monotonic_time(:millisecond)
    }

    # Check if we need to evict
    new_state =
      if state.item_count >= state.config.max_size do
        evict_entry(state)
      else
        state
      end

    # Insert into cache
    insert_into_cache(new_state.cache_table, cache_key, entry)

    # Update state
    new_state = %{
      new_state
      | item_count: new_state.item_count + 1,
        size_bytes: new_state.size_bytes + result_size,
        lru_list: [cache_key | new_state.lru_list] |> Enum.take(state.config.max_size)
    }

    update_stats(new_state, :item_count, new_state.item_count)
    update_stats(new_state, :size_bytes, new_state.size_bytes)

    {:noreply, new_state}
  end

  @impl true
  def handle_info(:ttl_cleanup, state) do
    # Remove expired entries
    expired_count = cleanup_expired(state.cache_table)

    new_state = %{state | item_count: max(0, state.item_count - expired_count)}

    # Schedule next cleanup
    schedule_ttl_cleanup()

    {:noreply, new_state}
  end

  # Private functions - Cache Operations

  defp create_cache_table(:ets) do
    :ets.new(@cache_table, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])
  end

  defp create_cache_table(:persistent_term) do
    # For persistent_term, we'll use a map stored in persistent_term
    :persistent_term.put({__MODULE__, :cache}, %{})
    :persistent_term
  end

  defp lookup_cache(table, key) when is_reference(table) or is_atom(table) do
    case :ets.lookup(table, key) do
      [{^key, entry}] -> {:ok, entry}
      [] -> :miss
    end
  end

  defp lookup_cache(:persistent_term, key) do
    cache = :persistent_term.get({__MODULE__, :cache}, %{})

    case Map.get(cache, key) do
      nil -> :miss
      entry -> {:ok, entry}
    end
  end

  defp insert_into_cache(table, key, entry) when is_reference(table) or is_atom(table) do
    :ets.insert(table, {key, entry})
  end

  defp insert_into_cache(:persistent_term, key, entry) do
    cache = :persistent_term.get({__MODULE__, :cache}, %{})
    new_cache = Map.put(cache, key, entry)
    :persistent_term.put({__MODULE__, :cache}, new_cache)
  end

  defp delete_from_cache(table, key) when is_reference(table) or is_atom(table) do
    :ets.delete(table, key)
  end

  defp delete_from_cache(:persistent_term, key) do
    cache = :persistent_term.get({__MODULE__, :cache}, %{})
    new_cache = Map.delete(cache, key)
    :persistent_term.put({__MODULE__, :cache}, new_cache)
  end

  defp clear_cache(table) when is_reference(table) or is_atom(table) do
    :ets.delete_all_objects(table)
  end

  defp clear_cache(:persistent_term) do
    :persistent_term.put({__MODULE__, :cache}, %{})
  end

  # Private functions - Eviction

  defp evict_entry(%{config: %{eviction_policy: :lru}} = state) do
    # Remove least recently used
    case List.last(state.lru_list) do
      nil ->
        state

      oldest_key ->
        delete_from_cache(state.cache_table, oldest_key)
        update_stats(state, :evictions)

        %{
          state
          | lru_list: List.delete(state.lru_list, oldest_key),
            item_count: state.item_count - 1
        }
    end
  end

  defp evict_entry(%{config: %{eviction_policy: :lfu}} = state) do
    # Remove least frequently used
    # This would require tracking access counts
    evict_lfu_entry(state)
  end

  defp evict_entry(%{config: %{eviction_policy: :ttl}} = state) do
    # Remove oldest by TTL
    evict_oldest_entry(state)
  end

  defp evict_lfu_entry(state) do
    # Find entry with lowest access count
    case find_lfu_entry(state.cache_table) do
      nil ->
        state

      {key, _entry} ->
        delete_from_cache(state.cache_table, key)
        update_stats(state, :evictions)

        %{state | lru_list: List.delete(state.lru_list, key), item_count: state.item_count - 1}
    end
  end

  defp find_lfu_entry(table) when is_reference(table) or is_atom(table) do
    :ets.foldl(
      fn {key, entry}, acc ->
        case acc do
          nil ->
            {key, entry}

          {_min_key, min_entry} ->
            if entry.access_count < min_entry.access_count do
              {key, entry}
            else
              acc
            end
        end
      end,
      nil,
      table
    )
  end

  defp evict_oldest_entry(state) do
    case find_oldest_entry(state.cache_table) do
      nil ->
        state

      {key, _entry} ->
        delete_from_cache(state.cache_table, key)
        update_stats(state, :evictions)

        %{state | lru_list: List.delete(state.lru_list, key), item_count: state.item_count - 1}
    end
  end

  defp find_oldest_entry(table) when is_reference(table) or is_atom(table) do
    :ets.foldl(
      fn {key, entry}, acc ->
        case acc do
          nil ->
            {key, entry}

          {_old_key, old_entry} ->
            if entry.inserted_at < old_entry.inserted_at do
              {key, entry}
            else
              acc
            end
        end
      end,
      nil,
      table
    )
  end

  # Private functions - LRU Management

  defp update_lru(state, key) do
    new_lru =
      [key | List.delete(state.lru_list, key)]
      |> Enum.take(state.config.max_size)

    %{state | lru_list: new_lru}
  end

  # Private functions - TTL Management

  defp expired?(%{inserted_at: inserted_at, ttl: ttl}) do
    current_time = System.monotonic_time(:millisecond)
    current_time > inserted_at + ttl
  end

  defp cleanup_expired(table) when is_reference(table) or is_atom(table) do
    current_time = System.monotonic_time(:millisecond)

    expired_keys =
      :ets.foldl(
        fn {key, entry}, acc ->
          if current_time > entry.inserted_at + entry.ttl do
            [key | acc]
          else
            acc
          end
        end,
        [],
        table
      )

    Enum.each(expired_keys, &:ets.delete(table, &1))
    length(expired_keys)
  end

  defp schedule_ttl_cleanup do
    Process.send_after(self(), :ttl_cleanup, :timer.minutes(1))
  end

  # Private functions - Invalidation

  defp invalidate_pattern(table, pattern) when is_binary(pattern) do
    # Pattern-based invalidation
    regex =
      pattern
      |> String.replace("*", ".*")
      |> Regex.compile!()

    invalidate_matching(table, fn key ->
      Regex.match?(regex, key)
    end)
  end

  defp invalidate_pattern(table, key) do
    # Direct key invalidation
    delete_from_cache(table, key)
    1
  end

  defp invalidate_matching(table, match_fn) when is_reference(table) or is_atom(table) do
    keys_to_delete =
      :ets.foldl(
        fn {key, _entry}, acc ->
          if match_fn.(key) do
            [key | acc]
          else
            acc
          end
        end,
        [],
        table
      )

    Enum.each(keys_to_delete, &:ets.delete(table, &1))
    length(keys_to_delete)
  end

  defp invalidate_by_tags_impl(table, tags) when is_reference(table) or is_atom(table) do
    tag_set = MapSet.new(tags)

    keys_to_delete =
      :ets.foldl(
        fn {key, entry}, acc ->
          entry_tags = MapSet.new(entry.tags)

          if MapSet.disjoint?(tag_set, entry_tags) do
            acc
          else
            [key | acc]
          end
        end,
        [],
        table
      )

    Enum.each(keys_to_delete, &:ets.delete(table, &1))
    length(keys_to_delete)
  end

  # Private functions - Compression

  defp should_compress?(size, options, config) do
    compress_override = Keyword.get(options, :compress)

    cond do
      compress_override == true -> true
      compress_override == false -> false
      config.compression && size >= config.compression_threshold -> true
      true -> false
    end
  end

  defp compress_result(result) do
    :erlang.term_to_binary(result)
    |> :zlib.compress()
  end

  defp decompress_result(compressed) do
    compressed
    |> :zlib.uncompress()
    |> :erlang.binary_to_term()
  end

  # Private functions - Statistics

  defp update_stats(%{stats_table: nil}, _key, _value), do: :ok

  defp update_stats(%{stats_table: table}, key, value) do
    :ets.update_counter(table, key, value, {key, 0})
  end

  defp update_stats(%{stats_table: nil}, _key), do: :ok

  defp update_stats(%{stats_table: table}, key) do
    :ets.update_counter(table, key, 1, {key, 0})
  end

  defp calculate_hit_rate(stats_table) do
    [{:hits, hits}] = :ets.lookup(stats_table, :hits)
    [{:misses, misses}] = :ets.lookup(stats_table, :misses)

    total = hits + misses

    if total > 0 do
      Float.round(hits / total * 100, 2)
    else
      0.0
    end
  end

  defp record_hit(cache_key) do
    :telemetry.execute([:selecto, :cache, :hit], %{count: 1}, %{key: cache_key})
  end

  defp record_miss(cache_key) do
    :telemetry.execute([:selecto, :cache, :miss], %{count: 1}, %{key: cache_key})
  end
end
