defmodule Selecto.Query do
  @moduledoc """
  Core query building operations for Selecto.

  This module contains the basic query building functions like select, filter,
  order_by, group_by, limit, and offset.
  """

  @doc """
  Add a field to the Select list. Send in one or a list of field names or selectable tuples.

  ## Examples

      selecto
      |> Selecto.Query.select(["name", "email"])
      |> Selecto.Query.select({:func, "COUNT", ["*"]})
  """
  @spec select(Selecto.Types.t(), [Selecto.Types.selector()]) :: Selecto.Types.t()
  def select(selecto, fields) when is_list(fields) do
    put_in(selecto.set.selected, Enum.uniq(selecto.set.selected ++ fields))
  end

  @spec select(Selecto.Types.t(), Selecto.Types.selector()) :: Selecto.Types.t()
  def select(selecto, field) do
    select(selecto, [field])
  end

  @doc """
  Add a filter to selecto. Send in a tuple with field name and filter value.

  ## Examples

      selecto
      |> Selecto.Query.filter([{"active", true}, {"age", {:gt, 18}}])
  """
  @spec filter(Selecto.Types.t(), [Selecto.Types.filter()]) :: Selecto.Types.t()
  def filter(selecto, filters) when is_list(filters) do
    required_filters = required_filters(selecto)

    # Track whether this filter is applied before or after pivot
    has_pivot = Selecto.Pivot.has_pivot?(selecto)
    pivot_config = Selecto.Pivot.get_pivot_config(selecto)

    # Separate filters into pre-pivot and post-pivot
    {pre_pivot_filters, post_pivot_filters} =
      case {has_pivot, pivot_config} do
        {false, _} ->
          # No pivot yet, all filters are pre-pivot
          {uniq_filters(selecto.set.filtered ++ filters ++ required_filters), []}

        {true, _} ->
          # Pivot exists, new filters are post-pivot
          {selecto.set.filtered, filters}
      end

    # Update the set with new filter lists
    updated_set =
      selecto.set
      |> Map.put(:filtered, pre_pivot_filters)
      |> Map.put(:required_filters, required_filters)
      |> Map.put(:post_pivot_filters, post_pivot_filters)

    %{selecto | set: updated_set}
  end

  @spec filter(Selecto.Types.t(), Selecto.Types.filter()) :: Selecto.Types.t()
  def filter(selecto, filter) do
    filter(selecto, [filter])
  end

  @doc """
  Explicitly append filters to the pre-pivot filter list (`set.filtered`).

  Use this when you want filters to be preserved as source-root constraints even
  when composing a pivoted query.
  """
  @spec pre_pivot_filter(Selecto.Types.t(), [Selecto.Types.filter()]) :: Selecto.Types.t()
  def pre_pivot_filter(selecto, filters) when is_list(filters) do
    current_required = required_filters(selecto)

    put_in(
      selecto.set.filtered,
      uniq_filters(selecto.set.filtered ++ filters ++ current_required)
    )
  end

  @spec pre_pivot_filter(Selecto.Types.t(), Selecto.Types.filter()) :: Selecto.Types.t()
  def pre_pivot_filter(selecto, filter) do
    pre_pivot_filter(selecto, [filter])
  end

  @doc """
  Explicitly append filters to the post-pivot filter list (`set.post_pivot_filters`).

  Use this when constraints should apply to the pivoted target root.
  """
  @spec post_pivot_filter(Selecto.Types.t(), [Selecto.Types.filter()]) :: Selecto.Types.t()
  def post_pivot_filter(selecto, filters) when is_list(filters) do
    current = Map.get(selecto.set, :post_pivot_filters, [])
    put_in(selecto.set[:post_pivot_filters], current ++ filters)
  end

  @spec post_pivot_filter(Selecto.Types.t(), Selecto.Types.filter()) :: Selecto.Types.t()
  def post_pivot_filter(selecto, filter) do
    post_pivot_filter(selecto, [filter])
  end

  @doc """
  Return only pre-pivot filters currently attached to the query.

  This reads `set.filtered` and does not include legacy or post-pivot buckets.
  """
  @spec pre_pivot_filters(Selecto.Types.t()) :: [Selecto.Types.filter()]
  def pre_pivot_filters(selecto) do
    Map.get(selecto.set, :filtered, [])
  end

  @doc """
  Return only post-pivot filters currently attached to the query.
  """
  @spec post_pivot_filters(Selecto.Types.t()) :: [Selecto.Types.filter()]
  def post_pivot_filters(selecto) do
    Map.get(selecto.set, :post_pivot_filters, [])
  end

  @doc """
  Return required filters currently attached to the query.

  This includes domain-level required filters and query-level required filters
  added at runtime.
  """
  @spec required_filters(Selecto.Types.t()) :: [Selecto.Types.filter()]
  def required_filters(selecto) do
    domain_required =
      selecto
      |> Selecto.domain()
      |> Map.get(:required_filters, [])

    set_required =
      selecto
      |> Map.get(:set, %{})
      |> Map.get(:required_filters, [])

    uniq_filters(domain_required ++ set_required)
  end

  @doc """
  Return query filters across legacy and current buckets as a flat list.

  This is useful for integrations that need to copy filters between Selecto and
  other query/update builders.

  ## Options

  - `:include_post_pivot` - include `set.post_pivot_filters` (default: `true`)
  """
  @spec query_filters(Selecto.Types.t(), keyword()) :: [Selecto.Types.filter()]
  def query_filters(selecto, opts \\ []) do
    include_post_pivot = Keyword.get(opts, :include_post_pivot, true)

    legacy_filters = Map.get(selecto, :filters, [])

    set_filters =
      case Map.get(selecto, :set) do
        %{} = set -> Map.get(set, :filtered) || Map.get(set, :filters) || []
        _ -> []
      end

    post_pivot_filters =
      if include_post_pivot do
        case Map.get(selecto, :set) do
          %{} = set -> Map.get(set, :post_pivot_filters) || []
          _ -> []
        end
      else
        []
      end

    [legacy_filters, required_filters(selecto), set_filters, post_pivot_filters]
    |> Enum.flat_map(fn
      filters when is_list(filters) -> filters
      _ -> []
    end)
    |> uniq_filters()
  end

  defp uniq_filters(filters) do
    Enum.reduce(filters, [], fn filter, acc ->
      if filter in acc do
        acc
      else
        acc ++ [filter]
      end
    end)
  end

  @doc """
  Add to the Order By clause.

  ## Examples

      selecto
      |> Selecto.Query.order_by(["created_at", {:desc, "name"}])
  """
  @spec order_by(Selecto.Types.t(), [Selecto.Types.order_spec()]) :: Selecto.Types.t()
  def order_by(selecto, orders) when is_list(orders) do
    put_in(selecto.set.order_by, selecto.set.order_by ++ orders)
  end

  @spec order_by(Selecto.Types.t(), Selecto.Types.order_spec()) :: Selecto.Types.t()
  def order_by(selecto, orders) do
    put_in(selecto.set.order_by, selecto.set.order_by ++ [orders])
  end

  @doc """
  Add to the Group By clause.

  ## Examples

      selecto
      |> Selecto.Query.group_by(["category", "region"])
  """
  @spec group_by(Selecto.Types.t(), [Selecto.Types.field_name()]) :: Selecto.Types.t()
  def group_by(selecto, groups) when is_list(groups) do
    put_in(selecto.set.group_by, selecto.set.group_by ++ groups)
  end

  @spec group_by(Selecto.Types.t(), Selecto.Types.field_name()) :: Selecto.Types.t()
  def group_by(selecto, groups) do
    put_in(selecto.set.group_by, selecto.set.group_by ++ [groups])
  end

  @doc """
  Limit the number of rows returned by the query.

  ## Examples

      # Limit to 10 rows
      selecto |> Selecto.Query.limit(10)

      # Limit with offset for pagination
      selecto |> Selecto.Query.limit(10) |> Selecto.Query.offset(20)
  """
  @spec limit(Selecto.Types.t(), non_neg_integer()) :: Selecto.Types.t()
  def limit(selecto, limit_value) when is_integer(limit_value) and limit_value >= 0 do
    put_in(selecto.set[:limit], limit_value)
  end

  @doc """
  Set the offset for the query results.

  ## Examples

      # Skip first 20 rows
      selecto |> Selecto.Query.offset(20)

      # Pagination: page 3 with 10 items per page
      selecto |> Selecto.Query.limit(10) |> Selecto.Query.offset(20)
  """
  @spec offset(Selecto.Types.t(), non_neg_integer()) :: Selecto.Types.t()
  def offset(selecto, offset_value) when is_integer(offset_value) and offset_value >= 0 do
    put_in(selecto.set[:offset], offset_value)
  end

  @doc """
  Generate SQL without executing - useful for debugging and caching.

  ## Examples

      {sql, params} = Selecto.Query.to_sql(selecto)
      IO.puts(sql)

  ## Options

  - `:pretty` - format SQL for readability
  - `:highlight` - apply highlighting (`:ansi` or `:markdown`)
  - `:indent` - indentation string used by pretty formatter
  """
  @spec to_sql(Selecto.Types.t(), keyword()) :: {String.t(), list()}
  def to_sql(selecto, opts \\ []) do
    {query, _aliases, params} = Selecto.gen_sql(selecto, opts)

    query =
      if Keyword.get(opts, :pretty, false) do
        Selecto.SQL.Formatter.format(query, opts)
      else
        query
      end

    query =
      case Keyword.get(opts, :highlight) do
        nil -> query
        false -> query
        style -> Selecto.SQL.Formatter.highlight(query, style)
      end

    {query, params}
  end
end
