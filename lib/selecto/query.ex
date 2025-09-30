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
    # Track whether this filter is applied before or after pivot
    has_pivot = Selecto.Pivot.has_pivot?(selecto)
    pivot_config = Selecto.Pivot.get_pivot_config(selecto)

    # Separate filters into pre-pivot and post-pivot
    {pre_pivot_filters, post_pivot_filters} = case {has_pivot, pivot_config} do
      {false, _} ->
        # No pivot yet, all filters are pre-pivot
        {selecto.set.filtered ++ filters, []}
      {true, _} ->
        # Pivot exists, new filters are post-pivot
        {selecto.set.filtered, filters}
    end

    # Update the set with new filter lists
    updated_set = selecto.set
    |> Map.put(:filtered, pre_pivot_filters)
    |> Map.put(:post_pivot_filters, post_pivot_filters)

    %{selecto | set: updated_set}
  end

  @spec filter(Selecto.Types.t(), Selecto.Types.filter()) :: Selecto.Types.t()
  def filter(selecto, filter) do
    filter(selecto, [filter])
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
  """
  @spec to_sql(Selecto.Types.t(), keyword()) :: {String.t(), list()}
  def to_sql(selecto, opts \\ []) do
    {query, _aliases, params} = Selecto.gen_sql(selecto, opts)
    {query, params}
  end
end