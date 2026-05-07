defmodule Selecto.Query do
  @moduledoc """
  Core query building operations for Selecto.

  This module contains the basic query building functions like select, filter,
  order_by, group_by, limit, and offset.
  """

  @doc """
  Add fields to the select list.

  For macro-free query composition, prefer importing `Selecto.Expr` and passing
  string field paths plus runtime helper constructors.

  ## Examples

      import Selecto.Expr

      selecto
      |> Selecto.Query.select(["name", "email", as(count(), "total")])
      |> Selecto.Query.select(avg("price"))
  """
  @spec select(Selecto.Types.t(), [Selecto.Types.selector()]) :: Selecto.Types.t()
  def select(selecto, fields) when is_list(fields) do
    normalized_fields = Selecto.Expr.normalize(fields)
    Selecto.QueryValidator.validate_selectors!(selecto, normalized_fields)
    put_in(selecto.set.selected, Enum.uniq(selecto.set.selected ++ normalized_fields))
  end

  @spec select(Selecto.Types.t(), Selecto.Types.selector()) :: Selecto.Types.t()
  def select(selecto, field) do
    select(selecto, [field])
  end

  @doc """
  Add filters to the query.

  For macro-free query composition, prefer importing `Selecto.Expr` and using
  runtime filter constructors like `eq/2`, `gte/2`, and `compact_and/1`.

  ## Examples

      import Selecto.Expr

      selecto
      |> Selecto.Query.filter(eq("active", true))
      |> Selecto.Query.filter(compact_and([gte("age", 18), not_null("email")]))
  """
  @spec filter(Selecto.Types.t(), [Selecto.Types.filter()]) :: Selecto.Types.t()
  def filter(selecto, filters) when is_list(filters) do
    normalized_filters = Selecto.Expr.normalize(filters)

    # Track whether this filter is applied before or after retargeting.
    has_retarget = Selecto.Retarget.has_retarget?(selecto)
    retarget_config = Selecto.Retarget.get_retarget_config(selecto)
    validate_filters_for_active_root!(selecto, normalized_filters, has_retarget)

    required_filters = required_filters(selecto)

    # Separate filters into pre-retarget and post-retarget
    {pre_retarget_filters, post_retarget_filters} =
      case {has_retarget, retarget_config} do
        {false, _} ->
          {uniq_filters(selecto.set.filtered ++ normalized_filters ++ required_filters), []}

        {true, _} ->
          {selecto.set.filtered, normalized_filters}
      end

    # Update the set with new filter lists
    updated_set =
      selecto.set
      |> Map.put(:filtered, pre_retarget_filters)
      |> Map.put(:required_filters, required_filters)
      |> Map.put(:post_retarget_filters, post_retarget_filters)

    %{selecto | set: updated_set}
  end

  @spec filter(Selecto.Types.t(), Selecto.Types.filter()) :: Selecto.Types.t()
  def filter(selecto, filter) do
    filter(selecto, [filter])
  end

  @doc """
  Explicitly append filters to the pre-retarget filter list (`set.filtered`).

  Use this when you want filters to be preserved as source-root constraints even
  when composing a retargeted query.
  """
  @spec pre_retarget_filter(Selecto.Types.t(), [Selecto.Types.filter()]) :: Selecto.Types.t()
  def pre_retarget_filter(selecto, filters) when is_list(filters) do
    normalized_filters = Selecto.Expr.normalize(filters)
    Selecto.QueryValidator.validate_filters!(selecto, normalized_filters)
    current_required = required_filters(selecto)

    put_in(
      selecto.set.filtered,
      uniq_filters(selecto.set.filtered ++ normalized_filters ++ current_required)
    )
  end

  @spec pre_retarget_filter(Selecto.Types.t(), Selecto.Types.filter()) :: Selecto.Types.t()
  def pre_retarget_filter(selecto, filter) do
    pre_retarget_filter(selecto, [filter])
  end

  @doc """
  Explicitly append filters to the post-retarget filter list (`set.post_retarget_filters`).

  Use this when constraints should apply to the retargeted target root.
  """
  @spec post_retarget_filter(Selecto.Types.t(), [Selecto.Types.filter()]) :: Selecto.Types.t()
  def post_retarget_filter(selecto, filters) when is_list(filters) do
    normalized_filters = Selecto.Expr.normalize(filters)
    validate_post_retarget_filters!(selecto, normalized_filters)

    current = Map.get(selecto.set, :post_retarget_filters, [])

    updated_set =
      selecto.set
      |> Map.put(:post_retarget_filters, current ++ normalized_filters)

    %{selecto | set: updated_set}
  end

  @spec post_retarget_filter(Selecto.Types.t(), Selecto.Types.filter()) :: Selecto.Types.t()
  def post_retarget_filter(selecto, filter) do
    post_retarget_filter(selecto, [filter])
  end

  @doc """
  Return only pre-retarget filters currently attached to the query.

  This reads `set.filtered` and does not include post-retarget buckets.
  """
  @spec pre_retarget_filters(Selecto.Types.t()) :: [Selecto.Types.filter()]
  def pre_retarget_filters(selecto) do
    Map.get(selecto.set, :filtered, [])
  end

  @doc """
  Return only post-retarget filters currently attached to the query.
  """
  @spec post_retarget_filters(Selecto.Types.t()) :: [Selecto.Types.filter()]
  def post_retarget_filters(selecto) do
    Map.get(selecto.set, :post_retarget_filters, [])
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
  Return query filters across current buckets as a flat list.

  This is useful for integrations that need to copy filters between Selecto and
  other query/update builders.

  ## Options

  - `:include_post_retarget` - include `set.post_retarget_filters` (default: `true`)
  """
  @spec query_filters(Selecto.Types.t(), keyword()) :: [Selecto.Types.filter()]
  def query_filters(selecto, opts \\ []) do
    include_post_retarget = Keyword.get(opts, :include_post_retarget, true)

    validate_tenant = Keyword.get(opts, :validate_tenant, true)

    if validate_tenant do
      Selecto.Tenant.ensure_scope!(selecto, opts)
    end

    set_filters =
      case Map.get(selecto, :set) do
        %{} = set -> Map.get(set, :filtered, [])
        _ -> []
      end

    post_retarget_filters =
      if include_post_retarget do
        case Map.get(selecto, :set) do
          %{} = set ->
            Map.get(set, :post_retarget_filters, [])

          _ ->
            []
        end
      else
        []
      end

    [required_filters(selecto), set_filters, post_retarget_filters]
    |> Enum.flat_map(fn
      filters when is_list(filters) -> filters
      _ -> []
    end)
    |> uniq_filters()
  end

  defp validate_filters_for_active_root!(selecto, filters, true),
    do: validate_post_retarget_filters!(selecto, filters)

  defp validate_filters_for_active_root!(selecto, filters, _has_retarget),
    do: Selecto.QueryValidator.validate_filters!(selecto, filters)

  defp validate_post_retarget_filters!(selecto, filters) do
    case retarget_validation_context(selecto) do
      nil ->
        Selecto.QueryValidator.validate_filters!(selecto, filters)

      {validation_selecto, target_schema} ->
        target_filters = normalize_retarget_filter_prefixes(filters, target_schema)
        Selecto.QueryValidator.validate_filters!(validation_selecto, target_filters)
    end
  end

  defp retarget_validation_context(selecto) do
    with %{target_schema: target_schema} <- Selecto.Retarget.get_retarget_config(selecto),
         {:ok, target_source} <- fetch_target_source(selecto, target_schema) do
      validation_config =
        selecto.config
        |> Map.put(:source, target_source)
        |> Map.put(:source_table, Map.get(target_source, :source_table))
        |> Map.put(:primary_key, Map.get(target_source, :primary_key))
        |> Map.put(:columns, target_columns(target_source))
        |> Map.put(:joins, %{})

      {%{selecto | config: validation_config}, target_schema}
    else
      _ -> nil
    end
  end

  defp fetch_target_source(selecto, target_schema) do
    schemas = Map.get(selecto.domain, :schemas, %{})

    case Map.get(schemas, target_schema) || Map.get(schemas, to_string(target_schema)) do
      nil -> :error
      target_source -> {:ok, target_source}
    end
  end

  defp target_columns(%{fields: fields, columns: columns})
       when is_list(fields) and is_map(columns) do
    Enum.into(fields, %{}, fn field ->
      field_string = to_string(field)
      field_config = Map.get(columns, field) || Map.get(columns, field_string) || %{}

      {field_string,
       field_config
       |> Map.put_new(:field, field)
       |> Map.put_new(:colid, field_string)
       |> Map.put_new(:requires_join, :selecto_root)}
    end)
  end

  defp target_columns(_target_source), do: %{}

  defp normalize_retarget_filter_prefixes(filters, target_schema) when is_list(filters),
    do: Enum.map(filters, &normalize_retarget_filter_prefixes(&1, target_schema))

  defp normalize_retarget_filter_prefixes({:and, filters}, target_schema) when is_list(filters),
    do: {:and, normalize_retarget_filter_prefixes(filters, target_schema)}

  defp normalize_retarget_filter_prefixes({:or, filters}, target_schema) when is_list(filters),
    do: {:or, normalize_retarget_filter_prefixes(filters, target_schema)}

  defp normalize_retarget_filter_prefixes({:not, filter}, target_schema),
    do: {:not, normalize_retarget_filter_prefixes(filter, target_schema)}

  defp normalize_retarget_filter_prefixes({op, field, values}, target_schema)
       when op in [:array_contains, :array_contained, :array_overlap, :array_eq] and
              (is_binary(field) or is_atom(field)) and is_list(values) do
    {op, strip_retarget_field_prefix(field, target_schema), values}
  end

  defp normalize_retarget_filter_prefixes({field, operator, value}, target_schema)
       when is_binary(field) or is_atom(field) do
    {strip_retarget_field_prefix(field, target_schema), operator, value}
  end

  defp normalize_retarget_filter_prefixes({field, value}, target_schema)
       when is_binary(field) or is_atom(field) do
    {strip_retarget_field_prefix(field, target_schema), value}
  end

  defp normalize_retarget_filter_prefixes(filter, _target_schema), do: filter

  defp strip_retarget_field_prefix(field, target_schema)
       when is_binary(field) or is_atom(field) do
    field_string = to_string(field)
    target_prefix = "#{target_schema}."

    if String.starts_with?(field_string, target_prefix) do
      String.replace_prefix(field_string, target_prefix, "")
    else
      field
    end
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

      import Selecto.Expr

      selecto
      |> Selecto.Query.order_by([asc("created_at"), desc("name")])
  """
  @spec order_by(Selecto.Types.t(), [Selecto.Types.order_spec()]) :: Selecto.Types.t()
  def order_by(selecto, orders) when is_list(orders) do
    normalized_orders = Selecto.Expr.normalize(orders)
    Selecto.QueryValidator.validate_order_specs!(selecto, normalized_orders)
    put_in(selecto.set.order_by, selecto.set.order_by ++ normalized_orders)
  end

  @spec order_by(Selecto.Types.t(), Selecto.Types.order_spec()) :: Selecto.Types.t()
  def order_by(selecto, orders) do
    normalized_order = Selecto.Expr.normalize(orders)
    Selecto.QueryValidator.validate_order_specs!(selecto, normalized_order)
    put_in(selecto.set.order_by, selecto.set.order_by ++ [normalized_order])
  end

  @doc """
  Add to the Group By clause.

  ## Examples

      import Selecto.Expr

      selecto
      |> Selecto.Query.group_by(["category", "region"])
      |> Selecto.Query.group_by(rollup(["status"]))
  """
  @spec group_by(Selecto.Types.t(), [Selecto.Types.field_name()]) :: Selecto.Types.t()
  def group_by(selecto, groups) when is_list(groups) do
    normalized_groups = Selecto.Expr.normalize(groups)
    Selecto.QueryValidator.validate_group_specs!(selecto, normalized_groups)
    put_in(selecto.set.group_by, selecto.set.group_by ++ normalized_groups)
  end

  @spec group_by(Selecto.Types.t(), Selecto.Types.field_name()) :: Selecto.Types.t()
  def group_by(selecto, groups) do
    normalized_group = Selecto.Expr.normalize(groups)
    Selecto.QueryValidator.validate_group_specs!(selecto, normalized_group)
    put_in(selecto.set.group_by, selecto.set.group_by ++ [normalized_group])
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
