defmodule Selecto.QueryGenerator do
  @moduledoc """
  SQL query generation engine for Selecto.

  Handles the generation of SQL queries from Selecto domain configurations
  and query specifications, with support for complex joins, CTEs, and OLAP functions.
  """

  @doc """
  Generate SQL query from Selecto configuration.

  ## Parameters

  - `selecto` - The Selecto struct containing domain and query configuration
  - `opts` - Generation options for customizing output

  ## Returns

  `{query_string, aliases_map, parameters_list}` - Complete SQL generation result

  ## Options

  - `:include_aliases` - Whether to include column aliases in output (default: true)
  - `:format` - SQL formatting style (`:compact` or `:pretty`, default: `:compact`)
  - `:validate` - Whether to validate query before generation (default: true)

  ## Examples

      {sql, aliases, params} = Selecto.QueryGenerator.generate_sql(selecto)
      IO.puts("Generated SQL: \#{sql}")
  """
  @spec generate_sql(Selecto.Types.t(), keyword()) :: {String.t(), map(), list()}
  def generate_sql(selecto, opts \\ []) do
    try do
      # Validate inputs if requested
      if Keyword.get(opts, :validate, true) do
        validate_selecto_structure(selecto)
      end

      # Extract configuration
      domain = selecto.domain
      _config = selecto.config || %{}
      set = selecto.set || %{}

      # Build query components
      {select_clause, aliases} = build_select_clause(set, domain, opts)
      from_clause = build_from_clause(domain)
      join_clauses = build_join_clauses(domain, set)
      where_clause = build_where_clause(set, domain)
      group_by_clause = build_group_by_clause(set, domain)
      having_clause = build_having_clause(set, domain)
      order_by_clause = build_order_by_clause(set, domain)
      limit_clause = build_limit_clause(set)

      # Handle CTEs if present
      cte_clause = build_cte_clause(set, domain)

      # Combine all components
      query_parts = [
        cte_clause,
        "SELECT #{select_clause}",
        "FROM #{from_clause}",
        join_clauses,
        where_clause,
        group_by_clause,
        having_clause,
        order_by_clause,
        limit_clause
      ]
      |> Enum.filter(&(&1 != nil and &1 != ""))

      query = Enum.join(query_parts, "\n")

      # Extract parameters from all components
      params = extract_all_parameters(set, domain)

      # Apply formatting if requested
      formatted_query = case Keyword.get(opts, :format, :compact) do
        :pretty -> format_pretty_sql(query)
        :compact -> String.replace(query, ~r/\s+/, " ") |> String.trim()
        _ -> query
      end

      {formatted_query, aliases, params}
    rescue
      error -> raise Selecto.Error.query_generation_error("Failed to generate SQL", %{error: error})
    end
  end

  @doc """
  Validate the structure of a Selecto configuration.

  Checks for required fields and validates domain configuration.
  """
  def validate_selecto_structure(selecto) do
    unless is_map(selecto.domain) do
      raise ArgumentError, "Selecto domain must be a map"
    end

    unless Map.has_key?(selecto.domain, :source) do
      raise ArgumentError, "Selecto domain must contain a source configuration"
    end

    :ok
  end

  @doc """
  Build the SELECT clause with proper aliasing.
  """
  def build_select_clause(set, domain, opts) do
    include_aliases = Keyword.get(opts, :include_aliases, true)

    case Map.get(set, :select) do
      nil -> build_default_select(domain, include_aliases)
      select_spec -> build_custom_select(select_spec, domain, include_aliases)
    end
  end

  @doc """
  Build the FROM clause from domain configuration.
  """
  def build_from_clause(domain) do
    source_table = get_in(domain, [:source, :source_table])

    if source_table do
      quote_identifier(source_table)
    else
      raise ArgumentError, "Domain must specify a source_table"
    end
  end

  @doc """
  Build JOIN clauses from domain joins configuration.
  """
  def build_join_clauses(domain, set) do
    joins = Map.get(domain, :joins, %{})
    active_joins = Map.get(set, :joins, [])

    active_joins
    |> Enum.map(&build_single_join(&1, joins, domain))
    |> Enum.join("\n")
    |> case do
      "" -> nil
      joined -> joined
    end
  end

  @doc """
  Build WHERE clause from filters and conditions.
  """
  def build_where_clause(set, domain) do
    _conditions = []

    # Add required filters from domain
    required_filters = Map.get(domain, :required_filters, [])
    domain_conditions = Enum.map(required_filters, &build_filter_condition(&1, domain))

    # Add dynamic filters from set
    dynamic_filters = Map.get(set, :where, [])
    dynamic_conditions = Enum.map(dynamic_filters, &build_filter_condition(&1, domain))

    all_conditions = domain_conditions ++ dynamic_conditions

    case all_conditions do
      [] -> nil
      conditions -> "WHERE " <> Enum.join(conditions, " AND ")
    end
  end

  @doc """
  Build GROUP BY clause for aggregation queries.
  """
  def build_group_by_clause(set, _domain) do
    case Map.get(set, :group_by) do
      nil -> nil
      [] -> nil
      group_fields -> "GROUP BY " <> Enum.join(group_fields, ", ")
    end
  end

  @doc """
  Build HAVING clause for aggregate filtering.
  """
  def build_having_clause(set, _domain) do
    case Map.get(set, :having) do
      nil -> nil
      [] -> nil
      having_conditions -> "HAVING " <> Enum.join(having_conditions, " AND ")
    end
  end

  @doc """
  Build ORDER BY clause for result sorting.
  """
  def build_order_by_clause(set, _domain) do
    case Map.get(set, :order_by) do
      nil -> nil
      [] -> nil
      order_fields -> "ORDER BY " <> Enum.join(order_fields, ", ")
    end
  end

  @doc """
  Build LIMIT clause for result pagination.
  """
  def build_limit_clause(set) do
    case Map.get(set, :limit) do
      nil -> nil
      limit when is_integer(limit) and limit > 0 -> "LIMIT #{limit}"
      _ -> nil
    end
  end

  @doc """
  Build CTE (Common Table Expression) clause.
  """
  def build_cte_clause(set, domain) do
    case Map.get(set, :cte) do
      nil -> nil
      ctes when is_list(ctes) and length(ctes) > 0 ->
        cte_definitions = Enum.map(ctes, &build_single_cte(&1, domain))
        "WITH " <> Enum.join(cte_definitions, ", ")
      _ -> nil
    end
  end

  # Private helper functions

  defp build_default_select(domain, include_aliases) do
    source = domain.source
    default_fields = Map.get(source, :default_selected, Map.get(source, :fields, []))

    field_clauses = Enum.map(default_fields, fn field ->
      if include_aliases do
        "#{quote_identifier(field)} AS #{quote_identifier(field)}"
      else
        quote_identifier(field)
      end
    end)

    select_clause = Enum.join(field_clauses, ", ")
    aliases = if include_aliases, do: Enum.into(default_fields, %{}, &{&1, &1}), else: %{}

    {select_clause, aliases}
  end

  defp build_custom_select(_select_spec, domain, include_aliases) do
    # TODO: Implement custom select handling
    build_default_select(domain, include_aliases)
  end

  defp build_single_join(_join_spec, _joins_config, _domain) do
    # TODO: Implement join building logic
    ""
  end

  defp build_filter_condition(_filter, _domain) do
    # TODO: Implement filter condition building
    "1=1"
  end

  defp build_single_cte(_cte_spec, _domain) do
    # TODO: Implement CTE building logic
    ""
  end

  defp extract_all_parameters(_set, _domain) do
    # TODO: Extract all parameters from the query
    []
  end

  defp quote_identifier(identifier) when is_atom(identifier) do
    quote_identifier(Atom.to_string(identifier))
  end

  defp quote_identifier(identifier) when is_binary(identifier) do
    "\"#{identifier}\""
  end

  defp format_pretty_sql(sql) do
    # Basic SQL formatting - could be enhanced
    sql
    |> String.replace(" FROM ", "\nFROM ")
    |> String.replace(" WHERE ", "\nWHERE ")
    |> String.replace(" JOIN ", "\nJOIN ")
    |> String.replace(" LEFT JOIN ", "\nLEFT JOIN ")
    |> String.replace(" GROUP BY ", "\nGROUP BY ")
    |> String.replace(" ORDER BY ", "\nORDER BY ")
    |> String.replace(" HAVING ", "\nHAVING ")
    |> String.replace(" LIMIT ", "\nLIMIT ")
  end
end
