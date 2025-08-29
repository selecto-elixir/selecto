defmodule Selecto.Subfilter.SQL do
  @moduledoc """
  Generate SQL WHERE clauses for subfilters from the Subfilter Registry.

  This module coordinates with strategy-specific builders to generate
  optimized SQL for EXISTS, IN, ANY, ALL, and aggregation subqueries.

  ## Main Functions

  - `generate/1`: Generate SQL for all subfilters in a registry.
  - `generate_for_subfilter/2`: Generate SQL for a single subfilter.

  ## Examples

      iex> {:ok, sql, params} = Selecto.Subfilter.SQL.generate(registry)
      iex> "WHERE (EXISTS (SELECT 1 FROM ...)) AND (film_id IN (SELECT ...))"
  """

  alias Selecto.Subfilter.{Registry, Spec, Error}
  alias Selecto.Subfilter.JoinPathResolver.JoinResolution
  alias Selecto.Subfilter.SQL.{ExistsBuilder, InBuilder, AnyAllBuilder, AggregationBuilder}

  @doc """
  Generate SQL WHERE clauses for all subfilters in the registry.

  This function iterates through all registered subfilters, determines the
  appropriate strategy, and delegates to the corresponding SQL builder.
  It then combines the generated SQL clauses using the specified compound
  operators (AND/OR).
  """
  @spec generate(Registry.t()) :: {:ok, String.t(), [any()]} | {:error, Error.t()}
  def generate(%Registry{} = registry) do
    case generate_all_subfilter_clauses(registry) do
      {:ok, clauses_map} ->
        # Convert map to list of clauses for param extraction
        clauses_list = Map.values(clauses_map)
        combined_sql = combine_sql_clauses(clauses_map, registry.compound_ops)

        # Extract params in correct order
        params = Enum.flat_map(clauses_list, fn %{params: p} -> p end)

        {:ok, "WHERE " <> combined_sql, params}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Generate SQL for a single subfilter.

  This is useful for debugging or for cases where you need to generate
  SQL for a single subfilter outside of the main registry flow.
  """
  @spec generate_for_subfilter(Spec.t(), JoinResolution.t(), Registry.t()) ::
    {:ok, String.t(), [any()]} | {:error, Error.t()}
  def generate_for_subfilter(%Spec{} = spec, %JoinResolution{} = join_resolution, %Registry{} = registry) do
    strategy = determine_strategy(spec, registry)

    case dispatch_to_builder(strategy, spec, join_resolution, registry) do
      {:ok, sql, params} -> {:ok, sql, params}
      {:error, reason} -> {:error, reason}
    end
  end

  # Private implementation functions

  defp generate_all_subfilter_clauses(%Registry{} = registry) do
    # This function will need to handle compound operations correctly.
    # For now, we'll generate clauses for all subfilters and combine with AND.

    Enum.reduce_while(registry.subfilters, {:ok, %{}}, fn {subfilter_id, spec}, {:ok, acc} ->
      join_resolution = Map.get(registry.join_resolutions, subfilter_id)

      case generate_for_subfilter(spec, join_resolution, registry) do
        {:ok, sql, params} ->
          clause_map = Map.put(acc, subfilter_id, %{sql: sql, params: params})
          {:cont, {:ok, clause_map}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  defp determine_strategy(%Spec{} = spec, %Registry{} = registry) do
    # The spec.id should match the key in the registry, but let's be defensive
    # and use the key from the registry enumeration instead
    subfilter_id = spec.id

    # Check for manual override first
    case Map.get(registry.strategy_overrides, subfilter_id) do
      nil ->
        # No override, use spec's strategy or auto-detect
        spec.strategy || auto_detect_strategy(spec)

      overridden_strategy ->
        overridden_strategy
    end
  end

  defp auto_detect_strategy(%Spec{filter_spec: %{type: :in_list}}) do
    :in
  end

  defp auto_detect_strategy(%Spec{filter_spec: %{type: :aggregation}}) do
    :aggregation
  end

  defp auto_detect_strategy(_spec) do
    :exists # Default strategy
  end

  defp dispatch_to_builder(:exists, spec, join_resolution, registry) do
    ExistsBuilder.generate(spec, join_resolution, registry)
  end

  defp dispatch_to_builder(:in, spec, join_resolution, registry) do
    InBuilder.generate(spec, join_resolution, registry)
  end

  defp dispatch_to_builder(:any, spec, join_resolution, registry) do
    AnyAllBuilder.generate(:any, spec, join_resolution, registry)
  end

  defp dispatch_to_builder(:all, spec, join_resolution, registry) do
    AnyAllBuilder.generate(:all, spec, join_resolution, registry)
  end

  defp dispatch_to_builder(:aggregation, spec, join_resolution, registry) do
    AggregationBuilder.generate(spec, join_resolution, registry)
  end

  defp dispatch_to_builder(invalid_strategy, _spec, _join_resolution, _registry) do
    {:error, %Error{
      type: :unknown_strategy,
      message: "Cannot generate SQL for unknown strategy",
      details: %{strategy: invalid_strategy}
    }}
  end

  defp combine_sql_clauses(clauses_map, compound_ops) do
    # If there are compound operations, use them to structure the WHERE clause
    if Enum.any?(compound_ops) do
      build_compound_where_clause(clauses_map, compound_ops)
    else
      # Default to ANDing all clauses together
      clauses_map
      |> Map.values()
      |> Enum.map(fn %{sql: sql} -> "(#{sql})" end)
      |> Enum.join(" AND ")
    end
  end

  defp build_compound_where_clause(clauses_map, compound_ops) do
    # This is a simplified implementation. A full implementation would need
    # to handle nested compound operations and complex boolean logic.

    Enum.map(compound_ops, fn %{type: op_type, subfilter_ids: ids} ->
      op_sql =
        ids
        |> Enum.map(fn id -> Map.get(clauses_map, id) end)
        |> Enum.reject(&is_nil/1)
        |> Enum.map(fn %{sql: sql} -> "(#{sql})" end)
        |> Enum.join(" #{String.upcase(to_string(op_type))} ")

      "(#{op_sql})"
    end)
    |> Enum.join(" AND ") # Assuming top-level operations are ANDed
  end
end
