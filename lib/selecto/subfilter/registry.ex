defmodule Selecto.Subfilter.Registry do
  @moduledoc """
  Registry system for managing multiple subfilters with strategy selection and optimization.

  The Registry handles:
  - Multiple subfilter registration and management
  - Strategy selection (EXISTS, IN, ANY, ALL) based on query patterns
  - Performance optimization through join analysis
  - Conflict detection and resolution
  - SQL generation coordination

  ## Examples

      iex> registry = Selecto.Subfilter.Registry.new(:film_domain)
      iex> registry = Registry.add_subfilter(registry, "film.rating", "R")
      iex> registry = Registry.add_subfilter(registry, "film.category.name", "Action")
      iex> Registry.generate_sql(registry, base_query)
      {:ok, optimized_query_with_subfilters}
  """

  alias Selecto.Subfilter
  alias Selecto.Subfilter.{Spec, Parser, JoinPathResolver, Error}

  # Registry structure to manage multiple subfilters
  defstruct [
    :domain_name,        # Domain configuration to use
    :base_table,         # Base table for the query
    :subfilters,         # Map of subfilter_id => Spec
    :join_resolutions,   # Map of subfilter_id => JoinResolution
    :strategy_overrides, # Manual strategy overrides
    :optimization_hints, # Performance optimization hints
    :compound_ops        # Compound operations (AND/OR between subfilters)
  ]

  @type t :: %__MODULE__{
    domain_name: atom(),
    base_table: atom() | nil,
    subfilters: %{String.t() => Spec.t()},
    join_resolutions: %{String.t() => JoinPathResolver.JoinResolution.t()},
    strategy_overrides: %{String.t() => atom()},
    optimization_hints: keyword(),
    compound_ops: [compound_operation()]
  }

  @type compound_operation :: %{
    type: :and | :or,
    subfilter_ids: [String.t()]
  }

  @doc """
  Create a new subfilter registry for the specified domain.

  ## Parameters

  - `domain_name` - Domain configuration to use (e.g., :film_domain)
  - `opts` - Options including :base_table, :optimization_hints
  """
  @spec new(atom(), keyword()) :: t()
  def new(domain_name, opts \\ []) do
    %__MODULE__{
      domain_name: domain_name,
      base_table: Keyword.get(opts, :base_table),
      subfilters: %{},
      join_resolutions: %{},
      strategy_overrides: %{},
      optimization_hints: Keyword.get(opts, :optimization_hints, []),
      compound_ops: []
    }
  end

  @doc """
  Add a subfilter to the registry.

  ## Examples

      add_subfilter(registry, "film.rating", "R")
      add_subfilter(registry, "film.category.name", ["Action", "Drama"], strategy: :in)
      add_subfilter(registry, "film", {:count, ">", 5}, id: "film_count_filter")
  """
  @spec add_subfilter(t(), String.t(), any(), keyword()) ::
    {:ok, t()} | {:error, Error.t()}
  def add_subfilter(%__MODULE__{} = registry, relationship_path, filter_spec, opts \\ []) do
    with {:ok, parsed_spec} <- Parser.parse(relationship_path, filter_spec, opts),
         {:ok, resolved_joins} <- JoinPathResolver.resolve(parsed_spec.relationship_path, registry.domain_name, registry.base_table) do

      subfilter_id = Keyword.get(opts, :id, parsed_spec.id)

      # Check for conflicts
      case check_for_conflicts(registry, subfilter_id, parsed_spec) do
        :ok ->
          # Update the spec with the final ID
          final_spec = %{parsed_spec | id: subfilter_id}

          updated_registry = %{registry |
            subfilters: Map.put(registry.subfilters, subfilter_id, final_spec),
            join_resolutions: Map.put(registry.join_resolutions, subfilter_id, resolved_joins)
          }

          # Apply strategy optimization
          optimized_registry = optimize_strategies(updated_registry)

          {:ok, optimized_registry}

        {:error, reason} ->
          {:error, reason}
      end
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Add compound subfilter operations (AND/OR).

  ## Examples

      add_compound(registry, :and, [
        {"film.rating", "R"},
        {"film.release_year", {">", 2000}}
      ])
  """
  @spec add_compound(t(), :and | :or, [{String.t(), any()}] | [{String.t(), any(), keyword()}], keyword()) ::
    {:ok, t()} | {:error, Error.t()}
  def add_compound(%__MODULE__{} = registry, compound_type, subfilter_specs, opts \\ []) do
    with {:ok, compound_spec} <- Parser.parse_compound(compound_type, subfilter_specs, opts) do

      # Add each individual subfilter first
      case add_compound_subfilters(registry, compound_spec.subfilters) do
        {:ok, updated_registry, subfilter_ids} ->
          compound_op = %{
            type: compound_type,
            subfilter_ids: subfilter_ids
          }

          final_registry = %{updated_registry |
            compound_ops: [compound_op | updated_registry.compound_ops]
          }

          {:ok, final_registry}

        {:error, reason} ->
          {:error, reason}
      end
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Remove a subfilter from the registry.
  """
  @spec remove_subfilter(t(), String.t()) :: t()
  def remove_subfilter(%__MODULE__{} = registry, subfilter_id) do
    %{registry |
      subfilters: Map.delete(registry.subfilters, subfilter_id),
      join_resolutions: Map.delete(registry.join_resolutions, subfilter_id),
      strategy_overrides: Map.delete(registry.strategy_overrides, subfilter_id),
      compound_ops: remove_from_compound_ops(registry.compound_ops, subfilter_id)
    }
  end

  @doc """
  Override the strategy for a specific subfilter.

  Useful for performance tuning when the automatic strategy selection
  doesn't produce optimal results.
  """
  @spec override_strategy(t(), String.t(), atom()) :: {:ok, t()} | {:error, Error.t()}
  def override_strategy(%__MODULE__{} = registry, subfilter_id, strategy)
      when strategy in [:exists, :in, :any, :all] do

    case Map.has_key?(registry.subfilters, subfilter_id) do
      true ->
        updated_registry = %{registry |
          strategy_overrides: Map.put(registry.strategy_overrides, subfilter_id, strategy)
        }
        {:ok, updated_registry}

      false ->
        {:error, %Error{
          type: :subfilter_not_found,
          message: "Subfilter not found in registry",
          details: %{subfilter_id: subfilter_id, available_ids: Map.keys(registry.subfilters)}
        }}
    end
  end

  def override_strategy(_registry, _subfilter_id, invalid_strategy) do
    {:error, %Error{
      type: :invalid_strategy,
      message: "Invalid strategy for override",
      details: %{strategy: invalid_strategy, valid_strategies: [:exists, :in, :any, :all]}
    }}
  end

  @doc """
  Get comprehensive analysis of all subfilters in the registry.

  Returns information about join patterns, strategy selections,
  performance implications, and optimization opportunities.
  """
  @spec analyze(t()) :: %{
    subfilter_count: non_neg_integer(),
    join_complexity: atom(),
    strategy_distribution: %{atom() => non_neg_integer()},
    performance_score: float(),
    optimization_suggestions: [String.t()]
  }
  def analyze(%__MODULE__{} = registry) do
    subfilter_count = map_size(registry.subfilters)

    %{
      subfilter_count: subfilter_count,
      join_complexity: assess_join_complexity(registry),
      strategy_distribution: calculate_strategy_distribution(registry),
      performance_score: calculate_performance_score(registry),
      optimization_suggestions: generate_optimization_suggestions(registry)
    }
  end

  @doc """
  Generate SQL for all subfilters in the registry.

  This coordinates with the SQL generation system to produce optimized
  subquery SQL that integrates with the main query.
  """
  @spec generate_sql(t(), String.t()) :: {:ok, String.t(), [any()]} | {:error, Error.t()}
  def generate_sql(%__MODULE__{} = registry, base_query) do
    # This would coordinate with the SQL builder system
    # For now, return a placeholder
    {:ok, "#{base_query} -- subfilters would be added here", []}
  end

  # Private implementation functions

  defp check_for_conflicts(%__MODULE__{} = registry, subfilter_id, _spec) do
    case Map.has_key?(registry.subfilters, subfilter_id) do
      true ->
        {:error, %Error{
          type: :duplicate_subfilter_id,
          message: "Subfilter ID already exists in registry",
          details: %{subfilter_id: subfilter_id}
        }}
      false ->
        :ok
    end
  end

  defp optimize_strategies(%__MODULE__{} = registry) do
    # Apply automatic strategy optimization based on subfilter patterns
    # This would analyze join complexity, filter selectivity, etc.
    # For now, return as-is
    registry
  end

  defp add_compound_subfilters(registry, subfilters) do
    add_compound_subfilters(registry, subfilters, [])
  end

  defp add_compound_subfilters(registry, [], subfilter_ids) do
    {:ok, registry, Enum.reverse(subfilter_ids)}
  end

  defp add_compound_subfilters(registry, [spec | rest], subfilter_ids) do
    # Generate a unique ID for this subfilter based on its spec
    subfilter_id = generate_compound_subfilter_id(spec)

    case add_parsed_subfilter(registry, subfilter_id, spec) do
      {:ok, updated_registry} ->
        add_compound_subfilters(updated_registry, rest, [subfilter_id | subfilter_ids])
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp generate_compound_subfilter_id(spec) do
    path_segments = spec.relationship_path.path_segments
    path_str = Enum.join(path_segments, "_")
    field = spec.relationship_path.target_field || "agg"

    "compound_#{path_str}_#{field}"
  end

  defp add_parsed_subfilter(registry, subfilter_id, spec) do
    with {:ok, resolved_joins} <- JoinPathResolver.resolve(spec.relationship_path, registry.domain_name, registry.base_table) do
      case check_for_conflicts(registry, subfilter_id, spec) do
        :ok ->
          updated_registry = %{registry |
            subfilters: Map.put(registry.subfilters, subfilter_id, spec),
            join_resolutions: Map.put(registry.join_resolutions, subfilter_id, resolved_joins)
          }
          {:ok, updated_registry}

        {:error, reason} ->
          {:error, reason}
      end
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp remove_from_compound_ops(compound_ops, subfilter_id) do
    Enum.map(compound_ops, fn op ->
      %{op | subfilter_ids: List.delete(op.subfilter_ids, subfilter_id)}
    end)
    |> Enum.reject(fn op -> Enum.empty?(op.subfilter_ids) end)
  end

  defp assess_join_complexity(%__MODULE__{join_resolutions: join_resolutions}) do
    total_joins =
      join_resolutions
      |> Map.values()
      |> Enum.map(fn resolution -> length(resolution.joins) end)
      |> Enum.sum()

    cond do
      total_joins == 0 -> :none
      total_joins <= 3 -> :low
      total_joins <= 8 -> :medium
      total_joins <= 15 -> :high
      true -> :very_high
    end
  end

  defp calculate_strategy_distribution(%__MODULE__{subfilters: subfilters, strategy_overrides: overrides}) do
    subfilters
    |> Map.keys()
    |> Enum.reduce(%{}, fn subfilter_id, acc ->
      strategy = Map.get(overrides, subfilter_id, Map.get(subfilters, subfilter_id).strategy)
      Map.update(acc, strategy, 1, &(&1 + 1))
    end)
  end

  defp calculate_performance_score(%__MODULE__{} = registry) do
    # Simple scoring based on join complexity and subfilter count
    complexity_penalty =
      case assess_join_complexity(registry) do
        :none -> 0.0
        :low -> 0.1
        :medium -> 0.3
        :high -> 0.5
        :very_high -> 0.7
      end

    subfilter_count_penalty = map_size(registry.subfilters) * 0.05

    max(0.0, 1.0 - complexity_penalty - subfilter_count_penalty)
  end

  defp generate_optimization_suggestions(%__MODULE__{} = registry) do
    suggestions = []

    # Check for high join complexity
    suggestions =
      case assess_join_complexity(registry) do
        complexity when complexity in [:high, :very_high] ->
          ["Consider reducing join complexity by using IN strategy for some subfilters" | suggestions]
        _ ->
          suggestions
      end

    # Check for too many EXISTS subfilters
    strategy_dist = calculate_strategy_distribution(registry)
    exists_count = Map.get(strategy_dist, :exists, 0)

    suggestions =
      if exists_count > 3 do
        ["Consider using IN strategy for some EXISTS subfilters to improve performance" | suggestions]
      else
        suggestions
      end

    # Check for compound operations optimization
    suggestions =
      if length(registry.compound_ops) > 2 do
        ["Complex compound operations may benefit from query restructuring" | suggestions]
      else
        suggestions
      end

    suggestions
  end
end
