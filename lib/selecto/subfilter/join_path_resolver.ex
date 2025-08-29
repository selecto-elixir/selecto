defmodule Selecto.Subfilter.JoinPathResolver do
  @moduledoc """
  Resolve relationship paths into join sequences using domain configurations.

  This module takes parsed relationship paths like "film.rating" or "film.category.name"
  and resolves them into concrete join sequences using the existing domain join
  configurations from Phase 1.1 parameterized joins.

  ## Examples

      iex> Selecto.Subfilter.JoinPathResolver.resolve("film.rating", :film_domain)
      {:ok, %JoinResolution{
        joins: [%{from: :film, to: :film, type: :self, field: :rating}],
        target_table: :film,
        target_field: :rating
      }}

      iex> Selecto.Subfilter.JoinPathResolver.resolve("film.category.name", :film_domain)
      {:ok, %JoinResolution{
        joins: [
          %{from: :film, to: :film_category, type: :inner, on: "film.film_id = film_category.film_id"},
          %{from: :film_category, to: :category, type: :inner, on: "film_category.category_id = category.category_id"}
        ],
        target_table: :category,
        target_field: :name
      }}
  """

  alias Selecto.Subfilter
  alias Selecto.Subfilter.{RelationshipPath, Error}

  # Structure to hold resolved join path information
  defmodule JoinResolution do
    @moduledoc """
    Structure representing a resolved join path with all necessary join information.
    """
    defstruct [
      :joins,           # List of join configurations
      :target_table,    # Final target table
      :target_field,    # Target field (nil for aggregations)
      :path_segments,   # Original path segments for debugging
      :is_aggregation   # Whether this is an aggregation subfilter
    ]

    @type t :: %__MODULE__{
      joins: [join_config()],
      target_table: atom(),
      target_field: String.t() | nil,
      path_segments: [String.t()],
      is_aggregation: boolean()
    }

    @type join_config :: %{
      from: atom(),
      to: atom(),
      type: :inner | :left | :right | :full | :self,
      on: String.t() | nil,
      field: atom() | nil
    }
  end

  @doc """
  Resolve relationship path into join sequence using domain configuration.

  ## Parameters

  - `relationship_path` - Parsed RelationshipPath struct
  - `domain_name` - Domain name to use for join resolution (e.g., :film_domain)
  - `base_table` - Base table for the query (defaults to first segment of path)

  ## Returns

  {:ok, JoinResolution.t()} | {:error, Subfilter.Error.t()}
  """
  @spec resolve(RelationshipPath.t(), atom(), atom() | nil) ::
    {:ok, JoinResolution.t()} | {:error, Error.t()}
  def resolve(%RelationshipPath{} = path, domain_name, base_table \\ nil) do
    with {:ok, domain_config} <- get_domain_config(domain_name),
         {:ok, resolved_base_table} <- resolve_base_table(path, base_table),
         {:ok, joins} <- build_join_sequence(path, domain_config, resolved_base_table) do

      resolution = %JoinResolution{
        joins: joins,
        target_table: determine_target_table(path, joins),
        target_field: path.target_field,
        path_segments: path.path_segments,
        is_aggregation: path.is_aggregation
      }

      {:ok, resolution}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Resolve multiple relationship paths at once for compound subfilters.

  This is more efficient than resolving paths individually when dealing with
  compound subfilters (AND/OR operations) as it can detect and reuse common
  join sequences.
  """
  @spec resolve_multiple([RelationshipPath.t()], atom(), atom() | nil) ::
    {:ok, [JoinResolution.t()]} | {:error, Error.t()}
  def resolve_multiple(paths, domain_name, base_table \\ nil) do
    case resolve_all_paths(paths, domain_name, base_table, []) do
      {:ok, resolutions} ->
        # Optimize by detecting common join patterns
        optimized_resolutions = optimize_join_sequences(resolutions)
        {:ok, optimized_resolutions}
      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Validate that a relationship path can be resolved with the given domain configuration.

  This is useful for early validation before attempting to build queries.
  """
  @spec validate_path(RelationshipPath.t(), atom()) :: :ok | {:error, Error.t()}
  def validate_path(%RelationshipPath{} = path, domain_name) do
    case resolve(path, domain_name) do
      {:ok, _resolution} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  # Private implementation functions

  defp get_domain_config(domain_name) do
    # Note: In a real implementation, this would integrate with the existing
    # domain configuration system from Phase 1.1. For now, we'll define
    # some example configurations to demonstrate the structure.

    case domain_name do
      :film_domain ->
        {:ok, %{
          tables: [:film, :category, :film_category, :actor, :film_actor, :language],
          joins: %{
            # Direct field access (no join needed)
            "film.rating" => %{from: :film, to: :film, type: :self, field: :rating},
            "film.title" => %{from: :film, to: :film, type: :self, field: :title},
            "film.release_year" => %{from: :film, to: :film, type: :self, field: :release_year},
            "film.rental_rate" => %{from: :film, to: :film, type: :self, field: :rental_rate},
            "film.film_id" => %{from: :film, to: :film, type: :self, field: :film_id},

            # Single-hop joins
            "film.category" => %{
              from: :film,
              to: :category,
              type: :inner,
              via: :film_category,
              on: "film.film_id = film_category.film_id AND film_category.category_id = category.category_id"
            },
            "film.actor" => %{
              from: :film,
              to: :actor,
              type: :inner,
              via: :film_actor,
              on: "film.film_id = film_actor.film_id AND film_actor.actor_id = actor.actor_id"
            },
            "film.actors" => %{
              from: :film,
              to: :film_actor,
              type: :inner,
              on: "film.film_id = film_actor.film_id"
            },
            "film.language" => %{
              from: :film,
              to: :language,
              type: :inner,
              on: "film.language_id = language.language_id"
            },

            # Multi-hop path resolution
            "film.category.name" => [
              %{from: :film, to: :film_category, type: :inner, on: "film.film_id = film_category.film_id"},
              %{from: :film_category, to: :category, type: :inner, on: "film_category.category_id = category.category_id"}
            ],
            "film.language.name" => [
              %{from: :film, to: :language, type: :inner, on: "film.language_id = language.language_id"}
            ],
            "film.actor.first_name" => [
              %{from: :film, to: :film_actor, type: :inner, on: "film.film_id = film_actor.film_id"},
              %{from: :film_actor, to: :actor, type: :inner, on: "film_actor.actor_id = actor.actor_id"}
            ]
          }
        }}

      :actor_domain ->
        {:ok, %{
          tables: [:actor, :film, :film_actor],
          joins: %{
            "actor.first_name" => %{from: :actor, to: :actor, type: :self, field: :first_name},
            "actor.last_name" => %{from: :actor, to: :actor, type: :self, field: :last_name},
            "actor.film" => %{
              from: :actor,
              to: :film,
              type: :inner,
              via: :film_actor,
              on: "actor.actor_id = film_actor.actor_id AND film_actor.film_id = film.film_id"
            }
          }
        }}

      _ ->
        {:error, %Error{
          type: :unknown_domain,
          message: "Domain configuration not found",
          details: %{domain: domain_name}
        }}
    end
  end

  defp resolve_base_table(%RelationshipPath{path_segments: [first_segment | _]}, nil) do
    {:ok, String.to_atom(first_segment)}
  end

  defp resolve_base_table(_path, base_table) when is_atom(base_table) do
    {:ok, base_table}
  end

  defp resolve_base_table(_path, base_table) do
    {:error, %Error{
      type: :invalid_base_table,
      message: "Base table must be an atom",
      details: %{base_table: base_table}
    }}
  end

  defp build_join_sequence(%RelationshipPath{is_aggregation: true, path_segments: [table]}, _config, base_table) do
    # Aggregation subfilter - no joins needed, just count/aggregate on the base table
    {:ok, [%{from: base_table, to: String.to_atom(table), type: :self, field: nil}]}
  end

  defp build_join_sequence(%RelationshipPath{path_segments: path_segments, target_field: target_field}, domain_config, base_table) do
    path_key = Enum.join(path_segments ++ [target_field], ".")

    case Map.get(domain_config.joins, path_key) do
      # Direct field access (self-join)
      %{type: :self} = join_config ->
        {:ok, [join_config]}

      # Single complex join with via table
      %{via: via_table} = join_config ->
        # Decompose complex join into sequence of simple joins
        joins = decompose_via_join(join_config, base_table)
        {:ok, joins}

      # Pre-configured multi-hop sequence
      joins when is_list(joins) ->
        {:ok, joins}

      # Single direct join
      %{} = join_config ->
        {:ok, [join_config]}

      # Path not found - try to auto-resolve
      nil ->
        auto_resolve_path(path_segments, target_field, domain_config, base_table)
    end
  end

  defp decompose_via_join(%{from: from, to: to, via: via, on: on_clause}, _base_table) do
    # Parse the compound ON clause to create individual join steps
    # For example: "film.film_id = film_category.film_id AND film_category.category_id = category.category_id"
    # becomes two separate joins

    [
      %{from: from, to: via, type: :inner, on: extract_first_join_condition(on_clause)},
      %{from: via, to: to, type: :inner, on: extract_second_join_condition(on_clause)}
    ]
  end

  defp extract_first_join_condition(on_clause) do
    # Simple parsing - in real implementation would be more robust
    case String.split(on_clause, " AND ") do
      [first_condition | _] -> String.trim(first_condition)
      _ -> on_clause
    end
  end

  defp extract_second_join_condition(on_clause) do
    case String.split(on_clause, " AND ") do
      [_, second_condition] -> String.trim(second_condition)
      _ -> on_clause
    end
  end

  defp auto_resolve_path(path_segments, target_field, domain_config, base_table) do
    # Attempt to automatically resolve path by looking for intermediate relationships
    case try_step_by_step_resolution(path_segments, target_field, domain_config, base_table) do
      {:ok, joins} -> {:ok, joins}
      {:error, _} ->
        {:error, %Error{
          type: :unresolvable_path,
          message: "Cannot resolve relationship path with available join configurations",
          details: %{
            path_segments: path_segments,
            target_field: target_field,
            base_table: base_table,
            available_joins: Map.keys(domain_config.joins)
          }
        }}
    end
  end

  defp try_step_by_step_resolution([single_table], target_field, domain_config, base_table) do
    # Simple field access
    field_path = "#{single_table}.#{target_field}"
    case Map.get(domain_config.joins, field_path) do
      %{} = join_config -> {:ok, [join_config]}
      nil -> {:error, :not_found}
    end
  end

  defp try_step_by_step_resolution(path_segments, target_field, _domain_config, _base_table) do
    # For now, return error for complex auto-resolution
    # In a full implementation, this would attempt to chain joins step-by-step
    {:error, %Error{
      type: :complex_auto_resolution_not_implemented,
      message: "Complex automatic path resolution not yet implemented",
      details: %{path_segments: path_segments, target_field: target_field}
    }}
  end

  defp determine_target_table(%RelationshipPath{target_table: target_table}, _joins) when is_binary(target_table) do
    String.to_atom(target_table)
  end

  defp determine_target_table(_path, []) do
    nil
  end

  defp determine_target_table(_path, joins) do
    %{to: target_table} = List.last(joins)
    target_table
  end

  defp resolve_all_paths([], _domain_name, _base_table, acc) do
    {:ok, Enum.reverse(acc)}
  end

  defp resolve_all_paths([path | rest], domain_name, base_table, acc) do
    case resolve(path, domain_name, base_table) do
      {:ok, resolution} ->
        resolve_all_paths(rest, domain_name, base_table, [resolution | acc])
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp optimize_join_sequences(resolutions) do
    # For now, return as-is. In a full implementation, this would:
    # 1. Detect common join prefixes across resolutions
    # 2. Eliminate duplicate joins where possible
    # 3. Optimize join order for performance
    resolutions
  end
end
