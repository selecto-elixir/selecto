defmodule Selecto.Subfilter.JoinPathResolver do
  @moduledoc """
  Resolve relationship paths into join sequences using domain join-path configuration.

  This module expects a domain configuration with a flat `joins` map keyed by
  relationship paths (e.g. `"orders.customer.name"`).
  """

  alias Selecto.Subfilter.{RelationshipPath, Error}

  # Structure to hold resolved join path information
  defmodule JoinResolution do
    @moduledoc """
    Structure representing a resolved join path with all necessary join information.
    """
    defstruct [
      # List of join configurations
      :joins,
      # Final target table
      :target_table,
      # Target field (nil for aggregations)
      :target_field,
      # Original path segments for debugging
      :path_segments,
      # Whether this is an aggregation subfilter
      :is_aggregation
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
  - `domain_name` - Domain identifier or a domain join-path config map
  - `base_table` - Base table for the query (defaults to first segment of path)

  ## Returns

  {:ok, JoinResolution.t()} | {:error, Subfilter.Error.t()}
  """
  @spec resolve(RelationshipPath.t(), atom() | map(), atom() | nil) ::
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
  @spec resolve_multiple([RelationshipPath.t()], atom() | map(), atom() | nil) ::
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
  @spec validate_path(RelationshipPath.t(), atom() | map()) :: :ok | {:error, Error.t()}
  def validate_path(%RelationshipPath{} = path, domain_name) do
    case resolve(path, domain_name) do
      {:ok, _resolution} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  # Private implementation functions

  defp get_domain_config(%{} = domain_config) do
    normalize_domain_config(domain_config)
  end

  defp get_domain_config(domain_name) when is_atom(domain_name) do
    {:error,
     %Error{
       type: :unknown_domain,
       message: "Domain configuration not found",
       details: %{domain: domain_name}
     }}
  end

  defp get_domain_config(other) do
    {:error,
     %Error{
       type: :invalid_domain_config,
       message: "Domain configuration must be an atom or map",
       details: %{domain: other}
     }}
  end

  defp normalize_domain_config(%{joins: joins} = domain_config) when is_map(joins) do
    normalized_joins =
      joins
      |> Enum.map(fn {key, value} -> {normalize_join_key(key), value} end)
      |> Map.new()

    {:ok,
     %{
       tables: Map.get(domain_config, :tables, []),
       joins: normalized_joins
     }}
  end

  defp normalize_domain_config(_domain_config) do
    {:error,
     %Error{
       type: :invalid_domain_config,
       message: "Domain configuration must include a joins map",
       details: %{}
     }}
  end

  defp normalize_join_key(key) when is_binary(key), do: key
  defp normalize_join_key(key) when is_atom(key), do: Atom.to_string(key)
  defp normalize_join_key(key), do: to_string(key)

  defp resolve_base_table(%RelationshipPath{path_segments: [first_segment | _]}, nil) do
    {:ok, String.to_atom(first_segment)}
  end

  defp resolve_base_table(_path, base_table) when is_atom(base_table) do
    {:ok, base_table}
  end

  defp resolve_base_table(_path, base_table) do
    {:error,
     %Error{
       type: :invalid_base_table,
       message: "Base table must be an atom",
       details: %{base_table: base_table}
     }}
  end

  defp build_join_sequence(
         %RelationshipPath{is_aggregation: true, path_segments: [table]},
         _config,
         base_table
       ) do
    # Aggregation subfilter - no joins needed, just count/aggregate on the base table
    {:ok, [%{from: base_table, to: String.to_atom(table), type: :self, field: nil}]}
  end

  defp build_join_sequence(
         %RelationshipPath{path_segments: path_segments, target_field: target_field},
         domain_config,
         base_table
       ) do
    path_key = Enum.join(path_segments ++ [target_field], ".")

    case Map.get(domain_config.joins, path_key) do
      # Direct field access (self-join)
      %{type: :self} = join_config ->
        {:ok, [join_config]}

      # Single complex join with via table
      %{via: _via_table} = join_config ->
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
      {:ok, joins} ->
        {:ok, joins}

      {:error, _} ->
        {:error,
         %Error{
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

  defp try_step_by_step_resolution([single_table], target_field, domain_config, _base_table) do
    # Simple field access
    field_path = "#{single_table}.#{target_field}"

    case Map.get(domain_config.joins, field_path) do
      %{} = join_config -> {:ok, [join_config]}
      nil -> {:error, :not_found}
    end
  end

  defp try_step_by_step_resolution(path_segments, target_field, domain_config, _base_table) do
    path_prefix = Enum.join(path_segments, ".")

    with {:error, :not_found} <- infer_join_sequence_from_known_paths(path_prefix, domain_config),
         {:error, :not_found} <- chain_relationship_segments(path_segments, domain_config) do
      {:error,
       %Error{
         type: :unresolvable_path,
         message: "No join sequence could be inferred for relationship path",
         details: %{path_segments: path_segments, target_field: target_field}
       }}
    else
      {:ok, joins} -> {:ok, joins}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp infer_join_sequence_from_known_paths(path_prefix, domain_config) do
    candidate_sequences =
      domain_config.joins
      |> Enum.filter(fn {key, _value} -> String.starts_with?(key, path_prefix <> ".") end)
      |> Enum.map(fn {_key, value} -> normalize_join_value(value) end)
      |> Enum.reject(&is_nil/1)

    unique_sequences =
      candidate_sequences
      |> Enum.uniq_by(&join_sequence_signature/1)

    case unique_sequences do
      [] -> {:error, :not_found}
      [joins] -> {:ok, joins}
      _ -> {:error, :not_found}
    end
  end

  defp chain_relationship_segments([first_segment | rest_segments], domain_config) do
    Enum.reduce_while(rest_segments, {:ok, first_segment, []}, fn next_segment,
                                                                  {:ok, current_segment,
                                                                   joins_acc} ->
      join_key = "#{current_segment}.#{next_segment}"

      case Map.get(domain_config.joins, join_key) do
        nil ->
          {:halt, {:error, :not_found}}

        join_value ->
          case normalize_join_value(join_value) do
            nil ->
              {:halt, {:error, :not_found}}

            normalized_joins ->
              {:cont, {:ok, next_segment, joins_acc ++ normalized_joins}}
          end
      end
    end)
    |> case do
      {:ok, _last_segment, []} -> {:error, :not_found}
      {:ok, _last_segment, joins} -> {:ok, joins}
      {:error, :not_found} -> {:error, :not_found}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp normalize_join_value(%{via: _via_table} = join_config) do
    decompose_via_join(join_config, nil)
  end

  defp normalize_join_value(joins) when is_list(joins) do
    joins
  end

  defp normalize_join_value(%{} = join_config) do
    [join_config]
  end

  defp normalize_join_value(_value) do
    nil
  end

  defp join_sequence_signature(joins) do
    Enum.map(joins, fn join ->
      {Map.get(join, :from), Map.get(join, :to), Map.get(join, :type), Map.get(join, :on)}
    end)
  end

  defp determine_target_table(%RelationshipPath{target_table: target_table}, _joins)
       when is_binary(target_table) do
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
