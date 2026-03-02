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
    {first_on, second_on} = extract_via_join_conditions(on_clause, from, via, to)

    [
      %{from: from, to: via, type: :inner, on: first_on},
      %{from: via, to: to, type: :inner, on: second_on}
    ]
  end

  defp extract_via_join_conditions(on_clause, from, via, to) do
    conditions = split_top_level_conditions(on_clause)

    from_via = find_join_condition(conditions, from, via)
    via_to = find_join_condition(conditions, via, to)

    first_fallback = extract_first_join_condition(on_clause)
    second_fallback = extract_second_join_condition(on_clause)

    {
      from_via || first_fallback,
      via_to || second_fallback
    }
  end

  defp split_top_level_conditions(on_clause) when is_binary(on_clause) do
    do_split_top_level_conditions(String.trim(on_clause), [])
  end

  defp split_top_level_conditions(other), do: [to_string(other)]

  defp do_split_top_level_conditions("", acc), do: Enum.reverse(acc)

  defp do_split_top_level_conditions(clause, acc) do
    case split_first_top_level_and(clause) do
      {left, nil} ->
        Enum.reverse([left | acc])

      {left, right} ->
        do_split_top_level_conditions(right, [left | acc])
    end
  end

  defp find_join_condition(conditions, left_table, right_table) do
    left = Atom.to_string(left_table)
    right = Atom.to_string(right_table)

    Enum.find(conditions, fn condition ->
      references_tables?(condition, left, right)
    end)
  end

  defp references_tables?(condition, left, right) when is_binary(condition) do
    references_table?(condition, left) and references_table?(condition, right)
  end

  defp references_tables?(_condition, _left, _right), do: false

  defp references_table?(condition, table_name) do
    pattern = ~r/(^|[^A-Za-z0-9_])#{Regex.escape(table_name)}\./i
    Regex.match?(pattern, condition)
  end

  defp extract_first_join_condition(on_clause) do
    case split_first_top_level_and(on_clause) do
      {first_condition, _second_condition} -> first_condition
      _ -> String.trim(on_clause)
    end
  end

  defp extract_second_join_condition(on_clause) do
    case split_first_top_level_and(on_clause) do
      {_first_condition, second_condition}
      when is_binary(second_condition) and second_condition != "" ->
        second_condition

      _ ->
        String.trim(on_clause)
    end
  end

  defp split_first_top_level_and(on_clause) when is_binary(on_clause) do
    len = byte_size(on_clause)

    case find_top_level_and(on_clause, len, 0, 0, false, false) do
      {:ok, and_index} ->
        left = on_clause |> binary_part(0, and_index) |> String.trim()
        right_start = skip_whitespace(on_clause, and_index + 3, len)
        right = on_clause |> binary_part(right_start, len - right_start) |> String.trim()
        {left, right}

      :error ->
        {String.trim(on_clause), nil}
    end
  end

  defp split_first_top_level_and(other), do: {to_string(other), nil}

  defp find_top_level_and(_clause, len, index, _depth, _single_quote?, _double_quote?)
       when index + 2 >= len,
       do: :error

  defp find_top_level_and(clause, len, index, depth, single_quote?, double_quote?) do
    current = :binary.at(clause, index)

    cond do
      current == ?' and not double_quote? ->
        find_top_level_and(clause, len, index + 1, depth, not single_quote?, double_quote?)

      current == ?" and not single_quote? ->
        find_top_level_and(clause, len, index + 1, depth, single_quote?, not double_quote?)

      single_quote? or double_quote? ->
        find_top_level_and(clause, len, index + 1, depth, single_quote?, double_quote?)

      current == ?( ->
        find_top_level_and(clause, len, index + 1, depth + 1, single_quote?, double_quote?)

      current == ?) and depth > 0 ->
        find_top_level_and(clause, len, index + 1, depth - 1, single_quote?, double_quote?)

      depth == 0 and top_level_and_at?(clause, index, len) ->
        {:ok, index}

      true ->
        find_top_level_and(clause, len, index + 1, depth, single_quote?, double_quote?)
    end
  end

  defp top_level_and_at?(clause, index, len) do
    with true <- index > 0,
         true <- index + 3 < len,
         true <- whitespace?(:binary.at(clause, index - 1)),
         true <- whitespace?(:binary.at(clause, index + 3)),
         true <- and_token_at?(clause, index) do
      true
    else
      _ -> false
    end
  end

  defp and_token_at?(clause, index) do
    ascii_up(:binary.at(clause, index)) == ?A and
      ascii_up(:binary.at(clause, index + 1)) == ?N and
      ascii_up(:binary.at(clause, index + 2)) == ?D
  end

  defp ascii_up(char) when char in ?a..?z, do: char - 32
  defp ascii_up(char), do: char

  defp whitespace?(char), do: char in [9, 10, 11, 12, 13, 32]

  defp skip_whitespace(_clause, index, len) when index >= len, do: len

  defp skip_whitespace(clause, index, len) do
    if whitespace?(:binary.at(clause, index)) do
      skip_whitespace(clause, index + 1, len)
    else
      index
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
    Enum.map(resolutions, fn %JoinResolution{joins: joins} = resolution ->
      %{resolution | joins: dedupe_join_sequence(joins)}
    end)
  end

  defp dedupe_join_sequence(joins) do
    joins
    |> Enum.reduce({MapSet.new(), []}, fn join, {seen, acc} ->
      signature = join_signature(join)

      if MapSet.member?(seen, signature) do
        {seen, acc}
      else
        {MapSet.put(seen, signature), acc ++ [join]}
      end
    end)
    |> elem(1)
  end

  defp join_signature(join) do
    {
      Map.get(join, :from),
      Map.get(join, :to),
      Map.get(join, :type),
      Map.get(join, :on),
      Map.get(join, :field)
    }
  end
end
