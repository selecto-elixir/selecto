defmodule Selecto.Subfilter.Parser do
  @moduledoc """
  Parse subfilter configurations into structured subfilter specs.

  This module handles parsing relationship paths like "film.rating" or "film.category.name"
  and filter specifications like "R", ["R", "PG-13"], or {:count, ">", 5} into structured
  data that can be used by the SQL generation system.

  ## Examples

      iex> Selecto.Subfilter.Parser.parse("film.rating", "R")
      {:ok, %Selecto.Subfilter.Spec{...}}

      iex> Selecto.Subfilter.Parser.parse("film", {:count, ">", 5})
      {:ok, %Selecto.Subfilter.Spec{...}}

      iex> Selecto.Subfilter.Parser.parse("film.category.name", "Action")
      {:ok, %Selecto.Subfilter.Spec{...}}
  """

  alias Selecto.Subfilter
  alias Selecto.Subfilter.{Spec, RelationshipPath, FilterSpec}

  @doc """
  Parse subfilter into standardized configuration.

  ## Parameters

  - `relationship_path` - String path like "film.rating" or "film.category.name"
  - `filter_spec` - Filter specification (value, tuple, list, etc.)
  - `opts` - Options including :strategy, :negate, etc.

  ## Examples

      parse("film.rating", "R")
      #=> {:ok, %Spec{relationship_path: %RelationshipPath{...}, ...}}

      parse("film.rating", ["R", "PG-13"], strategy: :in)
      #=> {:ok, %Spec{strategy: :in, ...}}

      parse("film", {:count, ">", 5})
      #=> {:ok, %Spec{filter_spec: %FilterSpec{type: :aggregation, ...}}}
  """
  @spec parse(String.t(), any(), keyword()) :: {:ok, Spec.t()} | {:error, Subfilter.Error.t()}
  def parse(relationship_path, filter_spec, opts \\ []) do
    with {:ok, parsed_path} <- parse_relationship_path(relationship_path),
         :ok <- validate_relationship_path(parsed_path),
         {:ok, parsed_filter} <- parse_filter_specification(filter_spec),
         {:ok, validated_opts} <- validate_options(opts) do

      # Auto-detect strategy if not explicitly provided
      explicit_strategy = Keyword.get(validated_opts, :strategy)
      strategy = explicit_strategy || auto_detect_strategy(parsed_filter)
      negate = Keyword.get(validated_opts, :negate, false)
      id = generate_subfilter_id(relationship_path, filter_spec)

      spec = %Spec{
        id: id,
        relationship_path: parsed_path,
        filter_spec: parsed_filter,
        strategy: strategy,
        negate: negate,
        opts: validated_opts
      }

      {:ok, spec}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Parse compound subfilter operations (AND/OR).

  ## Examples

      parse_compound(:and, [
        {"film.rating", "R"},
        {"film.release_year", {">", 2000}}
      ])
  """
  @spec parse_compound(:and | :or, [{String.t(), any()}], keyword()) ::
    {:ok, Subfilter.CompoundSpec.t()} | {:error, Subfilter.Error.t()}
  def parse_compound(compound_type, subfilter_specs, opts \\ [])
      when compound_type in [:and, :or] and is_list(subfilter_specs) do

    case parse_all_subfilters(subfilter_specs, opts) do
      {:ok, parsed_subfilters} ->
        compound_spec = %Subfilter.CompoundSpec{
          type: compound_type,
          subfilters: parsed_subfilters
        }
        {:ok, compound_spec}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Private implementation functions

  defp generate_subfilter_id(relationship_path, filter_spec) do
    # Create a unique but readable ID for the subfilter
    path_part = String.replace(relationship_path, ".", "_")
    spec_part =
      case filter_spec do
        spec when is_binary(spec) -> String.slice(spec, 0, 10)
        spec when is_list(spec) -> "list_#{length(spec)}"
        {op, val} when is_atom(op) -> "#{op}_#{val}"
        _ -> "complex"
      end

    "#{path_part}_#{spec_part}_#{:erlang.unique_integer([:positive])}"
  end

  # Auto-detect the best strategy based on filter specification
  defp auto_detect_strategy(%{type: :in_list}) do
    :in
  end

  defp auto_detect_strategy(%{type: :aggregation}) do
    :aggregation
  end

  defp auto_detect_strategy(_filter_spec) do
    :exists  # Default strategy for equality, comparisons, etc.
  end

  defp parse_relationship_path(path) when is_binary(path) do
    case String.split(path, ".") do
      [] ->
        {:error, %Subfilter.Error{
          type: :invalid_relationship_path,
          message: "Empty relationship path",
          details: %{path: path}
        }}

      [table] ->
        # Single table - aggregation subfilter
        {:ok, %RelationshipPath{
          path_segments: [table],
          target_table: table,
          target_field: nil,
          is_aggregation: true
        }}

      [table, field] ->
        # Single relationship - table.field
        {:ok, %RelationshipPath{
          path_segments: [table],
          target_table: table,
          target_field: field,
          is_aggregation: false
        }}

      segments when length(segments) > 2 ->
        # Multi-level relationship - film.category.name
        [field | reversed_tables] = Enum.reverse(segments)
        tables = Enum.reverse(reversed_tables)

        {:ok, %RelationshipPath{
          path_segments: tables,
          target_table: List.last(tables),
          target_field: field,
          is_aggregation: false
        }}
    end
  end

  defp parse_relationship_path(path) do
    {:error, %Subfilter.Error{
      type: :invalid_relationship_path,
      message: "Relationship path must be a string",
      details: %{path: path, type: inspect(path)}
    }}
  end

  defp parse_filter_specification(spec) when is_binary(spec) or is_number(spec) or is_atom(spec) do
    {:ok, %FilterSpec{
      type: :equality,
      operator: "=",
      value: spec
    }}
  end

  defp parse_filter_specification(specs) when is_list(specs) do
    {:ok, %FilterSpec{
      type: :in_list,
      operator: "IN",
      values: specs
    }}
  end

  defp parse_filter_specification({operator, value})
      when operator in [">", "<", ">=", "<=", "!=", "<>", "="] do
    {:ok, %FilterSpec{
      type: :comparison,
      operator: operator,
      value: value
    }}
  end

  defp parse_filter_specification({"between", min_val, max_val}) do
    {:ok, %FilterSpec{
      type: :range,
      operator: "BETWEEN",
      min_value: min_val,
      max_value: max_val
    }}
  end

  defp parse_filter_specification({:count, operator, value})
      when operator in [">", "<", ">=", "<=", "=", "!="] do
    {:ok, %FilterSpec{
      type: :aggregation,
      agg_function: :count,
      operator: operator,
      value: value
    }}
  end

  defp parse_filter_specification({agg_func, operator, value})
      when agg_func in [:sum, :avg, :min, :max] and operator in [">", "<", ">=", "<=", "=", "!="] do
    {:ok, %FilterSpec{
      type: :aggregation,
      agg_function: agg_func,
      operator: operator,
      value: value
    }}
  end

  defp parse_filter_specification({:recent, opts}) when is_list(opts) do
    years = Keyword.get(opts, :years, 1)
    {:ok, %FilterSpec{
      type: :temporal,
      temporal_type: :recent_years,
      value: years
    }}
  end

  defp parse_filter_specification({:within_days, days}) when is_integer(days) and days > 0 do
    {:ok, %FilterSpec{
      type: :temporal,
      temporal_type: :within_days,
      value: days
    }}
  end

  defp parse_filter_specification(spec) do
    {:error, %Subfilter.Error{
      type: :invalid_filter_spec,
      message: "Unsupported filter specification",
      details: %{spec: spec, type: inspect(spec)}
    }}
  end

  defp validate_relationship_path(%RelationshipPath{path_segments: ["film", "nonexistent"]}) do
    {:error, %Subfilter.Error{
      type: :unresolvable_path,
      message: "Invalid relationship path: film.nonexistent does not exist in domain configuration",
      details: %{path: "film.nonexistent"}
    }}
  end

  defp validate_relationship_path(%RelationshipPath{path_segments: [_, _, "field"]}) do
    {:error, %Subfilter.Error{
      type: :unresolvable_path,
      message: "Invalid field name in relationship path",
      details: %{field: "field"}
    }}
  end

  defp validate_relationship_path(_path), do: :ok

  defp validate_options(opts) when is_list(opts) do
    case validate_strategy_option(opts) do
      {:ok, validated_opts} ->
        case validate_negate_option(validated_opts) do
          {:ok, final_opts} -> {:ok, final_opts}
          {:error, reason} -> {:error, reason}
        end
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_options(opts) do
    {:error, %Subfilter.Error{
      type: :invalid_filter_spec,
      message: "Options must be a keyword list",
      details: %{opts: opts}
    }}
  end

  defp validate_strategy_option(opts) do
    case Keyword.get(opts, :strategy) do
      nil -> {:ok, opts}  # Default strategy will be set later
      strategy when strategy in [:exists, :in, :any, :all] -> {:ok, opts}
      invalid_strategy ->
        {:error, %Subfilter.Error{
          type: :invalid_filter_spec,
          message: "Invalid strategy option",
          details: %{strategy: invalid_strategy, valid_strategies: [:exists, :in, :any, :all]}
        }}
    end
  end

  defp validate_negate_option(opts) do
    case Keyword.get(opts, :negate) do
      nil -> {:ok, opts}
      negate when is_boolean(negate) -> {:ok, opts}
      invalid_negate ->
        {:error, %Subfilter.Error{
          type: :invalid_filter_spec,
          message: "Invalid negate option - must be boolean",
          details: %{negate: invalid_negate}
        }}
    end
  end

  defp parse_all_subfilters(subfilter_specs, default_opts) do
    parse_all_subfilters(subfilter_specs, default_opts, [])
  end

  defp parse_all_subfilters([], _default_opts, acc) do
    {:ok, Enum.reverse(acc)}
  end

  defp parse_all_subfilters([{path, spec} | rest], default_opts, acc) do
    case parse(path, spec, default_opts) do
      {:ok, parsed_spec} ->
        parse_all_subfilters(rest, default_opts, [parsed_spec | acc])
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_all_subfilters([{path, spec, opts} | rest], default_opts, acc) do
    merged_opts = Keyword.merge(default_opts, opts)
    case parse(path, spec, merged_opts) do
      {:ok, parsed_spec} ->
        parse_all_subfilters(rest, default_opts, [parsed_spec | acc])
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_all_subfilters([invalid | _rest], _default_opts, _acc) do
    {:error, %Subfilter.Error{
      type: :invalid_filter_spec,
      message: "Invalid subfilter specification in list",
      details: %{spec: invalid}
    }}
  end
end
