defmodule Selecto.Subfilter.SQL.Helpers do
  @moduledoc false

  alias Selecto.Subfilter.JoinPathResolver.JoinResolution

  @spec table_name(atom() | String.t() | nil) :: String.t() | nil
  def table_name(nil), do: nil
  def table_name(name) when is_atom(name), do: Atom.to_string(name)
  def table_name(name) when is_binary(name), do: name
  def table_name(other), do: to_string(other)

  @spec subquery_base_table(JoinResolution.t()) :: String.t() | nil
  def subquery_base_table(%JoinResolution{joins: [%{from: from} | _]}) do
    table_name(from)
  end

  def subquery_base_table(_), do: nil

  @spec outer_base_table(map(), JoinResolution.t()) :: String.t() | nil
  def outer_base_table(registry, join_resolution) do
    case Map.get(registry || %{}, :base_table) do
      nil -> subquery_base_table(join_resolution)
      base_table -> table_name(base_table)
    end
  end

  @spec infer_correlation_key(JoinResolution.t()) :: String.t()
  def infer_correlation_key(%JoinResolution{joins: joins}) when is_list(joins) do
    joins
    |> Enum.find(fn join ->
      Map.get(join, :type) != :self and is_binary(Map.get(join, :on))
    end)
    |> case do
      %{from: from, to: to, on: on_clause} ->
        extract_join_key(on_clause, table_name(from), table_name(to)) ||
          default_correlation_key(table_name(from))

      _ ->
        case joins do
          [%{from: from} | _] -> default_correlation_key(table_name(from))
          _ -> "id"
        end
    end
  end

  def infer_correlation_key(_), do: "id"

  @spec build_correlation_condition(JoinResolution.t(), map()) :: String.t()
  def build_correlation_condition(%JoinResolution{} = join_resolution, registry) do
    subquery_table = subquery_base_table(join_resolution)
    outer_table = outer_base_table(registry, join_resolution) || subquery_table
    correlation_key = infer_correlation_key(join_resolution)

    "#{subquery_table}.#{correlation_key} = #{outer_table}.#{correlation_key}"
  end

  @spec build_outer_field(JoinResolution.t(), map(), String.t()) :: String.t()
  def build_outer_field(%JoinResolution{} = join_resolution, registry, field_name)
      when is_binary(field_name) do
    outer_table = outer_base_table(registry, join_resolution) || subquery_base_table(join_resolution)
    "#{outer_table}.#{field_name}"
  end

  defp extract_join_key(on_clause, from_table, to_table) do
    on_clause
    |> String.split(~r/\s+AND\s+/i)
    |> Enum.find_value(fn condition ->
      case Regex.run(
             ~r/^\s*([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)\s*=\s*([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)\s*$/,
             String.trim(condition),
             capture: :all_but_first
           ) do
        [left_table, left_col, right_table, _right_col] when left_table == from_table and right_table == to_table ->
          left_col

        [_left_table, _left_col, right_table, right_col] when right_table == from_table ->
          right_col

        _ ->
          nil
      end
    end)
  end

  defp default_correlation_key(nil), do: "id"

  defp default_correlation_key(table_name) do
    singular =
      cond do
        String.ends_with?(table_name, "ies") ->
          String.trim_trailing(table_name, "ies") <> "y"

        String.ends_with?(table_name, "s") ->
          String.trim_trailing(table_name, "s")

        true ->
          table_name
      end

    "#{singular}_id"
  end
end
