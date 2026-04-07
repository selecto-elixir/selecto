defmodule Selecto.UDF do
  @moduledoc false

  @allowed_kinds [:scalar, :predicate, :table]
  @allowed_call_sites [:select, :filter, :order_by, :group_by, :lateral, :query_member]
  @allowed_arg_sources [:selector, :value, :literal]

  @qualified_name_regex ~r/^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$/

  def allowed_kinds, do: @allowed_kinds
  def allowed_call_sites, do: @allowed_call_sites
  def allowed_arg_sources, do: @allowed_arg_sources

  def normalize_id(function_id) when is_atom(function_id), do: Atom.to_string(function_id)
  def normalize_id(function_id) when is_binary(function_id), do: function_id

  def functions(%Selecto{} = selecto) do
    Map.get(selecto.config, :functions) || Map.get(selecto.domain, :functions, %{})
  end

  def functions(%{} = domain_or_config) do
    Map.get(domain_or_config, :functions, %{})
  end

  def functions(_other), do: %{}

  def fetch(container, function_id) do
    functions = functions(container)
    normalized_id = normalize_id(function_id)

    cond do
      Map.has_key?(functions, normalized_id) ->
        {:ok, Map.fetch!(functions, normalized_id)}

      safe_existing_atom(normalized_id) != nil and
          Map.has_key?(functions, safe_existing_atom(normalized_id)) ->
        {:ok, Map.fetch!(functions, safe_existing_atom(normalized_id))}

      true ->
        :error
    end
  end

  def fetch!(container, function_id) do
    case fetch(container, function_id) do
      {:ok, spec} ->
        spec

      :error ->
        available =
          container
          |> functions()
          |> Map.keys()
          |> Enum.map(&to_string/1)
          |> Enum.sort()
          |> Enum.join(", ")

        raise ArgumentError,
              "Unknown UDF '#{normalize_id(function_id)}'. Available functions: #{available}"
    end
  end

  def selector_call_site?(:select), do: true
  def selector_call_site?(:order_by), do: true
  def selector_call_site?(:group_by), do: true
  def selector_call_site?(_other), do: false

  def valid_sql_name?(sql_name) when is_binary(sql_name) do
    Regex.match?(@qualified_name_regex, sql_name)
  end

  def valid_sql_name?(_sql_name), do: false

  def valid_kind?(kind), do: kind in @allowed_kinds
  def valid_call_site?(call_site), do: call_site in @allowed_call_sites
  def valid_arg_source?(source), do: source in @allowed_arg_sources

  defp safe_existing_atom(value) when is_binary(value) do
    try do
      String.to_existing_atom(value)
    rescue
      ArgumentError -> nil
    end
  end

  defp safe_existing_atom(_value), do: nil
end
