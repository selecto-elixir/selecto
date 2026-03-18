defmodule Selecto.SQL.Params do
  @moduledoc false
  # Utilities for turning an iodata tree with {:param, value} markers into
  # a final SQL string with $n placeholders and the ordered params list.
  #
  # Extended in Phase 1 to support CTE markers for advanced join patterns.

  # import Selecto.Types - removed to avoid circular dependency

  @type fragment :: iodata() | {:param, any()} | {:cte, String.t(), iodata()}

  @spec finalize(iodata() | [fragment], Keyword.t()) :: {String.t(), [any()]}
  def finalize(fragments, opts \\ []) do
    adapter = Keyword.get(opts, :adapter, Selecto.AdapterSupport.default_adapter())
    placeholder_fun = placeholder_fun(adapter)

    {iodata, params, _idx} =
      traverse(List.wrap(fragments), {[], [], 0}, placeholder_fun)

    {IO.iodata_to_binary(:lists.reverse(iodata)), :lists.reverse(params)}
  end

  @doc """
  Handle CTE markers in iodata structures for advanced join patterns.

  CTE markers are processed before main query finalization to properly
  coordinate parameter numbering between CTEs and main query.

  Returns: {processed_ctes, main_sql, final_params}
  """
  @spec finalize_with_ctes(iodata() | [fragment], Keyword.t()) ::
          {[{String.t(), String.t()}], String.t(), [any()]}
  def finalize_with_ctes(iodata_with_ctes, opts \\ []) do
    adapter = Keyword.get(opts, :adapter, Selecto.AdapterSupport.default_adapter())
    placeholder_fun = placeholder_fun(adapter)
    {cte_sections, main_iodata, extracted_params} = extract_ctes(List.wrap(iodata_with_ctes))

    # Process CTEs first to establish parameter numbering baseline
    {processed_ctes, cte_params} =
      Enum.map_reduce(cte_sections, [], fn {cte_name, cte_iodata}, acc_params ->
        {cte_sql, cte_specific_params} = finalize(cte_iodata, adapter: adapter)

        {{cte_name, cte_sql}, acc_params ++ cte_specific_params}
      end)

    # Process main query with parameter offset to avoid conflicts
    param_offset = length(cte_params)

    {main_sql, main_specific_params} =
      finalize_with_offset(main_iodata, param_offset, placeholder_fun)

    # Combine all parameters in correct order
    final_params = cte_params ++ main_specific_params ++ extracted_params
    {processed_ctes, main_sql, final_params}
  end

  # Process iodata with parameter offset for coordinated numbering
  defp finalize_with_offset(fragments, offset, placeholder_fun) do
    {iodata, params, _idx} =
      traverse_with_offset(List.wrap(fragments), {[], [], offset}, placeholder_fun)

    {IO.iodata_to_binary(:lists.reverse(iodata)), :lists.reverse(params)}
  end

  # Extract CTE markers from iodata structure
  defp extract_ctes(iodata) do
    extract_ctes_recursive(iodata, {[], [], []})
  end

  defp extract_ctes_recursive([], {ctes, main_iodata, params}) do
    {Enum.reverse(ctes), Enum.reverse(main_iodata), Enum.reverse(params)}
  end

  defp extract_ctes_recursive([{:cte, name, cte_iodata} | rest], {ctes, main_iodata, params}) do
    extract_ctes_recursive(rest, {[{name, cte_iodata} | ctes], main_iodata, params})
  end

  defp extract_ctes_recursive([{:param, value} | rest], {ctes, main_iodata, params}) do
    extract_ctes_recursive(rest, {ctes, main_iodata, [value | params]})
  end

  defp extract_ctes_recursive([head | rest], {ctes, main_iodata, params}) when is_list(head) do
    {inner_ctes, inner_main, inner_params} = extract_ctes_recursive(head, {[], [], []})
    combined_ctes = inner_ctes ++ ctes
    combined_main = [inner_main | main_iodata]
    combined_params = Enum.reverse(inner_params) ++ params
    extract_ctes_recursive(rest, {combined_ctes, combined_main, combined_params})
  end

  defp extract_ctes_recursive([head | rest], {ctes, main_iodata, params}) do
    extract_ctes_recursive(rest, {ctes, [head | main_iodata], params})
  end

  # Traverse with parameter offset support
  defp traverse_with_offset([h | t], {acc_io, acc_params, idx}, placeholder_fun) do
    case h do
      {:param, v} ->
        placeholder = placeholder_fun.(idx + 1)

        traverse_with_offset(
          t,
          {[placeholder | acc_io], [v | acc_params], idx + 1},
          placeholder_fun
        )

      list when is_list(list) ->
        {inner_io, inner_params, inner_idx} =
          traverse_with_offset(list, {[], [], idx}, placeholder_fun)

        traverse_with_offset(
          t,
          {[Enum.reverse(inner_io) | acc_io], inner_params ++ acc_params, inner_idx},
          placeholder_fun
        )

      bin when is_binary(bin) ->
        traverse_with_offset(t, {[bin | acc_io], acc_params, idx}, placeholder_fun)

      other ->
        traverse_with_offset(t, {[to_string(other) | acc_io], acc_params, idx}, placeholder_fun)
    end
  end

  defp traverse_with_offset([], state, _placeholder_fun), do: state

  defp traverse([h | t], {acc_io, acc_params, idx}, placeholder_fun) do
    case h do
      {:param, v} ->
        placeholder = placeholder_fun.(idx + 1)
        traverse(t, {[placeholder | acc_io], [v | acc_params], idx + 1}, placeholder_fun)

      list when is_list(list) ->
        {inner_io, inner_params, inner_idx} = traverse(list, {[], [], idx}, placeholder_fun)

        traverse(
          t,
          {[Enum.reverse(inner_io) | acc_io], inner_params ++ acc_params, inner_idx},
          placeholder_fun
        )

      bin when is_binary(bin) ->
        traverse(t, {[bin | acc_io], acc_params, idx}, placeholder_fun)

      other ->
        # Allow numbers/atoms to be coerced; they should rarely appear directly.
        traverse(t, {[to_string(other) | acc_io], acc_params, idx}, placeholder_fun)
    end
  end

  defp traverse([], state, _placeholder_fun), do: state

  defp placeholder_fun(adapter) when is_atom(adapter) do
    if Selecto.AdapterSupport.callback_available?(adapter, :placeholder, 1) do
      &adapter.placeholder/1
    else
      &["$", Integer.to_string(&1)]
    end
  end

  defp placeholder_fun(_adapter), do: &["$", Integer.to_string(&1)]
end
