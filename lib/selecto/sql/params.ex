defmodule Selecto.SQL.Params do
  @moduledoc false
  # Utilities for turning an iodata tree with {:param, value} markers into
  # a final SQL string with $n placeholders and the ordered params list.

  @type fragment :: iodata() | {:param, any()}

  @spec finalize(iodata() | [fragment]) :: {String.t(), [any()]}
  def finalize(fragments) do
    {iodata, params, _idx} =
      traverse(List.wrap(fragments), {[], [], 0})

    {IO.iodata_to_binary(iodata), params}
  end

  defp traverse([h | t], {acc_io, acc_params, idx}) do
    case h do
      {:param, v} ->
        placeholder = [?$ | Integer.to_string(idx + 1)]
        traverse(t, {acc_io ++ [placeholder], acc_params ++ [v], idx + 1})
      list when is_list(list) ->
        {inner_io, inner_params, inner_idx} = traverse(list, {[], [], idx})
        traverse(t, {acc_io ++ [inner_io], acc_params ++ inner_params, inner_idx})
      bin when is_binary(bin) ->
        traverse(t, {acc_io ++ [bin], acc_params, idx})
      other ->
        # Allow numbers/atoms to be coerced; they should rarely appear directly.
        traverse(t, {acc_io ++ [to_string(other)], acc_params, idx})
    end
  end

  defp traverse([], state), do: state
end
