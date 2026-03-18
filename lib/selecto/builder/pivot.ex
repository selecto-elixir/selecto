defmodule Selecto.Builder.Pivot do
  @moduledoc """
  Deprecated compatibility wrapper for `Selecto.Builder.Retarget`.
  """

  @deprecated "Use Selecto.Builder.Retarget.build_retarget_query/2 instead."
  defdelegate build_pivot_query(selecto, opts \\ []),
    to: Selecto.Builder.Retarget,
    as: :build_retarget_query

  @deprecated "Use Selecto.Builder.Retarget.extract_retarget_conditions/3 instead."
  defdelegate extract_pivot_conditions(selecto, retarget_config, source_alias),
    to: Selecto.Builder.Retarget,
    as: :extract_retarget_conditions

  defdelegate build_join_chain_subquery(selecto, retarget_config, join_path),
    to: Selecto.Builder.Retarget
end
