defmodule Selecto.Pivot do
  @moduledoc """
  Deprecated compatibility wrapper for `Selecto.Retarget`.
  """

  @deprecated "Use Selecto.Retarget.retarget/3 instead."
  defdelegate pivot(selecto, target_schema, opts \\ []), to: Selecto.Retarget, as: :retarget

  @deprecated "Use Selecto.Retarget.validate_retarget_path/2 instead."
  defdelegate validate_pivot_path(selecto, join_path),
    to: Selecto.Retarget,
    as: :validate_retarget_path

  @deprecated "Use Selecto.Retarget.has_retarget?/1 instead."
  defdelegate has_pivot?(selecto), to: Selecto.Retarget, as: :has_retarget?

  @deprecated "Use Selecto.Retarget.get_retarget_config/1 instead."
  defdelegate get_pivot_config(selecto), to: Selecto.Retarget, as: :get_retarget_config

  @deprecated "Use Selecto.Retarget.reset_retarget/1 instead."
  defdelegate reset_pivot(selecto), to: Selecto.Retarget, as: :reset_retarget
end
