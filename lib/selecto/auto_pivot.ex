defmodule Selecto.AutoPivot do
  @moduledoc """
  Deprecated compatibility wrapper for `Selecto.AutoRetarget`.
  """

  @deprecated "Use Selecto.AutoRetarget.maybe_apply/2 instead."
  defdelegate maybe_apply(selecto, opts \\ []), to: Selecto.AutoRetarget

  @deprecated "Use Selecto.AutoRetarget.should_retarget?/2 instead."
  defdelegate should_pivot?(selecto, selected_columns),
    to: Selecto.AutoRetarget,
    as: :should_retarget?

  @deprecated "Use Selecto.AutoRetarget.find_retarget_target/2 instead."
  defdelegate find_pivot_target(selecto, selected_columns),
    to: Selecto.AutoRetarget,
    as: :find_retarget_target
end
