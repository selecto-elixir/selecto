defmodule Selecto.Helpers do
  @moduledoc """
  Shared utility helpers used across Selecto internals.

  This module currently focuses on safe phrase validation for values that may be
  interpolated into generated SQL metadata (for example aliases or user-facing
  configuration fragments).
  """

  def check_safe_phrase(nil), do: nil
  def check_safe_phrase(str) when is_integer(str), do: check_safe_phrase(to_string(str))
  def check_safe_phrase(str) when is_float(str), do: check_safe_phrase(to_string(str))
  def check_safe_phrase(str) when is_atom(str), do: check_safe_phrase(Atom.to_string(str))

  def check_safe_phrase(string) when is_binary(string) do
    if String.length(string) < 1 or String.match?(string, ~r/[^a-zA-Z0-9_ ]/) do
      raise RuntimeError, message: "Invalid String #{string}"
      false
    else
      string
    end
  end

  def check_safe_phrase(other) do
    case other do
      %Selecto{} -> raise "Cannot use Selecto struct as string"
      _ -> check_safe_phrase(to_string(other))
    end
  end
end
