defmodule Selecto.Helpers do

  def check_safe_phrase(string) do
    if String.length(string) < 1 or String.match?(string, ~r/[^a-zA-Z0-9_ ]/) do
      raise RuntimeError, message: "Invalid String #{string}"
      false
    else
      string
    end
  end


end
