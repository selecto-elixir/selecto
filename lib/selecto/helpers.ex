defmodule Selecto.Helpers do

  def check_string( string ) do
    if string |> String.match?(~r/^[^a-zA-Z0-9_]+$/) do
      raise "Invalid String #{string}"
    end
    string
  end


end
