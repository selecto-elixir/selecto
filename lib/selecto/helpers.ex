defmodule Selecto.Helpers do

  def check_string( string ) do
    if string |> String.match?(~r/^[^a-zA-Z0-9_]+$/) do
      raise "Invalid String #{string}"
    end
    string
  end


  def single_wrap(val) do
    #TODO! replace ' in val
    ~s"'#{val}'"
  end

  def double_wrap(str) do
    ## TODO! do not allow non- \w_ here
    ~s["#{str}"]
  end


  def prep_literal() do
    #TODO
  end

  def prep_selector() do
    #TODO
  end

  @spec prep_predicate :: nil
  def prep_predicate() do
    #TODO
  end

end
