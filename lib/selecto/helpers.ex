defmodule Selecto.Helpers do
alias Inspect.Regex


  ### TODO make sure these prevent sql injection, add some tests, allow valid strings though...


  def check_string( string ) do
    if string |> String.match?(~r/^[^a-zA-Z0-9_]+$/) do
      raise "Invalid String #{string}"
    end
    Regex.replace(~r/'/, string, "''")
  end


  def single_wrap(val) do
    #TODO! replace ' in val
    val = Regex.replace(~r/'/, val, "''")
    ~s"'#{val}'"
  end

  def double_wrap(str) when is_atom(str) do
    Atom.to_string(str) |> double_wrap()
  end

  def double_wrap(str) do
    ## TODO! do not allow non- \w_ here this is for field names etc
    if String.match?( str, ~r/[^a-zA-Z0-9_ &]/ ) do
      raise RuntimeError, message: "Invalid Table/Column/Alias Name"
    end
    ~s["#{str}"]
  end


  def prep_literal(literal) do
    #TODO
  end


  def prep_predicate(selecto, predicate) do
    #TODO
  end

end
