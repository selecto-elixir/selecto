defmodule Selecto.Helpers do

  ### TODO make sure these prevent sql injection, add some tests, allow valid strings though...
  ### what am i missing here?! ugh

  def check_string(string) do
    if string |> String.match?(~r/[^a-zA-Z0-9_]/) do
      raise RuntimeError, message: "Invalid String #{string}"
    end

    # String.replace(string, ~r/'/, "''")
    string
  end


  def check_safe_phrase(string) do
      if String.length(string) < 1 or String.match?(string, ~r/[^a-zA-Z0-9_ ]/) do
        raise RuntimeError, message: "Invalid String #{string}"
        false
      else
        string
      end
  end

  def single_wrap(val) do
    # TODO! replace ' in val
    val = String.replace(val, ~r/'/, "''")
    ~s"'#{val}'"
  end

  def double_wrap(str) when is_atom(str) do
    Atom.to_string(str) |> double_wrap()
  end

  def double_wrap(str) do
    ## TODO! do not allow non- \w_ here this is for field names etc
    if String.match?(str, ~r/[^a-zA-Z0-9_ :&-]/) do
      raise RuntimeError, message: "Invalid Table/Column/Alias Name #{str}"
    end

    ~s["#{str}"]
  end

  def build_selector_string(selecto, join, field) do

    " #{double_wrap(join)}.#{double_wrap(field)} "
  end

end
