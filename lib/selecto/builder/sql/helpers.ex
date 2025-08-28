defmodule Selecto.Builder.Sql.Helpers do


  ### SQL safety helpers - prevent injection via string validation

  def check_string(string) do
    if string |> String.match?(~r/[^a-zA-Z0-9_]/) do
      raise RuntimeError, message: "Invalid String #{string}"
    end

    string
  end

  def single_wrap(val) do
    val = String.replace(val, ~r/'/, "''")
    ~s"'#{val}'"
  end

  def double_wrap(str) when is_atom(str) do
    Atom.to_string(str) |> double_wrap()
  end

  def double_wrap(str) do
    if String.match?(str, ~r/[^a-zA-Z0-9_ :&-]/) do
      raise RuntimeError, message: "Invalid Table/Column/Alias Name #{str}"
    end

    ~s["#{str}"]
  end

  def build_selector_string(_selecto, join, field) do
    join_str = if is_atom(join), do: Atom.to_string(join), else: join
    "#{double_wrap(join_str)}.#{double_wrap(field)}"
  end

  def build_join_string(_selecto, join) do
    double_wrap(join)
  end

  @doc """
  Build selector string for parameterized joins with signature support.
  """
  def build_parameterized_selector_string(_selecto, join, field, parameter_signature \\ nil) do
    case parameter_signature do
      nil -> "#{double_wrap(join)}.#{double_wrap(field)}"
      "" -> "#{double_wrap(join)}.#{double_wrap(field)}"
      sig -> "#{double_wrap("#{join}_#{sig}")}.#{double_wrap(field)}"
    end
  end

  @doc """
  Build join alias string for parameterized joins.
  """
  def build_parameterized_join_string(_selecto, join, parameter_signature \\ nil) do
    case parameter_signature do
      nil -> double_wrap(join)
      "" -> double_wrap(join)
      sig -> double_wrap("#{join}_#{sig}")
    end
  end

end
