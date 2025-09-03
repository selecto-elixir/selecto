defmodule Selecto.Builder.Sql.Helpers do


  ### SQL safety helpers - prevent injection via string validation
  
  @doc """
  Get the appropriate quote character for identifiers based on the database adapter.
  PostgreSQL uses double quotes, MySQL uses backticks, SQLite uses double quotes.
  """
  def get_quote_char(selecto) do
    case Map.get(selecto, :adapter, Selecto.DB.PostgreSQL) do
      Selecto.DB.MySQL -> "`"
      Selecto.DB.MariaDB -> "`"
      _ -> "\""
    end
  end

  def check_string(nil), do: nil
  
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

  def double_wrap(nil), do: ""
  
  def double_wrap(str) when is_atom(str) do
    Atom.to_string(str) |> double_wrap()
  end

  def double_wrap(str) do
    if String.match?(str, ~r/[^a-zA-Z0-9_ :&-]/) do
      raise RuntimeError, message: "Invalid Table/Column/Alias Name #{str}"
    end

    ~s["#{str}"]
  end
  
  @doc """
  Wrap an identifier with the appropriate quotes for the database adapter.
  This is the adapter-aware version of double_wrap.
  """
  def quote_identifier(_selecto, nil), do: ""
  
  def quote_identifier(selecto, str) when is_atom(str) do
    Atom.to_string(str) |> quote_identifier(selecto)
  end
  
  def quote_identifier(selecto, str) do
    if String.match?(str, ~r/[^a-zA-Z0-9_ :&-]/) do
      raise RuntimeError, message: "Invalid Table/Column/Alias Name #{str}"
    end
    
    quote = get_quote_char(selecto)
    "#{quote}#{str}#{quote}"
  end

  def build_selector_string(selecto, join, field) do
    join_str = if is_atom(join), do: Atom.to_string(join), else: join
    # Handle nil values
    case {join_str, field} do
      {nil, _} -> quote_identifier(selecto, field)
      {_, nil} -> quote_identifier(selecto, join_str)
      _ -> "#{quote_identifier(selecto, join_str)}.#{quote_identifier(selecto, field)}"
    end
  end

  def build_join_string(selecto, join) do
    quote_identifier(selecto, join)
  end

  @doc """
  Build selector string for parameterized joins with signature support.
  """
  def build_parameterized_selector_string(selecto, join, field, parameter_signature \\ nil) do
    case parameter_signature do
      nil -> "#{quote_identifier(selecto, join)}.#{quote_identifier(selecto, field)}"
      "" -> "#{quote_identifier(selecto, join)}.#{quote_identifier(selecto, field)}"
      sig -> "#{quote_identifier(selecto, "#{join}_#{sig}")}.#{quote_identifier(selecto, field)}"
    end
  end

  @doc """
  Build join alias string for parameterized joins.
  """
  def build_parameterized_join_string(selecto, join, parameter_signature \\ nil) do
    case parameter_signature do
      nil -> quote_identifier(selecto, join)
      "" -> quote_identifier(selecto, join)
      sig -> quote_identifier(selecto, "#{join}_#{sig}")
    end
  end

end
