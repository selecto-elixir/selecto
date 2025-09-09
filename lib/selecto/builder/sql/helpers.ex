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
  
  @doc """
  Check if an identifier needs quoting.
  Only quote if it's a reserved word, contains special characters, or has mixed case.
  """
  def needs_quoting?(str) when is_binary(str) do
    # PostgreSQL reserved words that commonly appear as column names
    reserved_words = ~w(
      user order group select from where having limit offset join left right
      inner outer cross union all distinct as on using natural full exists
      case when then else end null is not and or in between like ilike
      primary key foreign references table column index create alter drop
      insert update delete values set into default unique check constraint
      view trigger function procedure return declare begin commit rollback
      transaction isolation level read write only deferrable serializable
      repeatable committed uncommitted work savepoint release cursor fetch
      close cast row array text integer boolean date time timestamp interval
      numeric decimal real double precision varchar char bit varying zone
    )
    
    # Check if it needs quoting
    cond do
      # Reserved words need quoting
      String.downcase(str) in reserved_words -> true
      # Mixed case identifiers need quoting to preserve case
      str != String.downcase(str) -> true
      # Identifiers starting with numbers need quoting
      String.match?(str, ~r/^[0-9]/) -> true
      # Identifiers with special characters (except underscore) need quoting
      String.match?(str, ~r/[^a-z0-9_]/) -> true
      # Simple lowercase identifiers with underscores don't need quoting
      true -> false
    end
  end
  
  def needs_quoting?(_), do: false
  
  @doc """
  Maybe quote an identifier - only adds quotes if necessary.
  """
  def maybe_quote_identifier(str) when is_binary(str) do
    if needs_quoting?(str) do
      ~s["#{str}"]
    else
      str
    end
  end
  
  def maybe_quote_identifier(str) when is_atom(str) do
    maybe_quote_identifier(Atom.to_string(str))
  end
  
  def maybe_quote_identifier(other), do: to_string(other)

  def check_string(nil), do: nil
  def check_string(str) when is_integer(str), do: check_string(to_string(str))
  def check_string(str) when is_float(str), do: check_string(to_string(str))
  def check_string(str) when is_atom(str), do: check_string(Atom.to_string(str))
  
  def check_string(string) when is_binary(string) do
    if string |> String.match?(~r/[^a-zA-Z0-9_]/) do
      raise RuntimeError, message: "Invalid String #{string}"
    end

    string
  end
  
  def check_string(other) do
    if match?(%Selecto{}, other) do
      raise ArgumentError, "Cannot use Selecto struct as string in check_string. Got: #{inspect(other, limit: 3)}"
    end
    
    try do
      check_string(to_string(other))
    rescue
      Protocol.UndefinedError ->
        raise ArgumentError, "Cannot convert #{inspect(other, limit: 3)} to string in check_string"
    end
  end

  def single_wrap(val) do
    val = String.replace(val, ~r/'/, "''")
    ~s"'#{val}'"
  end

  def double_wrap(nil), do: ""
  def double_wrap(str) when is_integer(str), do: double_wrap(to_string(str))
  def double_wrap(str) when is_float(str), do: double_wrap(to_string(str))
  
  def double_wrap(str) when is_atom(str) do
    Atom.to_string(str) |> double_wrap()
  end

  def double_wrap(str) when is_binary(str) do
    if String.match?(str, ~r/[^a-zA-Z0-9_ :&-]/) do
      raise RuntimeError, message: "Invalid Table/Column/Alias Name #{str}"
    end

    # Only quote if necessary
    maybe_quote_identifier(str)
  end
  
  def double_wrap(other) do
    # Don't try to wrap complex structs
    if match?(%Selecto{}, other) do
      raise ArgumentError, "Cannot use Selecto struct as identifier in double_wrap. Got: #{inspect(other, limit: 3)}"
    end
    
    # Fallback for any other type - convert to string
    try do
      double_wrap(to_string(other))
    rescue
      Protocol.UndefinedError ->
        raise ArgumentError, "Cannot convert #{inspect(other, limit: 3)} to string in double_wrap"
    end
  end
  
  @doc """
  Wrap an identifier with the appropriate quotes for the database adapter.
  This is the adapter-aware version of double_wrap.
  """
  def quote_identifier(_selecto, nil), do: ""
  def quote_identifier(selecto, str) when is_integer(str), do: quote_identifier(selecto, to_string(str))
  def quote_identifier(selecto, str) when is_float(str), do: quote_identifier(selecto, to_string(str))
  
  def quote_identifier(selecto, str) when is_atom(str) do
    quote_identifier(selecto, Atom.to_string(str))
  end
  
  def quote_identifier(selecto, str) when is_binary(str) do
    if String.match?(str, ~r/[^a-zA-Z0-9_ :&-]/) do
      raise RuntimeError, message: "Invalid Table/Column/Alias Name #{str}"
    end
    
    # Only quote if necessary
    if needs_quoting?(str) do
      quote = get_quote_char(selecto)
      "#{quote}#{str}#{quote}"
    else
      str
    end
  end
  
  def quote_identifier(selecto, other) do
    # Don't try to wrap complex structs
    if match?(%Selecto{}, other) do
      raise ArgumentError, "Cannot use Selecto struct as identifier in quote_identifier. Got: #{inspect(other, limit: 3)}"
    end
    
    # Fallback for any other type - convert to string
    try do
      quote_identifier(selecto, to_string(other))
    rescue
      Protocol.UndefinedError ->
        raise ArgumentError, "Cannot convert #{inspect(other, limit: 3)} to string in quote_identifier"
    end
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
