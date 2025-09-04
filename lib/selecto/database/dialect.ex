defmodule Selecto.Database.Dialect do
  @moduledoc """
  Base module for SQL dialect definitions.
  
  Provides common structure and utilities for database-specific SQL dialects.
  Each adapter should define its own dialect based on this structure.
  """

  defstruct [
    :name,
    :identifier_quote_char,
    :identifier_quote_end_char,
    :string_quote_char,
    :parameter_style,
    :limit_style,
    :boolean_style,
    :case_sensitivity,
    :null_ordering,
    :supports_returning,
    :supports_upsert,
    :supports_lateral_join,
    :supports_window_functions,
    :supports_cte,
    :supports_recursive_cte,
    :reserved_words,
    :max_identifier_length
  ]

  @type t :: %__MODULE__{
    name: String.t(),
    identifier_quote_char: String.t(),
    identifier_quote_end_char: String.t() | nil,
    string_quote_char: String.t(),
    parameter_style: parameter_style(),
    limit_style: limit_style(),
    boolean_style: boolean_style(),
    case_sensitivity: case_sensitivity(),
    null_ordering: null_ordering(),
    supports_returning: boolean(),
    supports_upsert: boolean(),
    supports_lateral_join: boolean(),
    supports_window_functions: boolean(),
    supports_cte: boolean(),
    supports_recursive_cte: boolean(),
    reserved_words: MapSet.t(String.t()),
    max_identifier_length: pos_integer() | nil
  }

  @type parameter_style :: :numbered | :question | :named | :at_named | :colon_named
  @type limit_style :: :limit_offset | :top | :fetch_first | :rownum
  @type boolean_style :: :true_false | :one_zero | :yes_no | :t_f
  @type case_sensitivity :: :preserve | :lowercase | :uppercase | :configurable
  @type null_ordering :: :nulls_first | :nulls_last | :database_default

  @doc """
  Returns the PostgreSQL dialect (default/built-in).
  """
  @spec postgresql() :: t()
  def postgresql do
    %__MODULE__{
      name: "PostgreSQL",
      identifier_quote_char: "\"",
      identifier_quote_end_char: nil,
      string_quote_char: "'",
      parameter_style: :numbered,
      limit_style: :limit_offset,
      boolean_style: :true_false,
      case_sensitivity: :preserve,
      null_ordering: :nulls_last,
      supports_returning: true,
      supports_upsert: true,
      supports_lateral_join: true,
      supports_window_functions: true,
      supports_cte: true,
      supports_recursive_cte: true,
      reserved_words: postgresql_reserved_words(),
      max_identifier_length: 63
    }
  end

  @doc """
  Quotes an identifier according to the dialect rules.
  """
  @spec quote_identifier(t(), String.t()) :: String.t()
  def quote_identifier(%__MODULE__{} = dialect, identifier) do
    start_char = dialect.identifier_quote_char
    end_char = dialect.identifier_quote_end_char || dialect.identifier_quote_char
    
    # Escape any quotes within the identifier
    escaped = String.replace(identifier, end_char, end_char <> end_char)
    
    start_char <> escaped <> end_char
  end

  @doc """
  Quotes a string literal according to the dialect rules.
  """
  @spec quote_string(t(), String.t()) :: String.t()
  def quote_string(%__MODULE__{} = dialect, string) do
    quote_char = dialect.string_quote_char
    
    # Escape quotes within the string
    escaped = String.replace(string, quote_char, quote_char <> quote_char)
    
    quote_char <> escaped <> quote_char
  end

  @doc """
  Generates a parameter placeholder based on the dialect's style.
  """
  @spec parameter_placeholder(t(), pos_integer()) :: String.t()
  def parameter_placeholder(%__MODULE__{} = dialect, index) do
    case dialect.parameter_style do
      :numbered -> "$#{index}"
      :question -> "?"
      :named -> ":param#{index}"
      :at_named -> "@param#{index}"
      :colon_named -> ":#{index}"
    end
  end

  @doc """
  Formats a boolean literal according to the dialect.
  """
  @spec boolean_literal(t(), boolean()) :: String.t()
  def boolean_literal(%__MODULE__{} = dialect, value) do
    case dialect.boolean_style do
      :true_false ->
        if value, do: "TRUE", else: "FALSE"
      :one_zero ->
        if value, do: "1", else: "0"
      :yes_no ->
        if value, do: "'Y'", else: "'N'"
      :t_f ->
        if value, do: "'T'", else: "'F'"
    end
  end

  @doc """
  Generates LIMIT/OFFSET clause based on dialect style.
  """
  @spec limit_clause(t(), limit :: pos_integer() | nil, offset :: non_neg_integer() | nil) :: String.t() | nil
  def limit_clause(%__MODULE__{} = dialect, limit, offset \\ nil) do
    case {dialect.limit_style, limit, offset} do
      {_, nil, nil} ->
        nil
      
      {:limit_offset, limit, nil} ->
        "LIMIT #{limit}"
      
      {:limit_offset, nil, offset} ->
        "OFFSET #{offset}"
      
      {:limit_offset, limit, offset} ->
        "LIMIT #{limit} OFFSET #{offset}"
      
      {:top, limit, nil} ->
        # TOP clause is handled differently (in SELECT)
        "TOP #{limit}"
      
      {:fetch_first, limit, nil} ->
        "FETCH FIRST #{limit} ROWS ONLY"
      
      {:fetch_first, limit, offset} ->
        "OFFSET #{offset} ROWS FETCH FIRST #{limit} ROWS ONLY"
      
      {:rownum, limit, nil} ->
        # ROWNUM is handled in WHERE clause
        "ROWNUM <= #{limit}"
      
      _ ->
        nil
    end
  end

  @doc """
  Checks if an identifier is a reserved word.
  """
  @spec reserved?(t(), String.t()) :: boolean()
  def reserved?(%__MODULE__{} = dialect, word) do
    normalized = case dialect.case_sensitivity do
      :uppercase -> String.upcase(word)
      :lowercase -> String.downcase(word)
      _ -> word
    end
    
    MapSet.member?(dialect.reserved_words, normalized)
  end

  @doc """
  Normalizes an identifier based on case sensitivity rules.
  """
  @spec normalize_identifier(t(), String.t()) :: String.t()
  def normalize_identifier(%__MODULE__{} = dialect, identifier) do
    case dialect.case_sensitivity do
      :uppercase -> String.upcase(identifier)
      :lowercase -> String.downcase(identifier)
      _ -> identifier
    end
  end

  # Private functions

  defp postgresql_reserved_words do
    MapSet.new([
      "ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC",
      "ASYMMETRIC", "BOTH", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN",
      "CONSTRAINT", "CREATE", "CURRENT_CATALOG", "CURRENT_DATE",
      "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
      "DEFAULT", "DEFERRABLE", "DESC", "DISTINCT", "DO", "ELSE", "END",
      "EXCEPT", "FALSE", "FETCH", "FOR", "FOREIGN", "FROM", "GRANT",
      "GROUP", "HAVING", "IN", "INITIALLY", "INTERSECT", "INTO", "IS",
      "ISNULL", "JOIN", "LATERAL", "LEADING", "LEFT", "LIKE", "LIMIT",
      "LOCALTIME", "LOCALTIMESTAMP", "NATURAL", "NOT", "NOTNULL", "NULL",
      "OFFSET", "ON", "ONLY", "OR", "ORDER", "OUTER", "OVER", "OVERLAPS",
      "PLACING", "PRIMARY", "REFERENCES", "RETURNING", "RIGHT", "SELECT",
      "SESSION_USER", "SIMILAR", "SOME", "SYMMETRIC", "TABLE", "THEN",
      "TO", "TRAILING", "TRUE", "UNION", "UNIQUE", "USER", "USING",
      "VARIADIC", "VERBOSE", "WHEN", "WHERE", "WINDOW", "WITH"
    ])
  end
end