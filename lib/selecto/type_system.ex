defmodule Selecto.TypeSystem do
  @moduledoc """
  Type inference and coercion system for Selecto SQL expressions.

  This module provides:
  - Type inference for expressions (fields, functions, literals, complex expressions)
  - Type compatibility checking for comparisons and set operations
  - Type coercion rules for implicit type conversions
  - Return type determination for SQL functions and aggregates

  ## Type Categories

  Selecto organizes SQL types into the following categories:

  - **Numeric**: `:integer`, `:bigint`, `:smallint`, `:decimal`, `:float`, `:numeric`
  - **String**: `:string`, `:text`, `:varchar`, `:char`
  - **Boolean**: `:boolean`
  - **DateTime**: `:date`, `:time`, `:datetime`, `:utc_datetime`, `:naive_datetime`, `:timestamp`
  - **JSON**: `:json`, `:jsonb`, `:map`
  - **Array**: `{:array, inner_type}`
  - **Binary**: `:binary`, `:bytea`
  - **UUID**: `:uuid`, `:binary_id`
  - **Spatial**: `:geometry`, `:geography`, `:point`, `:polygon`, etc.

  ## Usage

      # Infer type of an expression
      {:ok, :integer} = Selecto.TypeSystem.infer_type(selecto, {:count, "*"})
      {:ok, :decimal} = Selecto.TypeSystem.infer_type(selecto, {:sum, "price"})
      {:ok, :string} = Selecto.TypeSystem.infer_type(selecto, "product_name")

      # Check type compatibility
      true = Selecto.TypeSystem.compatible?(:integer, :decimal)
      false = Selecto.TypeSystem.compatible?(:string, :boolean)

      # Get coerced type for operation
      {:ok, :decimal} = Selecto.TypeSystem.coerce_types(:integer, :decimal, :arithmetic)
  """

  alias Selecto.FieldResolver

  @type sql_type ::
          :unknown
          | :integer
          | :bigint
          | :smallint
          | :decimal
          | :float
          | :numeric
          | :string
          | :text
          | :varchar
          | :char
          | :boolean
          | :date
          | :time
          | :datetime
          | :utc_datetime
          | :naive_datetime
          | :timestamp
          | :json
          | :jsonb
          | :map
          | :binary
          | :bytea
          | :uuid
          | :binary_id
          | :geometry
          | :geography
          | :point
          | :linestring
          | :polygon
          | :multipoint
          | :multilinestring
          | :multipolygon
          | :geometrycollection
          | {:array, sql_type()}

  @type type_category ::
          :numeric
          | :string
          | :boolean
          | :datetime
          | :json
          | :array
          | :binary
          | :uuid
          | :spatial
          | :unknown

  # Type category mappings
  @numeric_types [:integer, :bigint, :smallint, :decimal, :float, :numeric]
  @string_types [:string, :text, :varchar, :char]
  @datetime_types [:date, :time, :datetime, :utc_datetime, :naive_datetime, :timestamp]
  @json_types [:json, :jsonb, :map]
  @binary_types [:binary, :bytea]
  @uuid_types [:uuid, :binary_id]
  @spatial_types [
    :geometry,
    :geography,
    :point,
    :linestring,
    :polygon,
    :multipoint,
    :multilinestring,
    :multipolygon,
    :geometrycollection
  ]

  # Function return type mappings
  @aggregate_return_types %{
    # Count always returns integer
    :count => :bigint,
    # Sum promotes to decimal for safety
    :sum => :decimal,
    # Avg always returns decimal/float
    :avg => :decimal,
    # Min/Max preserve input type
    :min => :preserve,
    :max => :preserve,
    # String aggregation
    :string_agg => :string,
    # JSON aggregation
    :json_agg => :json,
    :jsonb_agg => :jsonb,
    :json_object_agg => :json,
    :jsonb_object_agg => :jsonb,
    :array_agg => :array,
    # Boolean aggregates
    :bool_and => :boolean,
    :bool_or => :boolean,
    :every => :boolean,
    # Statistical
    :stddev => :decimal,
    :stddev_pop => :decimal,
    :stddev_samp => :decimal,
    :variance => :decimal,
    :var_pop => :decimal,
    :var_samp => :decimal,
    :corr => :decimal,
    :covar_pop => :decimal,
    :covar_samp => :decimal,
    :regr_avgx => :decimal,
    :regr_avgy => :decimal,
    :regr_count => :bigint,
    :regr_intercept => :decimal,
    :regr_r2 => :decimal,
    :regr_slope => :decimal,
    :regr_sxx => :decimal,
    :regr_sxy => :decimal,
    :regr_syy => :decimal,
    # Bit aggregates
    :bit_and => :preserve,
    :bit_or => :preserve,
    :bit_xor => :preserve
  }

  @scalar_function_return_types %{
    # String functions
    :concat => :string,
    :concat_ws => :string,
    :upper => :string,
    :lower => :string,
    :trim => :string,
    :ltrim => :string,
    :rtrim => :string,
    :substring => :string,
    :substr => :string,
    :replace => :string,
    :reverse => :string,
    :repeat => :string,
    :lpad => :string,
    :rpad => :string,
    :left => :string,
    :right => :string,
    :split_part => :string,
    :initcap => :string,
    :translate => :string,
    :format => :string,
    :to_char => :string,
    :chr => :string,
    :ascii => :integer,
    :length => :integer,
    :char_length => :integer,
    :character_length => :integer,
    :octet_length => :integer,
    :bit_length => :integer,
    :position => :integer,
    :strpos => :integer,
    # Numeric functions
    :abs => :preserve,
    :ceil => :integer,
    :ceiling => :integer,
    :floor => :integer,
    :round => :preserve,
    :trunc => :preserve,
    :sign => :integer,
    :sqrt => :decimal,
    :cbrt => :decimal,
    :power => :decimal,
    :exp => :decimal,
    :ln => :decimal,
    :log => :decimal,
    :log10 => :decimal,
    :mod => :preserve,
    :div => :integer,
    :pi => :decimal,
    :random => :decimal,
    :degrees => :decimal,
    :radians => :decimal,
    :sin => :decimal,
    :cos => :decimal,
    :tan => :decimal,
    :cot => :decimal,
    :asin => :decimal,
    :acos => :decimal,
    :atan => :decimal,
    :atan2 => :decimal,
    :greatest => :preserve,
    :least => :preserve,
    :coalesce => :preserve,
    :nullif => :preserve,
    # Date/time functions
    :now => :timestamp,
    :current_date => :date,
    :current_time => :time,
    :current_timestamp => :timestamp,
    :localtime => :time,
    :localtimestamp => :timestamp,
    :date_trunc => :timestamp,
    :date_part => :decimal,
    :extract => :decimal,
    :age => :interval,
    :make_date => :date,
    :make_time => :time,
    :make_timestamp => :timestamp,
    :make_timestamptz => :timestamp,
    :to_date => :date,
    :to_timestamp => :timestamp,
    # Type conversion
    :cast => :dynamic,
    :to_number => :decimal,
    # JSON functions
    :to_json => :json,
    :to_jsonb => :jsonb,
    :json_build_object => :json,
    :jsonb_build_object => :jsonb,
    :json_build_array => :json,
    :jsonb_build_array => :jsonb,
    :json_typeof => :string,
    :jsonb_typeof => :string,
    :json_array_length => :integer,
    :jsonb_array_length => :integer,
    # Conditional
    :case => :dynamic,
    :case_when => :dynamic,
    # Boolean
    :not => :boolean,
    :and => :boolean,
    :or => :boolean
  }

  @doc """
  Infer the SQL type of an expression.

  Returns `{:ok, type}` for successfully inferred types,
  or `{:ok, :unknown}` when the type cannot be determined.

  ## Examples

      iex> infer_type(selecto, "product_name")
      {:ok, :string}

      iex> infer_type(selecto, {:count, "*"})
      {:ok, :bigint}

      iex> infer_type(selecto, {:sum, "price"})
      {:ok, :decimal}
  """
  @spec infer_type(Selecto.t(), term()) :: {:ok, sql_type()} | {:error, term()}
  def infer_type(selecto, expression) do
    {:ok, do_infer_type(selecto, expression)}
  rescue
    e -> {:error, Exception.message(e)}
  end

  @doc """
  Infer type and return just the type (for internal use where errors are handled upstream).
  """
  @spec infer_type!(Selecto.t(), term()) :: sql_type()
  def infer_type!(selecto, expression) do
    do_infer_type(selecto, expression)
  end

  # Literals
  defp do_infer_type(_selecto, val) when is_integer(val), do: :integer
  defp do_infer_type(_selecto, val) when is_float(val), do: :decimal
  defp do_infer_type(_selecto, val) when is_boolean(val), do: :boolean

  defp do_infer_type(_selecto, val) when is_binary(val) and byte_size(val) == 36 do
    # Could be a UUID string
    if String.match?(val, ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i) do
      :uuid
    else
      :string
    end
  end

  defp do_infer_type(_selecto, val) when is_binary(val), do: :string
  defp do_infer_type(_selecto, nil), do: :unknown

  # Literal expressions
  defp do_infer_type(_selecto, {:literal, val}) when is_integer(val), do: :integer
  defp do_infer_type(_selecto, {:literal, val}) when is_float(val), do: :decimal
  defp do_infer_type(_selecto, {:literal, val}) when is_boolean(val), do: :boolean
  defp do_infer_type(_selecto, {:literal, val}) when is_binary(val), do: :string
  defp do_infer_type(_selecto, {:literal, %Date{}}), do: :date
  defp do_infer_type(_selecto, {:literal, %Time{}}), do: :time
  defp do_infer_type(_selecto, {:literal, %DateTime{}}), do: :utc_datetime
  defp do_infer_type(_selecto, {:literal, %NaiveDateTime{}}), do: :naive_datetime
  defp do_infer_type(_selecto, {:literal, val}) when is_list(val), do: {:array, :unknown}
  defp do_infer_type(_selecto, {:literal, val}) when is_map(val), do: :jsonb
  defp do_infer_type(_selecto, {:literal, _}), do: :unknown
  defp do_infer_type(_selecto, {:literal_string, _}), do: :string

  # Count always returns bigint
  defp do_infer_type(_selecto, {:count}), do: :bigint
  defp do_infer_type(_selecto, {:count, "*"}), do: :bigint
  defp do_infer_type(_selecto, {:count, "*", _filter}), do: :bigint
  defp do_infer_type(_selecto, {:count, _field}), do: :bigint
  defp do_infer_type(_selecto, {:count, _field, _filter}), do: :bigint
  defp do_infer_type(_selecto, {:count_distinct, _field}), do: :bigint

  # Aggregate functions
  defp do_infer_type(selecto, {func, field}) when func in [:sum, :avg, :min, :max] do
    case Map.get(@aggregate_return_types, func) do
      :preserve -> do_infer_type(selecto, field)
      type -> type
    end
  end

  defp do_infer_type(selecto, {func, field, _filter}) when func in [:sum, :avg, :min, :max] do
    do_infer_type(selecto, {func, field})
  end

  # String aggregation
  defp do_infer_type(_selecto, {:string_agg, _field, _delimiter}), do: :string
  defp do_infer_type(_selecto, {:string_agg, _field, _delimiter, _opts}), do: :string

  # JSON aggregation
  defp do_infer_type(_selecto, {:json_agg, _}), do: :json
  defp do_infer_type(_selecto, {:jsonb_agg, _}), do: :jsonb
  defp do_infer_type(_selecto, {:json_object_agg, _, _}), do: :json
  defp do_infer_type(_selecto, {:jsonb_object_agg, _, _}), do: :jsonb

  # Array aggregation - infer inner type
  defp do_infer_type(selecto, {:array_agg, field}) do
    inner_type = do_infer_type(selecto, field)
    {:array, inner_type}
  end

  # Scalar functions with known return types
  defp do_infer_type(selecto, {func, args}) when is_atom(func) do
    case Map.get(@scalar_function_return_types, func) do
      nil ->
        :unknown

      :preserve ->
        # Return type matches first argument type
        first_arg = if is_list(args), do: List.first(args), else: args
        do_infer_type(selecto, first_arg)

      :dynamic ->
        :unknown

      type ->
        type
    end
  end

  defp do_infer_type(selecto, {func, args, _opts}) when is_atom(func) do
    do_infer_type(selecto, {func, args})
  end

  # Multi-arg functions like coalesce, greatest, least
  defp do_infer_type(selecto, {func, fields})
       when func in [:coalesce, :greatest, :least] and is_list(fields) do
    # Return type is the common type of all arguments
    field_types = Enum.map(fields, &do_infer_type(selecto, &1))
    find_common_type(field_types)
  end

  # CASE expressions - return type from first THEN clause
  defp do_infer_type(selecto, {:case, pairs}) when is_list(pairs) do
    case pairs do
      [{_condition, then_expr} | _] -> do_infer_type(selecto, then_expr)
      _ -> :unknown
    end
  end

  defp do_infer_type(selecto, {:case, pairs, else_expr}) when is_list(pairs) do
    case pairs do
      [{_condition, then_expr} | _] ->
        do_infer_type(selecto, then_expr)

      _ ->
        if else_expr, do: do_infer_type(selecto, else_expr), else: :unknown
    end
  end

  defp do_infer_type(selecto, {:case, %{} = case_spec}) do
    # New format CASE - check result_expressions
    case Map.get(case_spec, :when_clauses, []) do
      [{_cond, result} | _] ->
        do_infer_type(selecto, result)

      _ ->
        case Map.get(case_spec, :else) do
          nil -> :unknown
          else_expr -> do_infer_type(selecto, else_expr)
        end
    end
  end

  defp do_infer_type(selecto, {:case_when, case_spec}),
    do: do_infer_type(selecto, {:case, case_spec})

  # Cast expression - return target type
  defp do_infer_type(_selecto, {:cast, _expr, target_type}) when is_atom(target_type) do
    normalize_type(target_type)
  end

  defp do_infer_type(_selecto, {:cast, _expr, target_type}) when is_binary(target_type) do
    parse_sql_type(target_type)
  end

  # Extract returns numeric
  defp do_infer_type(_selecto, {:extract, _part, _field}), do: :decimal

  # Concat returns string
  defp do_infer_type(_selecto, {:concat, _fields}), do: :string

  # Subquery - type is unknown without analyzing the subquery
  defp do_infer_type(_selecto, {:subquery, _, _}), do: :unknown

  # Row constructor
  defp do_infer_type(_selecto, {:row, _, _}), do: :record

  # Aliased expression - infer type of inner expression
  defp do_infer_type(selecto, {:as, expression, _alias}), do: do_infer_type(selecto, expression)
  defp do_infer_type(selecto, {:field, field, _alias}), do: do_infer_type(selecto, field)

  # Field reference - look up in domain/joins
  defp do_infer_type(selecto, field) when is_binary(field) do
    case FieldResolver.resolve_field(selecto, field) do
      {:ok, %{type: type}} -> normalize_type(type)
      _ -> :unknown
    end
  end

  defp do_infer_type(selecto, {:field, field}) when is_binary(field) do
    do_infer_type(selecto, field)
  end

  defp do_infer_type(selecto, field) when is_atom(field) do
    do_infer_type(selecto, Atom.to_string(field))
  end

  # Raw SQL - type is unknown
  defp do_infer_type(_selecto, {:raw_sql, _}), do: :unknown
  defp do_infer_type(_selecto, {:custom_sql, _, _}), do: :unknown

  # Bucket functions return integer counts
  defp do_infer_type(_selecto, {:count_age_bucket, _, _, _}), do: :bigint
  defp do_infer_type(_selecto, {:count_age_bucket_other, _, _}), do: :bigint
  defp do_infer_type(_selecto, {:count_bucket, _, _, _}), do: :bigint
  defp do_infer_type(_selecto, {:count_bucket_other, _, _}), do: :bigint

  # Window functions - return type depends on the aggregate
  defp do_infer_type(selecto, {:over, expr, _window_spec}) do
    do_infer_type(selecto, expr)
  end

  defp do_infer_type(selecto, {:window, func, args, _window_spec}) when is_atom(func) do
    do_infer_type(selecto, {func, args})
  end

  # Unknown expression format
  defp do_infer_type(_selecto, _expression), do: :unknown

  @doc """
  Get the type category for a given SQL type.
  """
  @spec type_category(sql_type()) :: type_category()
  def type_category(type) when type in @numeric_types, do: :numeric
  def type_category(type) when type in @string_types, do: :string
  def type_category(:boolean), do: :boolean
  def type_category(type) when type in @datetime_types, do: :datetime
  def type_category(type) when type in @json_types, do: :json
  def type_category({:array, _}), do: :array
  def type_category(type) when type in @binary_types, do: :binary
  def type_category(type) when type in @uuid_types, do: :uuid
  def type_category(type) when type in @spatial_types, do: :spatial
  def type_category(_), do: :unknown

  @doc """
  Check if two types are compatible for comparisons or assignments.
  """
  @spec compatible?(sql_type(), sql_type()) :: boolean()
  def compatible?(:unknown, _), do: true
  def compatible?(_, :unknown), do: true
  def compatible?(type, type), do: true

  def compatible?(type1, type2) do
    cat1 = type_category(type1)
    cat2 = type_category(type2)
    cat1 == cat2 or cat1 == :unknown or cat2 == :unknown
  end

  @doc """
  Determine the result type when coercing two types for an operation.

  ## Operation Types
  - `:arithmetic` - Numeric operations (+, -, *, /)
  - `:comparison` - Comparison operations (=, <>, <, >, etc.)
  - `:concatenation` - String concatenation (||)
  - `:union` - Set operations (UNION, INTERSECT, EXCEPT)
  """
  @spec coerce_types(sql_type(), sql_type(), atom()) :: {:ok, sql_type()} | {:error, String.t()}
  def coerce_types(type1, type2, operation) do
    case operation do
      :arithmetic -> coerce_numeric(type1, type2)
      :comparison -> {:ok, :boolean}
      :concatenation -> coerce_string(type1, type2)
      :union -> coerce_union(type1, type2)
      _ -> {:ok, :unknown}
    end
  end

  defp coerce_numeric(type1, type2) do
    cond do
      type1 == :unknown or type2 == :unknown -> {:ok, :unknown}
      type1 == :decimal or type2 == :decimal -> {:ok, :decimal}
      type1 == :float or type2 == :float -> {:ok, :float}
      type1 == :numeric or type2 == :numeric -> {:ok, :numeric}
      type1 == :bigint or type2 == :bigint -> {:ok, :bigint}
      type1 == :integer or type2 == :integer -> {:ok, :integer}
      type1 == :smallint and type2 == :smallint -> {:ok, :smallint}
      true -> {:error, "Cannot perform arithmetic on types #{type1} and #{type2}"}
    end
  end

  defp coerce_string(type1, type2) do
    if type_category(type1) == :string and type_category(type2) == :string do
      {:ok, :text}
    else
      {:error, "Cannot concatenate types #{type1} and #{type2}"}
    end
  end

  defp coerce_union(type1, type2) do
    cond do
      type1 == type2 ->
        {:ok, type1}

      type1 == :unknown ->
        {:ok, type2}

      type2 == :unknown ->
        {:ok, type1}

      compatible?(type1, type2) ->
        # Return the wider type
        {:ok, wider_type(type1, type2)}

      true ->
        {:error, "Incompatible types for UNION: #{type1} and #{type2}"}
    end
  end

  # Determine the wider type between two compatible types
  defp wider_type(type1, type2) when type1 in @numeric_types and type2 in @numeric_types do
    # Order from narrowest to widest
    numeric_precedence = [:smallint, :integer, :bigint, :float, :decimal, :numeric]
    idx1 = Enum.find_index(numeric_precedence, &(&1 == type1)) || 99
    idx2 = Enum.find_index(numeric_precedence, &(&1 == type2)) || 99
    if idx1 >= idx2, do: type1, else: type2
  end

  defp wider_type(type1, type2) when type1 in @string_types and type2 in @string_types do
    :text
  end

  defp wider_type(type1, type2) when type1 in @datetime_types and type2 in @datetime_types do
    :timestamp
  end

  defp wider_type(type1, _type2), do: type1

  @doc """
  Check if a type is numeric.
  """
  @spec numeric_type?(sql_type()) :: boolean()
  def numeric_type?(type), do: type in @numeric_types

  @doc """
  Check if a type is a string type.
  """
  @spec string_type?(sql_type()) :: boolean()
  def string_type?(type), do: type in @string_types

  @doc """
  Check if a type is a date/time type.
  """
  @spec datetime_type?(sql_type()) :: boolean()
  def datetime_type?(type), do: type in @datetime_types

  @doc """
  Normalize Ecto types to Selecto's internal type representation.
  """
  @spec normalize_type(atom()) :: sql_type()
  def normalize_type(:id), do: :integer
  def normalize_type(:binary_id), do: :uuid
  def normalize_type(:utc_datetime_usec), do: :utc_datetime
  def normalize_type(:naive_datetime_usec), do: :naive_datetime
  def normalize_type({:array, inner}), do: {:array, normalize_type(inner)}
  def normalize_type(type), do: type

  @doc """
  Parse a SQL type string into an atom type.
  """
  @spec parse_sql_type(String.t()) :: sql_type()
  def parse_sql_type(type_str) when is_binary(type_str) do
    type_str
    |> String.downcase()
    |> String.trim()
    |> do_parse_sql_type()
  end

  defp do_parse_sql_type("integer"), do: :integer
  defp do_parse_sql_type("int"), do: :integer
  defp do_parse_sql_type("int4"), do: :integer
  defp do_parse_sql_type("bigint"), do: :bigint
  defp do_parse_sql_type("int8"), do: :bigint
  defp do_parse_sql_type("smallint"), do: :smallint
  defp do_parse_sql_type("int2"), do: :smallint
  defp do_parse_sql_type("decimal"), do: :decimal
  defp do_parse_sql_type("numeric"), do: :numeric
  defp do_parse_sql_type("real"), do: :float
  defp do_parse_sql_type("float4"), do: :float
  defp do_parse_sql_type("double precision"), do: :float
  defp do_parse_sql_type("float8"), do: :float
  defp do_parse_sql_type("float"), do: :float
  defp do_parse_sql_type("text"), do: :text
  defp do_parse_sql_type("varchar" <> _), do: :varchar
  defp do_parse_sql_type("character varying" <> _), do: :varchar
  defp do_parse_sql_type("char" <> _), do: :char
  defp do_parse_sql_type("character" <> _), do: :char
  defp do_parse_sql_type("boolean"), do: :boolean
  defp do_parse_sql_type("bool"), do: :boolean
  defp do_parse_sql_type("date"), do: :date
  defp do_parse_sql_type("time" <> _), do: :time
  defp do_parse_sql_type("timestamp" <> _), do: :timestamp
  defp do_parse_sql_type("timestamptz"), do: :utc_datetime
  defp do_parse_sql_type("json"), do: :json
  defp do_parse_sql_type("jsonb"), do: :jsonb
  defp do_parse_sql_type("bytea"), do: :bytea
  defp do_parse_sql_type("uuid"), do: :uuid
  defp do_parse_sql_type("geometry"), do: :geometry
  defp do_parse_sql_type("public.geometry"), do: :geometry
  defp do_parse_sql_type("geography"), do: :geography
  defp do_parse_sql_type("public.geography"), do: :geography
  defp do_parse_sql_type("point"), do: :point
  defp do_parse_sql_type("linestring"), do: :linestring
  defp do_parse_sql_type("polygon"), do: :polygon
  defp do_parse_sql_type("multipoint"), do: :multipoint
  defp do_parse_sql_type("multilinestring"), do: :multilinestring
  defp do_parse_sql_type("multipolygon"), do: :multipolygon
  defp do_parse_sql_type("geometrycollection"), do: :geometrycollection
  defp do_parse_sql_type("geometry(" <> _), do: :geometry
  defp do_parse_sql_type("geography(" <> _), do: :geography
  defp do_parse_sql_type(_), do: :unknown

  # Find common type among a list of types
  defp find_common_type([]), do: :unknown
  defp find_common_type([type]), do: type

  defp find_common_type([type | rest]) do
    Enum.reduce(rest, type, fn t, acc ->
      if compatible?(acc, t) do
        case coerce_types(acc, t, :union) do
          {:ok, common} -> common
          _ -> acc
        end
      else
        :unknown
      end
    end)
  end
end
