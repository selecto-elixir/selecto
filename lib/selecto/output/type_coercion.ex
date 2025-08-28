defmodule Selecto.Output.TypeCoercion do
  @moduledoc """
  Database type coercion system for transforming raw database values
  to appropriate Elixir types based on PostgreSQL column types.

  This module provides configurable type coercion strategies and can be extended
  to support custom type mappings and coercion functions.
  """

  @doc """
  PostgreSQL type mappings to Elixir types.

  This provides the standard mapping from PostgreSQL column types to
  their corresponding Elixir types.
  """
  @type_mappings %{
    # Integer types
    "integer" => :integer,
    "bigint" => :integer,
    "smallint" => :integer,
    "int4" => :integer,
    "int8" => :integer,
    "int2" => :integer,

    # Numeric types
    "decimal" => :decimal,
    "numeric" => :decimal,
    "real" => :float,
    "double precision" => :float,
    "float4" => :float,
    "float8" => :float,
    "money" => :decimal,

    # String types
    "varchar" => :string,
    "text" => :string,
    "char" => :string,
    "bpchar" => :string,  # char(n)
    "name" => :string,

    # Boolean
    "boolean" => :boolean,
    "bool" => :boolean,

    # Date/Time types
    "date" => :date,
    "time" => :time,
    "time without time zone" => :time,
    "time with time zone" => :time_with_timezone,
    "timestamp" => :naive_datetime,
    "timestamp without time zone" => :naive_datetime,
    "timestamp with time zone" => :utc_datetime,
    "timestamptz" => :utc_datetime,
    "interval" => :interval,

    # JSON types
    "json" => :map,
    "jsonb" => :map,

    # Array types
    "array" => :list,
    "_int4" => {:array, :integer},   # integer array
    "_text" => {:array, :string},    # text array
    "_varchar" => {:array, :string}, # varchar array

    # UUID
    "uuid" => :uuid,

    # Network types
    "inet" => :string,
    "cidr" => :string,
    "macaddr" => :string,

    # Geometric types (keep as string by default)
    "point" => :string,
    "line" => :string,
    "lseg" => :string,
    "box" => :string,
    "path" => :string,
    "polygon" => :string,
    "circle" => :string,

    # Binary data
    "bytea" => :binary
  }

  @doc """
  Coerce a database value to its appropriate Elixir type.

  ## Parameters

  - `value` - The raw database value
  - `column_type` - The PostgreSQL column type (optional)
  - `strategy` - Coercion strategy (:strict, :safe, :ignore, :custom)
  - `custom_coercions` - Map of custom coercion functions

  ## Coercion Strategies

  - `:strict` - Raise on coercion errors
  - `:safe` - Return original value on coercion errors
  - `:ignore` - Skip coercion, return raw values
  - `:custom` - Use custom coercion functions
  """
  @spec coerce_value(term(), String.t() | nil, atom(), map()) :: term()
  def coerce_value(value, column_type \\ nil, strategy \\ :safe, custom_coercions \\ %{})

  # Handle NULL values
  def coerce_value(nil, _column_type, _strategy, _custom_coercions), do: nil

  # Skip coercion if strategy is :ignore
  def coerce_value(value, _column_type, :ignore, _custom_coercions), do: value

  # Use custom coercion if available
  def coerce_value(value, column_type, :custom, custom_coercions) when is_map(custom_coercions) do
    case Map.get(custom_coercions, column_type) do
      nil -> coerce_value(value, column_type, :safe, %{})
      coercion_func when is_function(coercion_func, 1) ->
        apply_coercion_safely(coercion_func, value, :safe)
      coercion_func when is_function(coercion_func, 2) ->
        apply_coercion_safely(fn v -> coercion_func.(v, column_type) end, value, :safe)
    end
  end

  # Main coercion logic
  def coerce_value(value, column_type, strategy, custom_coercions) when column_type != nil do
    target_type = Map.get(@type_mappings, column_type)
    do_coerce_value(value, target_type, strategy)
  end

  # Fallback to safe auto-detection if no column type provided
  def coerce_value(value, nil, strategy, _custom_coercions) do
    auto_detect_and_coerce(value, strategy)
  end

  # Private coercion functions

  defp do_coerce_value(value, nil, _strategy), do: value  # Unknown type, return as-is

  defp do_coerce_value(value, :integer, strategy) do
    case value do
      val when is_integer(val) -> val
      val when is_binary(val) ->
        case Integer.parse(val) do
          {int, ""} -> int
          _ -> handle_coercion_error(value, :integer, strategy)
        end
      val when is_float(val) ->
        if val == Float.round(val), do: round(val), else: handle_coercion_error(value, :integer, strategy)
      _ -> handle_coercion_error(value, :integer, strategy)
    end
  end

  defp do_coerce_value(value, :float, strategy) do
    case value do
      val when is_float(val) -> val
      val when is_integer(val) -> val * 1.0
      val when is_binary(val) ->
        case Float.parse(val) do
          {float, ""} -> float
          _ -> handle_coercion_error(value, :float, strategy)
        end
      _ -> handle_coercion_error(value, :float, strategy)
    end
  end

  defp do_coerce_value(value, :decimal, strategy) do
    # For decimal types, we'd typically use the Decimal library
    # For now, we'll keep them as strings or try to parse as Decimal if available
    case Code.ensure_loaded(Decimal) do
      {:module, Decimal} ->
        try do
          case value do
            val when is_binary(val) -> Decimal.new(val)
            val when is_integer(val) -> Decimal.new(val)
            val when is_float(val) -> Decimal.from_float(val)
            %Decimal{} = val -> val
            _ -> handle_coercion_error(value, :decimal, strategy)
          end
        rescue
          _ -> handle_coercion_error(value, :decimal, strategy)
        end
      {:error, _} ->
        # Decimal not available, keep as string
        to_string(value)
    end
  end

  defp do_coerce_value(value, :boolean, strategy) do
    case value do
      val when is_boolean(val) -> val
      val when is_binary(val) ->
        case String.downcase(val) do
          v when v in ["true", "t", "yes", "y", "1", "on"] -> true
          v when v in ["false", "f", "no", "n", "0", "off"] -> false
          _ -> handle_coercion_error(value, :boolean, strategy)
        end
      1 -> true
      0 -> false
      _ -> handle_coercion_error(value, :boolean, strategy)
    end
  end

  defp do_coerce_value(value, :string, _strategy) do
    to_string(value)
  end

  defp do_coerce_value(value, :date, strategy) do
    case value do
      %Date{} = val -> val
      val when is_binary(val) ->
        case Date.from_iso8601(val) do
          {:ok, date} -> date
          _ -> handle_coercion_error(value, :date, strategy)
        end
      _ -> handle_coercion_error(value, :date, strategy)
    end
  end

  defp do_coerce_value(value, :time, strategy) do
    case value do
      %Time{} = val -> val
      val when is_binary(val) ->
        case Time.from_iso8601(val) do
          {:ok, time} -> time
          _ -> handle_coercion_error(value, :time, strategy)
        end
      _ -> handle_coercion_error(value, :time, strategy)
    end
  end

  defp do_coerce_value(value, :naive_datetime, strategy) do
    case value do
      %NaiveDateTime{} = val -> val
      val when is_binary(val) ->
        case NaiveDateTime.from_iso8601(val) do
          {:ok, ndt} -> ndt
          _ -> handle_coercion_error(value, :naive_datetime, strategy)
        end
      _ -> handle_coercion_error(value, :naive_datetime, strategy)
    end
  end

  defp do_coerce_value(value, :utc_datetime, strategy) do
    case value do
      %DateTime{} = val -> val
      val when is_binary(val) ->
        case DateTime.from_iso8601(val) do
          {:ok, dt, _offset} -> dt
          _ -> handle_coercion_error(value, :utc_datetime, strategy)
        end
      _ -> handle_coercion_error(value, :utc_datetime, strategy)
    end
  end

  defp do_coerce_value(value, :map, strategy) do
    case value do
      val when is_map(val) -> val
      val when is_binary(val) ->
        case Jason.decode(val) do
          {:ok, decoded} -> decoded
          _ -> handle_coercion_error(value, :map, strategy)
        end
      _ -> handle_coercion_error(value, :map, strategy)
    end
  end

  defp do_coerce_value(value, :list, strategy) do
    case value do
      val when is_list(val) -> val
      val when is_binary(val) ->
        # Try to parse as JSON array
        case Jason.decode(val) do
          {:ok, decoded} when is_list(decoded) -> decoded
          _ -> handle_coercion_error(value, :list, strategy)
        end
      _ -> handle_coercion_error(value, :list, strategy)
    end
  end

  defp do_coerce_value(value, {:array, _element_type}, strategy) do
    # For now, treat arrays as regular lists
    do_coerce_value(value, :list, strategy)
  end

  defp do_coerce_value(value, :uuid, strategy) do
    case value do
      val when is_binary(val) ->
        # Basic UUID format validation
        if Regex.match?(~r/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i, val) do
          val
        else
          handle_coercion_error(value, :uuid, strategy)
        end
      _ -> handle_coercion_error(value, :uuid, strategy)
    end
  end

  defp do_coerce_value(value, :binary, _strategy) do
    # Keep binary data as-is
    value
  end

  defp do_coerce_value(value, _unknown_type, _strategy) do
    # For unknown types, return the value as-is
    value
  end

  # Auto-detection for values without column type information
  defp auto_detect_and_coerce(value, strategy) when is_binary(value) do
    cond do
      # Integer detection
      Regex.match?(~r/^-?\d+$/, value) ->
        case Integer.parse(value) do
          {int, ""} -> int
          _ -> value
        end

      # Float detection
      Regex.match?(~r/^-?\d+\.\d+$/, value) ->
        case Float.parse(value) do
          {float, ""} -> float
          _ -> value
        end

      # Boolean detection
      String.downcase(value) in ["true", "false", "t", "f", "yes", "no", "y", "n", "1", "0"] ->
        do_coerce_value(value, :boolean, strategy)

      # Date detection (YYYY-MM-DD)
      Regex.match?(~r/^\d{4}-\d{2}-\d{2}$/, value) ->
        do_coerce_value(value, :date, strategy)

      # DateTime detection (ISO 8601-ish)
      Regex.match?(~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/, value) ->
        do_coerce_value(value, :utc_datetime, strategy)

      # UUID detection
      Regex.match?(~r/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i, value) ->
        value  # Keep as string UUID

      # JSON object detection
      String.starts_with?(value, "{") and String.ends_with?(value, "}") ->
        do_coerce_value(value, :map, strategy)

      # JSON array detection
      String.starts_with?(value, "[") and String.ends_with?(value, "]") ->
        do_coerce_value(value, :list, strategy)

      # Default: keep as string
      true -> value
    end
  end

  defp auto_detect_and_coerce(value, _strategy), do: value

  # Error handling based on strategy
  defp handle_coercion_error(value, target_type, :strict) do
    raise ArgumentError, "Cannot coerce #{inspect(value)} to #{target_type}"
  end

  defp handle_coercion_error(value, _target_type, :safe), do: value

  defp apply_coercion_safely(coercion_func, value, fallback_strategy) do
    try do
      coercion_func.(value)
    rescue
      _ -> handle_coercion_error(value, :custom, fallback_strategy)
    end
  end

  @doc """
  Get the Elixir type for a given PostgreSQL column type.
  """
  def get_elixir_type(postgres_type) do
    Map.get(@type_mappings, postgres_type, :unknown)
  end

  @doc """
  Get all supported PostgreSQL type mappings.
  """
  def supported_types() do
    @type_mappings
  end

  @doc """
  Batch coerce a list of values with their corresponding column types.

  This is more efficient than coercing values one by one when you have
  column type information for all values.
  """
  def batch_coerce(values, column_types, strategy \\ :safe, custom_coercions \\ %{}) do
    values
    |> Enum.zip(column_types)
    |> Enum.map(fn {value, col_type} ->
      coerce_value(value, col_type, strategy, custom_coercions)
    end)
  end
end
