defmodule Selecto.Database.Types do
  @moduledoc """
  Type system interface for database adapters.
  
  Provides type mapping and conversion between Elixir types and database types.
  Each adapter can customize type handling while maintaining a consistent interface.
  """

  @type elixir_type :: :string | :integer | :float | :boolean | :date | :time | 
                       :datetime | :naive_datetime | :utc_datetime | :decimal | 
                       :binary | :uuid | :json | :array | :map | atom()
  
  @type db_type :: String.t()
  @type cast_fun :: (any() -> any())

  @doc """
  Default type mappings for PostgreSQL (built-in).
  """
  @spec postgresql_types() :: %{elixir_type() => db_type()}
  def postgresql_types do
    %{
      # Basic types
      string: "text",
      text: "text",
      varchar: "varchar",
      char: "char",
      integer: "integer",
      bigint: "bigint",
      smallint: "smallint",
      float: "double precision",
      decimal: "decimal",
      numeric: "numeric",
      boolean: "boolean",
      
      # Date/Time types
      date: "date",
      time: "time",
      time_with_tz: "time with time zone",
      datetime: "timestamp",
      naive_datetime: "timestamp without time zone",
      utc_datetime: "timestamp with time zone",
      
      # Binary types
      binary: "bytea",
      bit: "bit",
      
      # Special types
      uuid: "uuid",
      json: "json",
      jsonb: "jsonb",
      array: "array",
      inet: "inet",
      cidr: "cidr",
      macaddr: "macaddr",
      
      # Geometric types
      point: "point",
      line: "line",
      box: "box",
      path: "path",
      polygon: "polygon",
      circle: "circle",
      
      # Range types
      int4range: "int4range",
      int8range: "int8range",
      numrange: "numrange",
      tsrange: "tsrange",
      tstzrange: "tstzrange",
      daterange: "daterange"
    }
  end

  @doc """
  Maps an Elixir type to a database type using the adapter's type system.
  """
  @spec to_db_type(module() | map(), elixir_type()) :: {:ok, db_type()} | {:error, :unknown_type}
  def to_db_type(adapter, elixir_type) when is_atom(adapter) do
    if function_exported?(adapter, :type_name, 1) do
      {:ok, adapter.type_name(elixir_type)}
    else
      {:error, :unknown_type}
    end
  end

  def to_db_type(type_map, elixir_type) when is_map(type_map) do
    case Map.get(type_map, elixir_type) do
      nil -> {:error, :unknown_type}
      db_type -> {:ok, db_type}
    end
  end

  @doc """
  Encodes an Elixir value for database storage.
  """
  @spec encode(module(), any(), elixir_type()) :: {:ok, any()} | {:error, term()}
  def encode(adapter, value, type) when is_atom(adapter) do
    if function_exported?(adapter, :encode_type, 2) do
      try do
        {:ok, adapter.encode_type(value, type)}
      rescue
        error -> {:error, error}
      end
    else
      # Default encoding
      default_encode(value, type)
    end
  end

  @doc """
  Decodes a database value to Elixir type.
  """
  @spec decode(module(), any(), elixir_type()) :: {:ok, any()} | {:error, term()}
  def decode(adapter, value, type) when is_atom(adapter) do
    if function_exported?(adapter, :decode_type, 2) do
      try do
        {:ok, adapter.decode_type(value, type)}
      rescue
        error -> {:error, error}
      end
    else
      # Default decoding
      default_decode(value, type)
    end
  end

  @doc """
  Returns common type aliases across databases.
  """
  @spec type_aliases() :: map()
  def type_aliases do
    %{
      # String aliases
      "varchar" => :varchar,
      "character varying" => :varchar,
      "char" => :char,
      "character" => :char,
      "text" => :string,
      "clob" => :text,
      "longtext" => :text,
      
      # Numeric aliases
      "int" => :integer,
      "int4" => :integer,
      "int8" => :bigint,
      "int2" => :smallint,
      "smallserial" => :smallint,
      "serial" => :integer,
      "bigserial" => :bigint,
      "double precision" => :float,
      "float8" => :float,
      "float4" => :float,
      "real" => :float,
      "number" => :numeric,
      
      # Boolean aliases
      "bool" => :boolean,
      "bit(1)" => :boolean,
      "tinyint(1)" => :boolean,
      
      # Date/Time aliases
      "timestamp" => :datetime,
      "timestamptz" => :utc_datetime,
      "datetime" => :naive_datetime,
      
      # Binary aliases
      "blob" => :binary,
      "bytea" => :binary,
      "varbinary" => :binary,
      "image" => :binary,
      
      # JSON aliases
      "jsonb" => :json,
      "json" => :json
    }
  end

  @doc """
  Validates if a type is supported by an adapter.
  """
  @spec supported_type?(module() | map(), elixir_type()) :: boolean()
  def supported_type?(adapter, type) when is_atom(adapter) do
    case to_db_type(adapter, type) do
      {:ok, _} -> true
      _ -> false
    end
  end

  def supported_type?(type_map, type) when is_map(type_map) do
    Map.has_key?(type_map, type)
  end

  @doc """
  Returns type casting functions for common conversions.
  """
  @spec cast_functions() :: %{elixir_type() => cast_fun()}
  def cast_functions do
    %{
      string: &to_string/1,
      integer: &cast_to_integer/1,
      float: &cast_to_float/1,
      boolean: &cast_to_boolean/1,
      date: &cast_to_date/1,
      time: &cast_to_time/1,
      datetime: &cast_to_datetime/1,
      decimal: &cast_to_decimal/1,
      uuid: &cast_to_uuid/1,
      json: &cast_to_json/1,
      array: &cast_to_array/1
    }
  end

  @doc """
  Infers Elixir type from a database type string.
  """
  @spec infer_elixir_type(String.t()) :: elixir_type()
  def infer_elixir_type(db_type) do
    normalized = db_type |> String.downcase() |> String.split("(") |> List.first()
    
    case Map.get(type_aliases(), normalized) do
      nil -> infer_by_pattern(normalized)
      type -> type
    end
  end

  # Private functions

  defp default_encode(value, :boolean) when is_boolean(value), do: {:ok, value}
  defp default_encode(value, :string) when is_binary(value), do: {:ok, value}
  defp default_encode(value, :integer) when is_integer(value), do: {:ok, value}
  defp default_encode(value, :float) when is_float(value), do: {:ok, value}
  defp default_encode(%Date{} = value, :date), do: {:ok, value}
  defp default_encode(%Time{} = value, :time), do: {:ok, value}
  defp default_encode(%DateTime{} = value, :datetime), do: {:ok, value}
  defp default_encode(%NaiveDateTime{} = value, :naive_datetime), do: {:ok, value}
  defp default_encode(%Decimal{} = value, :decimal), do: {:ok, value}
  
  defp default_encode(value, :json) when is_map(value) or is_list(value) do
    case Jason.encode(value) do
      {:ok, json} -> {:ok, json}
      error -> error
    end
  end
  
  defp default_encode(value, :array) when is_list(value), do: {:ok, value}
  defp default_encode(value, :uuid) when is_binary(value), do: {:ok, value}
  defp default_encode(value, :binary) when is_binary(value), do: {:ok, value}
  
  defp default_encode(nil, _), do: {:ok, nil}
  defp default_encode(value, type), do: {:error, {:cannot_encode, value, type}}

  defp default_decode(value, :boolean) when is_boolean(value), do: {:ok, value}
  defp default_decode(1, :boolean), do: {:ok, true}
  defp default_decode(0, :boolean), do: {:ok, false}
  defp default_decode("1", :boolean), do: {:ok, true}
  defp default_decode("0", :boolean), do: {:ok, false}
  defp default_decode("t", :boolean), do: {:ok, true}
  defp default_decode("f", :boolean), do: {:ok, false}
  defp default_decode("true", :boolean), do: {:ok, true}
  defp default_decode("false", :boolean), do: {:ok, false}
  
  defp default_decode(value, :string) when is_binary(value), do: {:ok, value}
  defp default_decode(value, :integer) when is_integer(value), do: {:ok, value}
  defp default_decode(value, :float) when is_float(value), do: {:ok, value}
  defp default_decode(value, :float) when is_integer(value), do: {:ok, value * 1.0}
  
  defp default_decode(value, :json) when is_binary(value) do
    Jason.decode(value)
  end
  defp default_decode(value, :json) when is_map(value) or is_list(value), do: {:ok, value}
  
  defp default_decode(value, :array) when is_list(value), do: {:ok, value}
  defp default_decode(nil, _), do: {:ok, nil}
  defp default_decode(value, type), do: {:error, {:cannot_decode, value, type}}

  defp cast_to_integer(value) when is_integer(value), do: value
  defp cast_to_integer(value) when is_binary(value), do: String.to_integer(value)
  defp cast_to_integer(value) when is_float(value), do: trunc(value)
  defp cast_to_integer(_), do: nil

  defp cast_to_float(value) when is_float(value), do: value
  defp cast_to_float(value) when is_integer(value), do: value * 1.0
  defp cast_to_float(value) when is_binary(value), do: String.to_float(value)
  defp cast_to_float(_), do: nil

  defp cast_to_boolean(true), do: true
  defp cast_to_boolean(false), do: false
  defp cast_to_boolean(1), do: true
  defp cast_to_boolean(0), do: false
  defp cast_to_boolean("true"), do: true
  defp cast_to_boolean("false"), do: false
  defp cast_to_boolean("1"), do: true
  defp cast_to_boolean("0"), do: false
  defp cast_to_boolean(_), do: nil

  defp cast_to_date(%Date{} = date), do: date
  defp cast_to_date(value) when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, date} -> date
      _ -> nil
    end
  end
  defp cast_to_date(_), do: nil

  defp cast_to_time(%Time{} = time), do: time
  defp cast_to_time(value) when is_binary(value) do
    case Time.from_iso8601(value) do
      {:ok, time} -> time
      _ -> nil
    end
  end
  defp cast_to_time(_), do: nil

  defp cast_to_datetime(%DateTime{} = dt), do: dt
  defp cast_to_datetime(%NaiveDateTime{} = ndt) do
    DateTime.from_naive!(ndt, "Etc/UTC")
  end
  defp cast_to_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, dt, _} -> dt
      _ -> nil
    end
  end
  defp cast_to_datetime(_), do: nil

  defp cast_to_decimal(%Decimal{} = decimal), do: decimal
  defp cast_to_decimal(value) when is_integer(value), do: Decimal.new(value)
  defp cast_to_decimal(value) when is_float(value), do: Decimal.from_float(value)
  defp cast_to_decimal(value) when is_binary(value), do: Decimal.new(value)
  defp cast_to_decimal(_), do: nil

  defp cast_to_uuid(value) when is_binary(value), do: value
  defp cast_to_uuid(_), do: nil

  defp cast_to_json(value) when is_map(value) or is_list(value), do: value
  defp cast_to_json(value) when is_binary(value) do
    case Jason.decode(value) do
      {:ok, decoded} -> decoded
      _ -> nil
    end
  end
  defp cast_to_json(_), do: nil

  defp cast_to_array(value) when is_list(value), do: value
  defp cast_to_array(_), do: nil

  defp infer_by_pattern(normalized) do
    cond do
      String.contains?(normalized, "char") -> :string
      String.contains?(normalized, "text") -> :string
      String.contains?(normalized, "int") -> :integer
      String.contains?(normalized, "float") or String.contains?(normalized, "double") -> :float
      String.contains?(normalized, "decimal") or String.contains?(normalized, "numeric") -> :decimal
      String.contains?(normalized, "bool") -> :boolean
      String.contains?(normalized, "date") -> :date
      String.contains?(normalized, "time") -> :datetime
      String.contains?(normalized, "json") -> :json
      String.contains?(normalized, "uuid") -> :uuid
      String.contains?(normalized, "binary") or String.contains?(normalized, "blob") -> :binary
      true -> :string
    end
  end
end