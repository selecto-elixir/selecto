defmodule Selecto.Database.Adapter do
  @moduledoc """
  Behavior definition for Selecto database adapters.
  
  All database adapters must implement this behavior to integrate with Selecto.
  Adapters can be built-in (like PostgreSQL) or external packages.
  """

  @type connection :: any()
  @type query_result :: %{
    rows: [[any()]],
    columns: [String.t()],
    num_rows: non_neg_integer(),
    metadata: map()
  }
  @type error :: {:error, term()}

  # Core Connection Management
  @callback connect(opts :: keyword()) :: {:ok, connection()} | error()
  @callback disconnect(connection()) :: :ok | error()
  @callback checkout(pool :: pid()) :: {:ok, connection()} | error()
  @callback checkin(pool :: pid(), connection()) :: :ok

  # Query Execution
  @callback execute(connection(), query :: String.t(), params :: list(), opts :: keyword()) :: 
    {:ok, query_result()} | error()
  @callback prepare(connection(), name :: String.t(), query :: String.t()) :: 
    {:ok, prepared :: any()} | error()
  @callback execute_prepared(connection(), prepared :: any(), params :: list(), opts :: keyword()) :: 
    {:ok, query_result()} | error()

  # Transaction Management
  @callback transaction(connection(), fun :: function(), opts :: keyword()) :: 
    {:ok, any()} | {:error, any()} | {:rollback, any()}
  @callback begin(connection(), opts :: keyword()) :: {:ok, connection()} | error()
  @callback commit(connection()) :: :ok | error()
  @callback rollback(connection()) :: :ok | error()
  @callback savepoint(connection(), name :: String.t()) :: :ok | error()
  @callback rollback_to_savepoint(connection(), name :: String.t()) :: :ok | error()

  # SQL Dialect Information
  @callback quote_identifier(identifier :: String.t()) :: String.t()
  @callback quote_string(string :: String.t()) :: String.t()
  @callback parameter_placeholder(index :: pos_integer()) :: String.t()
  @callback limit_syntax() :: :limit_offset | :top | :fetch_first | :rownum
  @callback boolean_literal(boolean()) :: String.t()

  # Feature Capabilities
  @callback supports?(feature :: atom()) :: boolean()
  @callback capabilities() :: map()
  @callback version() :: String.t() | nil

  # Type System
  @callback encode_type(value :: any(), type :: atom()) :: any()
  @callback decode_type(value :: any(), type :: atom()) :: any()
  @callback type_name(elixir_type :: atom()) :: String.t()

  # Introspection
  @callback list_tables(connection(), opts :: keyword()) :: {:ok, [String.t()]} | error()
  @callback table_exists?(connection(), table :: String.t(), opts :: keyword()) :: boolean()
  @callback describe_table(connection(), table :: String.t(), opts :: keyword()) :: 
    {:ok, map()} | error()

  # Performance & Optimization
  @callback explain(connection(), query :: String.t(), opts :: keyword()) :: 
    {:ok, String.t() | map()} | error()
  @callback analyze(connection(), table :: String.t(), opts :: keyword()) :: :ok | error()

  # Streaming Support (Optional)
  @callback stream(connection(), query :: String.t(), params :: list(), opts :: keyword()) :: 
    {:ok, Enumerable.t()} | error()
  @optional_callbacks stream: 4

  # Default implementations for optional features
  defmacro __using__(_opts) do
    quote do
      @behaviour Selecto.Database.Adapter

      # Default implementation for optional streaming
      def stream(_conn, _query, _params, _opts) do
        {:error, :not_supported}
      end

      defoverridable stream: 4
    end
  end
end