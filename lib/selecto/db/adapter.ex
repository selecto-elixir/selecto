defmodule Selecto.DB.Adapter do
  @moduledoc """
  Behaviour contract for Selecto database adapters.

  Adapter modules are responsible for:

  - establishing a connection handle from adapter-specific options,
  - executing SQL with bound params,
  - providing adapter placeholder strategy,
  - quoting identifiers when needed by adapter-specific tooling, and
  - declaring coarse feature support.

  Streaming contract:

  - `supports?(:stream)` should return `true` only when `stream/4` is
    implemented and can produce rows for the given connection context.
  - `stream/4` is optional; adapters that do not support streaming should omit
    it and return `false` for `supports?(:stream)`.
  """

  @type connection_options :: keyword() | map() | term()
  @type connection :: term()
  @type query :: String.t() | iodata()
  @type params :: [term()]
  @type execute_options :: keyword()
  @type result :: %{required(:rows) => list(), required(:columns) => [String.t()]}
  @type stream_result ::
          {:ok, Enumerable.t()}
          | {:ok, Enumerable.t(), [String.t()]}
          | {:error, term()}

  @callback name() :: atom()
  @callback connect(connection_options()) :: {:ok, connection()} | {:error, term()}

  @callback execute(connection(), query(), params(), execute_options()) ::
              {:ok, result()} | {:error, term()}

  @callback stream(connection(), query(), params(), execute_options()) :: stream_result()

  @callback placeholder(pos_integer()) :: iodata()
  @callback quote_identifier(String.t()) :: String.t()
  @callback supports?(atom()) :: boolean()

  @optional_callbacks stream: 4
end
