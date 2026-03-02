defmodule Selecto.DB.Adapter do
  @moduledoc """
  Behaviour contract for Selecto database adapters.

  Adapter modules are responsible for:

  - establishing a connection handle from adapter-specific options,
  - executing SQL with bound params,
  - providing adapter placeholder strategy,
  - quoting identifiers when needed by adapter-specific tooling, and
  - declaring coarse feature support.
  """

  @type connection_options :: keyword() | map() | term()
  @type connection :: term()
  @type query :: String.t() | iodata()
  @type params :: [term()]
  @type execute_options :: keyword()
  @type result :: %{required(:rows) => list(), required(:columns) => [String.t()]}

  @callback name() :: atom()
  @callback connect(connection_options()) :: {:ok, connection()} | {:error, term()}

  @callback execute(connection(), query(), params(), execute_options()) ::
              {:ok, result()} | {:error, term()}

  @callback placeholder(pos_integer()) :: iodata()
  @callback quote_identifier(String.t()) :: String.t()
  @callback supports?(atom()) :: boolean()
end
