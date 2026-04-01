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
  @type introspection_options :: keyword()
  @type schema_metadata :: map()
  @type stream_result ::
          {:ok, Enumerable.t()}
          | {:ok, Enumerable.t(), [String.t()]}
          | {:error, term()}
  @type server_version_result :: {:ok, pos_integer()} | {:error, term()}

  @callback name() :: atom()
  @callback connect(connection_options()) :: {:ok, connection()} | {:error, term()}

  @callback execute(connection(), query(), params(), execute_options()) ::
              {:ok, result()} | {:error, term()}

  @callback execute_pool(term(), query(), params(), execute_options()) ::
              {:ok, term()} | {:error, term()}

  @callback execute_raw(connection(), query(), params()) :: {:ok, result()} | {:error, term()}
  @callback stream(connection(), query(), params(), execute_options()) :: stream_result()
  @callback server_version_major(connection()) :: server_version_result()
  @callback validate_connection(connection()) :: :ok | {:error, term()}
  @callback connection_info(connection()) :: map()
  @callback with_connection(term(), (term() -> term())) :: {:ok, term()} | {:error, term()}
  @callback transaction(term(), (term() -> term()), keyword()) :: {:ok, term()} | {:error, term()}
  @callback execute_repo_fallback(module(), query(), params()) ::
              {:ok, result()} | {:error, term()}
  @callback start_pool(keyword(), keyword(), atom()) :: {:ok, term()} | {:error, term()}

  @callback list_tables(connection(), introspection_options()) ::
              {:ok, [String.t()]} | {:error, term()}

  @callback list_relations(connection(), introspection_options()) ::
              {:ok, [map()]} | {:error, term()}

  @callback introspect_table(connection(), String.t(), introspection_options()) ::
              {:ok, schema_metadata()} | {:error, term()}

  @callback placeholder(pos_integer()) :: iodata()
  @callback quote_identifier(String.t()) :: String.t()
  @callback format_datetime(iodata(), String.t()) :: iodata()
  @callback rollup_sql(iodata()) :: iodata()
  @callback rollup_literal_order(pos_integer()) :: iodata() | String.t()
  @callback rollup_sort_fix(connection()) :: boolean()
  @callback supports?(atom()) :: boolean()

  @optional_callbacks stream: 4,
                      server_version_major: 1,
                      validate_connection: 1,
                      connection_info: 1,
                      with_connection: 2,
                      transaction: 3,
                      execute_pool: 4,
                      execute_raw: 3,
                      execute_repo_fallback: 3,
                      start_pool: 3,
                      list_tables: 2,
                      list_relations: 2,
                      introspect_table: 3,
                      format_datetime: 2,
                      rollup_sql: 1,
                      rollup_literal_order: 1,
                      rollup_sort_fix: 1
end
