defmodule SelectoDBMySQL.Adapter do
  @behaviour Selecto.DB.Adapter

  def name, do: :mysql
  def connect(_opts), do: {:error, {:adapter_dependency_missing, :myxql}}

  def execute(_connection, _query, _params, _opts),
    do: {:error, {:adapter_dependency_missing, :myxql}}

  def placeholder(_index), do: "?"
  def quote_identifier(identifier), do: "`#{String.replace(to_string(identifier), "`", "``")}`"
  def supports?(_feature), do: false
end

defmodule SelectoDBMariaDB.Adapter do
  @behaviour Selecto.DB.Adapter

  def name, do: :mariadb
  def connect(_opts), do: {:error, {:adapter_dependency_missing, :myxql}}

  def execute(_connection, _query, _params, _opts),
    do: {:error, {:adapter_dependency_missing, :myxql}}

  def placeholder(_index), do: "?"
  def quote_identifier(identifier), do: "`#{String.replace(to_string(identifier), "`", "``")}`"
  def supports?(_feature), do: false
end

defmodule SelectoDBMSSQL.Adapter do
  @behaviour Selecto.DB.Adapter

  def name, do: :mssql
  def connect(connection) when is_pid(connection) or is_atom(connection), do: {:ok, connection}
  def connect(_opts), do: {:error, {:adapter_dependency_missing, :tds}}

  def execute(connection, _query, _params, _opts)
      when is_pid(connection) or is_atom(connection) do
    {:ok, %{rows: [], columns: []}}
  end

  def execute(_connection, _query, _params, _opts),
    do: {:error, {:adapter_dependency_missing, :tds}}

  def placeholder(index), do: ["@p", Integer.to_string(index)]

  def quote_identifier(identifier) do
    escaped = String.replace(to_string(identifier), "]", "]]")
    "[#{escaped}]"
  end

  def supports?(_feature), do: false
end

defmodule SelectoDBDuckDB.Adapter do
  @behaviour Selecto.DB.Adapter

  def name, do: :duckdb
  def connect(_opts), do: {:error, {:adapter_dependency_missing, :duckdbex}}

  def execute(_connection, _query, _params, _opts),
    do: {:error, {:adapter_dependency_missing, :duckdbex}}

  def placeholder(index), do: ["$", Integer.to_string(index)]

  def quote_identifier(identifier) do
    escaped = String.replace(to_string(identifier), "\"", "\"\"")
    "\"#{escaped}\""
  end

  def supports?(_feature), do: false
end

defmodule SelectoDBSQLite.Adapter do
  @behaviour Selecto.DB.Adapter

  def name, do: :sqlite
  def connect(_opts), do: {:error, {:adapter_dependency_missing, :exqlite}}

  def execute(_connection, _query, _params, _opts),
    do: {:error, {:adapter_dependency_missing, :exqlite}}

  def placeholder(_index), do: "?"

  def quote_identifier(identifier) do
    escaped = String.replace(to_string(identifier), "\"", "\"\"")
    "\"#{escaped}\""
  end

  def supports?(_feature), do: false
end
