defmodule SelectoDBMySQL.Adapter do
  @behaviour Selecto.DB.Adapter

  def name, do: :mysql
  def connect(_opts), do: {:error, {:adapter_dependency_missing, :myxql}}

  def execute(_connection, _query, _params, _opts),
    do: {:error, {:adapter_dependency_missing, :myxql}}

  def placeholder(_index), do: "?"
  def quote_identifier(identifier), do: "`#{String.replace(to_string(identifier), "`", "``")}`"
  def supports?(:rollup_with_rollup), do: true
  def supports?(:json_table), do: true
  def supports?(:text_search), do: true
  def supports?(:text_search_multi_field), do: true
  def supports?(:text_search_boolean), do: true
  def supports?(:text_search_boolean_mode), do: true
  def supports?(:text_search_query_expansion), do: true
  def supports?(:text_search_query_expansion_mode), do: true
  def supports?(:match_against), do: true
  def supports?(:on_duplicate_key_update), do: true
  def supports?(_feature), do: false

  def rollup_sql(grouped_clauses), do: [grouped_clauses, " with rollup"]
end

defmodule SelectoDBMariaDB.Adapter do
  @behaviour Selecto.DB.Adapter

  def name, do: :mariadb
  def connect(_opts), do: {:error, {:adapter_dependency_missing, :myxql}}

  def execute(_connection, _query, _params, _opts),
    do: {:error, {:adapter_dependency_missing, :myxql}}

  def placeholder(_index), do: "?"
  def quote_identifier(identifier), do: "`#{String.replace(to_string(identifier), "`", "``")}`"
  def supports?(:rollup_with_rollup), do: true
  def supports?(_feature), do: false

  def rollup_sql(grouped_clauses), do: [grouped_clauses, " with rollup"]
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

  def supports?(:offset_fetch_pagination), do: true
  def supports?(:requires_order_for_pagination), do: true
  def supports?(:lateral_join), do: true
  def supports?(:apply_join), do: true
  def supports?(_feature), do: false

  def format_datetime(sel_iodata, "YYYY-MM-DD") do
    ["CONVERT(varchar(10), CAST(", sel_iodata, " AS datetime2), 23)"]
  end

  def format_datetime(sel_iodata, "YYYY-MM") do
    ["LEFT(CONVERT(varchar(10), CAST(", sel_iodata, " AS datetime2), 23), 7)"]
  end

  def format_datetime(sel_iodata, "YYYY") do
    ["FORMAT(CAST(", sel_iodata, " AS datetime2), 'yyyy')"]
  end

  def format_datetime(sel_iodata, "YYYY-WW") do
    [
      "CONCAT(FORMAT(CAST(",
      sel_iodata,
      " AS datetime2), 'yyyy'), '-', RIGHT('0' + CAST(DATEPART(ISO_WEEK, CAST(",
      sel_iodata,
      " AS datetime2)) AS varchar(2)), 2))"
    ]
  end

  def format_datetime(sel_iodata, "YYYY-Q") do
    [
      "CONCAT(FORMAT(CAST(",
      sel_iodata,
      " AS datetime2), 'yyyy'), '-', DATEPART(QUARTER, CAST(",
      sel_iodata,
      " AS datetime2)))"
    ]
  end

  def format_datetime(sel_iodata, "MM") do
    ["FORMAT(CAST(", sel_iodata, " AS datetime2), 'MM')"]
  end

  def format_datetime(sel_iodata, "DD") do
    ["FORMAT(CAST(", sel_iodata, " AS datetime2), 'dd')"]
  end

  def format_datetime(sel_iodata, "D") do
    ["CAST(DATEPART(WEEKDAY, CAST(", sel_iodata, " AS datetime2)) AS varchar(2))"]
  end

  def format_datetime(sel_iodata, "HH24") do
    ["FORMAT(CAST(", sel_iodata, " AS datetime2), 'HH')"]
  end

  def format_datetime(sel_iodata, _format) do
    ["CONVERT(varchar(33), CAST(", sel_iodata, " AS datetime2), 126)"]
  end
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

  def format_datetime(sel_iodata, "YYYY-MM-DD") do
    ["strftime(CAST(", sel_iodata, " AS TIMESTAMP), '%Y-%m-%d')"]
  end

  def format_datetime(sel_iodata, "YYYY-MM") do
    ["strftime(CAST(", sel_iodata, " AS TIMESTAMP), '%Y-%m')"]
  end

  def format_datetime(sel_iodata, "YYYY") do
    ["strftime(CAST(", sel_iodata, " AS TIMESTAMP), '%Y')"]
  end

  def format_datetime(sel_iodata, "YYYY-WW") do
    ["strftime(CAST(", sel_iodata, " AS TIMESTAMP), '%G-%V')"]
  end

  def format_datetime(sel_iodata, "YYYY-Q") do
    [
      "strftime(CAST(",
      sel_iodata,
      " AS TIMESTAMP), '%Y') || '-' || CAST(quarter(CAST(",
      sel_iodata,
      " AS TIMESTAMP)) AS VARCHAR)"
    ]
  end

  def format_datetime(sel_iodata, "MM") do
    ["strftime(CAST(", sel_iodata, " AS TIMESTAMP), '%m')"]
  end

  def format_datetime(sel_iodata, "DD") do
    ["strftime(CAST(", sel_iodata, " AS TIMESTAMP), '%d')"]
  end

  def format_datetime(sel_iodata, "D") do
    ["strftime(CAST(", sel_iodata, " AS TIMESTAMP), '%u')"]
  end

  def format_datetime(sel_iodata, "HH24") do
    ["strftime(CAST(", sel_iodata, " AS TIMESTAMP), '%H')"]
  end

  def format_datetime(sel_iodata, _format) do
    ["CAST(", sel_iodata, " AS VARCHAR)"]
  end
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

  def supports?(:json_rowset), do: true
  def supports?(_feature), do: false
end
