defmodule Selecto.CrossDBBaselineTest do
  use ExUnit.Case, async: false

  @moduletag :requires_db
  @moduletag timeout: 120_000

  @tag :postgres
  test "postgres adapter executes baseline query" do
    assert {:ok, conn} =
             connect_with_retry(fn -> Selecto.DB.PostgreSQL.connect(postgres_opts()) end)

    on_exit(fn ->
      close_connection(conn)
    end)

    assert_single_value_query(Selecto.DB.PostgreSQL, conn, "SELECT 1 AS value")
  end

  @tag :mysql
  test "mysql adapter executes baseline query" do
    assert {:ok, conn} = connect_with_retry(fn -> Selecto.DB.MySQL.connect(mysql_opts()) end)

    on_exit(fn ->
      close_connection(conn)
    end)

    assert_single_value_query(Selecto.DB.MySQL, conn, "SELECT 1 AS value")
  end

  @tag :mariadb
  test "mariadb adapter executes baseline query" do
    assert {:ok, conn} = connect_with_retry(fn -> Selecto.DB.MariaDB.connect(mariadb_opts()) end)

    on_exit(fn ->
      close_connection(conn)
    end)

    assert_single_value_query(Selecto.DB.MariaDB, conn, "SELECT 1 AS value")
  end

  @tag :mssql
  test "mssql adapter executes baseline query" do
    assert {:ok, conn} = connect_with_retry(fn -> Selecto.DB.MSSQL.connect(mssql_opts()) end)

    on_exit(fn ->
      close_connection(conn)
    end)

    assert_single_value_query(Selecto.DB.MSSQL, conn, "SELECT CAST(1 AS INT) AS value")
  end

  @tag :sqlite
  test "sqlite adapter executes baseline query" do
    assert {:ok, conn} = Selecto.DB.SQLite.connect(sqlite_opts())

    on_exit(fn ->
      close_connection(conn)
    end)

    assert_single_value_query(Selecto.DB.SQLite, conn, "SELECT 1 AS value")
  end

  defp assert_single_value_query(adapter, conn, sql) do
    assert {:ok, %{rows: rows, columns: columns}} = adapter.execute(conn, sql, [], [])
    assert [[value]] = rows
    assert normalize_scalar(value) == "1"
    assert [column] = columns
    assert String.downcase(to_string(column)) == "value"
  end

  defp connect_with_retry(fun, attempts \\ 60)

  defp connect_with_retry(fun, attempts) when attempts > 1 do
    case fun.() do
      {:ok, _} = ok ->
        ok

      {:error, _reason} ->
        Process.sleep(1_000)
        connect_with_retry(fun, attempts - 1)
    end
  end

  defp connect_with_retry(fun, 1), do: fun.()

  defp close_connection(conn) when is_pid(conn) do
    if Process.alive?(conn) do
      Process.exit(conn, :normal)
    end

    :ok
  end

  defp close_connection(conn) when is_reference(conn) do
    if Code.ensure_loaded?(Exqlite.Sqlite3) and function_exported?(Exqlite.Sqlite3, :close, 1) do
      _ = Exqlite.Sqlite3.close(conn)
    end

    :ok
  end

  defp close_connection(_), do: :ok

  defp normalize_scalar(%Decimal{} = value), do: Decimal.to_string(value, :normal)
  defp normalize_scalar(value) when is_binary(value), do: value
  defp normalize_scalar(value), do: to_string(value)

  defp postgres_opts do
    [
      hostname: env("SELECTO_POSTGRES_HOST", "localhost"),
      port: env_int("SELECTO_POSTGRES_PORT", 5432),
      username: env("SELECTO_POSTGRES_USER", "postgres"),
      password: env("SELECTO_POSTGRES_PASSWORD", "postgres"),
      database: env("SELECTO_POSTGRES_DATABASE", "selecto_test")
    ]
  end

  defp mysql_opts do
    [
      hostname: env("SELECTO_MYSQL_HOST", "localhost"),
      port: env_int("SELECTO_MYSQL_PORT", 3306),
      username: env("SELECTO_MYSQL_USER", "root"),
      password: env("SELECTO_MYSQL_PASSWORD", "root"),
      database: env("SELECTO_MYSQL_DATABASE", "selecto_test")
    ]
  end

  defp mariadb_opts do
    [
      hostname: env("SELECTO_MARIADB_HOST", "localhost"),
      port: env_int("SELECTO_MARIADB_PORT", 3306),
      username: env("SELECTO_MARIADB_USER", "root"),
      password: env("SELECTO_MARIADB_PASSWORD", "root"),
      database: env("SELECTO_MARIADB_DATABASE", "selecto_test")
    ]
  end

  defp mssql_opts do
    [
      hostname: env("SELECTO_MSSQL_HOST", "localhost"),
      port: env_int("SELECTO_MSSQL_PORT", 1433),
      username: env("SELECTO_MSSQL_USER", "sa"),
      password: env("SELECTO_MSSQL_PASSWORD", "StrongPass123!"),
      database: env("SELECTO_MSSQL_DATABASE", "master"),
      ssl: false
    ]
  end

  defp sqlite_opts do
    [
      database: env("SELECTO_SQLITE_DATABASE", ":memory:")
    ]
  end

  defp env(name, default), do: System.get_env(name, default)

  defp env_int(name, default) do
    name
    |> env(Integer.to_string(default))
    |> String.to_integer()
  end
end
