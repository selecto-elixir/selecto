defmodule Selecto.DB.AdaptersTest do
  use ExUnit.Case, async: true

  @adapters [
    SelectoDBPostgreSQL.Adapter,
    SelectoDBMySQL.Adapter,
    SelectoDBMariaDB.Adapter,
    SelectoDBMSSQL.Adapter,
    SelectoDBSQLite.Adapter
  ]

  test "adapter modules expose required functions" do
    Enum.each(@adapters, fn adapter ->
      assert Code.ensure_loaded?(adapter)
      assert function_exported?(adapter, :name, 0)
      assert function_exported?(adapter, :connect, 1)
      assert function_exported?(adapter, :execute, 4)
      assert function_exported?(adapter, :placeholder, 1)
      assert function_exported?(adapter, :quote_identifier, 1)
      assert function_exported?(adapter, :supports?, 1)
    end)
  end

  test "adapter stream capability contract is explicit" do
    Enum.each(@adapters, fn adapter ->
      if adapter.supports?(:stream) do
        assert function_exported?(adapter, :stream, 4)
      end
    end)
  end

  test "adapter placeholder strategies are explicit" do
    assert SelectoDBPostgreSQL.Adapter.placeholder(3) |> IO.iodata_to_binary() == "$3"
    assert SelectoDBMySQL.Adapter.placeholder(3) == "?"
    assert SelectoDBMariaDB.Adapter.placeholder(3) == "?"
    assert SelectoDBSQLite.Adapter.placeholder(3) == "?"
    assert SelectoDBMSSQL.Adapter.placeholder(3) |> IO.iodata_to_binary() == "@p3"
  end

  test "adapter identifier quoting differs by backend" do
    assert SelectoDBPostgreSQL.Adapter.quote_identifier("order") == "\"order\""
    assert SelectoDBMySQL.Adapter.quote_identifier("order") == "`order`"
    assert SelectoDBMariaDB.Adapter.quote_identifier("order") == "`order`"
    assert SelectoDBSQLite.Adapter.quote_identifier("order") == "\"order\""
    assert SelectoDBMSSQL.Adapter.quote_identifier("order") == "[order]"
  end

  test "non-postgresql adapters return dependency errors when driver is unavailable" do
    mysql_result = SelectoDBMySQL.Adapter.connect([])
    mariadb_result = SelectoDBMariaDB.Adapter.connect([])
    mssql_result = SelectoDBMSSQL.Adapter.connect([])
    sqlite_result = SelectoDBSQLite.Adapter.connect([])

    if Code.ensure_loaded?(MyXQL) do
      assert match?({:ok, _}, mysql_result) or match?({:error, _}, mysql_result)
      assert match?({:ok, _}, mariadb_result) or match?({:error, _}, mariadb_result)
    else
      assert mysql_result == {:error, {:adapter_dependency_missing, :myxql}}
      assert mariadb_result == {:error, {:adapter_dependency_missing, :myxql}}
    end

    if Code.ensure_loaded?(Tds) do
      assert match?({:ok, _}, mssql_result) or match?({:error, _}, mssql_result)
    else
      assert mssql_result == {:error, {:adapter_dependency_missing, :tds}}
    end

    if Code.ensure_loaded?(Exqlite.Sqlite3) do
      assert match?({:ok, _}, sqlite_result) or match?({:error, _}, sqlite_result)
    else
      assert sqlite_result == {:error, {:adapter_dependency_missing, :exqlite}}
    end
  end

  test "postgres adapter returns invalid connection for unsupported value" do
    assert SelectoDBPostgreSQL.Adapter.execute(123, "select 1", [], []) ==
             {:error, {:invalid_connection, 123}}
  end

  test "external sqlite adapter executes simple query when dependency is available" do
    if Code.ensure_loaded?(Exqlite.Sqlite3) do
      assert {:ok, conn} = SelectoDBSQLite.Adapter.connect(database: ":memory:")

      assert {:ok, %{rows: [[1]], columns: ["value"]}} =
               SelectoDBSQLite.Adapter.execute(conn, "SELECT 1 AS value", [], [])

      _ = Exqlite.Sqlite3.close(conn)
    else
      assert SelectoDBSQLite.Adapter.execute(:invalid, "SELECT 1", [], []) ==
               {:error, {:adapter_dependency_missing, :exqlite}}
    end
  end
end
