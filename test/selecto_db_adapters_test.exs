defmodule Selecto.DB.AdaptersTest do
  use ExUnit.Case, async: true

  @adapters [
    Selecto.DB.PostgreSQL,
    Selecto.DB.MySQL,
    Selecto.DB.MariaDB,
    Selecto.DB.MSSQL,
    Selecto.DB.SQLite
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

  test "adapter placeholder strategies are explicit" do
    assert Selecto.DB.PostgreSQL.placeholder(3) |> IO.iodata_to_binary() == "$3"
    assert Selecto.DB.MySQL.placeholder(3) == "?"
    assert Selecto.DB.MariaDB.placeholder(3) == "?"
    assert Selecto.DB.SQLite.placeholder(3) == "?"
    assert Selecto.DB.MSSQL.placeholder(3) |> IO.iodata_to_binary() == "@p3"
  end

  test "adapter identifier quoting differs by backend" do
    assert Selecto.DB.PostgreSQL.quote_identifier("order") == "\"order\""
    assert Selecto.DB.MySQL.quote_identifier("order") == "`order`"
    assert Selecto.DB.MariaDB.quote_identifier("order") == "`order`"
    assert Selecto.DB.SQLite.quote_identifier("order") == "\"order\""
    assert Selecto.DB.MSSQL.quote_identifier("order") == "[order]"
  end

  test "non-postgresql adapters return dependency errors when driver is unavailable" do
    mysql_result = Selecto.DB.MySQL.connect([])
    mariadb_result = Selecto.DB.MariaDB.connect([])
    mssql_result = Selecto.DB.MSSQL.connect([])
    sqlite_result = Selecto.DB.SQLite.connect([])

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

    assert sqlite_result == {:error, {:adapter_dependency_missing, :exqlite}}
  end

  test "postgres adapter returns invalid connection for unsupported value" do
    assert Selecto.DB.PostgreSQL.execute(123, "select 1", [], []) ==
             {:error, {:invalid_connection, 123}}
  end
end
