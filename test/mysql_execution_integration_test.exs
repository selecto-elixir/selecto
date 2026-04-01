defmodule Selecto.MySQLExecutionIntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :requires_db
  @moduletag timeout: 120_000

  @tag :mysql
  test "mysql adapter executes json_table queries" do
    with_mysql_connection(fn conn ->
      sql = """
      SELECT jt.sku, jt.qty
      FROM JSON_TABLE(
        '[{"sku":"SKU-1","qty":2},{"sku":"SKU-2","qty":1}]',
        '$[*]' COLUMNS (
          sku VARCHAR(32) PATH '$.sku',
          qty INT PATH '$.qty'
        )
      ) AS jt
      ORDER BY jt.sku
      """

      assert {:ok, %{rows: rows, columns: columns}} =
               SelectoDBMySQL.Adapter.execute(conn, sql, [], [])

      assert normalize_columns(columns) == ["sku", "qty"]

      assert Enum.map(rows, fn [sku, qty] -> {normalize_scalar(sku), normalize_scalar(qty)} end) ==
               [{"SKU-1", "2"}, {"SKU-2", "1"}]
    end)
  end

  @tag :mysql
  test "mysql adapter executes match against queries" do
    with_mysql_connection(fn conn ->
      exec!(conn, "DROP TEMPORARY TABLE IF EXISTS selecto_mysql_ft_probe")

      exec!(conn, """
      CREATE TEMPORARY TABLE selecto_mysql_ft_probe (
        id INTEGER PRIMARY KEY AUTO_INCREMENT,
        title TEXT,
        body TEXT,
        FULLTEXT KEY ft_title_body (title, body)
      ) ENGINE=InnoDB
      """)

      exec!(conn, "INSERT INTO selecto_mysql_ft_probe (title, body) VALUES (?, ?)", [
        "wireless charger",
        "portable charging pad"
      ])

      exec!(conn, "INSERT INTO selecto_mysql_ft_probe (title, body) VALUES (?, ?)", [
        "mechanical keyboard",
        "wired office keyboard"
      ])

      assert {:ok, %{rows: rows, columns: columns}} =
               SelectoDBMySQL.Adapter.execute(
                 conn,
                 """
                 SELECT id
                 FROM selecto_mysql_ft_probe
                 WHERE MATCH(title, body) AGAINST (? IN BOOLEAN MODE)
                 ORDER BY id
                 """,
                 ["+wireless"],
                 []
               )

      assert normalize_columns(columns) == ["id"]
      assert Enum.map(rows, fn [id] -> normalize_scalar(id) end) == ["1"]
    end)
  end

  @tag :mysql
  test "mysql adapter executes on duplicate key update queries" do
    with_mysql_connection(fn conn ->
      exec!(conn, "DROP TEMPORARY TABLE IF EXISTS selecto_mysql_upsert_probe")

      exec!(conn, """
      CREATE TEMPORARY TABLE selecto_mysql_upsert_probe (
        sku VARCHAR(32) PRIMARY KEY,
        name VARCHAR(255)
      )
      """)

      exec!(conn, "INSERT INTO selecto_mysql_upsert_probe (sku, name) VALUES (?, ?)", [
        "SKU-1",
        "Original"
      ])

      exec!(
        conn,
        """
        INSERT INTO selecto_mysql_upsert_probe (sku, name)
        VALUES (?, ?)
        ON DUPLICATE KEY UPDATE name = VALUES(name)
        """,
        ["SKU-1", "Updated"]
      )

      assert {:ok, %{rows: rows, columns: columns}} =
               SelectoDBMySQL.Adapter.execute(
                 conn,
                 "SELECT sku, name FROM selecto_mysql_upsert_probe ORDER BY sku",
                 [],
                 []
               )

      assert normalize_columns(columns) == ["sku", "name"]

      assert Enum.map(rows, fn [sku, name] -> {normalize_scalar(sku), normalize_scalar(name)} end) ==
               [{"SKU-1", "Updated"}]
    end)
  end

  defp with_mysql_connection(fun) when is_function(fun, 1) do
    case connect_with_retry(fn -> SelectoDBMySQL.Adapter.connect(mysql_opts()) end) do
      {:ok, conn} ->
        on_exit(fn ->
          close_connection(conn)
        end)

        fun.(conn)

      {:error, {:adapter_dependency_missing, :myxql}} ->
        assert true

      {:error, reason} ->
        flunk("live MySQL execution probes are unavailable: #{inspect(reason)}")
    end
  end

  defp exec!(conn, sql, params \\ []) do
    assert {:ok, _result} = SelectoDBMySQL.Adapter.execute(conn, sql, params, [])
  end

  defp connect_with_retry(fun, attempts \\ 60)

  defp connect_with_retry(fun, attempts) when attempts > 1 do
    case fun.() do
      {:ok, _} = ok ->
        ok

      {:error, {:adapter_dependency_missing, :myxql}} = error ->
        error

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

  defp close_connection(_), do: :ok

  defp normalize_columns(columns) do
    Enum.map(columns, fn col -> col |> to_string() |> String.downcase() end)
  end

  defp normalize_scalar(%Decimal{} = value), do: Decimal.to_string(value, :normal)
  defp normalize_scalar(value) when is_binary(value), do: value
  defp normalize_scalar(value), do: to_string(value)

  defp mysql_opts do
    [
      hostname: env("SELECTO_MYSQL_HOST", "localhost"),
      port: env_int("SELECTO_MYSQL_PORT", 3306),
      username: env("SELECTO_MYSQL_USER", "root"),
      password: env("SELECTO_MYSQL_PASSWORD", "root"),
      database: env("SELECTO_MYSQL_DATABASE", "selecto_test")
    ]
  end

  defp env(name, default), do: System.get_env(name, default)

  defp env_int(name, default) do
    name
    |> env(Integer.to_string(default))
    |> String.to_integer()
  end
end
