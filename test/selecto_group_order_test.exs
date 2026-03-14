defmodule Selecto.GroupOrderTest do
  use ExUnit.Case

  defmodule Pg18MockRepo do
    def query("show server_version_num", []) do
      {:ok, %{rows: [["180001"]]}}
    end
  end

  defmodule NoRollupAdapter do
    def connect(connection), do: {:ok, connection}
    def supports?(:rollup), do: false
    def supports?(_feature), do: false
    def placeholder(_index), do: "?"
    def quote_identifier(identifier), do: to_string(identifier)
  end

  defmodule MySQLRollupAdapter do
    def name, do: :mysql
    def connect(connection), do: {:ok, connection}
    def supports?(:rollup), do: true
    def supports?(_feature), do: false
    def placeholder(_index), do: "?"
    def quote_identifier(identifier), do: to_string(identifier)
  end

  defmodule MariaDBRollupAdapter do
    def name, do: :mariadb
    def connect(connection), do: {:ok, connection}
    def supports?(:rollup), do: true
    def supports?(_feature), do: false
    def placeholder(_index), do: "?"
    def quote_identifier(identifier), do: to_string(identifier)
  end

  defmodule MSSQLRollupAdapter do
    def name, do: :mssql
    def connect(connection), do: {:ok, connection}
    def supports?(:rollup), do: true
    def supports?(_feature), do: false
    def placeholder(index), do: ["@p", Integer.to_string(index)]
    def quote_identifier(identifier), do: to_string(identifier)
  end

  test "GROUP BY and ORDER BY with new iodata parameterization (phase 2)" do
    # Domain configuration
    domain = %{
      source: %{
        source_table: "users",
        primary_key: :id,
        fields: [:id, :name, :email, :age],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          email: %{type: :string},
          age: %{type: :integer}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      name: "User"
    }

    selecto = Selecto.configure(domain, :mock_connection)

    # Test with GROUP BY and ORDER BY
    selecto =
      selecto
      |> Selecto.select([{:count}])
      |> Selecto.group_by(["age"])
      |> Selecto.order_by([{"age", :desc}])

    {sql, aliases, params} = Selecto.gen_sql(selecto, [])

    # Verify SQL structure
    assert String.contains?(sql, "select")
    assert String.contains?(sql, "count(*)")
    assert String.contains?(sql, "group by")
    assert String.contains?(sql, "order by")
    assert String.contains?(sql, "selecto_root.age")
    assert String.contains?(sql, "desc")

    # Verify no legacy sentinel remains
    refute String.contains?(sql, "^SelectoParam^")

    # Verify params structure (should be empty for this query)
    assert is_list(params)

    # Verify aliases structure  
    assert is_list(aliases)
    # count(*)
    assert length(aliases) == 1
  end

  test "ROLLUP functionality preserves special handling" do
    domain = %{
      source: %{
        source_table: "sales",
        primary_key: :id,
        fields: [:id, :region, :amount],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          region: %{type: :string},
          amount: %{type: :decimal}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      name: "Sales"
    }

    selecto = Selecto.configure(domain, :mock_connection)

    # Test with ROLLUP - this should trigger the special case handling
    selecto =
      selecto
      |> Selecto.select([{:sum, "amount"}])
      |> Selecto.group_by(rollup: ["region"])
      |> Selecto.order_by([{"region", :asc}])

    {sql, _aliases, _params} = Selecto.gen_sql(selecto, [])

    # Verify ROLLUP special case handling
    assert String.contains?(sql, "rollup")
    assert String.contains?(sql, "select * from (")
    assert String.contains?(sql, ") as rollupfix")

    # Verify no legacy sentinel remains
    refute String.contains?(sql, "^SelectoParam^")
  end

  test "ROLLUP compatibility wrapper is auto-disabled for PostgreSQL 18+" do
    domain = %{
      source: %{
        source_table: "sales",
        primary_key: :id,
        fields: [:id, :region, :amount],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          region: %{type: :string},
          amount: %{type: :decimal}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      name: "Sales"
    }

    selecto = Selecto.configure(domain, Pg18MockRepo)

    selecto =
      selecto
      |> Selecto.select([{:sum, "amount"}])
      |> Selecto.group_by(rollup: ["region"])
      |> Selecto.order_by([{"region", :asc}])

    {sql, _aliases, _params} = Selecto.gen_sql(selecto, [])

    assert String.contains?(sql, "rollup")
    refute String.contains?(sql, "select * from (")
    refute String.contains?(sql, ") as rollupfix")
  end

  test "ROLLUP falls back to plain grouping for adapters without rollup support" do
    domain = %{
      source: %{
        source_table: "sales",
        primary_key: :id,
        fields: [:id, :region, :amount],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          region: %{type: :string},
          amount: %{type: :decimal}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      name: "Sales"
    }

    selecto =
      Selecto.configure(domain, :mock_connection, adapter: NoRollupAdapter, validate: false)

    selecto =
      selecto
      |> Selecto.select([{:sum, "amount"}])
      |> Selecto.group_by(rollup: ["region"])
      |> Selecto.order_by([{"region", :asc}])

    {sql, _aliases, _params} = Selecto.gen_sql(selecto, [])

    assert String.contains?(String.downcase(sql), "group by")
    refute String.contains?(String.downcase(sql), "rollup")
    refute String.contains?(sql, ") as rollupfix")
  end

  test "MySQL rollup uses WITH ROLLUP syntax and derived ordering" do
    domain = %{
      source: %{
        source_table: "sales",
        primary_key: :id,
        fields: [:id, :region, :amount],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          region: %{type: :string},
          amount: %{type: :decimal}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      name: "Sales"
    }

    selecto =
      Selecto.configure(domain, :mock_connection, adapter: MySQLRollupAdapter, validate: false)

    selecto =
      selecto
      |> Selecto.select(["region", {:sum, "amount"}])
      |> Selecto.group_by(rollup: ["region"])
      |> Selecto.order_by([{"region", :asc}])

    {sql, _aliases, _params} = Selecto.gen_sql(selecto, [])
    normalized_sql = String.replace(sql, ~r/\s+/, " ")

    assert String.contains?(
             String.downcase(normalized_sql),
             "group by selecto_root.region with rollup"
           )

    assert String.contains?(normalized_sql, "select * from (")
    assert String.contains?(normalized_sql, ") as rollupfix")
    refute String.contains?(String.downcase(normalized_sql), "nulls")
  end

  test "MariaDB rollup uses WITH ROLLUP syntax and derived ordering" do
    domain = %{
      source: %{
        source_table: "sales",
        primary_key: :id,
        fields: [:id, :region, :amount],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          region: %{type: :string},
          amount: %{type: :decimal}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      name: "Sales"
    }

    selecto =
      Selecto.configure(domain, :mock_connection, adapter: MariaDBRollupAdapter, validate: false)

    selecto =
      selecto
      |> Selecto.select(["region", {:sum, "amount"}])
      |> Selecto.group_by(rollup: ["region"])
      |> Selecto.order_by([{"region", :asc}])

    {sql, _aliases, _params} = Selecto.gen_sql(selecto, [])
    normalized_sql = String.replace(sql, ~r/\s+/, " ")

    assert String.contains?(
             String.downcase(normalized_sql),
             "group by selecto_root.region with rollup"
           )

    assert String.contains?(normalized_sql, "select * from (")
    assert String.contains?(normalized_sql, ") as rollupfix")
    refute String.contains?(String.downcase(normalized_sql), "nulls")
  end

  test "MSSQL rollup keeps ISO syntax without NULLS FIRST ordering" do
    domain = %{
      source: %{
        source_table: "sales",
        primary_key: :id,
        fields: [:id, :region, :amount],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          region: %{type: :string},
          amount: %{type: :decimal}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      name: "Sales"
    }

    selecto =
      Selecto.configure(domain, :mock_connection, adapter: MSSQLRollupAdapter, validate: false)

    selecto =
      selecto
      |> Selecto.select(["region", {:sum, "amount"}])
      |> Selecto.group_by(rollup: ["region"])
      |> Selecto.order_by([{"region", :asc}])

    {sql, _aliases, _params} = Selecto.gen_sql(selecto, [])
    normalized_sql = String.replace(sql, ~r/\s+/, " ")

    assert String.contains?(
             String.downcase(normalized_sql),
             "group by rollup( selecto_root.region )"
           )

    refute String.contains?(String.downcase(normalized_sql), "nulls")
  end
end
