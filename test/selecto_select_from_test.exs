defmodule Selecto.SelectFromTest do
  use ExUnit.Case

  test "SELECT with iodata parameterization (phase 3)" do
    # Domain configuration with simple fields
    domain = %{
      source: %{
        source_table: "users",
        primary_key: :id,
        fields: [:id, :name, :email, :active],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string}, 
          email: %{type: :string},
          active: %{type: :boolean}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      name: "User"
    }

    selecto = Selecto.configure(domain, :mock_connection)

    # Test basic field selection
    selecto_basic = 
      selecto
      |> Selecto.select(["name", "email"])

    {sql, aliases, params} = Selecto.gen_sql(selecto_basic, [])

    # Verify SQL structure
    assert String.contains?(sql, "select")
    assert String.contains?(sql, "\"selecto_root\".\"name\"")
    assert String.contains?(sql, "\"selecto_root\".\"email\"")
    assert String.contains?(sql, "from users")
    
    # Verify no legacy sentinel remains
    refute String.contains?(sql, "^SelectoParam^")
    
    # Verify structure
    assert is_list(params)
    assert is_list(aliases)
    assert length(aliases) == 2  # name, email
  end

  test "SELECT with literal values and parameterization" do
    domain = %{
      source: %{
        source_table: "products",
        primary_key: :id,
        fields: [:id, :name, :price],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          price: %{type: :decimal}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      name: "Product"
    }

    selecto = Selecto.configure(domain, :mock_connection)

    # Test with literal values that should be parameterized
    selecto_literals = 
      selecto
      |> Selecto.select([
        "name",
        {:literal, 100},
        {:literal, "test_string"}
      ])

    {sql, aliases, params} = Selecto.gen_sql(selecto_literals, [])

    # Verify SQL contains parameters
    assert String.contains?(sql, "$")  # Parameter placeholders
    assert String.contains?(sql, "select")
    assert String.contains?(sql, "\"selecto_root\".\"name\"")
    
    # Verify parameters are present (should contain our values)
    assert 100 in params
    assert "test_string" in params
    
    # Verify no legacy sentinel remains
    refute String.contains?(sql, "^SelectoParam^")
    
    # Verify aliases structure
    assert is_list(aliases)
    assert length(aliases) == 3  # name, 100, "test_string"
  end

  test "SELECT with function calls (count, sum, etc)" do
    domain = %{
      source: %{
        source_table: "orders",
        primary_key: :id,
        fields: [:id, :amount, :status],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          amount: %{type: :decimal},
          status: %{type: :string}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      name: "Order"
    }

    selecto = Selecto.configure(domain, :mock_connection)

    # Test function calls
    selecto_functions = 
      selecto
      |> Selecto.select([
        {:count},
        {:sum, "amount"},
        {:max, "amount"},
        {:coalesce, ["status", {:literal, "pending"}]}
      ])

    {sql, aliases, params} = Selecto.gen_sql(selecto_functions, [])

    # Verify function SQL
    assert String.contains?(sql, "count(*)")
    assert String.contains?(sql, "sum(")
    assert String.contains?(sql, "max(") 
    assert String.contains?(sql, "coalesce(")
    assert String.contains?(sql, "\"selecto_root\".\"amount\"")
    assert String.contains?(sql, "\"selecto_root\".\"status\"")
    
    # Verify literal is parameterized
    assert "pending" in params
    
    # Verify no legacy sentinel remains
    refute String.contains?(sql, "^SelectoParam^")
    
    # Verify structure
    assert length(aliases) == 4
  end

  test "SELECT with case expressions" do
    domain = %{
      source: %{
        source_table: "users",
        primary_key: :id,
        fields: [:id, :age, :status],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          age: %{type: :integer},
          status: %{type: :string}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      name: "User"
    }

    selecto = Selecto.configure(domain, :mock_connection)

    # Test case expression
    selecto_case = 
      selecto
      |> Selecto.select([
        "status",
        {:case, [
          {[age: {:gte, 18}], {:literal, "adult"}},
          {[age: {:lt, 18}], {:literal, "minor"}}
        ], {:literal, "unknown"}}
      ])
      |> Selecto.filter([status: "active"])

    {sql, aliases, params} = Selecto.gen_sql(selecto_case, [])

    # Verify case expression SQL
    assert String.contains?(sql, "case")
    assert String.contains?(sql, "when")
    assert String.contains?(sql, "then")
    assert String.contains?(sql, "else")
    assert String.contains?(sql, "end")
    
    # Verify parameters for literals and filters
    assert "adult" in params
    assert "minor" in params 
    assert "unknown" in params
    assert "active" in params
    
    # Verify no legacy sentinel remains
    refute String.contains?(sql, "^SelectoParam^")
  end

  test "FROM with table alias" do
    domain = %{
      source: %{
        source_table: "user_profiles", 
        primary_key: :id,
        fields: [:id, :user_id, :bio],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          user_id: %{type: :integer},
          bio: %{type: :string}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      name: "UserProfile"
    }

    selecto = Selecto.configure(domain, :mock_connection)

    selecto_from = 
      selecto
      |> Selecto.select(["bio"])

    {sql, _aliases, _params} = Selecto.gen_sql(selecto_from, [])

    # Verify FROM clause with proper table name and alias
    assert String.contains?(sql, "from user_profiles")
    assert String.contains?(sql, "\"selecto_root\"")
    
    # Verify no legacy sentinel remains
    refute String.contains?(sql, "^SelectoParam^")
  end

  test "Complex SELECT with multiple function types" do
    domain = %{
      source: %{
        source_table: "analytics",
        primary_key: :id,
        fields: [:id, :event_date, :value, :category],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          event_date: %{type: :date},
          value: %{type: :decimal}, 
          category: %{type: :string}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      name: "Analytics"
    }

    selecto = Selecto.configure(domain, :mock_connection)

    # Test complex selection with various function types
    selecto_complex = 
      selecto
      |> Selecto.select([
        {:extract, "event_date", "month"},  # extract month from event_date
        {:concat, ["category", {:literal, ": "}, "value"]},
        {:greatest, ["value", {:literal, 0}]},  # Remove tuple format
        {:nullif, ["category", {:literal, ""}]}
      ])

    {sql, aliases, params} = Selecto.gen_sql(selecto_complex, [])

    # Verify complex functions
    assert String.contains?(sql, "extract(")
    assert String.contains?(sql, "month") 
    assert String.contains?(sql, "concat(")
    assert String.contains?(sql, "greatest(")
    assert String.contains?(sql, "nullif(")
    
    # Verify parameters
    assert ": " in params
    assert 0 in params
    assert "" in params
    
    # Verify no legacy sentinel remains
    refute String.contains?(sql, "^SelectoParam^")
    
    # Verify structure
    assert length(aliases) == 4
    assert length(params) >= 3  # May have duplicates due to param handling
  end
end