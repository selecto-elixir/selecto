defmodule Selecto.IntegrationTest do
  use ExUnit.Case

  test "SQL generation with new parameterization (phase 1)" do
    # Simplified domain without full database setup
    domain = %{
      source: %{
        source_table: "users",
        primary_key: :id,
        fields: [:id, :name, :email],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          email: %{type: :string}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},  # Add required joins field
      name: "User",
      required_filters: [{"name", "test"}]
    }

    # Mock postgrex opts (not used for SQL generation)
    selecto = Selecto.configure(domain, :mock_connection)

    # Test simple select with filter
    selecto =
      selecto
      |> Selecto.select(["name", "email"])
      |> Selecto.filter([{"id", 42}, {"email", {:like, "%@example.com"}}])

    # Generate SQL (this should not require database connection)
    {sql, aliases, params} = Selecto.gen_sql(selecto, [])

    # Verify SQL structure
    assert String.contains?(sql, "select")
    assert String.contains?(sql, "from users")
    assert String.contains?(sql, "where")

    # Verify parameterization worked (should have $1, $2, etc.)
    assert String.contains?(sql, "$1")
    assert String.contains?(sql, "$2")
    assert String.contains?(sql, "$3")

    # Verify no legacy sentinel remains
    refute String.contains?(sql, "^SelectoParam^")

    # Verify params contain expected values (may have duplicates due to parameter handling)
    assert length(params) >= 3
    assert "test" in params
    assert 42 in params
    assert "%@example.com" in params

    # Verify aliases structure
    assert is_list(aliases)
    assert length(aliases) == 2  # name, email
  end
end
