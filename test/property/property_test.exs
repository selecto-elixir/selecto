Code.require_file("query_generators.exs", __DIR__)

defmodule Selecto.PropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Selecto.Property.QueryGenerators

  property "to_sql is deterministic and emits expected SQL clauses" do
    check all(spec <- QueryGenerators.query_spec_generator(), max_runs: 75) do
      query =
        :mock_connection
        |> base_query()
        |> apply_query_spec(spec)

      {sql_1, params_1} = Selecto.to_sql(query)
      {sql_2, params_2} = Selecto.to_sql(query)

      assert sql_1 == sql_2
      assert params_1 == params_2
      assert sql_1 =~ ~r/\bselect\b/i
      assert sql_1 =~ ~r/\bfrom\b/i

      if spec.filters != [] do
        assert sql_1 =~ ~r/\bwhere\b/i
      end

      if spec.order_by != [] do
        assert sql_1 =~ ~r/\border\s+by\b/i
      end

      if not is_nil(spec.limit) do
        assert sql_1 =~ ~r/\blimit\b/i
      end

      if not is_nil(spec.offset) do
        assert sql_1 =~ ~r/\boffset\b/i
      end

      placeholder_indexes = placeholder_indexes(sql_1)

      if placeholder_indexes == [] do
        assert params_1 == []
      else
        assert Enum.min(placeholder_indexes) >= 1
        assert Enum.max(placeholder_indexes) <= length(params_1)
      end
    end
  end

  property "query builders return new structs without mutating the input" do
    check all(operation <- QueryGenerators.operation_generator(), max_runs: 120) do
      selecto = base_query(:mock_connection)
      snapshot = selecto

      updated =
        case operation do
          {:select, fields} -> Selecto.select(selecto, fields)
          {:filter, filter} -> Selecto.filter(selecto, filter)
          {:order_by, order_spec} -> Selecto.order_by(selecto, order_spec)
          {:group_by, group_field} -> Selecto.group_by(selecto, group_field)
          {:limit, limit_value} -> Selecto.limit(selecto, limit_value)
          {:offset, offset_value} -> Selecto.offset(selecto, offset_value)
        end

      assert selecto == snapshot
      assert %Selecto{} = updated
    end
  end

  property "invalid field references fail predictably" do
    check all(
            invalid_field <- QueryGenerators.invalid_field_generator(),
            value <- QueryGenerators.simple_value_generator(),
            max_runs: 60
          ) do
      assert_raise RuntimeError, ~r/not found/i, fn ->
        base_query(:mock_connection)
        |> Selecto.select([invalid_field])
        |> Selecto.to_sql()
      end

      assert_raise RuntimeError, ~r/not found/i, fn ->
        base_query(:mock_connection)
        |> Selecto.select(["id"])
        |> Selecto.filter({invalid_field, value})
        |> Selecto.to_sql()
      end
    end
  end

  defp base_query(connection, source_table \\ "selecto_property_users") do
    source_table
    |> QueryGenerators.default_domain()
    |> Selecto.configure(connection, validate: false)
  end

  defp apply_query_spec(selecto, spec) do
    selecto
    |> Selecto.select(spec.selected)
    |> maybe_filter(spec.filters)
    |> maybe_order_by(spec.order_by)
    |> maybe_limit(spec.limit)
    |> maybe_offset(spec.offset)
  end

  defp maybe_filter(selecto, []), do: selecto
  defp maybe_filter(selecto, filters), do: Selecto.filter(selecto, filters)

  defp maybe_order_by(selecto, []), do: selecto
  defp maybe_order_by(selecto, order_specs), do: Selecto.order_by(selecto, order_specs)

  defp maybe_limit(selecto, nil), do: selecto
  defp maybe_limit(selecto, limit_value), do: Selecto.limit(selecto, limit_value)

  defp maybe_offset(selecto, nil), do: selecto
  defp maybe_offset(selecto, offset_value), do: Selecto.offset(selecto, offset_value)

  defp placeholder_indexes(sql) do
    ~r/\$(\d+)/
    |> Regex.scan(sql, capture: :all_but_first)
    |> List.flatten()
    |> Enum.map(&String.to_integer/1)
  end
end

defmodule Selecto.PropertyDbTest do
  use ExUnit.Case
  use ExUnitProperties

  alias Selecto.Property.QueryGenerators

  @moduletag :requires_db

  setup_all do
    postgrex_opts = [
      hostname: System.get_env("SELECTO_POSTGRES_HOST", "localhost"),
      port: env_integer("SELECTO_POSTGRES_PORT", 5432),
      username: System.get_env("SELECTO_POSTGRES_USER", "postgres"),
      password: System.get_env("SELECTO_POSTGRES_PASSWORD", "password"),
      database: System.get_env("SELECTO_POSTGRES_DATABASE", "selecto_test")
    ]

    {:ok, connection} = Postgrex.start_link(postgrex_opts)
    source_table = "selecto_property_users_#{System.unique_integer([:positive])}"

    Postgrex.query!(
      connection,
      """
      CREATE TABLE #{source_table} (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        age INTEGER NOT NULL,
        active BOOLEAN NOT NULL
      )
      """,
      []
    )

    Enum.each(1..40, fn index ->
      Postgrex.query!(
        connection,
        "INSERT INTO #{source_table} (name, age, active) VALUES ($1, $2, $3)",
        ["user_#{index}", rem(index * 7, 80), rem(index, 2) == 0]
      )
    end)

    on_exit(fn ->
      Postgrex.query!(connection, "DROP TABLE IF EXISTS #{source_table}", [])
    end)

    {:ok, connection: connection, source_table: source_table}
  end

  property "generated query specs execute successfully against postgres", %{
    connection: connection,
    source_table: source_table
  } do
    check all(spec <- QueryGenerators.query_spec_generator(), max_runs: 35) do
      query =
        source_table
        |> QueryGenerators.default_domain()
        |> Selecto.configure(connection, validate: false)
        |> apply_query_spec(spec)

      assert {:ok, {rows, columns, aliases}} = Selecto.execute(query)

      assert is_list(rows)
      assert is_list(columns)
      assert is_list(aliases)

      if not is_nil(spec.limit) do
        assert length(rows) <= spec.limit
      end
    end
  end

  defp apply_query_spec(selecto, spec) do
    selecto
    |> Selecto.select(spec.selected)
    |> maybe_filter(spec.filters)
    |> maybe_order_by(spec.order_by)
    |> maybe_limit(spec.limit)
    |> maybe_offset(spec.offset)
  end

  defp maybe_filter(selecto, []), do: selecto
  defp maybe_filter(selecto, filters), do: Selecto.filter(selecto, filters)

  defp maybe_order_by(selecto, []), do: selecto
  defp maybe_order_by(selecto, order_specs), do: Selecto.order_by(selecto, order_specs)

  defp maybe_limit(selecto, nil), do: selecto
  defp maybe_limit(selecto, limit_value), do: Selecto.limit(selecto, limit_value)

  defp maybe_offset(selecto, nil), do: selecto
  defp maybe_offset(selecto, offset_value), do: Selecto.offset(selecto, offset_value)

  defp env_integer(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end
end
