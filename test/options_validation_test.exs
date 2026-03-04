defmodule Selecto.OptionsValidationTest do
  use ExUnit.Case, async: true

  defmodule FakeAdapter do
    @behaviour Selecto.DB.Adapter

    @impl true
    def name, do: :options_validation_fake

    @impl true
    def connect(_opts), do: {:ok, :fake_conn}

    @impl true
    def execute(_conn, _query, _params, _opts), do: {:ok, %{rows: [[1]], columns: ["id"]}}

    @impl true
    def placeholder(_index), do: "?"

    @impl true
    def quote_identifier(identifier), do: "`#{identifier}`"

    @impl true
    def supports?(_feature), do: false
  end

  test "configure accepts valid known options and ignores unknown passthrough keys" do
    selecto =
      Selecto.configure(
        domain(),
        [],
        validate: false,
        pool: false,
        adapter: FakeAdapter,
        rollup_sort_fix: :auto,
        custom_flag: :ignored
      )

    assert %Selecto{} = selecto
    assert selecto.adapter == FakeAdapter
    assert selecto.connection == :fake_conn
  end

  test "configure rejects invalid validate option type" do
    assert_raise NimbleOptions.ValidationError, ~r/validate/, fn ->
      Selecto.configure(domain(), [], validate: "false")
    end
  end

  test "configure rejects invalid pool_options type" do
    assert_raise NimbleOptions.ValidationError, ~r/pool_options/, fn ->
      Selecto.configure(domain(), [], validate: false, pool_options: %{pool_size: 10})
    end
  end

  test "configuration module validates configure options too" do
    assert_raise NimbleOptions.ValidationError, ~r/rollup_sort_fix/, fn ->
      Selecto.Configuration.configure(domain(), [], validate: false, rollup_sort_fix: :sometimes)
    end
  end

  test "execute rejects invalid timeout type" do
    assert_raise NimbleOptions.ValidationError, ~r/timeout/, fn ->
      Selecto.execute(selecto(), timeout: "1000", analyze_complexity: false)
    end
  end

  test "execute_with_metadata rejects invalid cache option type" do
    assert_raise NimbleOptions.ValidationError, ~r/cache/, fn ->
      Selecto.execute_with_metadata(selecto(), cache: "yes", analyze_complexity: false)
    end
  end

  test "execute_stream validates stream options" do
    assert_raise NimbleOptions.ValidationError, ~r/max_rows/, fn ->
      Selecto.execute_stream(selecto(), max_rows: "many", analyze_complexity: false)
    end

    assert_raise NimbleOptions.ValidationError, ~r/stream_producer/, fn ->
      Selecto.execute_stream(selecto(),
        stream_producer: fn -> :ok end,
        analyze_complexity: false
      )
    end
  end

  test "execute accepts unknown passthrough opts while validating known keys" do
    assert {:ok, {[[1]], ["id"], aliases}} =
             Selecto.execute(selecto(), analyze_complexity: false, custom_runtime_opt: :kept)

    assert is_list(aliases)
    assert length(aliases) == 1
  end

  test "to_sql validates known formatting options" do
    assert_raise NimbleOptions.ValidationError, ~r/pretty/, fn ->
      Selecto.to_sql(selecto(), pretty: "yes")
    end

    assert_raise NimbleOptions.ValidationError, ~r/highlight/, fn ->
      Selecto.to_sql(selecto(), highlight: :html)
    end

    {sql, params} = Selecto.to_sql(selecto(), pretty: true, highlight: false)
    assert is_binary(sql)
    assert is_list(params)
  end

  test "explain and explain_analyze validate diagnostic options" do
    assert_raise NimbleOptions.ValidationError, ~r/format/, fn ->
      Selecto.explain(selecto(), format: :pdf)
    end

    assert_raise NimbleOptions.ValidationError, ~r/to_sql_opts/, fn ->
      Selecto.explain(selecto(), to_sql_opts: %{pretty: true})
    end

    assert {:ok, explain} = Selecto.explain(selecto(), format: :text, costs: false)
    assert is_binary(explain.explain_sql)
    assert String.contains?(String.upcase(explain.explain_sql), "EXPLAIN")

    assert {:ok, analyzed} = Selecto.explain_analyze(selecto(), format: :json, timing: false)
    assert String.contains?(String.upcase(analyzed.explain_sql), "ANALYZE")
  end

  defp selecto do
    domain()
    |> Selecto.configure([], validate: false, adapter: FakeAdapter)
    |> Selecto.select(["id"])
  end

  defp domain do
    %{
      name: "Options validation test",
      source: %{
        source_table: "users",
        primary_key: :id,
        fields: [:id],
        redact_fields: [],
        columns: %{
          id: %{type: :integer}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end
end
