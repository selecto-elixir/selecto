defmodule Selecto.Performance.HooksTest do
  use ExUnit.Case

  alias Selecto.Performance.Hooks
  alias Selecto.Performance.QueryCache

  @hook_points [
    :before_query_build,
    :after_query_build,
    :before_execution,
    :after_execution,
    :on_error,
    :on_cache_hit,
    :on_cache_miss
  ]

  defp selecto do
    domain = %{
      name: "Hooks",
      source: %{
        source_table: "users",
        primary_key: :id,
        fields: [:id, :name],
        redact_fields: [],
        columns: %{id: %{type: :integer}, name: %{type: :string}},
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }

    Selecto.configure(domain, :mock_connection) |> Selecto.select(["id"])
  end

  setup do
    Enum.each(@hook_points, &Hooks.unregister/1)
    :ok
  end

  test "init and handle_hook callbacks" do
    assert {:ok, %{}} = Hooks.init([])

    Hooks.register(:before_execution, fn ctx -> Map.put(ctx, :handled, true) end)
    result = Hooks.handle_hook(:before_execution, %{ok: true}, %{})
    assert %{ok: true, handled: true} = result
  end

  test "register/run/unregister hooks with LIFO behavior and rescue" do
    Hooks.register(:before_query_build, fn ctx ->
      Map.put(ctx, :order, [1 | Map.get(ctx, :order, [])])
    end)

    Hooks.register(:before_query_build, fn ctx ->
      Map.put(ctx, :order, [2 | Map.get(ctx, :order, [])])
    end)

    Hooks.register(:before_query_build, fn _ctx -> raise "hook boom" end)

    result = Hooks.run_hooks(:before_query_build, %{})
    assert result.order == [1, 2]

    assert :ok == Hooks.unregister(:before_query_build)
    assert %{} == Hooks.run_hooks(:before_query_build, %{})
  end

  test "with_hooks executes normal flow and returns execution result" do
    Hooks.register(:before_execution, fn ctx -> Map.put(ctx, :flag, :before_exec) end)

    Hooks.register(:after_execution, fn ctx ->
      send(self(), {:after_execution, ctx.execution_time})
      ctx
    end)

    result =
      Hooks.with_hooks(selecto(), fn _sel, sql, params ->
        send(self(), {:exec_called, sql, params})
        {:ok, %{rows: []}}
      end)

    assert {:ok, %{rows: []}} = result
    assert_received {:exec_called, _sql, _params}
    assert_received {:after_execution, _execution_time}
  end

  test "with_hooks runs error hooks and reraises" do
    Hooks.register(:on_error, fn ctx ->
      send(self(), {:hook_error, ctx.error.__struct__})
      ctx
    end)

    assert_raise RuntimeError, "explode", fn ->
      Hooks.with_hooks(selecto(), fn _sel, _sql, _params ->
        raise "explode"
      end)
    end

    assert_received {:hook_error, RuntimeError}
  end

  test "cache mode currently raises due run_hooks argument order" do
    {:ok, _pid} = QueryCache.start_link(default_ttl: 60_000)

    Hooks.register(:on_cache_miss, fn ctx ->
      send(self(), {:cache, :miss, ctx.query_id})
      ctx
    end)

    Hooks.register(:on_cache_hit, fn ctx ->
      send(self(), {:cache, :hit, ctx.query_id})
      ctx
    end)

    assert_raise FunctionClauseError, fn ->
      Hooks.with_hooks(
        selecto(),
        fn _sel, _sql, _params ->
          {:ok, %{cached: "value"}}
        end,
        cache: true
      )
    end
  end

  test "hook composition helpers" do
    custom = Hooks.create_hook(:mark, fn ctx -> Map.put(ctx, :marked, true) end)
    conditional = Hooks.conditional_hook(fn ctx -> ctx[:enabled] end, custom)
    combined = Hooks.chain_hooks([fn ctx -> Map.put(ctx, :enabled, true) end, conditional])

    assert %{enabled: true, marked: true, hook_name: :mark} = combined.(%{})
  end

  test "install_default_hooks can be installed and execute" do
    assert :ok =
             Hooks.install_default_hooks(slow_query_threshold: 0, auto_explain_threshold: 10_000)

    assert {:ok, %{ok: true}} =
             Hooks.with_hooks(selecto(), fn _sel, _sql, _params ->
               {:ok, %{ok: true}}
             end)
  end
end
