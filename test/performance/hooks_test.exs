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

  test "with_hooks supports arity-4 execution functions and alias metadata" do
    result =
      Hooks.with_hooks(
        selecto(),
        fn _sel, _sql, _params, context ->
          send(self(), {:aliases, context.aliases})
          {:ok, %{rows: []}}
        end,
        include_aliases: true
      )

    assert {:ok, %{rows: []}} = result
    assert_received {:aliases, aliases}
    assert is_list(aliases)
    assert length(aliases) == 1
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

  test "cache hooks on miss then hit" do
    {:ok, _pid} = QueryCache.start_link(default_ttl: 60_000)

    Hooks.register(:on_cache_miss, fn ctx ->
      send(self(), {:cache, :miss, ctx.query_id})
      ctx
    end)

    Hooks.register(:on_cache_hit, fn ctx ->
      send(self(), {:cache, :hit, ctx.query_id})
      ctx
    end)

    first =
      Hooks.with_hooks(
        selecto(),
        fn _sel, _sql, _params ->
          {:ok, %{cached: "value"}}
        end,
        cache: true
      )

    assert {:ok, %{cached: "value"}} = first
    assert_received {:cache, :miss, _qid1}

    second =
      Hooks.with_hooks(
        selecto(),
        fn _sel, _sql, _params ->
          flunk("execution_fn should not run on cache hit")
        end,
        cache: true
      )

    assert {:ok, %{cached: "value"}} = second
    assert_received {:cache, :hit, _qid2}
  end

  test "cache key includes tenant namespace to avoid cross-tenant cache bleed" do
    {:ok, _pid} = QueryCache.start_link(default_ttl: 60_000)

    tenant_a = Selecto.with_tenant(selecto(), %{tenant_id: "a"})
    tenant_b = Selecto.with_tenant(selecto(), %{tenant_id: "b"})

    assert {:ok, %{tenant: "a"}} =
             Hooks.with_hooks(
               tenant_a,
               fn _sel, _sql, _params ->
                 send(self(), {:executed, :a})
                 {:ok, %{tenant: "a"}}
               end,
               cache: true
             )

    assert {:ok, %{tenant: "b"}} =
             Hooks.with_hooks(
               tenant_b,
               fn _sel, _sql, _params ->
                 send(self(), {:executed, :b})
                 {:ok, %{tenant: "b"}}
               end,
               cache: true
             )

    assert {:ok, %{tenant: "a"}} =
             Hooks.with_hooks(
               tenant_a,
               fn _sel, _sql, _params ->
                 flunk("tenant a should read from cache")
               end,
               cache: true
             )

    assert_received {:executed, :a}
    assert_received {:executed, :b}
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

  test "hook registrations are isolated per process" do
    Hooks.register(:before_execution, fn ctx -> Map.put(ctx, :parent_hook, true) end)

    assert %{parent_hook: true} = Hooks.run_hooks(:before_execution, %{})

    task =
      Task.async(fn ->
        Hooks.run_hooks(:before_execution, %{})
      end)

    assert %{} = Task.await(task)
  end

  test "hook snapshots can be restored in another process" do
    Hooks.register(:before_execution, fn ctx -> Map.put(ctx, :restored, true) end)
    snapshot = Hooks.snapshot_hooks()

    task =
      Task.async(fn ->
        :ok = Hooks.restore_hooks(snapshot)
        Hooks.run_hooks(:before_execution, %{})
      end)

    assert %{restored: true} = Task.await(task)
  end
end
