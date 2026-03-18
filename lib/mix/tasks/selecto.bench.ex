defmodule Mix.Tasks.Selecto.Bench do
  use Mix.Task

  @shortdoc "Benchmark Selecto against Ecto SQL generation"
  @moduledoc """
  Benchmarks comparable query-build and SQL-compilation paths for Selecto and Ecto.

  This task reports separate `build` and `sql` phases, and includes Selecto-only
  clause benchmarks for the same query shapes:

    * `build` constructs the query struct only
    * `sql` compiles a prebuilt query struct to SQL only
    * `clause/*` benchmarks individual Selecto clause builders directly

  It does not execute queries against a database.

  Examples:

      mix selecto.bench
      mix selecto.bench --scenario simple
      mix selecto.bench --scenario joined --time 8 --memory-time 2

  Options:

    * `--scenario` - `all` (default), `simple`, or `joined`
    * `--time` - benchmark time in seconds when Benchee is available (default: `5`)
    * `--memory-time` - memory benchmark time in seconds with Benchee (default: `1`)
    * `--iterations` - fallback iteration count when Benchee is unavailable (default: `1000`)
    * `--validate` - enable Selecto domain validation during setup
  """

  @default_time 5
  @default_memory_time 1
  @default_iterations 1_000

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("app.start")

    {opts, _argv, _invalid} =
      OptionParser.parse(args,
        strict: [
          scenario: :string,
          time: :integer,
          memory_time: :integer,
          iterations: :integer,
          validate: :boolean
        ]
      )

    ensure_ecto_available!()
    ensure_runtime_modules!()

    context = build_context(validate: Keyword.get(opts, :validate, false))
    jobs = jobs_for(selected_scenarios(Keyword.get(opts, :scenario, "all")), context)

    if benchee_available?() do
      run_with_benchee(jobs, opts)
    else
      run_with_fallback(jobs, opts)
    end
  end

  defp benchee_available? do
    Code.ensure_loaded?(Benchee) and function_exported?(Benchee, :run, 2)
  end

  defp ensure_ecto_available! do
    missing =
      [Ecto.Query, Ecto.Schema, Ecto.Adapter.Queryable, Ecto.Adapters.Postgres]
      |> Enum.reject(&Code.ensure_loaded?/1)

    if missing != [] do
      modules = Enum.map_join(missing, ", ", &inspect/1)

      Mix.raise(
        "mix selecto.bench requires ecto_sql in the current environment. Missing: #{modules}"
      )
    end
  end

  defp ensure_runtime_modules! do
    ensure_module(bench_user_module(), bench_user_definition())
    ensure_module(bench_post_module(), bench_post_definition())
    ensure_module(bench_queries_module(), bench_queries_definition())
  end

  defp ensure_module(module, quoted_definition) do
    unless Code.ensure_loaded?(module) do
      [{^module, _bytecode}] = Code.compile_quoted(quoted_definition)
    end
  end

  defp run_with_benchee(jobs, opts) do
    Mix.shell().info("Running Selecto/Ecto benchmark")
    Mix.shell().info("This reports separate build and sql phases; no DB calls are made.\n")

    Benchee.run(jobs,
      time: Keyword.get(opts, :time, @default_time),
      memory_time: Keyword.get(opts, :memory_time, @default_memory_time),
      print: [benchmarking: true, configuration: true, fast_warning: false]
    )
  end

  defp run_with_fallback(jobs, opts) do
    iterations = Keyword.get(opts, :iterations, @default_iterations)

    Mix.shell().info("Running Selecto/Ecto benchmark")

    Mix.shell().info(
      "Benchee is unavailable, using timer fallback with #{iterations} iterations.\n"
    )

    Enum.each(jobs, fn {name, fun} ->
      {micros, _result} =
        :timer.tc(fn ->
          for _ <- 1..iterations do
            fun.()
          end
        end)

      avg_ms = micros / iterations / 1_000
      Mix.shell().info("#{String.pad_trailing(name, 24)} #{Float.round(avg_ms, 3)} ms avg")
    end)
  end

  defp selected_scenarios("all"), do: [:simple, :joined]
  defp selected_scenarios("simple"), do: [:simple]
  defp selected_scenarios("joined"), do: [:joined]

  defp selected_scenarios(other) do
    Mix.raise("unknown scenario #{inspect(other)}; expected one of: all, simple, joined")
  end

  defp jobs_for(scenarios, context) do
    scenarios
    |> Enum.flat_map(fn
      :simple ->
        [
          {"ecto/simple build", context.ecto_simple_build},
          {"ecto/simple sql", context.ecto_simple_sql},
          {"selecto/simple build", context.selecto_simple_build},
          {"selecto/simple sql", context.selecto_simple_sql},
          {"selecto/simple clause/select", context.selecto_simple_clause_select},
          {"selecto/simple clause/where", context.selecto_simple_clause_where},
          {"selecto/simple clause/order", context.selecto_simple_clause_order},
          {"selecto/simple stage/join_order", context.selecto_simple_stage_join_order},
          {"selecto/simple stage/from", context.selecto_simple_stage_from},
          {"selecto/simple stage/finalize", context.selecto_simple_stage_finalize}
        ]

      :joined ->
        [
          {"ecto/joined build", context.ecto_joined_build},
          {"ecto/joined sql", context.ecto_joined_sql},
          {"selecto/joined build", context.selecto_joined_build},
          {"selecto/joined sql", context.selecto_joined_sql},
          {"selecto/joined clause/select", context.selecto_joined_clause_select},
          {"selecto/joined clause/where", context.selecto_joined_clause_where},
          {"selecto/joined clause/order", context.selecto_joined_clause_order},
          {"selecto/joined stage/join_order", context.selecto_joined_stage_join_order},
          {"selecto/joined stage/from", context.selecto_joined_stage_from},
          {"selecto/joined stage/finalize", context.selecto_joined_stage_finalize}
        ]
    end)
    |> Map.new()
  end

  defp build_context(opts) do
    conn = %{__struct__: Postgrex.Connection, pid: self(), parameters: %{}}
    bench_queries = bench_queries_module()

    simple_selecto =
      Selecto.configure(simple_domain(), conn, validate: Keyword.get(opts, :validate, false))

    joined_selecto =
      Selecto.configure(joined_domain(), conn, validate: Keyword.get(opts, :validate, false))

    simple_selecto_query = simple_selecto_query(simple_selecto)
    joined_selecto_query = joined_selecto_query(joined_selecto)
    simple_ecto_query = bench_queries.simple_query()
    joined_ecto_query = bench_queries.joined_query()
    simple_sql_components = Selecto.Builder.Sql.benchmark_components(simple_selecto_query)
    joined_sql_components = Selecto.Builder.Sql.benchmark_components(joined_selecto_query)

    %{
      ecto_simple_build: &bench_queries.simple_query/0,
      ecto_simple_sql: fn -> bench_queries.compile_to_sql(simple_ecto_query) end,
      ecto_joined_build: &bench_queries.joined_query/0,
      ecto_joined_sql: fn -> bench_queries.compile_to_sql(joined_ecto_query) end,
      selecto_simple_build: fn -> simple_selecto_query(simple_selecto) end,
      selecto_simple_sql: fn -> Selecto.to_sql(simple_selecto_query) end,
      selecto_simple_clause_select: fn -> build_select_clause(simple_selecto_query) end,
      selecto_simple_clause_where: fn -> build_where_clause(simple_selecto_query) end,
      selecto_simple_clause_order: fn -> build_order_clause(simple_selecto_query) end,
      selecto_simple_stage_join_order: fn ->
        Selecto.Builder.Sql.benchmark_join_order(
          simple_selecto_query,
          simple_sql_components.requested_joins
        )
      end,
      selecto_simple_stage_from: fn ->
        Selecto.Builder.Sql.benchmark_build_from(
          simple_selecto_query,
          simple_sql_components.joins_in_order
        )
      end,
      selecto_simple_stage_finalize: fn ->
        Selecto.SQL.Params.finalize(simple_sql_components.final_query_iodata,
          adapter: simple_sql_components.adapter
        )
      end,
      selecto_joined_build: fn -> joined_selecto_query(joined_selecto) end,
      selecto_joined_sql: fn -> Selecto.to_sql(joined_selecto_query) end,
      selecto_joined_clause_select: fn -> build_select_clause(joined_selecto_query) end,
      selecto_joined_clause_where: fn -> build_where_clause(joined_selecto_query) end,
      selecto_joined_clause_order: fn -> build_order_clause(joined_selecto_query) end,
      selecto_joined_stage_join_order: fn ->
        Selecto.Builder.Sql.benchmark_join_order(
          joined_selecto_query,
          joined_sql_components.requested_joins
        )
      end,
      selecto_joined_stage_from: fn ->
        Selecto.Builder.Sql.benchmark_build_from(
          joined_selecto_query,
          joined_sql_components.joins_in_order
        )
      end,
      selecto_joined_stage_finalize: fn ->
        Selecto.SQL.Params.finalize(joined_sql_components.final_query_iodata,
          adapter: joined_sql_components.adapter
        )
      end
    }
  end

  defp build_select_clause(selecto) do
    {aliases, joins, selects_iodata, params} =
      selecto.set.selected
      |> Enum.map(fn selector -> Selecto.Builder.Sql.Select.build(selecto, selector, %{}) end)
      |> Enum.reduce({[], [], [], []}, fn {select_iodata, join, param, as},
                                          {aliases, joins, selects, params} ->
        {[as | aliases], [join | joins], [select_iodata | selects], Enum.reverse(param, params)}
      end)

    {
      Enum.reverse(aliases),
      Enum.reverse(joins),
      Enum.intersperse(Enum.reverse(selects_iodata), ", "),
      Enum.reverse(params)
    }
  end

  defp build_where_clause(selecto) do
    Selecto.Builder.Sql.Where.build(selecto, {:and, where_filters_for(selecto)})
  end

  defp build_order_clause(selecto) do
    Selecto.Builder.Sql.Order.build(selecto)
  end

  defp where_filters_for(selecto) do
    set_filters =
      selecto.set.filtered
      |> Enum.reject(fn
        filter when is_binary(filter) ->
          String.match?(filter, ~r/^\d+-\d+,\d+\+$|^\d+,\d+-\d+|\d+\+/)

        _ ->
          false
      end)

    domain_required_filters = Map.get(Selecto.domain(selecto), :required_filters, [])
    set_required_filters = Map.get(selecto.set, :required_filters, [])

    regular_filters = Enum.uniq(domain_required_filters ++ set_required_filters ++ set_filters)

    json_filters =
      case Map.get(selecto.set, :json_filters) do
        nil ->
          []

        json_specs when is_list(json_specs) ->
          Enum.map(json_specs, fn spec ->
            {:raw_sql_filter, Selecto.Builder.JsonOperations.build_json_filter(spec)}
          end)
      end

    array_filters =
      case Map.get(selecto.set, :array_filters) do
        nil ->
          []

        array_specs when is_list(array_specs) ->
          Enum.map(array_specs, fn spec ->
            {:array_filter, spec}
          end)
      end

    regular_filters ++ json_filters ++ array_filters
  end

  defp simple_selecto_query(selecto) do
    selecto
    |> Selecto.select(["name", "email"])
    |> Selecto.filter([{"active", true}, {"name", {:ilike, "A%"}}])
    |> Selecto.order_by([{"name", :asc}])
  end

  defp joined_selecto_query(selecto) do
    selecto
    |> Selecto.select(["name", {:func, "count", ["posts.id"]}, {:func, "sum", ["posts.views"]}])
    |> Selecto.filter([{"active", true}, {"posts.published", true}])
    |> Selecto.group_by(["name"])
    |> Selecto.order_by([{"name", :asc}])
    |> Selecto.limit(10)
  end

  defp simple_domain do
    %{
      name: "users",
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
        associations: %{
          posts: %{queryable: :posts, field: :posts, owner_key: :id, related_key: :user_id}
        }
      },
      schemas: %{
        posts: %{
          source_table: "posts",
          primary_key: :id,
          fields: [:id, :user_id, :title, :published, :views],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            user_id: %{type: :integer},
            title: %{type: :string},
            published: %{type: :boolean},
            views: %{type: :integer}
          },
          associations: %{}
        }
      },
      joins: %{posts: %{type: :left, name: "posts"}}
    }
  end

  defp joined_domain, do: simple_domain()

  defp bench_user_module, do: Module.concat(__MODULE__, BenchUser)
  defp bench_post_module, do: Module.concat(__MODULE__, BenchPost)
  defp bench_queries_module, do: Module.concat(__MODULE__, BenchQueries)

  defp bench_user_definition do
    user = bench_user_module()
    post = bench_post_module()

    quote do
      defmodule unquote(user) do
        use Ecto.Schema

        schema "users" do
          field(:name, :string)
          field(:email, :string)
          field(:active, :boolean)

          has_many(:posts, unquote(post), foreign_key: :user_id)
        end
      end
    end
  end

  defp bench_post_definition do
    user = bench_user_module()
    post = bench_post_module()

    quote do
      defmodule unquote(post) do
        use Ecto.Schema

        schema "posts" do
          field(:title, :string)
          field(:published, :boolean)
          field(:views, :integer)

          belongs_to(:user, unquote(user))
        end
      end
    end
  end

  defp bench_queries_definition do
    queries = bench_queries_module()
    user = bench_user_module()

    quote do
      defmodule unquote(queries) do
        import Ecto.Query

        def simple_query do
          from(u in unquote(user),
            where: u.active == ^true and ilike(u.name, ^"A%"),
            order_by: [asc: u.name],
            select: %{name: u.name, email: u.email}
          )
        end

        def joined_query do
          from(u in unquote(user),
            join: p in assoc(u, :posts),
            where: u.active == ^true and p.published == ^true,
            group_by: [u.name],
            order_by: [desc: sum(p.views)],
            limit: 10,
            select: %{name: u.name, post_count: count(p.id), total_views: sum(p.views)}
          )
        end

        def compile_to_sql(queryable) do
          {query, _cast_params, dump_params} =
            Ecto.Adapter.Queryable.plan_query(:all, Ecto.Adapters.Postgres, queryable)

          {:cache, {_cache_key, sql}} = Ecto.Adapters.Postgres.prepare(:all, query)
          {sql, dump_params}
        end
      end
    end
  end
end
