defmodule Selecto.MixProject do
  use Mix.Project

  def project do
    [
      app: :selecto,
      version: "0.4.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "Selecto",
      description:
        "Alpha: composable SQL query builder for Elixir domains with joins, CTEs, OLAP, and hierarchical patterns",
      package: package(),

      # Test coverage
      test_coverage: [tool: ExCoveralls],

      # Dialyzer configuration
      dialyzer: [
        plt_core_path: "priv/plts",
        plt_file: {:no_warn, "priv/plts/dialyzer.plt"},
        plt_add_apps: [:postgrex, :jason, :timex],
        flags: [:error_handling, :underspecs],
        ignore_warnings: ".dialyzer_ignore.exs",
        list_unused_filters: true
      ],

      # ExDoc configuration for better documentation
      docs: [
        main: "Selecto",
        extras: [
          "README.md",
          "guides/complex_join_patterns.md",
          "guides/olap_and_hierarchical_patterns.md",
          "guides/advanced_usage.md"
        ],
        groups_for_modules: [
          Core: [Selecto, Selecto.Types],
          Builders: [
            Selecto.Builder.Sql,
            Selecto.Builder.CteSql,
            Selecto.Builder.Join
          ],
          "SQL Builders": [
            Selecto.Builder.Sql.Select,
            Selecto.Builder.Sql.Where,
            Selecto.Builder.Sql.Group,
            Selecto.Builder.Sql.Order,
            Selecto.Builder.Sql.Hierarchy,
            Selecto.Builder.Sql.Tagging,
            Selecto.Builder.Sql.Olap
          ],
          Schema: [
            Selecto.Schema,
            Selecto.Schema.Join,
            Selecto.Schema.Column
          ],
          Utilities: [
            Selecto.SQL.Params,
            Selecto.DomainValidator,
            Selecto.Helpers
          ]
        ]
      ]
    ]
  end

  def cli do
    [
      preferred_envs: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test,
        "coveralls.json": :test
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {Selecto.Application, []},
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      adapter_contract_dep(),
      {:postgrex, ">= 0.0.0"},
      {:jason, "~> 1.2"},
      {:nimble_options, "~> 1.0"},
      {:uuid, "~> 1.1"},
      {:ex_doc, "~> 0.29", only: :dev, runtime: false},
      {:timex, "~> 3.7.9"},
      {:mneme, ">= 0.0.0", only: [:dev, :test]},
      mysql_adapter_dep(),
      mariadb_adapter_dep(),
      mssql_adapter_dep(),
      sqlite_adapter_dep(),
      {:benchee, "~> 1.0", only: [:dev, :test], optional: true},
      {:benchee_html, "~> 1.0", only: [:dev, :test], optional: true},
      {:dialyxir, "~> 1.3", only: [:dev], runtime: false},
      {:ecto_sql, "~> 3.12", optional: true},
      {:stream_data, "~> 1.1", only: :test},
      {:excoveralls, "~> 0.18", only: :test}
    ]
  end

  defp adapter_contract_dep do
    case maybe_local_dep_path("selecto_db_adapter") do
      nil -> {:selecto_db_adapter, ">= 0.1.0 and < 0.2.0"}
      path -> {:selecto_db_adapter, path: path}
    end
  end

  defp sqlite_adapter_dep do
    case maybe_local_dep_path("selecto_db_sqlite") do
      nil -> {:selecto_db_sqlite, ">= 0.1.0 and < 0.2.0", only: :test, optional: true}
      path -> {:selecto_db_sqlite, path: path, only: :test, optional: true}
    end
  end

  defp mysql_adapter_dep do
    case maybe_local_dep_path("selecto_db_mysql") do
      nil -> {:selecto_db_mysql, ">= 0.1.0 and < 0.2.0", only: :test, optional: true}
      path -> {:selecto_db_mysql, path: path, only: :test, optional: true}
    end
  end

  defp mariadb_adapter_dep do
    case maybe_local_dep_path("selecto_db_mariadb") do
      nil -> {:selecto_db_mariadb, ">= 0.1.0 and < 0.2.0", only: :test, optional: true}
      path -> {:selecto_db_mariadb, path: path, only: :test, optional: true}
    end
  end

  defp mssql_adapter_dep do
    case maybe_local_dep_path("selecto_db_mssql") do
      nil -> {:selecto_db_mssql, ">= 0.1.0 and < 0.2.0", only: :test, optional: true}
      path -> {:selecto_db_mssql, path: path, only: :test, optional: true}
    end
  end

  defp maybe_local_dep_path(dep_name) do
    if use_local_ecosystem?() do
      [
        "../#{dep_name}",
        "../selecto_test/vendor/#{dep_name}"
      ]
      |> Enum.map(&Path.expand(&1, __DIR__))
      |> Enum.find(&File.dir?/1)
    end
  end

  defp use_local_ecosystem? do
    case System.get_env("SELECTO_ECOSYSTEM_USE_LOCAL") do
      value when value in ["1", "true", "TRUE", "yes", "YES", "on", "ON"] -> true
      _ -> false
    end
  end

  defp package() do
    [
      files: [
        "lib",
        "mix.exs",
        "README.md",
        "LICENSE",
        "CHANGELOG.md",
        "guides",
        ".formatter.exs"
      ],
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/selecto-elixir/selecto",
        "SQL Patterns" => "https://seeken.github.io/selecto-sql-patterns",
        "Demo (Fly)" => "https://testselecto.fly.dev"
      },
      source_url: "https://github.com/selecto-elixir/selecto"
    ]
  end
end
