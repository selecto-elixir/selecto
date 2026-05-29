defmodule Selecto.MixProject do
  use Mix.Project

  def project do
    [
      app: :selecto,
      version: "0.4.5",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      cli: cli(),
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
        plt_add_apps: [:jason, :timex],
        flags: [:error_handling, :underspecs],
        ignore_warnings: ".dialyzer_ignore.exs",
        list_unused_filters: true
      ],

      # ExDoc configuration for better documentation
      docs: [
        main: "Selecto",
        extras: [
          "README.md",
          "docs/domain_schema_v1.md",
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
            Selecto.Capabilities,
            Selecto.Capabilities.Decision,
            Selecto.Capabilities.Request,
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
      {:postgrex, "~> 0.22", only: :test},
      {:exqlite, "~> 0.35", only: :test},
      {:jason, "~> 1.2"},
      {:nimble_options, "~> 1.1"},
      {:uuid, "~> 1.1"},
      {:telemetry, "~> 1.4"},
      {:db_connection, "~> 2.0"},
      {:decimal, ">= 0.0.0"},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},
      {:timex, "~> 3.7"},
      {:mneme, ">= 0.0.0", only: [:dev, :test]},
      {:benchee, "~> 1.5", only: [:dev, :test], optional: true},
      {:benchee_html, "~> 1.0", only: [:dev, :test], optional: true},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false},
      {:ecto_sql, "~> 3.13", optional: true},
      {:stream_data, "~> 1.3", only: :test},
      {:excoveralls, "~> 0.18", only: :test}
    ]
  end

  defp package() do
    [
      files: [
        "lib",
        "mix.exs",
        "README.md",
        "LICENSE",
        "CHANGELOG.md",
        "docs/domain_schema_v1.md",
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
