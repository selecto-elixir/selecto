defmodule Selecto.MixProject do
  use Mix.Project

  def project do
    [
      app: :selecto,
      version: "0.3.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "Selecto",
      description: "A query builder",
      licenses: "MIT",
      package: package(),
      
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
          "Core": [Selecto, Selecto.Types],
          "Builders": [
            Selecto.Builder.Sql,
            Selecto.Builder.Cte,
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
          "Schema": [
            Selecto.Schema,
            Selecto.Schema.Join,
            Selecto.Schema.Column,
            Selecto.Schema.Filter
          ],
          "Utilities": [
            Selecto.SQL.Params,
            Selecto.DomainValidator,
            Selecto.Helpers
          ]
        ]
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:postgrex, ">= 0.0.0"},
      {:jason, "~> 1.2"},
      {:uuid, "~> 1.1"},
      {:ex_doc, "~> 0.29", only: :dev, runtime: false},
      {:timex, "~> 3.7.9"},
      {:mneme, ">= 0.0.0", only: :test},
      {:benchee, "~> 1.0", only: [:dev, :test], optional: true},
      {:benchee_html, "~> 1.0", only: [:dev, :test], optional: true},
      {:dialyxir, "~> 1.3", only: [:dev], runtime: false},
      {:ecto_sql, "~> 3.12", optional: true}
    ]
  end

  defp package() do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/selecto-elixir/selecto"},
      source_url: "https://github.com/selecto-elixir/selecto"
    ]
  end
end
