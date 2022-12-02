defmodule Selecto.MixProject do
  use Mix.Project

  def project do
    [
      app: :selecto,
      version: "0.2.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "Selecto",
      description: "A query builder",
      licenses: "MIT",
      package: package()
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
      {:ecto, "~> 3.9.1"},
      {:uuid, "~> 1.1"},
      {:ex_doc, "~> 0.27", only: :dev, runtime: false}
    ]
  end

  defp package() do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/seeken/selecto"},
      source_url: "https://github.com/seeken/selecto"
    ]
  end
end
