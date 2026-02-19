defmodule Selecto.Subfilter.RegistryTest do
  use ExUnit.Case, async: true

  alias Selecto.Subfilter.Registry
  alias Selecto.Subfilter.Error

  defp film_domain_config do
    %{
      tables: [:film, :category, :film_category, :actor, :film_actor, :language],
      joins: %{
        "film.rating" => %{from: :film, to: :film, type: :self, field: :rating},
        "film.release_year" => %{from: :film, to: :film, type: :self, field: :release_year},
        "film.category.name" => [
          %{from: :film, to: :film_category, type: :inner, on: "film.film_id = film_category.film_id"},
          %{from: :film_category, to: :category, type: :inner, on: "film_category.category_id = category.category_id"}
        ]
      }
    }
  end

  describe "Selecto.Subfilter.Registry" do
    setup do
      registry = Registry.new(film_domain_config(), base_table: :film)
      {:ok, registry: registry}
    end

    test "adds a single subfilter to the registry", %{registry: registry} do
      {:ok, updated_registry} = Registry.add_subfilter(registry, "film.rating", "R")

      assert map_size(updated_registry.subfilters) == 1
      assert map_size(updated_registry.join_resolutions) == 1
    end

    test "returns an error for a duplicate subfilter ID", %{registry: registry} do
      {:ok, registry} = Registry.add_subfilter(registry, "film.rating", "R", id: "rating_filter")
      {:error, %Error{type: :duplicate_subfilter_id}} = Registry.add_subfilter(registry, "film.rating", "PG", id: "rating_filter")
    end

    test "adds a compound AND subfilter", %{registry: registry} do
      subfilters = [
        {"film.rating", "R"},
        {"film.release_year", {">", 2000}}
      ]
      {:ok, updated_registry} = Registry.add_compound(registry, :and, subfilters)

      assert map_size(updated_registry.subfilters) == 2
      assert length(updated_registry.compound_ops) == 1
      assert %{type: :and, subfilter_ids: _} = List.first(updated_registry.compound_ops)
    end

    test "removes a subfilter from the registry", %{registry: registry} do
      {:ok, registry} = Registry.add_subfilter(registry, "film.rating", "R", id: "rating_filter")
      updated_registry = Registry.remove_subfilter(registry, "rating_filter")

      assert map_size(updated_registry.subfilters) == 0
    end

    test "overrides a strategy for a subfilter", %{registry: registry} do
      {:ok, registry} = Registry.add_subfilter(registry, "film.rating", "R", id: "rating_filter")
      {:ok, updated_registry} = Registry.override_strategy(registry, "rating_filter", :in)

      assert updated_registry.strategy_overrides["rating_filter"] == :in
    end

    test "analyzes the registry and returns stats", %{registry: registry} do
      {:ok, registry} = Registry.add_subfilter(registry, "film.rating", "R")
      {:ok, registry} = Registry.add_subfilter(registry, "film.category.name", "Action")

      analysis = Registry.analyze(registry)

      assert analysis.subfilter_count == 2
      assert analysis.join_complexity == :low
      assert analysis.strategy_distribution == %{exists: 2}
    end

    test "generate_sql returns base query unchanged when no subfilters", %{registry: registry} do
      base_query = "SELECT * FROM film"
      {:ok, sql, params} = Registry.generate_sql(registry, base_query)

      assert sql == base_query
      assert params == []
    end

    test "generate_sql appends generated subfilter where clause to base query", %{registry: registry} do
      {:ok, registry} = Registry.add_subfilter(registry, "film.rating", "R")
      base_query = "SELECT * FROM film"

      {:ok, sql, params} = Registry.generate_sql(registry, base_query)

      assert String.starts_with?(sql, base_query)
      assert sql =~ "WHERE"
      assert params == ["R"]
      refute sql =~ "-- subfilters would be added here"
    end

    test "generate_sql merges into existing WHERE clause", %{registry: registry} do
      {:ok, registry} = Registry.add_subfilter(registry, "film.rating", "R")
      base_query = "SELECT * FROM film WHERE film.active = true"

      {:ok, sql, params} = Registry.generate_sql(registry, base_query)

      assert String.starts_with?(sql, base_query)
      assert sql =~ "AND ("
      assert params == ["R"]
    end
  end
end
