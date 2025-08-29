defmodule Selecto.Subfilter.RegistryTest do
  use ExUnit.Case, async: true

  alias Selecto.Subfilter.Registry
  alias Selecto.Subfilter.Error

  describe "Selecto.Subfilter.Registry" do
    setup do
      registry = Registry.new(:film_domain, base_table: :film)
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
  end
end
