defmodule Selecto.Subfilter.SQLTest do
  use ExUnit.Case, async: true

  alias Selecto.Subfilter.Registry
  alias Selecto.Subfilter.SQL

  describe "Selecto.Subfilter.SQL.generate/1" do
    test "generates SQL for a single EXISTS subfilter" do
      registry = Registry.new(:film_domain, base_table: :film)
      {:ok, registry} = Registry.add_subfilter(registry, "film.category.name", "Action")

      {:ok, sql, params} = SQL.generate(registry)

      assert sql =~ "WHERE (EXISTS ("
      assert sql =~ "FROM film"
      assert sql =~ "JOIN film_category ON"
      assert sql =~ "JOIN category ON"
      assert sql =~ "WHERE category.film_id = film.film_id AND category.name = ?"
      assert params == ["Action"]
    end

    test "generates SQL for a single IN subfilter" do
      registry = Registry.new(:film_domain, base_table: :film)
      {:ok, registry} = Registry.add_subfilter(registry, "film.category.name", ["Action", "Comedy"], strategy: :in)

      {:ok, sql, params} = SQL.generate(registry)

      assert sql =~ "WHERE (film.film_id IN ("
      assert sql =~ "SELECT film.film_id"
      assert sql =~ "FROM film"
      assert sql =~ "WHERE category.name IN (?, ?)"
      assert params == ["Action", "Comedy"]
    end

    test "generates SQL for an aggregation subfilter" do
      registry = Registry.new(:film_domain, base_table: :film)
      {:ok, registry} = Registry.add_subfilter(registry, "film.actors", {:count, ">", 5})

      {:ok, sql, params} = SQL.generate(registry)

      # The implementation uses EXISTS format for aggregations
      assert sql =~ "WHERE (EXISTS ("
      assert sql =~ "FROM film"
      assert sql =~ "JOIN film_actor ON"
      assert sql =~ "WHERE film.film_id = film.film_id AND film.actors > ?"
      assert params == [5]
    end

    test "generates SQL for compound AND subfilters" do
      registry = Registry.new(:film_domain, base_table: :film)
      subfilters = [
        {"film.rating", "R"},
        {"film.release_year", {">", 2000}}
      ]
      {:ok, registry} = Registry.add_compound(registry, :and, subfilters)

      {:ok, sql, _params} = SQL.generate(registry)

      assert sql =~ "WHERE ((EXISTS"
      assert sql =~ "AND (EXISTS"
    end
  end
end
