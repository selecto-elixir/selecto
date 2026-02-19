defmodule Selecto.Subfilter.SQLTest do
  use ExUnit.Case, async: true

  alias Selecto.Subfilter.Registry
  alias Selecto.Subfilter.SQL

  defp film_domain_config do
    %{
      tables: [:film, :category, :film_category, :actor, :film_actor, :language],
      joins: %{
        "film.rating" => %{from: :film, to: :film, type: :self, field: :rating},
        "film.release_year" => %{from: :film, to: :film, type: :self, field: :release_year},
        "film.rental_rate" => %{from: :film, to: :film, type: :self, field: :rental_rate},
        "film.category.name" => [
          %{from: :film, to: :film_category, type: :inner, on: "film.film_id = film_category.film_id"},
          %{from: :film_category, to: :category, type: :inner, on: "film_category.category_id = category.category_id"}
        ],
        "film.actors" => %{
          from: :film,
          to: :film_actor,
          type: :inner,
          on: "film.film_id = film_actor.film_id"
        }
      }
    }
  end

  describe "Selecto.Subfilter.SQL.generate/1" do
    test "generates SQL for a single EXISTS subfilter" do
      registry = Registry.new(film_domain_config(), base_table: :film)
      {:ok, registry} = Registry.add_subfilter(registry, "film.category.name", "Action")

      {:ok, sql, params} = SQL.generate(registry)

      assert sql =~ "WHERE (EXISTS ("
      assert sql =~ "FROM film"
      assert sql =~ "JOIN film_category ON"
      assert sql =~ "JOIN category ON"
      assert sql =~ "WHERE film.film_id = film.film_id AND category.name = ?"
      assert params == ["Action"]
    end

    test "generates SQL for a single IN subfilter" do
      registry = Registry.new(film_domain_config(), base_table: :film)
      {:ok, registry} = Registry.add_subfilter(registry, "film.category.name", ["Action", "Comedy"], strategy: :in)

      {:ok, sql, params} = SQL.generate(registry)

      assert sql =~ "WHERE (film.film_id IN ("
      assert sql =~ "SELECT film.film_id"
      assert sql =~ "FROM film"
      assert sql =~ "WHERE category.name IN (?, ?)"
      assert params == ["Action", "Comedy"]
    end

    test "generates SQL for an aggregation subfilter" do
      registry = Registry.new(film_domain_config(), base_table: :film)
      {:ok, registry} = Registry.add_subfilter(registry, "film.actors", {:count, ">", 5})

      {:ok, sql, params} = SQL.generate(registry)

      assert sql =~ "WHERE (("
      assert sql =~ "FROM film"
      assert sql =~ "JOIN film_actor ON"
      assert sql =~ "WHERE film.film_id = film.film_id"
      assert sql =~ "COUNT(*)"
      assert params == [5]
    end

    test "generates SQL for compound AND subfilters" do
      registry = Registry.new(film_domain_config(), base_table: :film)
      subfilters = [
        {"film.rating", "R"},
        {"film.release_year", {">", 2000}}
      ]
      {:ok, registry} = Registry.add_compound(registry, :and, subfilters)

      {:ok, sql, _params} = SQL.generate(registry)

      assert sql =~ "WHERE ((EXISTS"
      assert sql =~ "AND (EXISTS"
    end

    test "generates SQL for temporal subfilter - recent years" do
      registry = Registry.new(film_domain_config(), base_table: :film)
      {:ok, registry} = Registry.add_subfilter(registry, "film.release_year", {:recent, years: 5})

      {:ok, sql, params} = SQL.generate(registry)

      assert sql =~ "EXISTS ("
      assert sql =~ "film.release_year > (CURRENT_DATE - INTERVAL '5 years')"
      assert params == []
    end

    test "generates SQL for temporal subfilter - within days" do
      registry = Registry.new(film_domain_config(), base_table: :film)
      {:ok, registry} = Registry.add_subfilter(registry, "film.release_year", {:within_days, 30})

      {:ok, sql, params} = SQL.generate(registry)

      assert sql =~ "EXISTS ("
      assert sql =~ "film.release_year > (CURRENT_DATE - INTERVAL '30 days')"
      assert params == []
    end

    test "generates SQL for temporal subfilter - since date" do
      date = ~D[2023-01-01]
      registry = Registry.new(film_domain_config(), base_table: :film)
      {:ok, registry} = Registry.add_subfilter(registry, "film.release_year", {:since_date, date})

      {:ok, sql, params} = SQL.generate(registry)

      assert sql =~ "EXISTS ("
      assert sql =~ "film.release_year > ?"
      assert params == [date]
    end

    test "generates SQL for range subfilter" do
      registry = Registry.new(film_domain_config(), base_table: :film)
      {:ok, registry} = Registry.add_subfilter(registry, "film.rental_rate", {"between", 2.99, 4.99})

      {:ok, sql, params} = SQL.generate(registry)

      assert sql =~ "EXISTS ("
      assert sql =~ "film.rental_rate BETWEEN ? AND ?"
      assert params == [2.99, 4.99]
    end

    test "generates SQL for temporal subfilter with IN strategy" do
      registry = Registry.new(film_domain_config(), base_table: :film)
      {:ok, registry} = Registry.add_subfilter(registry, "film.release_year", {:within_days, 7}, strategy: :in)

      {:ok, sql, params} = SQL.generate(registry)

      assert sql =~ "film.film_id IN ("
      assert sql =~ "film.release_year > (CURRENT_DATE - INTERVAL '7 days')"
      assert params == []
    end

    test "generates SQL for range subfilter with IN strategy" do
      registry = Registry.new(film_domain_config(), base_table: :film)
      {:ok, registry} = Registry.add_subfilter(registry, "film.rental_rate", {"between", 2.99, 4.99}, strategy: :in)

      {:ok, sql, params} = SQL.generate(registry)

      assert sql =~ "film.film_id IN ("
      assert sql =~ "film.rental_rate BETWEEN ? AND ?"
      assert params == [2.99, 4.99]
    end
  end
end
