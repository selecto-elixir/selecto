defmodule Selecto.Subfilter.JoinPathResolverTest do
  use ExUnit.Case, async: true

  alias Selecto.Subfilter.Parser
  alias Selecto.Subfilter.JoinPathResolver
  alias Selecto.Subfilter.JoinPathResolver.JoinResolution
  alias Selecto.Subfilter.Error

  describe "Selecto.Subfilter.JoinPathResolver.resolve/3" do
    test "resolves a simple direct field access" do
      {:ok, spec} = Parser.parse("film.rating", "R")
      {:ok, resolution} = JoinPathResolver.resolve(spec.relationship_path, :film_domain)

      assert %JoinResolution{
               joins: [%{from: :film, to: :film, type: :self, field: :rating}],
               target_table: :film,
               target_field: "rating"
             } = resolution
    end

    test "resolves a single-hop join with a via table" do
      {:ok, spec} = Parser.parse("film.category", "Action")
      {:ok, resolution} = JoinPathResolver.resolve(spec.relationship_path, :film_domain)

      assert %JoinResolution{
               joins: [
                 %{from: :film, to: :film_category, on: "film.film_id = film_category.film_id", type: :inner},
                 %{from: :film_category, to: :category, on: "film_category.category_id = category.category_id", type: :inner}
               ],
               target_table: :film,
               is_aggregation: false,
               path_segments: ["film"],
               target_field: "category"
             } = resolution
    end

    test "resolves a pre-configured multi-hop join" do
      {:ok, spec} = Parser.parse("film.category.name", "Action")
      {:ok, resolution} = JoinPathResolver.resolve(spec.relationship_path, :film_domain)

      assert %JoinResolution{
               joins: [
                 %{from: :film, to: :film_category},
                 %{from: :film_category, to: :category}
               ],
               target_table: :category,
               target_field: "name"
             } = resolution
    end

    test "resolves an aggregation subfilter path" do
      {:ok, spec} = Parser.parse("film", {:count, ">", 5})
      {:ok, resolution} = JoinPathResolver.resolve(spec.relationship_path, :film_domain)

      assert %JoinResolution{
               joins: [%{from: :film, to: :film, type: :self}],
               target_table: :film,
               is_aggregation: true
             } = resolution
    end

    test "returns an error for an unknown domain" do
      {:ok, spec} = Parser.parse("film.rating", "R")
      {:error, %Error{type: :unknown_domain}} = JoinPathResolver.resolve(spec.relationship_path, :unknown_domain)
    end

    test "returns an error for an unresolvable path" do
      {:ok, spec} = Parser.parse("film.director.name", "Spielberg")
      {:error, %Error{type: :unresolvable_path}} = JoinPathResolver.resolve(spec.relationship_path, :film_domain)
    end
  end

  describe "Selecto.Subfilter.JoinPathResolver.validate_path/2" do
    test "returns :ok for a valid path" do
      {:ok, spec} = Parser.parse("film.category.name", "Action")
      assert :ok == JoinPathResolver.validate_path(spec.relationship_path, :film_domain)
    end

    test "returns an error for an invalid path" do
      {:ok, spec} = Parser.parse("film.director.name", "Spielberg")
      assert {:error, %Error{}} = JoinPathResolver.validate_path(spec.relationship_path, :film_domain)
    end
  end
end
