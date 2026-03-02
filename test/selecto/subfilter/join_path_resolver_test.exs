defmodule Selecto.Subfilter.JoinPathResolverTest do
  use ExUnit.Case, async: true

  alias Selecto.Subfilter.Parser
  alias Selecto.Subfilter.JoinPathResolver
  alias Selecto.Subfilter.JoinPathResolver.JoinResolution
  alias Selecto.Subfilter.Error

  defp film_domain_config do
    %{
      tables: [:film, :category, :film_category, :actor, :film_actor, :language],
      joins: %{
        "film.rating" => %{from: :film, to: :film, type: :self, field: :rating},
        "film.title" => %{from: :film, to: :film, type: :self, field: :title},
        "film.release_year" => %{from: :film, to: :film, type: :self, field: :release_year},
        "film.rental_rate" => %{from: :film, to: :film, type: :self, field: :rental_rate},
        "film.film_id" => %{from: :film, to: :film, type: :self, field: :film_id},
        "film.category" => %{
          from: :film,
          to: :category,
          type: :inner,
          via: :film_category,
          on:
            "film.film_id = film_category.film_id AND film_category.category_id = category.category_id"
        },
        "film.actor" => %{
          from: :film,
          to: :actor,
          type: :inner,
          via: :film_actor,
          on: "film.film_id = film_actor.film_id AND film_actor.actor_id = actor.actor_id"
        },
        "film.actors" => %{
          from: :film,
          to: :film_actor,
          type: :inner,
          on: "film.film_id = film_actor.film_id"
        },
        "film.language" => %{
          from: :film,
          to: :language,
          type: :inner,
          on: "film.language_id = language.language_id"
        },
        "film.category.name" => [
          %{
            from: :film,
            to: :film_category,
            type: :inner,
            on: "film.film_id = film_category.film_id"
          },
          %{
            from: :film_category,
            to: :category,
            type: :inner,
            on: "film_category.category_id = category.category_id"
          }
        ],
        "film.language.name" => [
          %{
            from: :film,
            to: :language,
            type: :inner,
            on: "film.language_id = language.language_id"
          }
        ],
        "film.actor.first_name" => [
          %{from: :film, to: :film_actor, type: :inner, on: "film.film_id = film_actor.film_id"},
          %{
            from: :film_actor,
            to: :actor,
            type: :inner,
            on: "film_actor.actor_id = actor.actor_id"
          }
        ]
      }
    }
  end

  describe "Selecto.Subfilter.JoinPathResolver.resolve/3" do
    test "resolves a simple direct field access" do
      {:ok, spec} = Parser.parse("film.rating", "R")
      {:ok, resolution} = JoinPathResolver.resolve(spec.relationship_path, film_domain_config())

      assert %JoinResolution{
               joins: [%{from: :film, to: :film, type: :self, field: :rating}],
               target_table: :film,
               target_field: "rating"
             } = resolution
    end

    test "resolves a single-hop join with a via table" do
      {:ok, spec} = Parser.parse("film.category", "Action")
      {:ok, resolution} = JoinPathResolver.resolve(spec.relationship_path, film_domain_config())

      assert %JoinResolution{
               joins: [
                 %{
                   from: :film,
                   to: :film_category,
                   on: "film.film_id = film_category.film_id",
                   type: :inner
                 },
                 %{
                   from: :film_category,
                   to: :category,
                   on: "film_category.category_id = category.category_id",
                   type: :inner
                 }
               ],
               target_table: :film,
               is_aggregation: false,
               path_segments: ["film"],
               target_field: "category"
             } = resolution
    end

    test "decomposes via join ON clauses with lowercase and" do
      domain =
        put_in(
          film_domain_config(),
          [:joins, "film.category", :on],
          "film.film_id = film_category.film_id and film_category.category_id = category.category_id"
        )

      {:ok, spec} = Parser.parse("film.category", "Action")
      {:ok, resolution} = JoinPathResolver.resolve(spec.relationship_path, domain)

      assert %JoinResolution{
               joins: [
                 %{on: "film.film_id = film_category.film_id"},
                 %{on: "film_category.category_id = category.category_id"}
               ]
             } = resolution
    end

    test "decomposes via join ON clauses without splitting nested AND" do
      domain =
        put_in(
          film_domain_config(),
          [:joins, "film.category", :on],
          "film.film_id = film_category.film_id AND (film_category.active = true AND category.active = true)"
        )

      {:ok, spec} = Parser.parse("film.category", "Action")
      {:ok, resolution} = JoinPathResolver.resolve(spec.relationship_path, domain)

      assert %JoinResolution{
               joins: [
                 %{on: "film.film_id = film_category.film_id"},
                 %{on: "(film_category.active = true AND category.active = true)"}
               ]
             } = resolution
    end

    test "resolves a pre-configured multi-hop join" do
      {:ok, spec} = Parser.parse("film.category.name", "Action")
      {:ok, resolution} = JoinPathResolver.resolve(spec.relationship_path, film_domain_config())

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
      {:ok, resolution} = JoinPathResolver.resolve(spec.relationship_path, film_domain_config())

      assert %JoinResolution{
               joins: [%{from: :film, to: :film, type: :self}],
               target_table: :film,
               is_aggregation: true
             } = resolution
    end

    test "returns an error for an unknown domain" do
      {:ok, spec} = Parser.parse("film.rating", "R")

      {:error, %Error{type: :unknown_domain}} =
        JoinPathResolver.resolve(spec.relationship_path, :unknown_domain)
    end

    test "returns an error for an unresolvable path" do
      {:ok, spec} = Parser.parse("film.director.name", "Spielberg")

      {:error, %Error{type: :unresolvable_path}} =
        JoinPathResolver.resolve(spec.relationship_path, film_domain_config())
    end

    test "auto-resolves a multi-hop path from known path prefixes" do
      {:ok, spec} = Parser.parse("film.actor.last_name", "Smith")
      {:ok, resolution} = JoinPathResolver.resolve(spec.relationship_path, film_domain_config())

      assert %JoinResolution{
               joins: [
                 %{from: :film, to: :film_actor},
                 %{from: :film_actor, to: :actor}
               ],
               target_table: :actor,
               target_field: "last_name"
             } = resolution
    end

    test "resolve_multiple removes duplicate join entries in a sequence" do
      duplicate_domain =
        update_in(film_domain_config(), [:joins, "film.category.name"], fn joins ->
          [hd(joins) | joins]
        end)

      {:ok, spec} = Parser.parse("film.category.name", "Action")

      {:ok, [resolution]} =
        JoinPathResolver.resolve_multiple([spec.relationship_path], duplicate_domain)

      assert length(resolution.joins) == 2
      assert Enum.at(resolution.joins, 0).from == :film
      assert Enum.at(resolution.joins, 1).from == :film_category
    end
  end

  describe "Selecto.Subfilter.JoinPathResolver.validate_path/2" do
    test "returns :ok for a valid path" do
      {:ok, spec} = Parser.parse("film.category.name", "Action")
      assert :ok == JoinPathResolver.validate_path(spec.relationship_path, film_domain_config())
    end

    test "returns an error for an invalid path" do
      {:ok, spec} = Parser.parse("film.director.name", "Spielberg")

      assert {:error, %Error{}} =
               JoinPathResolver.validate_path(spec.relationship_path, film_domain_config())
    end
  end
end
