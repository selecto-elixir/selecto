defmodule Selecto.Subfilter.ParserTest do
  use ExUnit.Case, async: true

  alias Selecto.Subfilter.Parser
  alias Selecto.Subfilter.{Spec, RelationshipPath, FilterSpec, CompoundSpec, Error}

  describe "Selecto.Subfilter.Parser.parse/3" do
    test "parses a simple equality filter" do
      {:ok, spec} = Parser.parse("film.rating", "R")

      assert %Spec{
               relationship_path: %RelationshipPath{
                 path_segments: ["film"],
                 target_table: "film",
                 target_field: "rating",
                 is_aggregation: false
               },
               filter_spec: %FilterSpec{
                 type: :equality,
                 operator: "=",
                 value: "R"
               },
               strategy: :exists,
               negate: false
             } = spec
    end

    test "parses a filter with an IN list" do
      {:ok, spec} = Parser.parse("film.rating", ["R", "PG-13"], strategy: :in)

      assert %Spec{
               filter_spec: %FilterSpec{
                 type: :in_list,
                 operator: "IN",
                 values: ["R", "PG-13"]
               },
               strategy: :in
             } = spec
    end

    test "parses a comparison filter" do
      {:ok, spec} = Parser.parse("film.release_year", {">", 2000})

      assert %Spec{
               filter_spec: %FilterSpec{
                 type: :comparison,
                 operator: ">",
                 value: 2000
               }
             } = spec
    end

    test "parses a range filter" do
      {:ok, spec} = Parser.parse("film.release_year", {"between", 2000, 2010})

      assert %FilterSpec{
               type: :range,
               operator: "BETWEEN",
               min_value: 2000,
               max_value: 2010
             } = spec.filter_spec
    end

    test "parses a count aggregation filter" do
      {:ok, spec} = Parser.parse("film.actors", {:count, ">", 5})

      assert %Spec{
               relationship_path: %RelationshipPath{
                 is_aggregation: false
               },
               filter_spec: %FilterSpec{
                 type: :aggregation,
                 agg_function: :count,
                 operator: ">",
                 value: 5
               }
             } = spec
    end

    test "parses a multi-level relationship path" do
      {:ok, spec} = Parser.parse("film.category.name", "Action")

      assert %RelationshipPath{
        path_segments: ["film", "category"],
        target_table: "category",
        target_field: "name",
        is_aggregation: false
      } = spec.relationship_path
    end

    test "returns an error for an invalid relationship path" do
      {:error, %Error{type: :invalid_relationship_path, message: message}} = Parser.parse(123, "R")
      assert message == "Relationship path must be a string"
    end

    test "returns an error for an unsupported filter specification" do
      {:error, %Error{type: :invalid_filter_spec, message: message}} = Parser.parse("film.rating", %{})
      assert message == "Unsupported filter specification"
    end

    test "returns an error for an invalid strategy option" do
      {:error, %Error{type: :invalid_filter_spec, message: message}} = Parser.parse("film.rating", "R", strategy: :invalid)
      assert message == "Invalid strategy option"
    end
  end

  describe "Selecto.Subfilter.Parser.parse_compound/3" do
    test "parses a compound AND filter" do
      subfilters = [
        {"film.rating", "R"},
        {"film.release_year", {">", 2000}}
      ]

      {:ok, compound_spec} = Parser.parse_compound(:and, subfilters)

      assert %CompoundSpec{
               type: :and,
               subfilters: [
                 %Spec{relationship_path: %RelationshipPath{target_field: "rating"}},
                 %Spec{relationship_path: %RelationshipPath{target_field: "release_year"}}
               ]
             } = compound_spec
      assert length(compound_spec.subfilters) == 2
    end

    test "parses a compound OR filter" do
      subfilters = [
        {"category.name", "Action"},
        {"category.name", "Comedy"}
      ]

      {:ok, compound_spec} = Parser.parse_compound(:or, subfilters)
      assert %CompoundSpec{type: :or} = compound_spec
      assert length(compound_spec.subfilters) == 2
    end

    test "returns an error for invalid compound filter specs" do
      {:error, %Error{type: :invalid_filter_spec}} = Parser.parse_compound(:and, ["invalid"])
    end
  end
end
