defmodule Selecto.TaggingTest do
  use ExUnit.Case
  alias Selecto.Builder.Sql.Tagging

  @moduledoc """
  Tests for Phase 3 many-to-many tagging implementation.
  
  Validates tag aggregation, faceted filtering, and join table handling
  for complex many-to-many relationships.
  """

  describe "Tag Aggregation" do
    test "build_tag_aggregation_column generates proper string_agg SQL" do
      result = Tagging.build_tag_aggregation_column("tags", "name", "tag_list")
      sql = IO.iodata_to_binary(result)
      
      assert sql == "string_agg(tags.name, ', ') as tag_list"
    end
    
    test "build_tag_aggregation_column handles different field names" do
      result = Tagging.build_tag_aggregation_column("categories", "title", "category_names")
      sql = IO.iodata_to_binary(result)
      
      assert sql == "string_agg(categories.title, ', ') as category_names"
    end
  end
  
  describe "Tag Count Columns" do
    test "build_tag_count_column generates proper COUNT DISTINCT SQL" do
      result = Tagging.build_tag_count_column("tags", "tag_count")
      sql = IO.iodata_to_binary(result)
      
      assert sql == "COUNT(DISTINCT tags.id) as tag_count"
    end
    
    test "build_tag_count_column handles custom aliases" do
      result = Tagging.build_tag_count_column("t", "total_tags")
      sql = IO.iodata_to_binary(result)
      
      assert sql == "COUNT(DISTINCT t.id) as total_tags"
    end
  end
  
  describe "Faceted Tag Filtering" do
    setup do
      config = %{
        source: "tags",
        tag_field: "name",
        join_table: "post_tags",
        main_foreign_key: "post_id",
        tag_foreign_key: "tag_id"
      }
      {:ok, config: config}
    end
    
    test "build_faceted_tag_filter generates single tag EXISTS filter", %{config: config} do
      {where_iodata, params} = Tagging.build_faceted_tag_filter(config, "programming", :single)
      where_sql = IO.iodata_to_binary(where_iodata)
      
      assert String.contains?(where_sql, "EXISTS (")
      assert String.contains?(where_sql, "FROM post_tags jt")
      assert String.contains?(where_sql, "JOIN tags t")
      assert String.contains?(where_sql, "WHERE jt.post_id = main.id")
      assert String.contains?(where_sql, "AND t.name = $1")
      assert String.contains?(where_sql, ")")
      
      assert params == ["programming"]
    end
    
    test "build_faceted_tag_filter generates ANY match filter for multiple tags", %{config: config} do
      {where_iodata, params} = Tagging.build_faceted_tag_filter(config, ["elixir", "phoenix"], :any)
      where_sql = IO.iodata_to_binary(where_iodata)
      
      assert String.contains?(where_sql, "EXISTS (")
      assert String.contains?(where_sql, "AND t.name = ANY($1)")
      
      assert params == [["elixir", "phoenix"]]
    end
    
    test "build_faceted_tag_filter generates ALL match filter requiring all tags", %{config: config} do
      tag_list = ["web", "backend", "api"]
      {where_iodata, params} = Tagging.build_faceted_tag_filter(config, tag_list, :all)
      where_sql = IO.iodata_to_binary(where_iodata)
      
      assert String.contains?(where_sql, "SELECT COUNT(DISTINCT t.name)")
      assert String.contains?(where_sql, "AND t.name = ANY($1)")
      assert String.contains?(where_sql, ") = $2")
      
      assert params == [tag_list, 3]  # [tags_array, required_count]
    end
  end
  
  describe "Tag Count Filtering" do
    setup do
      config = %{
        source: "tags",
        join_table: "post_tags", 
        main_foreign_key: "post_id"
      }
      {:ok, config: config}
    end
    
    test "build_tag_count_filter generates >= filter", %{config: config} do
      {where_iodata, params} = Tagging.build_tag_count_filter(config, {:gte, 3})
      where_sql = IO.iodata_to_binary(where_iodata)
      
      assert String.contains?(where_sql, "SELECT COUNT(*) FROM post_tags jt")
      assert String.contains?(where_sql, "WHERE jt.post_id = main.id")
      assert String.contains?(where_sql, ") >= $1")
      
      assert params == [3]
    end
    
    test "build_tag_count_filter generates = filter", %{config: config} do
      {where_iodata, params} = Tagging.build_tag_count_filter(config, {:eq, 1})
      where_sql = IO.iodata_to_binary(where_iodata)
      
      assert String.contains?(where_sql, ") = $1")
      assert params == [1]
    end
    
    test "build_tag_count_filter generates BETWEEN filter", %{config: config} do
      {where_iodata, params} = Tagging.build_tag_count_filter(config, {:between, 2, 5})
      where_sql = IO.iodata_to_binary(where_iodata)
      
      assert String.contains?(where_sql, ") BETWEEN $1 AND $2")
      assert params == [2, 5]
    end
    
    test "build_tag_count_filter handles different operators", %{config: config} do
      operators = [
        {:gte, 3, ">="},
        {:gt, 2, ">"},
        {:lte, 10, "<="},
        {:lt, 5, "<"},
        {:eq, 1, "="}
      ]
      
      for {op, value, sql_op} <- operators do
        {where_iodata, params} = Tagging.build_tag_count_filter(config, {op, value})
        where_sql = IO.iodata_to_binary(where_iodata)
        
        assert String.contains?(where_sql, ") #{sql_op} $1")
        assert params == [value]
      end
    end
  end
  
  describe "Many-to-Many Join Integration" do
    test "build_tagging_join_with_aggregation creates double JOIN pattern" do
      # Mock selecto struct
      selecto = %{domain: %{source: %{source_table: "posts"}}}
      
      # Mock tagging configuration  
      config = %{
        source: "tags",
        join_table: "post_tags",
        tag_field: "name",
        main_foreign_key: "post_id",
        tag_foreign_key: "tag_id",
        requires_join: :selecto_root,
        owner_key: "id",
        my_key: "id"
      }
      
      result = Tagging.build_tagging_join_with_aggregation(selecto, :tags, config, [], [], [])
      
      # Should return {from_clause, params, ctes}
      assert is_tuple(result)
      assert tuple_size(result) == 3
      
      {from_clause, params, ctes} = result
      
      # Should have JOIN clauses
      assert is_list(from_clause)
      join_sql = IO.iodata_to_binary(from_clause)
      
      # Should contain double JOIN pattern
      assert String.contains?(join_sql, "LEFT JOIN post_tags tags_jt")
      assert String.contains?(join_sql, "LEFT JOIN tags")
      assert String.contains?(join_sql, "ON posts.id = tags_jt.post_id")
      assert String.contains?(join_sql, "ON tags_jt.tag_id =")
      
      # Basic join shouldn't add parameters
      assert params == []
      
      # Basic join shouldn't add CTEs  
      assert ctes == []
    end
    
    test "tagging join handles configuration defaults" do
      # Minimal configuration - should use reasonable defaults
      selecto = %{domain: %{source: %{source_table: "articles"}}}
      config = %{
        source: "categories",
        requires_join: :selecto_root,
        owner_key: "id", 
        my_key: "id"
      }
      
      result = Tagging.build_tagging_join_with_aggregation(selecto, :categories, config, [], [], [])
      {from_clause, _params, _ctes} = result
      
      join_sql = IO.iodata_to_binary(from_clause)
      
      # Should use default join table naming and foreign keys
      assert String.contains?(join_sql, "LEFT JOIN")
      assert String.contains?(join_sql, "categories_jt") or 
             String.contains?(join_sql, "categorie_categories")  # Default naming fallback
    end
  end
end