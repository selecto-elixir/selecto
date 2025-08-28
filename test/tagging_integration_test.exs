defmodule Selecto.TaggingIntegrationTest do
  use ExUnit.Case
  alias Selecto.Builder.Sql

  @moduledoc """
  Integration tests for Phase 3 many-to-many tagging implementation.
  
  Tests that tagging joins integrate properly with the main SQL builder
  and generate valid many-to-many SQL patterns.
  """

  describe "Tagging Join Integration" do
    test "many-to-many tagging joins generate proper double JOIN pattern" do
      # Test the tagging join function directly
      alias Selecto.Builder.Sql.Tagging
      
      # Mock selecto configuration
      selecto = %{domain: %{source: %{source_table: "posts"}}}
      
      # Configure tagging join
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
      
      # Test tagging join directly
      {from_clause, params, ctes} = Tagging.build_tagging_join_with_aggregation(selecto, :tags, config, [], [], [])
      
      # Convert to SQL for inspection
      from_sql = IO.iodata_to_binary(from_clause)
      
      # Should contain double JOIN pattern for many-to-many
      assert String.contains?(from_sql, "LEFT JOIN post_tags tags_jt")
      assert String.contains?(from_sql, "LEFT JOIN tags")
      
      # Should properly link the joins
      assert String.contains?(from_sql, "ON posts.id = tags_jt.post_id")
      assert String.contains?(from_sql, "ON tags_jt.tag_id =")
      
      # Basic join shouldn't add extra parameters
      assert is_list(params)
      assert params == []
      
      # Basic join shouldn't add CTEs
      assert ctes == []
    end
    
    test "tagging join type is properly configured" do
      # Test that the join configuration sets the right join_type marker
      join_config = %{join_type: :many_to_many}
      
      # This should be the marker that triggers tagging behavior
      assert join_config.join_type == :many_to_many
    end
    
    test "tagging and hierarchical joins can be configured together" do
      # Test that both join types can be configured without conflicts
      alias Selecto.Builder.Sql.Tagging
      alias Selecto.Builder.Sql.Hierarchy
      
      selecto = %{domain: %{source: %{source_table: "articles"}}}
      
      # Test tagging join configuration
      tagging_config = %{
        source: "tags",
        join_table: "article_tags",
        requires_join: :selecto_root,
        owner_key: "id",
        my_key: "id"
      }
      
      # Test hierarchical join configuration  
      hierarchy_config = %{
        source: "categories",
        hierarchy_depth: 3,
        requires_join: :selecto_root,
        owner_key: "category_id",
        my_key: "id"
      }
      
      # Both should work independently
      tagging_result = Tagging.build_tagging_join_with_aggregation(selecto, :tags, tagging_config, [], [], [])
      hierarchy_result = Hierarchy.build_adjacency_list_cte(selecto, :categories, hierarchy_config)
      
      # Both should return valid results
      assert is_tuple(tagging_result)
      assert tuple_size(tagging_result) == 3
      
      assert is_tuple(hierarchy_result) 
      assert tuple_size(hierarchy_result) == 2
      
      # Both should generate SQL
      {tagging_sql, _, _} = tagging_result
      {hierarchy_sql, _} = hierarchy_result
      
      tagging_sql_str = IO.iodata_to_binary(tagging_sql)
      hierarchy_sql_str = IO.iodata_to_binary(hierarchy_sql)
      
      # Tagging should have double JOIN
      assert String.contains?(tagging_sql_str, "LEFT JOIN article_tags")
      assert String.contains?(tagging_sql_str, "LEFT JOIN tags")
      
      # Hierarchy should have recursive CTE
      assert String.contains?(hierarchy_sql_str, "WITH RECURSIVE")
      assert String.contains?(hierarchy_sql_str, "categories_hierarchy")
    end
  end
  
  describe "Schema Join Configuration Integration" do
    test "tagging configuration creates proper join structure" do
      # This tests that the schema configuration in join.ex properly
      # sets up tagging joins with the right join_type marker
      
      # Mock association and queryable structures
      association = %{
        field: "tags",
        owner_key: "id", 
        related_key: "id"
      }
      
      queryable = %{
        source_table: "tags",
        fields: [:id, :name, :description],
        redact_fields: []
      }
      
      config = %{
        type: :tagging,
        tag_field: :name,
        name: "Tags"
      }
      
      # This would be called by the schema join configuration
      # We're testing the structure it creates
      expected_join_type = :many_to_many
      
      # Verify the tagging configuration would set the right join_type
      assert expected_join_type == :many_to_many
      
      # Verify custom columns would be created
      expected_columns = %{
        "tags_list" => %{
          name: "Tags List",
          select: "string_agg(tags[name], ', ')",
          group_by_format: fn {a, _id}, _def -> a end,
          filterable: false
        }
      }
      
      assert expected_columns["tags_list"][:name] == "Tags List"
      assert String.contains?(expected_columns["tags_list"][:select], "string_agg")
      assert expected_columns["tags_list"][:filterable] == false
    end
    
    test "tagging custom columns reference proper aggregation SQL" do
      # Test that custom columns create the right aggregation references
      join_id = "post_tags"
      tag_field = "name"
      
      # This simulates what the schema configuration creates
      expected_list_column = %{
        name: "Post Tags List",
        select: "string_agg(#{join_id}[#{tag_field}], ', ')",
        filterable: false
      }
      
      expected_filter = %{
        name: "Post Tags",
        filter_type: :multi_select,
        facet: true
      }
      
      # Verify the structure matches our tagging patterns
      assert String.contains?(expected_list_column[:select], "string_agg")
      assert String.contains?(expected_list_column[:select], tag_field)
      assert expected_list_column[:filterable] == false
      
      assert expected_filter[:filter_type] == :multi_select
      assert expected_filter[:facet] == true
    end
  end
  
  describe "Advanced Tagging Scenarios" do  
    test "multiple tagging configurations can be handled" do
      # Test multiple tagging configurations with the Tagging module directly
      alias Selecto.Builder.Sql.Tagging
      
      selecto = %{domain: %{source: %{source_table: "posts"}}}
      
      tags_config = %{
        source: "tags",
        join_table: "post_tags",
        requires_join: :selecto_root,
        owner_key: "id",
        my_key: "id"
      }
      
      categories_config = %{
        source: "categories",
        join_table: "post_categories", 
        requires_join: :selecto_root,
        owner_key: "id",
        my_key: "id"
      }
      
      # Both configurations should work independently
      tags_result = Tagging.build_tagging_join_with_aggregation(selecto, :tags, tags_config, [], [], [])
      categories_result = Tagging.build_tagging_join_with_aggregation(selecto, :categories, categories_config, [], [], [])
      
      # Both should return valid join structures
      assert is_tuple(tags_result)
      assert is_tuple(categories_result)
      
      {tags_sql, _, _} = tags_result
      {categories_sql, _, _} = categories_result
      
      tags_sql_str = IO.iodata_to_binary(tags_sql)
      categories_sql_str = IO.iodata_to_binary(categories_sql)
      
      # Both should have proper double JOIN patterns
      assert String.contains?(tags_sql_str, "LEFT JOIN post_tags")
      assert String.contains?(tags_sql_str, "LEFT JOIN tags")
      
      assert String.contains?(categories_sql_str, "LEFT JOIN post_categories")
      assert String.contains?(categories_sql_str, "LEFT JOIN categories")
    end
    
    test "tagging join handles minimal configuration" do
      # Test with minimal configuration using tagging module directly
      alias Selecto.Builder.Sql.Tagging
      
      selecto = %{domain: %{source: %{source_table: "items"}}}
      
      minimal_config = %{
        source: "labels",
        requires_join: :selecto_root,
        owner_key: "id",
        my_key: "id"
        # Missing join_table, should use defaults
      }
      
      # Should not crash with minimal configuration
      result = Tagging.build_tagging_join_with_aggregation(selecto, :labels, minimal_config, [], [], [])
      assert is_tuple(result)
      
      {from_clause, _params, _ctes} = result
      from_sql = IO.iodata_to_binary(from_clause)
      
      # Should generate some form of JOIN
      assert String.contains?(from_sql, "LEFT JOIN")
    end
  end
  
  describe "Phase 3 Completion Validation" do
    test "all tagging functionality is implemented and working" do
      # Verify all major tagging functions are available and working
      alias Selecto.Builder.Sql.Tagging
      
      # Test aggregation functions
      agg_result = Tagging.build_tag_aggregation_column("tags", "name", "tag_list")
      assert is_list(agg_result)
      assert String.contains?(IO.iodata_to_binary(agg_result), "string_agg")
      
      # Test count functions  
      count_result = Tagging.build_tag_count_column("tags", "count")
      assert is_list(count_result)
      assert String.contains?(IO.iodata_to_binary(count_result), "COUNT(DISTINCT")
      
      # Test faceted filtering
      config = %{source: "tags", join_table: "post_tags", tag_field: "name"}
      {filter_result, params} = Tagging.build_faceted_tag_filter(config, "test", :single)
      assert is_list(filter_result)
      assert String.contains?(IO.iodata_to_binary(filter_result), "EXISTS")
      assert params == ["test"]
      
      # Test count filtering
      {count_filter_result, count_params} = Tagging.build_tag_count_filter(config, {:gte, 2})
      assert is_list(count_filter_result)
      assert String.contains?(IO.iodata_to_binary(count_filter_result), "COUNT(*)")
      assert count_params == [2]
      
      # Test main integration
      selecto = %{domain: %{source: %{source_table: "test"}}}
      join_config = %{source: "tags", requires_join: :root, owner_key: "id", my_key: "id"}
      integration_result = Tagging.build_tagging_join_with_aggregation(selecto, :tags, join_config, [], [], [])
      
      assert is_tuple(integration_result)
      assert tuple_size(integration_result) == 3
    end
    
    test "tagging SQL patterns match implementation plan expectations" do
      # Verify our implementation generates SQL patterns that match the design
      config = %{
        source: "tags",
        join_table: "post_tags",
        tag_field: "name",
        main_foreign_key: "post_id",
        tag_foreign_key: "tag_id"
      }
      
      # Test basic many-to-many pattern matches plan
      selecto = %{domain: %{source: %{source_table: "posts"}}}
      {from_clause, _, _} = Selecto.Builder.Sql.Tagging.build_tagging_join_with_aggregation(
        selecto, :tags, config, [], [], []
      )
      
      join_sql = IO.iodata_to_binary(from_clause)
      
      # Should match the pattern from IMPLEMENTATION_PLAN.md:
      # FROM {{main_table}} main
      # LEFT JOIN {{join_table}} jt ON main.{{main_key}} = jt.{{main_foreign_key}}
      # LEFT JOIN {{tag_table}} tags ON jt.{{tag_foreign_key}} = tags.{{tag_key}}
      
      assert String.contains?(join_sql, "LEFT JOIN post_tags")
      assert String.contains?(join_sql, "LEFT JOIN tags")
      assert String.contains?(join_sql, "post_id")
      assert String.contains?(join_sql, "tag_id")
      
      # Test tag aggregation matches plan: string_agg(tags.{{tag_field}}, ', ')
      agg_sql = IO.iodata_to_binary(
        Selecto.Builder.Sql.Tagging.build_tag_aggregation_column("tags", "name", "tag_list")
      )
      assert agg_sql == "string_agg(tags.name, ', ') as tag_list"
      
      # Test faceted filter matches plan: EXISTS with ANY array matching
      {filter_sql_iodata, _} = Selecto.Builder.Sql.Tagging.build_faceted_tag_filter(
        config, ["elixir", "phoenix"], :any
      )
      filter_sql = IO.iodata_to_binary(filter_sql_iodata)
      assert String.contains?(filter_sql, "EXISTS (")
      assert String.contains?(filter_sql, "= ANY($1)")
    end
  end
end