defmodule Selecto.Builder.JoinComprehensiveTest do
  use ExUnit.Case

  alias Selecto.Builder.Join

  describe "from_selects/2" do
    setup do
      fields = %{
        "id" => %{requires_join: :selecto_root},
        "name" => %{requires_join: :selecto_root},
        "posts[title]" => %{requires_join: :posts},
        "posts[author]" => %{requires_join: :posts},
        "tags[name]" => %{requires_join: :tags},
        "category[name]" => %{requires_join: :categories},
        "comments[text]" => %{requires_join: :comments}
      }
      {:ok, fields: fields}
    end

    test "extracts joins from simple field selections", %{fields: fields} do
      selected = ["name", "posts[title]"]
      
      result = Join.from_selects(fields, selected)
      
      assert :selecto_root in result
      assert :posts in result
      assert length(result) == 2
    end

    test "extracts joins from tuple selections with field data", %{fields: fields} do
      selected = [
        {"name", "name", :string},
        {"posts_title", "posts[title]", :string}
      ]
      
      result = Join.from_selects(fields, selected)
      
      assert :selecto_root in result
      assert :posts in result
    end

    test "extracts joins from complex tuple selections", %{fields: fields} do
      selected = [
        {"name", {"name", :display}, :string},
        {"posts_title", {"posts[title]", :display}, :string}
      ]
      
      result = Join.from_selects(fields, selected)
      
      assert :selecto_root in result
      assert :posts in result
    end

    test "handles array aggregations", %{fields: fields} do
      selected = [
        {:array, "tags", ["tags[name]"]},
        {"name", "name", :string}
      ]
      
      result = Join.from_selects(fields, selected)
      
      assert :selecto_root in result
      assert :tags in result
    end

    test "handles coalesce operations", %{fields: fields} do
      selected = [
        {:coalesce, "user_name", ["name", "posts[author]"]}
      ]
      
      result = Join.from_selects(fields, selected)
      
      assert :selecto_root in result
      assert :posts in result
    end

    test "handles case expressions", %{fields: fields} do
      case_map = %{
        "when_active" => ["name"],
        "when_inactive" => ["posts[title]"]
      }
      selected = [
        {:case, "status_name", case_map}
      ]
      
      result = Join.from_selects(fields, selected)
      
      assert :selecto_root in result
      assert :posts in result
    end

    test "filters out literal values", %{fields: fields} do
      selected = [
        {:literal, "static_value", "Some Value"},
        {"name", "name", :string}
      ]
      
      result = Join.from_selects(fields, selected)
      
      assert :selecto_root in result
      assert length(result) == 1
    end

    test "filters out literal selections from field processing", %{fields: fields} do
      selected = [
        {"name", {:literal, "Static"}, :string},
        {"posts_title", "posts[title]", :string}
      ]
      
      result = Join.from_selects(fields, selected)
      
      # Should only include :posts, not :selecto_root since the literal is filtered out
      assert :posts in result
    end

    test "handles single field selections", %{fields: fields} do
      selected = [
        {"name"}  # Single field tuple
      ]
      
      result = Join.from_selects(fields, selected)
      
      # Single field tuples return nil, which should be filtered out
      assert result == []
    end

    test "handles nil field references gracefully", %{fields: fields} do
      selected = [
        "nonexistent_field",
        "name"
      ]
      
      result = Join.from_selects(fields, selected)
      
      # Should only include joins for fields that exist in the fields map
      assert :selecto_root in result
      assert length(result) == 1
    end

    test "deduplicates joins from multiple fields requiring same join", %{fields: fields} do
      selected = [
        "posts[title]",
        "posts[author]"
      ]
      
      result = Join.from_selects(fields, selected)
      
      # Both fields require :posts join, should be deduplicated
      assert :posts in result
      assert length(result) == 1
    end

    test "handles empty selected list", %{fields: _fields} do
      selected = []
      
      result = Join.from_selects(%{}, selected)
      
      assert result == []
    end

    test "handles complex mixed selections", %{fields: fields} do
      selected = [
        "name",  # selecto_root
        {:array, "tags", ["tags[name]"]},  # tags
        {:coalesce, "title", ["posts[title]"]},  # posts
        {:literal, "static", "value"},  # filtered out
        {"category", "category[name]", :string}  # categories
      ]
      
      result = Join.from_selects(fields, selected)
      
      assert :selecto_root in result
      assert :tags in result
      assert :posts in result
      assert :categories in result
      assert length(result) == 4
    end
  end

  describe "get_join_order/2" do
    test "handles simple join with no dependencies" do
      joins = %{
        posts: %{requires_join: :selecto_root}
      }
      requested_joins = [:posts]
      
      result = Join.get_join_order(joins, requested_joins)
      
      # Should include the dependency :selecto_root and the requested join :posts
      assert :selecto_root in result
      assert :posts in result
    end

    test "orders joins based on dependencies" do
      joins = %{
        posts: %{requires_join: :selecto_root},
        comments: %{requires_join: :posts}
      }
      requested_joins = [:comments, :posts]
      
      result = Join.get_join_order(joins, requested_joins)
      
      # posts should come before comments since comments depends on posts
      posts_index = Enum.find_index(result, &(&1 == :posts))
      comments_index = Enum.find_index(result, &(&1 == :comments))
      
      assert posts_index < comments_index
      assert :posts in result
      assert :comments in result
    end

    test "handles complex dependency chains" do
      joins = %{
        users: %{requires_join: :selecto_root},
        posts: %{requires_join: :users},
        comments: %{requires_join: :posts},
        likes: %{requires_join: :comments}
      }
      requested_joins = [:likes, :comments, :posts, :users]
      
      result = Join.get_join_order(joins, requested_joins)
      
      # Verify proper ordering: users -> posts -> comments -> likes
      users_idx = Enum.find_index(result, &(&1 == :users))
      posts_idx = Enum.find_index(result, &(&1 == :posts))
      comments_idx = Enum.find_index(result, &(&1 == :comments))
      likes_idx = Enum.find_index(result, &(&1 == :likes))
      
      assert users_idx < posts_idx
      assert posts_idx < comments_idx
      assert comments_idx < likes_idx
    end

    test "handles missing dependencies gracefully" do
      joins = %{
        posts: %{requires_join: :selecto_root}
      }
      requested_joins = [:posts, :nonexistent]
      
      result = Join.get_join_order(joins, requested_joins)
      
      # Should include both, even though nonexistent has no join config
      assert :posts in result
      assert :nonexistent in result
    end

    test "deduplicates joins in dependency resolution" do
      joins = %{
        posts: %{requires_join: :users},
        comments: %{requires_join: :posts},
        users: %{requires_join: :selecto_root}
      }
      # Request same join multiple times
      requested_joins = [:posts, :comments, :posts]
      
      result = Join.get_join_order(joins, requested_joins)
      
      # Should only have unique joins
      assert length(Enum.uniq(result)) == length(result)
      assert :users in result
      assert :posts in result
      assert :comments in result
    end

    test "handles self-referencing joins" do
      # Test joins that reference themselves (adjacency list pattern)
      joins = %{
        categories: %{requires_join: :selecto_root},
        parent_categories: %{requires_join: :categories}
      }
      requested_joins = [:parent_categories]
      
      result = Join.get_join_order(joins, requested_joins)
      
      assert :categories in result
      assert :parent_categories in result
      
      # categories should come before parent_categories
      cat_idx = Enum.find_index(result, &(&1 == :categories))
      parent_idx = Enum.find_index(result, &(&1 == :parent_categories))
      assert cat_idx < parent_idx
    end

    test "handles empty requested joins" do
      joins = %{
        posts: %{requires_join: :selecto_root}
      }
      requested_joins = []
      
      result = Join.get_join_order(joins, requested_joins)
      
      assert result == []
    end

    test "handles joins with nil requirements" do
      joins = %{
        posts: %{requires_join: nil}
      }
      requested_joins = [:posts]
      
      result = Join.get_join_order(joins, requested_joins)
      
      assert result == [:posts]
    end

    test "handles mixed dependency scenarios" do
      joins = %{
        posts: %{requires_join: :selecto_root},
        tags: %{requires_join: :selecto_root}, 
        post_tags: %{requires_join: :posts},
        categories: %{}  # No requires_join
      }
      requested_joins = [:post_tags, :tags, :categories, :posts]
      
      result = Join.get_join_order(joins, requested_joins)
      
      # posts should come before post_tags
      posts_idx = Enum.find_index(result, &(&1 == :posts))
      post_tags_idx = Enum.find_index(result, &(&1 == :post_tags))
      
      assert posts_idx < post_tags_idx
      assert :tags in result
      assert :categories in result
    end
  end

  describe "from_filters/2" do
    setup do
      config = %{
        columns: %{
          "name" => %{requires_join: :selecto_root},
          "email" => %{requires_join: :selecto_root},
          "posts[title]" => %{requires_join: :posts},
          "posts[content]" => %{requires_join: :posts},
          "tags[name]" => %{requires_join: :tags},
          "category[name]" => %{requires_join: :categories},
          "comments[text]" => %{requires_join: :comments}
        }
      }
      {:ok, config: config}
    end

    test "extracts joins from simple filters", %{config: config} do
      filters = [
        {"name", "John"},
        {"posts[title]", "Hello World"}
      ]
      
      result = Join.from_filters(config, filters)
      
      assert :selecto_root in result
      assert :posts in result
      assert length(result) == 2
    end

    test "handles OR filter combinations", %{config: config} do
      filters = [
        {:or, [
          {"name", "John"},
          {"posts[title]", "Hello"}
        ]}
      ]
      
      result = Join.from_filters(config, filters)
      
      assert :selecto_root in result
      assert :posts in result
    end

    test "handles AND filter combinations", %{config: config} do
      filters = [
        {:and, [
          {"email", "john@example.com"},
          {"tags[name]", "elixir"}
        ]}
      ]
      
      result = Join.from_filters(config, filters)
      
      assert :selecto_root in result
      assert :tags in result
    end

    test "handles nested OR/AND combinations", %{config: config} do
      filters = [
        {:or, [
          {"name", "John"},
          {:and, [
            {"posts[title]", "Hello"},
            {"category[name]", "Tech"}
          ]}
        ]}
      ]
      
      result = Join.from_filters(config, filters)
      
      assert :selecto_root in result
      assert :posts in result
      assert :categories in result
    end

    test "deduplicates joins from multiple filters on same table", %{config: config} do
      filters = [
        {"posts[title]", "Hello"},
        {"posts[content]", "World"}
      ]
      
      result = Join.from_filters(config, filters)
      
      # Both filters require :posts, should be deduplicated
      assert :posts in result
      assert length(result) == 1
    end

    test "handles deeply nested logical combinations", %{config: config} do
      filters = [
        {:or, [
          {"name", "John"},
          {:and, [
            {"posts[title]", "Hello"},
            {:or, [
              {"tags[name]", "elixir"},
              {"comments[text]", "Great post"}
            ]}
          ]}
        ]}
      ]
      
      result = Join.from_filters(config, filters)
      
      assert :selecto_root in result
      assert :posts in result
      assert :tags in result
      assert :comments in result
      assert length(result) == 4
    end

    test "handles empty filter list", %{config: config} do
      filters = []
      
      result = Join.from_filters(config, filters)
      
      assert result == []
    end

    test "handles empty OR filter", %{config: config} do
      filters = [
        {:or, []}
      ]
      
      result = Join.from_filters(config, filters)
      
      assert result == []
    end

    test "handles empty AND filter", %{config: config} do
      filters = [
        {:and, []}
      ]
      
      result = Join.from_filters(config, filters)
      
      assert result == []
    end

    test "handles filters with various value types", %{config: config} do
      filters = [
        {"name", "John"},           # String value
        {"posts[title]", nil},      # Nil value
        {"tags[name]", 123},        # Integer value
        {"category[name]", true}    # Boolean value
      ]
      
      result = Join.from_filters(config, filters)
      
      # Should extract joins regardless of value type
      assert :selecto_root in result
      assert :posts in result
      assert :tags in result
      assert :categories in result
    end

    test "handles mixed logical operators with multiple levels", %{config: config} do
      filters = [
        {:and, [
          {"name", "John"},
          {:or, [
            {"posts[title]", "Hello"},
            {"posts[content]", "World"}
          ]}
        ]},
        {:or, [
          {"tags[name]", "elixir"},
          {:and, [
            {"category[name]", "Tech"},
            {"comments[text]", "Nice"}
          ]}
        ]}
      ]
      
      result = Join.from_filters(config, filters)
      
      assert :selecto_root in result
      assert :posts in result
      assert :tags in result
      assert :categories in result
      assert :comments in result
    end

    test "handles complex realistic filtering scenario", %{config: config} do
      # Simulate a complex query: (name = "John" OR email = "john@example.com") 
      # AND (posts contain "elixir" OR tags contain "programming") 
      # AND comments exist
      filters = [
        {:and, [
          {:or, [
            {"name", "John"},
            {"email", "john@example.com"}
          ]},
          {:or, [
            {"posts[content]", {:like, "%elixir%"}},
            {"tags[name]", "programming"}
          ]},
          {"comments[text]", {:not_null}}
        ]}
      ]
      
      result = Join.from_filters(config, filters)
      
      assert :selecto_root in result
      assert :posts in result
      assert :tags in result
      assert :comments in result
    end
  end

  describe "integration scenarios" do
    test "from_selects and from_filters produce consistent join requirements" do
      fields = %{
        "name" => %{requires_join: :selecto_root},
        "posts[title]" => %{requires_join: :posts}
      }
      
      config = %{
        columns: %{
          "name" => %{requires_join: :selecto_root},
          "posts[title]" => %{requires_join: :posts}
        }
      }
      
      selected = ["name", "posts[title]"]
      filters = [{"name", "John"}, {"posts[title]", "Hello"}]
      
      select_joins = Join.from_selects(fields, selected)
      filter_joins = Join.from_filters(config, filters)
      
      # Both should identify the same required joins
      assert Enum.sort(select_joins) == Enum.sort(filter_joins)
    end

    test "get_join_order handles joins from both selects and filters" do
      joins = %{
        posts: %{requires_join: :selecto_root},
        comments: %{requires_join: :posts},
        tags: %{requires_join: :selecto_root}
      }
      
      # Joins needed for selection
      select_joins = [:posts, :tags]
      # Joins needed for filtering  
      filter_joins = [:comments, :posts]
      
      # Combined required joins
      all_joins = (select_joins ++ filter_joins) |> Enum.uniq()
      
      result = Join.get_join_order(joins, all_joins)
      
      # Should properly order all required joins
      assert :posts in result
      assert :tags in result
      assert :comments in result
      
      # Verify dependency ordering
      posts_idx = Enum.find_index(result, &(&1 == :posts))
      comments_idx = Enum.find_index(result, &(&1 == :comments))
      
      assert posts_idx < comments_idx
    end
  end
end