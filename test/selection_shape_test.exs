defmodule Selecto.SelectionShapeTest do
  use ExUnit.Case, async: true

  defp domain do
    %{
      name: "Selection Shape",
      source: %{
        source_table: "users",
        primary_key: :id,
        fields: [:id, :name, :email, :active],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          email: %{type: :string},
          active: %{type: :boolean}
        },
        associations: %{
          posts: %{
            queryable: :posts,
            field: :posts,
            owner_key: :id,
            related_key: :user_id
          }
        }
      },
      schemas: %{
        posts: %{
          name: "Post",
          source_table: "posts",
          primary_key: :id,
          fields: [:id, :title, :published, :user_id],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            title: %{type: :string},
            published: %{type: :boolean},
            user_id: %{type: :integer}
          },
          associations: %{}
        }
      },
      joins: %{
        posts: %{type: :left}
      }
    }
  end

  test "select_shape compiles flat selectors and subselect nodes" do
    selecto =
      domain()
      |> Selecto.configure(:mock_connection, validate: false)
      |> Selecto.select_shape(["name", {"email", "active"}, ["posts.title", "posts.published"]])

    assert selecto.set.selected == ["name", "email", "active"]
    assert length(selecto.set.subselected) == 1

    [subselect] = selecto.set.subselected
    assert subselect.target_schema == :posts
    assert subselect.fields == ["title", "published"]

    assert %{selected_count: 3, subselect_count: 1} = selecto.set.selection_shape
  end

  test "to_sql includes correlated json_agg subselect for nested join container" do
    query =
      domain()
      |> Selecto.configure(:mock_connection, validate: false)
      |> Selecto.select_shape(["name", ["posts.title", "posts.published"]])

    {sql, params} = Selecto.to_sql(query)

    assert params == []
    assert sql =~ ~r/select\s+selecto_root\.name/i
    assert sql =~ ~r/json_agg\(json_build_object/i
    assert sql =~ ~r/from\s+posts/i
    assert sql =~ ~r/where\s+sub_posts\."user_id"\s*=\s*selecto_root\."id"/i
  end

  test "shape_rows reconstructs list and tuple containers including subselect values" do
    query =
      domain()
      |> Selecto.configure(:mock_connection, validate: false)
      |> Selecto.select_shape(["name", {"email", "active"}, ["posts.title", "posts.published"]])

    row = [
      "Alice",
      "alice@example.com",
      true,
      [
        %{"title" => "First", "published" => true},
        %{"title" => "Second", "published" => false}
      ]
    ]

    shaped = Selecto.SelectionShape.shape_rows([row], query.set.selection_shape)

    assert shaped == [
             [
               "Alice",
               {"alice@example.com", true},
               [["First", true], ["Second", false]]
             ]
           ]
  end

  test "tuple subselect node materializes as list of tuples" do
    query =
      domain()
      |> Selecto.configure(:mock_connection, validate: false)
      |> Selecto.select_shape(["name", {"posts.title", "posts.published"}])

    row = [
      "Alice",
      [
        %{"title" => "First", "published" => true},
        %{"title" => "Second", "published" => false}
      ]
    ]

    shaped = Selecto.SelectionShape.shape_rows([row], query.set.selection_shape)

    assert shaped == [["Alice", [{"First", true}, {"Second", false}]]]
  end
end
