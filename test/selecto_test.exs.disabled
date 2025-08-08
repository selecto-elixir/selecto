defmodule SelectoTest do
  use ExUnit.Case

  doctest Selecto

  defmodule Schema do
    def domain do
      %{
        source: %{
          source_table: "users",
          primary_key: :id,
          fields: [:id, :name, :email, :age, :active, :created_at, :updated_at],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            email: %{type: :string},
            age: %{type: :integer},
            active: %{type: :boolean},
            created_at: %{type: :utc_datetime},
            updated_at: %{type: :utc_datetime}
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
            source_table: "posts",
            primary_key: :id,
            fields: [:id, :title, :body, :user_id, :created_at, :updated_at],
            redact_fields: [],
            columns: %{
              id: %{type: :integer},
              title: %{type: :string},
              body: %{type: :string},
              user_id: %{type: :integer},
              created_at: %{type: :utc_datetime},
              updated_at: %{type: :utc_datetime}
            },
            associations: %{
              tags: %{
                queryable: :post_tags,
                field: :tags,
                owner_key: :id,
                related_key: :post_id
              }
            }
          },
          post_tags: %{
            source_table: "post_tags",
            primary_key: :id,
            fields: [:id, :name, :post_id],
            redact_fields: [],
            columns: %{
              id: %{type: :integer},
              name: %{type: :string},
              post_id: %{type: :integer}
            }
          }
        },
        name: "User",
        default_selected: ["name", "email"],
        default_aggregate: [{"id", %{"format" => "count"}}],
        required_filters: [{"active", true}],
        joins: %{
          posts: %{
            type: :left,
            name: "posts",
            parameters: [
              {:tag, :name}
            ],
            joins: %{
              tags: %{
                type: :left,
                name: "tags"
              }
            }
          }
        },
        filters: %{
          "active" => %{
            name: "Active",
            type: "boolean",
            default: true
          }
        }
      }
    end
  end

  setup_all do
    # Give the docker container a moment to start
    Process.sleep(5000)

    postgrex_opts = [
      hostname: "localhost",
      port: 5432,
      username: "postgres",
      password: "password",
      database: "selecto_test"
    ]

    {:ok, pid} = Postgrex.start_link(postgrex_opts)

    # Create tables
    Postgrex.query!(pid, "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, email TEXT, age INTEGER, active BOOLEAN, created_at TIMESTAMP, updated_at TIMESTAMP)", [])
    Postgrex.query!(pid, "CREATE TABLE posts (id SERIAL PRIMARY KEY, title TEXT, body TEXT, user_id INTEGER, created_at TIMESTAMP, updated_at TIMESTAMP)", [])
    Postgrex.query!(pid, "CREATE TABLE post_tags (id SERIAL PRIMARY KEY, name TEXT, post_id INTEGER)", [])

    # Seed data
    Postgrex.query!(pid, "INSERT INTO users (name, email, age, active) VALUES ('John Doe', 'john.doe@example.com', 30, true)", [])
    Postgrex.query!(pid, "INSERT INTO posts (title, body, user_id) VALUES ('My first post', 'This is my first post.', 1)", [])
    Postgrex.query!(pid, "INSERT INTO post_tags (name, post_id) VALUES ('elixir', 1)", [])


    selecto = Selecto.configure(Schema.domain(), pid)
    
    on_exit(fn ->
      Postgrex.query!(pid, "DROP TABLE users", [])
      Postgrex.query!(pid, "DROP TABLE posts", [])
      Postgrex.query!(pid, "DROP TABLE post_tags", [])
    end)

    {:ok, selecto: selecto, postgrex_opts: postgrex_opts}
  end

  test "configure/2", %{selecto: selecto} do
    assert %Selecto{} = selecto
  end

  test "simple select", %{selecto: selecto} do
    {rows, _, _} = Selecto.select(selecto, ["name", "email"]) |> Selecto.execute()
    assert length(rows) == 1
    assert List.first(rows) == ["John Doe", "john.doe@example.com"]
  end

  test "select with join", %{selecto: selecto} do
    {rows, _, _} = Selecto.select(selecto, ["name", "posts[title]"]) |> Selecto.execute()
    assert length(rows) == 1
    assert List.first(rows) == ["John Doe", "My first post"]
  end

  test "select with filter", %{selecto: selecto} do
    {rows, _, _} = Selecto.select(selecto, ["name", "email"]) |> Selecto.filter([{"name", "John Doe"}]) |> Selecto.execute()
    assert length(rows) == 1
    assert List.first(rows) == ["John Doe", "john.doe@example.com"]
  end

end