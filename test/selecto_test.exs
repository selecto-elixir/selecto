defmodule SelectoTest do
  use ExUnit.Case

  use Mneme

  doctest Selecto

  defmodule Repo do
    use Ecto.Repo,
      otp_app: :selecto_test,
      adapter: Ecto.Adapters.Postgres
  end

  # Some test schemas
  defmodule SchemaUsers do
    use Ecto.Schema

    schema "users" do
      field(:name, :string)
      field(:email, :string)
      field(:age, :integer)
      field(:active, :boolean)
      field(:created_at, :utc_datetime)
      field(:updated_at, :utc_datetime)
      has_many(:posts, SelectoTest.SchemaPosts)
    end
  end

  defmodule SchemaPosts do
    use Ecto.Schema

    schema "posts" do
      field(:title, :string)
      field(:body, :string)
      field(:created_at, :utc_datetime)
      field(:updated_at, :utc_datetime)
      belongs_to(:user, SelectoTest.SchemaUsers)
    end
  end

  test "single quotes" do
    assert Selecto.Builder.Sql.Helpers.single_wrap("It's") == "'It''s'"
  end

  test "double quotes" do
    assert Selecto.Builder.Sql.Helpers.double_wrap(~s[Hi There]) == ~s["Hi There"]
  end

  test "double quotes escape" do
    assert_raise RuntimeError, ~r/Invalid Table/, fn ->
      Selecto.Builder.Sql.Helpers.double_wrap(~s["Hi," she said])
    end
  end

  setup_all do
    domain = %{
      source: SelectoTest.SchemaUsers,
      name: "User",
      default_selected: ["name", "email"],
      default_aggregate: [{"id", %{"format" => "count"}}],
      required_filters: [{"active", true}],
      joins: %{
        posts: %{
          on: [user_id: :id],
          type: :left,
          name: "posts"
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

    selecto = Selecto.configure(SelectoTest.Repo, domain)
    {:ok, selecto: selecto}
  end

  def gen_sql(selecto) do
    {sql, _, params} = Selecto.gen_sql(selecto, %{})
    String.replace(sql, ~r/\t|\n| +/, " ")
  end

  # test Generate SQL
  test "generate sql", %{selecto: selecto} do
    selecto =
      Selecto.select(selecto, ["name", "email", "age", "active", "created_at", "updated_at"])
      |> Selecto.order_by("name")

    auto_assert "  select \"selecto_root\".\"name\", \"selecto_root\".\"email\", \"selecto_root\".\"age\", \"selecto_root\".\"active\", \"selecto_root\".\"created_at\", \"selecto_root\".\"updated_at\"  from users \"selecto_root\"    where (( \"selecto_root\".\"active\" = $1 ))    order by \"selecto_root\".\"name\" asc nulls first  " <-
                  gen_sql(selecto)
  end

  test "generate sql with joins", %{selecto: selecto} do
    selecto =
      Selecto.select(selecto, [
        "name",
        "email",
        "age",
        "active",
        "created_at",
        "updated_at",
        "posts[title]"
      ])

    auto_assert "  select \"selecto_root\".\"name\", \"selecto_root\".\"email\", \"selecto_root\".\"age\", \"selecto_root\".\"active\", \"selecto_root\".\"created_at\", \"selecto_root\".\"updated_at\", \"posts\".\"title\"  from users \"selecto_root\" left join posts \"posts\" on \"posts\".\"schema_users_id\" = \"selecto_root\".\"id\"    where (( \"selecto_root\".\"active\" = $1 ))  " <-
                  gen_sql(selecto)
  end
end
