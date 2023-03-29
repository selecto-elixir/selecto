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
      has_many(:tags, SelectoTest.SchemaPostTags)
    end
  end

  defmodule SchemaPostTags do
    use Ecto.Schema

    schema "post_tags" do
      field(:name, :string)
      belongs_to(:post, SelectoTest.SchemaPosts)
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

    selecto = Selecto.configure(SelectoTest.Repo, domain)
    {:ok, selecto: selecto}
  end

  def gen_sql(selecto) do
    {sql, _, _params} = Selecto.gen_sql(selecto, %{})
    String.replace(sql, ~r/(\t|\n| )+/, " ")
  end

  # test Generate SQL
  test "generate sql", %{selecto: selecto} do
    selecto =
      Selecto.select(selecto, ["name", "email", "age", "active", "created_at", "updated_at"])
      |> Selecto.order_by("name")

    auto_assert " select \"selecto_root\".\"name\", \"selecto_root\".\"email\", \"selecto_root\".\"age\", \"selecto_root\".\"active\", \"selecto_root\".\"created_at\", \"selecto_root\".\"updated_at\" from users \"selecto_root\" where (( \"selecto_root\".\"active\" = $1 )) order by \"selecto_root\".\"name\" asc nulls first " <-
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
      |> Selecto.order_by("name")

    auto_assert " select \"selecto_root\".\"name\", \"selecto_root\".\"email\", \"selecto_root\".\"age\", \"selecto_root\".\"active\", \"selecto_root\".\"created_at\", \"selecto_root\".\"updated_at\", \"posts\".\"title\" from users \"selecto_root\" left join posts \"posts\" on \"posts\".\"schema_users_id\" = \"selecto_root\".\"id\" where (( \"selecto_root\".\"active\" = $1 )) order by \"selecto_root\".\"name\" asc nulls first " <-
                  gen_sql(selecto)
  end

  test "Where with OR", %{selecto: selecto} do
    ### TODO partly fails, should be able to just put {"active"} to indicate true
    selecto = Selecto.filter(selecto, {:or, [{"active", true}, {"active", false}]})

    auto_assert " select from users \"selecto_root\" where (( \"selecto_root\".\"active\" = $1 ) and ((( \"selecto_root\".\"active\" = $2 ) or ( \"selecto_root\".\"active\" = $3 )))) " <-
                  gen_sql(selecto)
  end

  test "Where with AND", %{selecto: selecto} do
    selecto = Selecto.filter(selecto, {:and, [{"active", true}, {"active", false}]})

    auto_assert " select from users \"selecto_root\" where (( \"selecto_root\".\"active\" = $1 ) and ((( \"selecto_root\".\"active\" = $2 ) and ( \"selecto_root\".\"active\" = $3 )))) " <-
                  gen_sql(selecto)
  end

  test "Where with NOT", %{selecto: selecto} do
    selecto = Selecto.filter(selecto, {:not, {"active", true}})

    auto_assert " select from users \"selecto_root\" where (( \"selecto_root\".\"active\" = $1 ) and (not ( \"selecto_root\".\"active\" = $2 ) )) " <-
                  gen_sql(selecto)
  end

  test "Concatenate Select", %{selecto: selecto} do
    selecto = Selecto.select(selecto, [{:concat, ["name", {:literal, " "}, "email"]}])

    auto_assert " select concat( \"selecto_root\".\"name\", ' ', \"selecto_root\".\"email\" ) from users \"selecto_root\" where (( \"selecto_root\".\"active\" = $1 )) " <-
                  gen_sql(selecto)
  end

  test "Predicate Like", %{selecto: selecto} do
    selecto = Selecto.filter(selecto, [{"name", {:like, {:literal, "%John%"}}}])

    auto_assert " select from users \"selecto_root\" where (( \"selecto_root\".\"active\" = $1 ) and ( \"selecto_root\".\"name\" like $2 )) " <-
                  gen_sql(selecto)
  end

  test "Predicate Null", %{selecto: selecto} do
    selecto = Selecto.filter(selecto, [{"name", nil}])

    auto_assert " select from users \"selecto_root\" where (( \"selecto_root\".\"active\" = $1 ) and ( \"selecto_root\".\"name\" is null )) " <-
                  gen_sql(selecto)
  end

  test "Predicate Not Null", %{selecto: selecto} do
    selecto = Selecto.filter(selecto, [{"name", :not_nil}])

    auto_assert " select from users \"selecto_root\" where (( \"selecto_root\".\"active\" = $1 ) and ( \"selecto_root\".\"name\" = $2 )) " <-
                  gen_sql(selecto)
  end

  # test "Predicate Equals", %{selecto: selecto} do
  #   ### Fails - how to indicate this is a column and not a value?
  #   selecto = Selecto.filter(selecto, [{"name", {:field, "name"}}])

  #   auto_assert gen_sql(selecto)
  # end

  test "Predicate Not Equals", %{selecto: selecto} do
    selecto = Selecto.filter(selecto, [{"name", {"!=", "John"}}])

    auto_assert " select from users \"selecto_root\" where (( \"selecto_root\".\"active\" = $1 ) and ( \"selecto_root\".\"name\" != $2 )) " <-
                  gen_sql(selecto)
  end

  test "Select Literal", %{selecto: selecto} do
    ### FAILS should be able to select {:literal, 3.0}
    selecto = Selecto.select(selecto, [1, 1.0, true, {:literal, "1"}, {:literal, 2}])

    auto_assert " select 1, 1.0, true, '1', 2 from users \"selecto_root\" where (( \"selecto_root\".\"active\" = $1 )) " <-
                  gen_sql(selecto)
  end

  test "Select Function Calls", %{selecto: selecto} do
    selecto = Selecto.select(selecto, [{:lower, "name"}])
    selecto = Selecto.select(selecto, [{:upper, "name"}])

    auto_assert " select lower(\"selecto_root\".\"name\"), upper(\"selecto_root\".\"name\") from users \"selecto_root\" where (( \"selecto_root\".\"active\" = $1 )) " <-
                  gen_sql(selecto)
  end

  test "Select Count", %{selecto: selecto} do
    selecto = Selecto.select(selecto, [{:count}])

    auto_assert " select count(*) from users \"selecto_root\" where (( \"selecto_root\".\"active\" = $1 )) " <-
                  gen_sql(selecto)
  end

  test "Select COALESCE", %{selecto: selecto} do
    selecto = Selecto.select(selecto, [{:coalesce, ["name", "email"]}])

    auto_assert " select coalesce( \"selecto_root\".\"name\", \"selecto_root\".\"email\" ) from users \"selecto_root\" where (( \"selecto_root\".\"active\" = $1 )) " <-
                  gen_sql(selecto)
  end

  test "Select CASE", %{selecto: selecto} do
    selecto = Selecto.select(selecto, {:case, [{{"name", "John"}, {:literal, "John!!"}}], "name"})

    auto_assert " select case when (( \"selecto_root\".\"name\" = $1 )) then 'John!!' else \"selecto_root\".\"name\" end from users \"selecto_root\" where (( \"selecto_root\".\"active\" = $2 )) " <-
                  gen_sql(selecto)
  end


  test "Predicate ANY subquery", %{selecto: selecto} do
    selecto = Selecto.filter(selecto, [{"name", "=", {:subquery, :any, "select name from users where name = 'John'", []}}])

    auto_assert " select from users \"selecto_root\" where (( \"selecto_root\".\"active\" = $1 ) and ( \"selecto_root\".\"name\" = any (select name from users where name = 'John') )) " <-
                  gen_sql(selecto)
  end

  # Test subquery IN, Exists, comparison

  # test "Parameterized Select", %{selecto: selecto} do
  # selecto = Selecto.select(selecto, "posts:cool[title]")
  # auto_assert gen_sql(selecto)
  # end
end
