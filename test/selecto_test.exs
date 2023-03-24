defmodule SelectoTest do
  use ExUnit.Case
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
      field :name, :string
      field :email, :string
      field :age, :integer
      field :active, :boolean
      field :created_at, :utc_datetime
      field :updated_at, :utc_datetime
      has_many :posts, SelectoTest.SchemaPosts
    end
  end

  defmodule SchemaPosts do
    use Ecto.Schema
    schema "posts" do
      field :title, :string
      field :body, :string
      field :created_at, :utc_datetime
      field :updated_at, :utc_datetime
      belongs_to :user, SelectoTest.SchemaUsers
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
      default_order_by: ["name"],
      default_group_by: ["name"],
      default_aggregate: [{"id", %{"format" => "count"}}],
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

  # Test Default filters


  # Test Default columns

  # Test joins

  # Test order

  # Test limit

  # test Generate SQL
  test "generate sql", %{selecto: selecto} do
    selecto = Selecto.select(selecto, ["name", "email", "age", "active", "created_at", "updated_at"])
    {sql, _, _} = Selecto.gen_sql(selecto, %{})
    assert sql == ~s[\n        select "selecto_root"."name", "selecto_root"."email", "selecto_root"."age", "selecto_root"."active", "selecto_root"."created_at", "selecto_root"."updated_at"\n        from users "selecto_root"\n    ]
  end

  # test Generate SQL with joins



end
