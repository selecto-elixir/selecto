defmodule SelectoPhoenixEctoExamples do
  @moduledoc """
  Examples of using Selecto with Phoenix and Ecto.
  
  This file demonstrates various integration patterns for using Selecto
  in Phoenix applications with Ecto schemas and repositories.
  """

  # Example Ecto schemas for demonstration
  defmodule MyApp.User do
    use Ecto.Schema

    schema "users" do
      field :name, :string
      field :email, :string
      field :age, :integer
      field :active, :boolean, default: true
      
      has_many :posts, MyApp.Post
      has_one :profile, MyApp.Profile
      
      timestamps()
    end
  end

  defmodule MyApp.Post do
    use Ecto.Schema

    schema "posts" do
      field :title, :string
      field :body, :string
      field :published, :boolean, default: false
      
      belongs_to :user, MyApp.User
      has_many :comments, MyApp.Comment
      
      timestamps()
    end
  end

  defmodule MyApp.Profile do
    use Ecto.Schema

    schema "profiles" do
      field :bio, :string
      field :website, :string
      
      belongs_to :user, MyApp.User
    end
  end

  defmodule MyApp.Comment do
    use Ecto.Schema

    schema "comments" do
      field :content, :string
      field :approved, :boolean, default: false
      
      belongs_to :post, MyApp.Post
      
      timestamps()
    end
  end

  @doc """
  Basic Phoenix Controller Example
  
  Shows how to use Selecto in a standard Phoenix controller.
  """
  def controller_example do
    quote do
      defmodule MyAppWeb.UserController do
        use MyAppWeb, :controller
        
        def index(conn, params) do
          # Configure Selecto from Ecto schema
          selecto = Selecto.from_ecto(MyApp.Repo, MyApp.User, 
            joins: [:posts, :profile]
          )
          
          # Apply URL parameters (pagination, filters, sorting)
          selecto = Selecto.PhoenixHelpers.from_params(selecto, params)
          
          # Execute query
          case Selecto.execute(selecto) do
            {:ok, {users, columns, aliases}} ->
              render(conn, "index.html", 
                users: users, 
                columns: columns,
                selecto: selecto
              )
              
            {:error, error} ->
              conn
              |> put_flash(:error, "Failed to load users")
              |> render("index.html", users: [], columns: [])
          end
        end
        
        def show(conn, %{"id" => id}) do
          selecto = Selecto.from_ecto(MyApp.Repo, MyApp.User)
          |> Selecto.filter({"id", id})
          |> Selecto.select(["name", "email", "posts[title]", "profile[bio]"])
          
          case Selecto.execute_one(selecto) do
            {:ok, {user, aliases}} ->
              render(conn, "show.html", user: user, aliases: aliases)
              
            {:error, :no_results} ->
              conn
              |> put_status(:not_found)
              |> render(MyAppWeb.ErrorView, "404.html")
              
            {:error, error} ->
              conn
              |> put_status(:internal_server_error)
              |> render(MyAppWeb.ErrorView, "500.html")
          end
        end
      end
    end
  end

  @doc """
  Phoenix LiveView Example
  
  Shows integration with LiveView for interactive data exploration.
  """
  def liveview_example do
    quote do
      defmodule MyAppWeb.UserLive.Index do
        use MyAppWeb, :live_view

        @impl true
        def mount(_params, _session, socket) do
          # Initialize base Selecto configuration
          selecto = Selecto.from_ecto(MyApp.Repo, MyApp.User,
            joins: [:posts, :profile],
            redact_fields: [:password_hash]
          )
          
          socket = assign(socket,
            selecto: selecto,
            users: [],
            loading: false,
            total_count: 0
          )
          
          {:ok, socket}
        end

        @impl true
        def handle_params(params, _uri, socket) do
          # Apply URL parameters to selecto
          selecto = Selecto.PhoenixHelpers.from_params(socket.assigns.selecto, params)
          
          socket = assign(socket, selecto: selecto)
          |> load_users()
          
          {:noreply, socket}
        end

        @impl true
        def handle_event("filter", %{"filter" => filter_params}, socket) do
          # Update filters based on form input
          selecto = Selecto.PhoenixHelpers.update_selecto(
            socket.assigns.selecto,
            :filter,
            filter_params
          )
          
          # Generate new URL with updated params
          params = Selecto.PhoenixHelpers.to_params(selecto)
          
          {:noreply, push_patch(socket, to: Routes.user_index_path(socket, :index, params))}
        end
        
        @impl true
        def handle_event("sort", %{"field" => field}, socket) do
          selecto = Selecto.PhoenixHelpers.update_selecto(
            socket.assigns.selecto,
            :sort,
            field
          )
          
          params = Selecto.PhoenixHelpers.to_params(selecto)
          {:noreply, push_patch(socket, to: Routes.user_index_path(socket, :index, params))}
        end
        
        @impl true
        def handle_event("paginate", %{"page" => page}, socket) do
          selecto = Selecto.PhoenixHelpers.update_selecto(
            socket.assigns.selecto,
            :page,
            page
          )
          
          params = Selecto.PhoenixHelpers.to_params(selecto)
          {:noreply, push_patch(socket, to: Routes.user_index_path(socket, :index, params))}
        end

        defp load_users(socket) do
          assign(socket, loading: true)
          
          # Execute main query
          case Selecto.execute(socket.assigns.selecto) do
            {:ok, {users, columns, aliases}} ->
              # Get total count for pagination
              count_selecto = socket.assigns.selecto
              |> Selecto.select([{"id", %{"format" => "count"}}])
              
              total_count = case Selecto.execute_one(count_selecto) do
                {:ok, {[count], _}} -> count
                _ -> 0
              end
              
              # Generate pagination info
              pagination = Selecto.PhoenixHelpers.pagination_info(
                socket.assigns.selecto, 
                total_count
              )
              
              assign(socket,
                users: users,
                columns: columns,
                aliases: aliases,
                total_count: total_count,
                pagination: pagination,
                loading: false
              )
              
            {:error, _error} ->
              assign(socket,
                users: [],
                total_count: 0,
                loading: false
              )
              |> put_flash(:error, "Failed to load users")
          end
        end
      end
    end
  end

  @doc """
  API Controller Example
  
  Shows how to build JSON APIs with Selecto and Phoenix.
  """
  def api_controller_example do
    quote do
      defmodule MyAppWeb.API.UserController do
        use MyAppWeb, :controller
        
        def index(conn, params) do
          selecto = Selecto.from_ecto(MyApp.Repo, MyApp.User,
            joins: [:posts, :profile]
          )
          |> Selecto.PhoenixHelpers.from_params(params)
          
          case Selecto.execute(selecto) do
            {:ok, {users, columns, aliases}} ->
              # Transform to JSON-friendly format
              formatted_users = transform_selecto_results(users, aliases)
              
              # Get pagination metadata
              total_count = get_total_count(selecto)
              pagination = Selecto.PhoenixHelpers.pagination_info(selecto, total_count)
              
              json(conn, %{
                data: formatted_users,
                meta: %{
                  pagination: pagination,
                  columns: columns
                }
              })
              
            {:error, error} ->
              conn
              |> put_status(:internal_server_error)
              |> json(%{error: "Failed to fetch users"})
          end
        end
        
        def search(conn, %{"q" => query} = params) do
          selecto = Selecto.from_ecto(MyApp.Repo, MyApp.User)
          |> Selecto.filter({"name", {:ilike, "%#{query}%"}})
          |> Selecto.PhoenixHelpers.from_params(params)
          
          case Selecto.execute(selecto) do
            {:ok, {users, _columns, aliases}} ->
              json(conn, %{
                data: transform_selecto_results(users, aliases),
                query: query
              })
              
            {:error, _error} ->
              conn
              |> put_status(:internal_server_error) 
              |> json(%{error: "Search failed"})
          end
        end
        
        defp transform_selecto_results(rows, aliases) do
          Enum.map(rows, fn row ->
            Enum.zip(aliases, row)
            |> Enum.into(%{})
          end)
        end
        
        defp get_total_count(selecto) do
          count_selecto = selecto
          |> Selecto.select([{"id", %{"format" => "count"}}])
          
          case Selecto.execute_one(count_selecto) do
            {:ok, {[count], _}} -> count
            _ -> 0
          end
        end
      end
    end
  end

  @doc """
  Dashboard/Analytics Example
  
  Shows how to build analytics dashboards with aggregation queries.
  """
  def dashboard_example do
    quote do
      defmodule MyAppWeb.DashboardLive do
        use MyAppWeb, :live_view

        @impl true
        def mount(_params, _session, socket) do
          socket = assign(socket,
            user_stats: %{},
            post_stats: %{},
            loading: true
          )
          |> load_dashboard_data()
          
          {:ok, socket}
        end
        
        defp load_dashboard_data(socket) do
          # User statistics
          user_stats = get_user_statistics()
          
          # Post statistics  
          post_stats = get_post_statistics()
          
          assign(socket,
            user_stats: user_stats,
            post_stats: post_stats,
            loading: false
          )
        end
        
        defp get_user_statistics do
          # Total users
          total_users = Selecto.from_ecto(MyApp.Repo, MyApp.User)
          |> Selecto.select([{"id", %{"format" => "count"}}])
          |> Selecto.execute_one!()
          |> elem(0) |> List.first()
          
          # Active users
          active_users = Selecto.from_ecto(MyApp.Repo, MyApp.User)
          |> Selecto.filter({"active", true})
          |> Selecto.select([{"id", %{"format" => "count"}}])
          |> Selecto.execute_one!()
          |> elem(0) |> List.first()
          
          # Users by age group
          age_groups = Selecto.from_ecto(MyApp.Repo, MyApp.User)
          |> Selecto.select([
            {:case, [
              {{:lt, ["age", 25]}, {:literal, "Under 25"}},
              {{:between, ["age", 25, 44]}, {:literal, "25-44"}},
              {{:between, ["age", 45, 64]}, {:literal, "45-64"}},
              {:else, {:literal, "65+"}}
            ]},
            {"id", %{"format" => "count"}}
          ])
          |> Selecto.group_by(["age_group"])
          |> Selecto.execute!()
          
          %{
            total: total_users,
            active: active_users,
            age_groups: format_age_groups(age_groups)
          }
        end
        
        defp get_post_statistics do
          # Posts per month
          monthly_posts = Selecto.from_ecto(MyApp.Repo, MyApp.Post)
          |> Selecto.select([
            {:date_trunc, ["month", "inserted_at"]},
            {"id", %{"format" => "count"}}
          ])
          |> Selecto.group_by(["month"])
          |> Selecto.order_by([{"month", :desc}])
          |> Selecto.execute!()
          
          # Published vs draft
          status_breakdown = Selecto.from_ecto(MyApp.Repo, MyApp.Post) 
          |> Selecto.select([
            "published",
            {"id", %{"format" => "count"}}
          ])
          |> Selecto.group_by(["published"])
          |> Selecto.execute!()
          
          %{
            monthly: format_monthly_data(monthly_posts),
            status: format_status_data(status_breakdown)
          }
        end
        
        defp format_age_groups({rows, _columns, aliases}), do: format_rows(rows, aliases)
        defp format_monthly_data({rows, _columns, aliases}), do: format_rows(rows, aliases)  
        defp format_status_data({rows, _columns, aliases}), do: format_rows(rows, aliases)
        
        defp format_rows(rows, aliases) do
          Enum.map(rows, fn row ->
            Enum.zip(aliases, row) |> Enum.into(%{})
          end)
        end
      end
    end
  end

  @doc """
  Form Integration Example
  
  Shows how to build dynamic forms with Selecto-driven options.
  """
  def form_integration_example do
    quote do
      defmodule MyAppWeb.PostLive.Form do
        use MyAppWeb, :live_view

        @impl true
        def mount(_params, _session, socket) do
          # Load form options dynamically
          socket = assign(socket,
            changeset: MyApp.Post.changeset(%MyApp.Post{}, %{}),
            user_options: load_user_options(),
            form_fields: get_form_fields()
          )
          
          {:ok, socket}
        end
        
        defp load_user_options do
          # Get active users for dropdown
          case Selecto.from_ecto(MyApp.Repo, MyApp.User)
               |> Selecto.filter({"active", true})
               |> Selecto.select(["id", "name"])
               |> Selecto.order_by([{"name", :asc}])
               |> Selecto.execute() do
            {:ok, {users, _columns, aliases}} ->
              Enum.map(users, fn row ->
                user_data = Enum.zip(aliases, row) |> Enum.into(%{})
                {user_data["name"], user_data["id"]}
              end)
            _ -> []
          end
        end
        
        defp get_form_fields do
          # Generate form fields from Selecto schema
          Selecto.EctoAdapter.get_fields(MyApp.Post)
          |> Enum.map(fn {field, config} ->
            %{
              name: field,
              type: config.type,
              required: field in [:title, :user_id]
            }
          end)
        end
      end
    end
  end
end