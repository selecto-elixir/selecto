defmodule Selecto do
  @derive {Inspect, only: [:postgrex_opts, :set]}
  defstruct [:postgrex_opts, :domain, :config, :set]
  
  # import Selecto.Types - removed to avoid circular dependency
  
  @type t :: Selecto.Types.t()

  @moduledoc """
  Selecto is a query builder for Elixir that uses Postgrex to execute queries.
  It is designed to be a flexible and powerful tool for building complex SQL queries
  without writing SQL by hand.

  ## Domain Configuration

  Selecto is configured using a domain map. This map defines the database schema,
  including tables, columns, and associations. Here is an example of a domain map:

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

  ## Query Execution

  Selecto provides two execution patterns for better error handling and control flow:

  ### Safe Execution (Non-raising)

  Use `execute/2` and `execute_one/2` for applications that prefer explicit error handling:

      # Multiple rows
      case Selecto.execute(selecto) do
        {:ok, {rows, columns, aliases}} ->
          # Process successful results
          Enum.map(rows, &process_row/1)
          
        {:error, %Postgrex.Error{} = error} ->
          # Handle database errors gracefully
          Logger.error("Query failed: \#{inspect(error)}")
          {:error, :database_error}
      end

      # Single row (useful for COUNT, aggregate queries, or lookups)
      case Selecto.execute_one(selecto) do
        {:ok, {row, aliases}} ->
          # Process single row
          extract_values(row, aliases)
          
        {:error, :no_results} ->
          # Handle empty result set
          {:error, :not_found}
          
        {:error, :multiple_results} ->
          # Handle unexpected multiple rows
          {:error, :ambiguous_result}
      end

  ### Raising Execution (Legacy/Simple)

  Use `execute!/2` and `execute_one!/2` when you want to fail fast:

      # Will raise on any error
      {rows, columns, aliases} = Selecto.execute!(selecto)
      process_results(rows)

      # Will raise if not exactly 1 row
      {row, aliases} = Selecto.execute_one!(selecto) 
      process_single_result(row)

  ### Error Types

  The safe execution functions return these error patterns:

  - `{:error, %Postgrex.Error{}}` - Database connection or SQL errors
  - `{:error, :no_results}` - execute_one/2 when 0 rows returned
  - `{:error, :multiple_results}` - execute_one/2 when >1 rows returned
  - `{:error, {:exit, reason}}` - Process/connection failures
  - `{:error, reason}` - Other execution failures

  """

  @doc """
    Generate a selecto structure from a domain configuration and database connection.
    
    ## Parameters
    
    - `domain` - Domain configuration map (see domain configuration docs)
    - `postgrex_opts` - Postgrex connection options or PID
    - `opts` - Configuration options
    
    ## Options
    
    - `:validate` - (boolean, default: false) Whether to validate the domain configuration
      before processing. When `true`, will raise `Selecto.DomainValidator.ValidationError`
      if the domain has structural issues like missing schemas, circular join dependencies,
      or invalid advanced join configurations.
      
    ## Validation
    
    Domain validation checks for:
    
    - Required top-level keys (source, schemas)
    - Schema structural integrity (required keys, column definitions)
    - Association references to valid schemas
    - Join references to existing associations
    - Join dependency cycles that would cause infinite recursion
    - Advanced join type requirements (dimension keys, hierarchy parameters, etc.)
    - Field reference validity in filters and selectors
    
    ## Examples
    
        # Basic usage (no validation)
        selecto = Selecto.configure(domain, postgrex_opts)
        
        # With validation enabled
        selecto = Selecto.configure(domain, postgrex_opts, validate: true)
        
        # With Ecto repository and schema
        selecto = Selecto.from_ecto(MyApp.Repo, MyApp.User)
        
        # Validation can also be called explicitly
        :ok = Selecto.DomainValidator.validate_domain!(domain)
        selecto = Selecto.configure(domain, postgrex_opts)
  """
#  @spec configure(Selecto.Types.domain(), Postgrex.conn(), Selecto.Types.configure_options()) :: t()
  def configure(domain, postgrex_opts, opts \\ []) do
    validate? = Keyword.get(opts, :validate, false)
    
    if validate? do
      Selecto.DomainValidator.validate_domain!(domain)
    end
    
    %Selecto{
      postgrex_opts: postgrex_opts,
      domain: domain,
      config: configure_domain(domain),
      set: %{
        selected: Map.get(domain, :required_selected, []),
        filtered: [],
        order_by: Map.get(domain, :required_order_by, []),
        group_by: Map.get(domain, :required_group_by, [])
      }
    }
  end

  @doc """
    Configure Selecto from an Ecto repository and schema.
    
    This convenience function automatically introspects the Ecto schema
    and configures Selecto with the appropriate domain and database connection.
    
    ## Parameters
    
    - `repo` - The Ecto repository module (e.g., MyApp.Repo)
    - `schema` - The Ecto schema module to use as the source table
    - `opts` - Configuration options (passed to EctoAdapter.configure/3)
    
    ## Examples
    
        # Basic usage
        selecto = Selecto.from_ecto(MyApp.Repo, MyApp.User)
        
        # With joins and options
        selecto = Selecto.from_ecto(MyApp.Repo, MyApp.User,
          joins: [:posts, :profile],
          redact_fields: [:password_hash]
        )
        
        # With validation
        selecto = Selecto.from_ecto(MyApp.Repo, MyApp.User, validate: true)
  """
  def from_ecto(repo, schema, opts \\ []) do
    Selecto.EctoAdapter.configure(repo, schema, opts)
  end

  # generate the selecto configuration
#  @spec configure_domain(Selecto.Types.domain()) :: Selecto.Types.processed_config()
  defp configure_domain(%{source: source} = domain) do
    primary_key = source.primary_key

    fields =
      Selecto.Schema.Column.configure_columns(
        :selecto_root,
        source.fields -- source.redact_fields,
        source,
        domain
      )

    joins = Selecto.Schema.Join.recurse_joins(source, domain)
    ## Combine fields from Joins into fields list
    fields =
      List.flatten([fields | Enum.map(Map.values(joins), fn e -> e.fields end)])
      |> Enum.reduce(%{}, fn m, acc -> Map.merge(m, acc) end)

    ### Extra filters (all normal fields can be a filter) These are custom, which is really passed into Selecto Components to deal with
    filters = Map.get(domain, :filters, %{})

    filters =
      Enum.reduce(
        Map.values(joins),
        filters,
        fn e, acc ->
          Map.merge(Map.get(e, :filters, %{}), acc)
        end
      )
      |> Enum.map(fn {f, v} -> {f, Map.put(v, :id, f)} end)
      |> Enum.into(%{})

    %{
      source: source,
      source_table: source.source_table,
      primary_key: primary_key,
      columns: fields,
      joins: joins,
      filters: filters,
      domain_data: Map.get(domain, :domain_data)
    }
  end

  ### These use 'selecto_struct' to prevent global replace from hitting them, will switch back later!
#  @spec filters(t()) :: %{String.t() => term()}
  def filters(selecto_struct) do
    selecto_struct.config.filters
  end

#  @spec columns(t()) :: %{String.t() => %{required(:name) => String.t()}}
  def columns(selecto_struct) do
    selecto_struct.config.columns
  end

#  @spec joins(t()) :: %{atom() => processed_join()}
  def joins(selecto_struct) do
    selecto_struct.config.joins
  end

#  @spec source_table(t()) :: table_name()
  def source_table(selecto_struct) do
    selecto_struct.config.source_table
  end

#  @spec domain(t()) :: domain()
  def domain(selecto_struct) do
    selecto_struct.domain
  end

#  @spec domain_data(t()) :: term()
  def domain_data(selecto_struct) do
    selecto_struct.config.domain_data
  end

#  @spec field(t(), field_name()) :: %{required(:name) => String.t()} | nil
  def field(selecto_struct, field) do
    selecto_struct.config.columns[field]
  end

#  @spec set(t()) :: query_set()
  def set(selecto_struct) do
    selecto_struct.set
  end


  #### TODO join stuff, CTE stuff
  ### options:
  ### paramterize: value -- will cause a special case of this join with the indicated parameter, and fields/filters to be made available
  ### inner: true -- change the default

  #def join(selecto_struct, join_id, options \\ []) do
  #end

  ### returns a key to use to add filters, selects, etc from this join
  #def join_paramterize(selecto_struct, join_id, parameter, options) do
  #end

  #def join(selecto_struct, join_id, join_selecto, options \\ []) do
  #end

  ### CTEs. once a CTE is entered, further CTEs can reference it. CTEs are meant to be added as configuration not dynamically!
  #def with(selecto_struct, cte_name, cte, params, options \\ []) do
  #end
  #def with(selecto_struct, cte_name, cte_selecto, options \\ []) do
  #end

  ### Modify an existing CTE
  #def on_with(selecto_struct, cte_name, fn selecto, cte_selecto -> selecto end, options \\ [])
  #end


  @doc """
    add a field to the Select list. Send in one or a list of field names or selectable tuples
    TODO allow to send single, and special forms..
  """
#  @spec select(t(), [selector()]) :: t()
  def select(selecto, fields) when is_list(fields) do
    put_in(selecto.set.selected, Enum.uniq(selecto.set.selected ++ fields))
  end

#  @spec select(t(), selector()) :: t()
  def select(selecto, field) do
    Selecto.select(selecto, [field])
  end

  @doc """
    add a filter to selecto. Send in a tuple with field name and filter value
  """
#  @spec filter(t(), [filter()]) :: t()
  def filter(selecto, filters) when is_list(filters) do
    put_in(selecto.set.filtered, selecto.set.filtered ++ filters)
  end

#  @spec filter(t(), filter()) :: t()
  def filter(selecto, filters) do
    put_in(selecto.set.filtered, selecto.set.filtered ++ [filters])
  end

  @doc """
    Add to the Order By
  """
#  @spec order_by(t(), [order_spec()]) :: t()
  def order_by(selecto, orders) when is_list(orders) do
    put_in(selecto.set.order_by, selecto.set.order_by ++ orders)
  end

#  @spec order_by(t(), order_spec()) :: t()
  def order_by(selecto, orders) do
    put_in(selecto.set.order_by, selecto.set.order_by ++ [orders])
  end

  @doc """
    Add to the Group By
  """
#  @spec group_by(t(), [field_name()]) :: t()
  def group_by(selecto, groups) when is_list(groups) do
    put_in(selecto.set.group_by, selecto.set.group_by ++ groups)
  end

#  @spec group_by(t(), field_name()) :: t()
  def group_by(selecto, groups) do
    put_in(selecto.set.group_by, selecto.set.group_by ++ [groups])
  end

#  @spec gen_sql(t(), sql_generation_options()) :: {String.t(), %{String.t() => String.t()}, sql_params()}
  def gen_sql(selecto, opts) do
    Selecto.Builder.Sql.build(selecto, opts)
  end

  @doc """
    Generate and run the query, returning {:ok, result} or {:error, reason}.
    
    Non-raising version that returns tagged tuples for better error handling.
    Result format: {:ok, {rows, columns, aliases}} | {:error, reason}
    
    ## Examples
    
        case Selecto.execute(selecto) do
          {:ok, {rows, columns, aliases}} -> 
            # Handle successful query
            process_results(rows, columns)
          {:error, reason} ->
            # Handle database error
            Logger.error("Query failed: \#{inspect(reason)}")
        end
  """
  @spec execute(Selecto.Types.t(), Selecto.Types.execute_options()) :: Selecto.Types.safe_execute_result()
  def execute(selecto, opts \\ []) do
    try do
      {query, aliases, params} = gen_sql(selecto, opts)
      
      # Debug: Always log SQL and parameters
      IO.puts("\n=== SELECTO SQL DEBUG ===")
      IO.puts("SQL: #{query}")
      IO.puts("Params: #{inspect(params)}")
      IO.puts("Aliases: #{inspect(aliases)}")
      IO.puts("========================\n")
      
      # Handle both Ecto repos and direct Postgrex connections
      result = case selecto.postgrex_opts do
        # If it's an Ecto repo (module), try to use Ecto.Adapters.SQL.query
        repo when is_atom(repo) and not is_nil(repo) ->
          # Try to call Ecto.Adapters.SQL.query dynamically
          try do
            # Use apply to avoid compile-time dependency on Ecto.Adapters.SQL
            case apply(Ecto.Adapters.SQL, :query, [repo, query, params]) do
              {:ok, result} -> {:ok, {result.rows, result.columns, aliases}}
              {:error, reason} -> {:error, reason}
            end
          rescue
            UndefinedFunctionError ->
              IO.puts("Ecto.Adapters.SQL not available, falling back to temporary connection")
              # Ecto.Adapters.SQL not available, fall back to temporary connection
              config = apply(repo, :config, [])
              postgrex_opts = [
                username: config[:username],
                password: config[:password], 
                hostname: config[:hostname] || "localhost",
                database: config[:database],
                port: config[:port] || 5432,
                supervisor: false
              ]
              case Postgrex.start_link(postgrex_opts) do
                {:ok, conn} ->
                  result = case Postgrex.query(conn, query, params) do
                    {:ok, result} -> {:ok, {result.rows, result.columns, aliases}}
                    {:error, reason} -> {:error, reason}
                  end
                  GenServer.stop(conn)
                  result
                {:error, reason} -> {:error, reason}
              end
            error -> {:error, error}
          end
        # If it's a Postgrex connection, use Postgrex.query directly  
        conn ->
          case Postgrex.query(conn, query, params) do
            {:ok, result} -> {:ok, {result.rows, result.columns, aliases}}
            {:error, reason} -> {:error, reason}
          end
      end
      
      result
    rescue
      error -> {:error, error}
    catch
      :exit, reason -> {:error, {:exit, reason}}
    end
  end

  @doc """
    Generate and run the query, raising on errors.
    
    Raising version that maintains the original behavior for backward compatibility.
    Returns the result directly or raises an exception on error.
    
    ## Examples
    
        {rows, columns, aliases} = Selecto.execute!(selecto)
        process_results(rows, columns)
  """
  @spec execute!(Selecto.Types.t(), Selecto.Types.execute_options()) :: Selecto.Types.execute_result()
  def execute!(selecto, opts \\ []) do
    case execute(selecto, opts) do
      {:ok, result} -> result
      {:error, {:exit, reason}} -> raise RuntimeError, "Database connection failed: #{inspect(reason)}"
      {:error, %{__exception__: true} = error} -> raise error
      {:error, error} -> raise RuntimeError, "Execution failed: #{inspect(error)}"
    end
  end

  @doc """
    Execute a query expecting exactly one row, returning {:ok, row} or {:error, reason}.
    
    Useful for queries that should return a single record (e.g., with LIMIT 1 or aggregate functions).
    Returns an error if zero rows or multiple rows are returned.
    
    ## Examples
    
        case Selecto.execute_one(selecto) do
          {:ok, row} -> 
            # Handle single row result
            process_single_result(row)
          {:error, :no_results} ->
            # Handle case where no rows were found
          {:error, :multiple_results} ->
            # Handle case where multiple rows were found  
          {:error, error} ->
            # Handle database or other errors
        end
  """
  @spec execute_one(Selecto.Types.t(), Selecto.Types.execute_options()) :: Selecto.Types.safe_execute_one_result()
  def execute_one(selecto, opts \\ []) do
    case execute(selecto, opts) do
      {:ok, {[], _columns, _aliases}} -> 
        {:error, :no_results}
      {:ok, {[single_row], _columns, aliases}} -> 
        {:ok, {single_row, aliases}}
      {:ok, {_multiple_rows, _columns, _aliases}} -> 
        {:error, :multiple_results}
      {:error, reason} -> 
        {:error, reason}
    end
  end

  @doc """
    Execute a query expecting exactly one row, raising on errors or unexpected row counts.
    
    Raising version of execute_one/2 for when you want to fail fast.
    
    ## Examples
    
        {row, aliases} = Selecto.execute_one!(selecto)
        process_single_result(row)
  """
  @spec execute_one!(Selecto.Types.t(), Selecto.Types.execute_options()) :: Selecto.Types.single_row_result()
  def execute_one!(selecto, opts \\ []) do
    case execute_one(selecto, opts) do
      {:ok, result} -> result
      {:error, :no_results} -> raise RuntimeError, "Expected exactly 1 row, got 0"
      {:error, :multiple_results} -> raise RuntimeError, "Expected exactly 1 row, got multiple"
      {:error, {:exit, reason}} -> raise RuntimeError, "Database connection failed: #{inspect(reason)}"
      {:error, %{__exception__: true} = error} -> raise error
      {:error, error} -> raise RuntimeError, "Execution failed: #{inspect(error)}"
    end
  end

  @doc """
    Generate SQL without executing - useful for debugging and caching
  """
#  @spec to_sql(t(), sql_generation_options()) :: sql_result()
  def to_sql(selecto, opts \\ []) do
    {query, _aliases, params} = gen_sql(selecto, opts)
    {query, params}
  end
end
