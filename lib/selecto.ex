defmodule Selecto do
  @derive {Inspect, only: [:postgrex_opts, :adapter, :connection, :set, :tenant]}
  defstruct [:postgrex_opts, :adapter, :connection, :domain, :config, :set, :extensions, :tenant]

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

  ### Error Types

  All execution functions return structured `Selecto.Error` for consistent error handling:

  - `{:error, %Selecto.Error{type: :connection_error}}` - Database connection failures
  - `{:error, %Selecto.Error{type: :query_error}}` - SQL execution errors
  - `{:error, %Selecto.Error{type: :no_results}}` - execute_one/2 when 0 rows returned
  - `{:error, %Selecto.Error{type: :multiple_results}}` - execute_one/2 when >1 rows returned
  - `{:error, %Selecto.Error{type: :timeout_error}}` - Query timeout failures

  """

  @doc """
    Generate a selecto structure from a domain configuration and database connection.

    ## Parameters

    - `domain` - Domain configuration map (see domain configuration docs)
    - `postgrex_opts` - Postgrex connection options, PID, or pooled connection
    - `opts` - Configuration options

    ## Options

    - `:validate` - (boolean, default: true) Whether to validate the domain configuration
      before processing. When `true`, will raise `Selecto.DomainValidator.ValidationError`
      if the domain has structural issues like missing schemas, circular join dependencies,
      or invalid advanced join configurations.
    - `:pool` - (boolean, default: false) Whether to enable connection pooling
    - `:pool_options` - Connection pool configuration options
    - `:adapter` - (module, default: Selecto.DB.PostgreSQL) Database adapter module

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

        # Basic usage (validation enabled by default)
        selecto = Selecto.configure(domain, postgrex_opts)

        # With connection pooling
        selecto = Selecto.configure(domain, postgrex_opts, pool: true)

        # Custom pool configuration
        pool_opts = [pool_size: 20, max_overflow: 10]
        selecto = Selecto.configure(domain, postgrex_opts, pool: true, pool_options: pool_opts)

        # Using existing pooled connection
        {:ok, pool} = Selecto.ConnectionPool.start_pool(postgrex_opts)
        selecto = Selecto.configure(domain, {:pool, pool})

        # Disable validation for performance-critical scenarios
        selecto = Selecto.configure(domain, postgrex_opts, validate: false)

        # With Ecto repository and schema
        selecto = Selecto.from_ecto(MyApp.Repo, MyApp.User)

        # Validation can also be called explicitly
        :ok = Selecto.DomainValidator.validate_domain!(domain)
        selecto = Selecto.configure(domain, postgrex_opts)
  """
  @spec configure(Selecto.Types.domain(), Postgrex.conn(), keyword()) :: t()
  def configure(domain, postgrex_opts, opts \\ []) do
    Selecto.OptionsValidator.validate_configure_opts!(opts)
    Selecto.Configuration.configure(domain, postgrex_opts, opts)
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
    Selecto.Configuration.from_ecto(repo, schema, opts)
  end

  ### Delegate to Selecto.Fields module
  @spec filters(t()) :: %{String.t() => term()}
  defdelegate filters(selecto), to: Selecto.Fields

  @spec columns(t()) :: map()
  defdelegate columns(selecto), to: Selecto.Fields

  @spec joins(t()) :: map()
  defdelegate joins(selecto), to: Selecto.Fields

  @spec source_table(t()) :: Selecto.Types.table_name() | nil
  defdelegate source_table(selecto), to: Selecto.Fields

  @spec domain(t()) :: Selecto.Types.domain()
  defdelegate domain(selecto), to: Selecto.Fields

  @spec domain_data(t()) :: term()
  defdelegate domain_data(selecto), to: Selecto.Fields

  @spec field(t(), Selecto.Types.field_name()) :: map() | nil
  defdelegate field(selecto, field_name), to: Selecto.Fields

  @spec extensions(t()) :: [{module(), keyword()}]
  defdelegate extensions(selecto), to: Selecto.Fields

  @doc """
  Enhanced field resolution with disambiguation and error handling.

  Provides detailed field information and helpful error messages.
  """
  @spec resolve_field(t(), Selecto.Types.field_name()) :: {:ok, map()} | {:error, term()}
  defdelegate resolve_field(selecto, field), to: Selecto.Fields

  @doc """
  Get all available fields across all joins and the source table.
  """
  @spec available_fields(t()) :: [String.t()]
  defdelegate available_fields(selecto), to: Selecto.Fields

  @doc """
  Infer the SQL type of an expression.

  Returns the type of a field, function, literal, or complex expression.
  Useful for type checking, validation, and UI components that need type information.

  ## Examples

      # Field type lookup
      {:ok, :string} = Selecto.infer_type(selecto, "product_name")

      # Aggregate function
      {:ok, :bigint} = Selecto.infer_type(selecto, {:count, "*"})

      # Numeric aggregate
      {:ok, :decimal} = Selecto.infer_type(selecto, {:sum, "price"})

      # Literal
      {:ok, :integer} = Selecto.infer_type(selecto, {:literal, 42})
  """
  @spec infer_type(t(), term()) :: {:ok, Selecto.TypeSystem.sql_type()} | {:error, term()}
  defdelegate infer_type(selecto, expression), to: Selecto.TypeSystem

  @doc """
  Check if two SQL types are compatible for comparisons or assignments.

  ## Examples

      true = Selecto.types_compatible?(:integer, :decimal)
      false = Selecto.types_compatible?(:string, :boolean)
  """
  @spec types_compatible?(Selecto.TypeSystem.sql_type(), Selecto.TypeSystem.sql_type()) ::
          boolean()
  defdelegate types_compatible?(type1, type2), to: Selecto.TypeSystem, as: :compatible?

  @doc """
  Get the type category for a given SQL type.

  Categories: :numeric, :string, :boolean, :datetime, :json, :array, :binary, :uuid, :unknown
  """
  @spec type_category(Selecto.TypeSystem.sql_type()) :: Selecto.TypeSystem.type_category()
  defdelegate type_category(type), to: Selecto.TypeSystem

  @doc """
  Get field suggestions for autocomplete or error recovery.
  """
  @spec field_suggestions(t(), String.t()) :: [String.t()]
  defdelegate field_suggestions(selecto, partial_name), to: Selecto.Fields

  @spec set(t()) :: Selecto.Types.query_set()
  defdelegate set(selecto), to: Selecto.Fields

  @doc """
  Enable a join from the domain configuration or add a custom join dynamically.

  This allows adding joins at runtime that either:
  - Enable predefined joins from the domain configuration
  - Add completely custom joins not in the domain

  ## Parameters

  - `join_id` - The join identifier (atom)
  - `options` - Optional configuration overrides

  ## Options

  - `:type` - Join type (:left, :inner, :right, :full). Default: :left
  - `:source` - Source table name (required for custom joins)
  - `:on` - Join conditions as list of maps with :left and :right keys
  - `:owner_key` - The key on the parent table
  - `:related_key` - The key on the joined table
  - `:fields` - Map of field configurations to expose from the joined table

  ## Examples

      # Enable domain-configured join
      selecto |> Selecto.join(:category)

      # Custom join with explicit configuration
      selecto |> Selecto.join(:audit_log,
        source: "audit_logs",
        on: [%{left: "id", right: "record_id"}],
        type: :left,
        fields: %{
          action: %{type: :string},
          created_at: %{type: :naive_datetime}
        }
      )
  """
  @spec join(t(), atom(), keyword()) :: t()
  defdelegate join(selecto, join_id, options \\ []), to: Selecto.DynamicJoin

  @doc """
  Create a parameterized instance of an existing join.

  Parameterized joins allow the same association to be joined multiple times
  with different filter conditions. The parameter creates a unique instance
  that can be referenced using dot notation: `join_name:parameter.field_name`

  ## Parameters

  - `join_id` - Base join identifier to parameterize
  - `parameter` - Unique parameter value to identify this instance
  - `options` - Filter conditions and options

  ## Examples

      # Create parameterized join for electronics products
      selecto
      |> Selecto.join_parameterize(:products, "electronics", category_id: 1)
      |> Selecto.select(["products:electronics.product_name"])

      # Multiple parameterized instances for comparison
      selecto
      |> Selecto.join_parameterize(:orders, "active", status: "active")
      |> Selecto.join_parameterize(:orders, "completed", status: "completed")
      |> Selecto.select([
          "orders:active.total as active_total",
          "orders:completed.total as completed_total"
        ])
  """
  @spec join_parameterize(t(), atom(), String.t() | atom(), keyword()) :: t()
  defdelegate join_parameterize(selecto, join_id, parameter, options \\ []),
    to: Selecto.DynamicJoin

  @doc """
  Join with another Selecto query as a subquery.

  This creates a join using a separate Selecto query as the right side,
  enabling complex subquery joins for aggregations and derived tables.

  ## Parameters

  - `join_id` - Identifier for this join
  - `join_selecto` - The Selecto struct to use as subquery
  - `options` - Join configuration

  ## Options

  - `:type` - Join type (:left, :inner, :right, :full). Default: :left
  - `:on` - Join conditions referencing the subquery alias

  ## Examples

      # Create a subquery for aggregated data
      order_totals = Selecto.configure(order_domain, connection)
      |> Selecto.select(["customer_id", {:sum, "total", as: "total_spent"}])
      |> Selecto.group_by(["customer_id"])

      # Join aggregated subquery to main query
      selecto
      |> Selecto.join_subquery(:customer_totals, order_totals,
          on: [%{left: "customer_id", right: "customer_id"}]
        )
      |> Selecto.select(["name", "customer_totals.total_spent"])
  """
  @spec join_subquery(t(), atom(), t(), keyword()) :: t()
  defdelegate join_subquery(selecto, join_id, join_selecto, options \\ []),
    to: Selecto.DynamicJoin

  @doc """
  Add a field to the Select list. Send in one or a list of field names or selectable tuples.
  """
  @spec select(t(), [Selecto.Types.selector()]) :: t()
  @spec select(t(), Selecto.Types.selector()) :: t()
  defdelegate select(selecto, fields_or_field), to: Selecto.Query

  @doc """
  Compile a nested selection shape and attach it to the query.

  This is an opt-in structured selection API. Use `execute_shape/2` to
  materialize results into the same list/tuple structure.

  Nested lists/tuples that only reference a single joined schema are treated as
  subselect nodes.
  """
  @spec select_shape(t(), list() | tuple()) :: t()
  defdelegate select_shape(selecto, shape), to: Selecto.SelectionShape

  @doc """
  Execute a query configured with `select_shape/2` and return shaped rows.

  Returns `{:ok, shaped_rows}` where each row mirrors the selection shape.
  """
  @spec execute_shape(t(), Selecto.Types.execute_options()) ::
          {:ok, list()} | {:error, Selecto.Error.t()}
  defdelegate execute_shape(selecto, opts \\ []), to: Selecto.SelectionShape

  @doc """
  Add a filter to selecto. Send in a tuple with field name and filter value.
  """
  @spec filter(t(), [Selecto.Types.filter()]) :: t()
  @spec filter(t(), Selecto.Types.filter()) :: t()
  defdelegate filter(selecto, filters_or_filter), to: Selecto.Query

  @doc """
  Return required filters currently attached to the query.

  This includes domain-level required filters and tenant-scope required filters
  attached at runtime.
  """
  @spec required_filters(t()) :: [Selecto.Types.filter()]
  defdelegate required_filters(selecto), to: Selecto.Query

  @doc """
  Attach tenant context to the query state.
  """
  @spec with_tenant(t(), map() | keyword() | String.t() | atom() | nil) :: t()
  defdelegate with_tenant(selecto, tenant_context), to: Selecto.Tenant

  @doc """
  Read tenant context from query state.
  """
  @spec tenant(t()) :: map() | nil
  defdelegate tenant(selecto), to: Selecto.Tenant

  @doc """
  Apply tenant scope as required filters.
  """
  @spec apply_tenant_scope(t(), keyword()) :: t()
  defdelegate apply_tenant_scope(selecto, opts \\ []), to: Selecto.Tenant

  @doc """
  Add a required tenant filter to query state.
  """
  @spec require_tenant_filter(t(), Selecto.Types.filter()) :: t()
  @spec require_tenant_filter(t(), atom() | String.t(), term()) :: t()
  def require_tenant_filter(selecto, {_, _} = filter) do
    Selecto.Tenant.require_tenant_filter(selecto, filter)
  end

  def require_tenant_filter(selecto, tenant_field, tenant_id) do
    Selecto.Tenant.require_tenant_filter(selecto, tenant_field, tenant_id)
  end

  @doc """
  Append filters explicitly to the pre-pivot filter list.

  These filters stay attached to the original root side when using `pivot/3`.
  """
  @spec pre_pivot_filter(t(), [Selecto.Types.filter()]) :: t()
  @spec pre_pivot_filter(t(), Selecto.Types.filter()) :: t()
  defdelegate pre_pivot_filter(selecto, filters_or_filter), to: Selecto.Query

  @doc """
  Append filters explicitly to the post-pivot filter list.

  These filters apply to the pivoted target root.
  """
  @spec post_pivot_filter(t(), [Selecto.Types.filter()]) :: t()
  @spec post_pivot_filter(t(), Selecto.Types.filter()) :: t()
  defdelegate post_pivot_filter(selecto, filters_or_filter), to: Selecto.Query

  @doc """
  Read pre-pivot filters from the query set (`set.filtered`).
  """
  @spec pre_pivot_filters(t()) :: [Selecto.Types.filter()]
  defdelegate pre_pivot_filters(selecto), to: Selecto.Query

  @doc """
  Read post-pivot filters from the query set (`set.post_pivot_filters`).
  """
  @spec post_pivot_filters(t()) :: [Selecto.Types.filter()]
  defdelegate post_pivot_filters(selecto), to: Selecto.Query

  @doc """
  Return query filters from current filter buckets.

  Options:
  - `:include_post_pivot` (default: `true`)
  """
  @spec query_filters(t(), keyword()) :: [Selecto.Types.filter()]
  defdelegate query_filters(selecto, opts \\ []), to: Selecto.Query

  @doc """
  Return whether tenant scope is required for this query.
  """
  @spec tenant_required?(t(), keyword()) :: boolean()
  defdelegate tenant_required?(selecto, opts \\ []), to: Selecto.Tenant

  @doc """
  Validate tenant scope and return `:ok` or structured validation error.
  """
  @spec validate_tenant_scope(t(), keyword()) :: :ok | {:error, Selecto.Error.t()}
  defdelegate validate_tenant_scope(selecto, opts \\ []), to: Selecto.Tenant, as: :validate_scope

  @doc """
  Raise if tenant scope is required and missing.
  """
  @spec ensure_tenant_scope!(t(), keyword()) :: :ok
  defdelegate ensure_tenant_scope!(selecto, opts \\ []), to: Selecto.Tenant, as: :ensure_scope!

  @doc """
  Add to the Order By clause.
  """
  @spec order_by(t(), [Selecto.Types.order_spec()]) :: t()
  @spec order_by(t(), Selecto.Types.order_spec()) :: t()
  defdelegate order_by(selecto, orders), to: Selecto.Query

  @doc """
  Add to the Group By clause.
  """
  @spec group_by(t(), [Selecto.Types.field_name()]) :: t()
  @spec group_by(t(), Selecto.Types.field_name()) :: t()
  defdelegate group_by(selecto, groups), to: Selecto.Query

  @doc """
  Limit the number of rows returned by the query.

  ## Examples

      # Limit to 10 rows
      selecto |> Selecto.limit(10)

      # Limit with offset for pagination
      selecto |> Selecto.limit(10) |> Selecto.offset(20)
  """
  @spec limit(t(), non_neg_integer()) :: t()
  defdelegate limit(selecto, limit_value), to: Selecto.Query

  @doc """
  Set the offset for the query results.

  ## Examples

      # Skip first 20 rows
      selecto |> Selecto.offset(20)

      # Pagination: page 3 with 10 items per page
      selecto |> Selecto.limit(10) |> Selecto.offset(20)
  """
  @spec offset(t(), non_neg_integer()) :: t()
  defdelegate offset(selecto, offset_value), to: Selecto.Query

  @doc """
  Pivot the query to focus on a different table while preserving existing context.

  This allows you to retarget a Selecto query from the source table to any joined
  table, while preserving existing filters through subqueries.

  ## Examples

      # Pivot from events to orders while preserving event filters
      selecto
      |> Selecto.filter([{"event_id", 123}])
      |> Selecto.pivot(:orders)
      |> Selecto.select(["product_name", "quantity"])

  ## Options

  - `:preserve_filters` - Whether to preserve existing filters (default: true)
  - `:subquery_strategy` - How to generate the subquery (`:in`, `:exists`, `:join`)

  See `Selecto.Pivot` module for more details.
  """
  def pivot(selecto, target_schema, opts \\ []) do
    Selecto.Pivot.pivot(selecto, target_schema, opts)
  end

  @doc """
  Add subselect fields to return related data as aggregated arrays.

  This prevents result set denormalization while maintaining relational context
  by returning related data as JSON arrays, PostgreSQL arrays, or other formats.

  ## Examples

      # Basic subselect - get orders as JSON for each attendee
      selecto
      |> Selecto.select(["attendee.name"])
      |> Selecto.subselect(["order.product_name", "order.quantity"])

      # With custom configuration
      selecto
      |> Selecto.subselect([
           %{
             fields: ["product_name", "quantity"],
             target_schema: :order,
             format: :json_agg,
             alias: "order_items"
           }
         ])

  ## Options

  - `:format` - Aggregation format (`:json_agg`, `:array_agg`, `:string_agg`, `:count`)
  - `:alias_prefix` - Prefix for generated field aliases

  See `Selecto.Subselect` module for more details.
  """
  def subselect(selecto, field_specs, opts \\ []) do
    Selecto.Subselect.subselect(selecto, field_specs, opts)
  end

  @spec gen_sql(t(), keyword()) :: {String.t(), map(), list()}
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
  @spec execute(Selecto.Types.t(), Selecto.Types.execute_options()) ::
          Selecto.Types.safe_execute_result()
  def execute(selecto, opts \\ []) do
    Selecto.OptionsValidator.validate_execute_opts!(opts)

    # Delegate to the extracted Executor module
    Selecto.Executor.execute(selecto, Selecto.Tenant.merge_execution_opts(selecto, opts))
  end

  @doc """
  Execute a query and return results with metadata including SQL, params, and execution time.

  ## Parameters

  - `selecto` - The Selecto struct containing connection and query info
  - `opts` - Execution options

  ## Returns

  - `{:ok, result, metadata}` - Successful execution with results and metadata
  - `{:error, error}` - Execution failure with detailed error

  The metadata map includes:
  - `:sql` - The generated SQL query string
  - `:params` - The query parameters
  - `:execution_time` - Query execution time in milliseconds

  ## Examples

      case Selecto.execute_with_metadata(selecto) do
        {:ok, {rows, columns, aliases}, _metadata} ->
          # Process successful results with metadata
          handle_results(rows, columns, aliases)
        {:error, error} ->
          # Handle database error
          Logger.error("Query failed: \#{inspect(error)}")
      end
  """
  @spec execute_with_metadata(Selecto.Types.t(), Selecto.Types.execute_options()) ::
          {:ok, Selecto.Types.execute_result(), map()} | {:error, Selecto.Error.t()}
  def execute_with_metadata(selecto, opts \\ []) do
    Selecto.OptionsValidator.validate_execute_opts!(opts)

    # Delegate to the extracted Executor module
    Selecto.Executor.execute_with_metadata(
      selecto,
      Selecto.Tenant.merge_execution_opts(selecto, opts)
    )
  end

  @doc """
  Execute a query as a database-backed stream.

  Returns a stream of `{row, columns, aliases}` tuples for incremental result
  consumption.

  ## Options

  - `:max_rows` - PostgreSQL cursor batch size (default `500`)
  - `:receive_timeout` - stream consumer wait timeout in ms (default `60000`)
  - `:queue_timeout` - internal task yield timeout in ms (default `100`)
  - `:stream_timeout` - transaction timeout for cursor execution (default `30000`)

  ## Notes

  - Direct PostgreSQL connections use cursor-backed streaming.
  - Adapter-backed streaming requires `adapter.stream/4` support.
  - Ecto repo and pooled PostgreSQL stream paths currently return structured
    `:validation_error` responses.
  """
  @spec execute_stream(Selecto.Types.t(), keyword()) :: Selecto.Types.safe_execute_stream_result()
  def execute_stream(selecto, opts \\ []) do
    Selecto.OptionsValidator.validate_execute_opts!(opts)
    Selecto.Executor.execute_stream(selecto, Selecto.Tenant.merge_execution_opts(selecto, opts))
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
  @spec execute_one(Selecto.Types.t(), Selecto.Types.execute_options()) ::
          Selecto.Types.safe_execute_one_result()
  def execute_one(selecto, opts \\ []) do
    Selecto.OptionsValidator.validate_execute_opts!(opts)

    # Delegate to the extracted Executor module
    Selecto.Executor.execute_one(selecto, Selecto.Tenant.merge_execution_opts(selecto, opts))
  end

  @doc """
  Generate SQL without executing - useful for debugging and caching.

  Supports optional readability controls:
  - `pretty: true`
  - `highlight: :ansi | :markdown`
  """
  @spec to_sql(t(), keyword()) :: {String.t(), list()}
  def to_sql(selecto, opts \\ []) do
    Selecto.OptionsValidator.validate_to_sql_opts!(opts)
    Selecto.Query.to_sql(selecto, opts)
  end

  @doc """
  Format SQL output for readability.
  """
  @spec format_sql(String.t(), keyword()) :: String.t()
  defdelegate format_sql(sql, opts \\ []), to: Selecto.SQL.Formatter, as: :format

  @doc """
  Apply optional highlighting to SQL (`:ansi` or `:markdown`).
  """
  @spec highlight_sql(String.t(), :ansi | :markdown | nil) :: String.t()
  defdelegate highlight_sql(sql, style), to: Selecto.SQL.Formatter, as: :highlight

  @doc """
  Run EXPLAIN for a query and return plan details.
  """
  @spec explain(t(), keyword()) :: {:ok, map()} | {:error, Selecto.Error.t()}
  def explain(selecto, opts \\ []) do
    Selecto.OptionsValidator.validate_diagnostic_opts!(opts)
    Selecto.Diagnostics.explain(selecto, opts)
  end

  @doc """
  Run EXPLAIN ANALYZE for a query and return plan details.
  """
  @spec explain_analyze(t(), keyword()) :: {:ok, map()} | {:error, Selecto.Error.t()}
  def explain_analyze(selecto, opts \\ []) do
    Selecto.OptionsValidator.validate_diagnostic_opts!(opts)
    Selecto.Diagnostics.explain_analyze(selecto, opts)
  end

  @doc """
  Add a window function to the query.

  Window functions provide analytical capabilities over a set of rows related to
  the current row, without grouping rows into a single result.

  ## Examples

      # Add row numbers within each category
      selecto |> Selecto.window_function(:row_number,
        over: [partition_by: ["category"], order_by: ["created_at"]])

      # Calculate running total
      selecto |> Selecto.window_function(:sum, ["amount"],
        over: [partition_by: ["user_id"], order_by: ["date"]],
        as: "running_total")

      # Get previous value for comparison
      selecto |> Selecto.window_function(:lag, ["amount", 1],
        over: [partition_by: ["user_id"], order_by: ["date"]],
        as: "prev_amount")
  """
  def window_function(selecto, function, arguments \\ [], options) do
    Selecto.Window.add_window_function(selecto, function, arguments, options)
  end

  @doc """
  Add an UNNEST operation to expand array columns into rows.

  UNNEST transforms array values into a set of rows, one for each array element.
  Can optionally include ordinality to track position in the array.

  ## Examples

      # Basic unnest
      selecto |> Selecto.unnest("tags", as: "tag")

      # Unnest with ordinality (includes position)
      selecto |> Selecto.unnest("tags", as: "tag", ordinality: "tag_position")

      # Multiple unnests (will be cross-joined)
      selecto
      |> Selecto.unnest("tags", as: "tag")
      |> Selecto.unnest("categories", as: "category")
  """
  def unnest(selecto, array_field, opts \\ []) do
    alias_name = Keyword.get(opts, :as, "unnested_#{array_field}")
    ordinality = Keyword.get(opts, :ordinality)

    unnest_spec = %{
      field: array_field,
      alias: alias_name,
      ordinality: ordinality
    }

    current_unnests = Map.get(selecto.set, :unnest, [])
    updated_selecto = put_in(selecto.set[:unnest], current_unnests ++ [unnest_spec])

    # Register the unnested field in the configuration
    # This allows it to be selected and used in GROUP BY
    current_columns = Map.get(updated_selecto.config, :columns, %{})

    # Add the unnested field as a column
    unnested_column = %{
      name: alias_name,
      field: alias_name,
      requires_join: nil,
      # Default type for unnested array elements
      type: :text
    }

    # Also add ordinality column if requested
    columns_to_add =
      if ordinality do
        %{
          alias_name => unnested_column,
          "#{alias_name}_ordinality" => %{
            name: "#{alias_name}_ordinality",
            field: "#{alias_name}_ordinality",
            requires_join: nil,
            type: :integer
          }
        }
      else
        %{alias_name => unnested_column}
      end

    put_in(updated_selecto.config[:columns], Map.merge(current_columns, columns_to_add))
  end

  # DUPLICATE REMOVED - Consolidated with the version below that uses Advanced.CTE
  # This version accepted (selecto, cte_name, base_fn, recursive_fn, opts)
  # The consolidated version below now handles both parameter formats

  # DUPLICATE REMOVED - Consolidated with the version below that uses Advanced.LateralJoin
  # This version accepted (selecto, lateral_source, opts)
  # The consolidated version below now handles both parameter formats

  @doc """
  Create a UNION set operation between two queries.

  Combines results from multiple queries using UNION or UNION ALL.
  All queries must have compatible column counts and types.

  ## Options

  - `:all` - Use UNION ALL to include duplicates (default: false)
  - `:column_mapping` - Map columns between incompatible schemas

  ## Examples

      # Basic UNION (removes duplicates)
      query1 |> Selecto.union(query2)

      # UNION ALL (includes duplicates, faster)
      query1 |> Selecto.union(query2, all: true)

      # UNION with column mapping
      customers |> Selecto.union(vendors,
        column_mapping: [
          {"name", "company_name"},
          {"email", "contact_email"}
        ]
      )
  """
  def union(left_query, right_query, opts \\ []) do
    Selecto.SetOperations.union(left_query, right_query, opts)
  end

  @doc """
  Create an INTERSECT set operation between two queries.

  Returns only rows that appear in both queries.

  ## Options

  - `:all` - Use INTERSECT ALL to include duplicate intersections (default: false)
  - `:column_mapping` - Map columns between incompatible schemas

  ## Examples

      # Find users who are both active and premium
      active_users |> Selecto.intersect(premium_users)

      # Include duplicate intersections
      query1 |> Selecto.intersect(query2, all: true)
  """
  def intersect(left_query, right_query, opts \\ []) do
    Selecto.SetOperations.intersect(left_query, right_query, opts)
  end

  @doc """
  Create an EXCEPT set operation between two queries.

  Returns rows from the first query that don't appear in the second query.

  ## Options

  - `:all` - Use EXCEPT ALL to include duplicates in difference (default: false)
  - `:column_mapping` - Map columns between incompatible schemas

  ## Examples

      # Find free users (all users except premium)
      all_users |> Selecto.except(premium_users)

      # Include duplicates in difference
      query1 |> Selecto.except(query2, all: true)
  """
  def except(left_query, right_query, opts \\ []) do
    Selecto.SetOperations.except(left_query, right_query, opts)
  end

  @doc """
  Add a LATERAL join to the query.

  LATERAL joins allow the right side of the join to reference columns from the
  left side, enabling powerful correlated subquery patterns.

  ## Parameters

  - `join_type` - Type of join (:left, :inner, :right, :full)
  - `subquery_builder_or_function` - Function that builds correlated subquery or table function tuple
  - `alias_name` - Alias for the LATERAL join results
  - `opts` - Additional options

  ## Examples

      # LATERAL join with correlated subquery
      selecto
      |> Selecto.lateral_join(
        :left,
        fn base_query ->
          Selecto.configure(rental_domain, connection)
          |> Selecto.select([{:func, "COUNT", ["*"], as: "rental_count"}])
          |> Selecto.filter([{"customer_id", {:ref, "customer.customer_id"}}])
          |> Selecto.limit(5)
        end,
        "recent_rentals"
      )

      # LATERAL join with table function
      selecto
      |> Selecto.lateral_join(
        :inner,
        {:unnest, "film.special_features"},
        "features"
      )

      # LATERAL join with generate_series
      selecto
      |> Selecto.lateral_join(
        :inner,
        {:function, :generate_series, [1, 10]},
        "numbers"
      )
  """
  # Consolidated version that handles both parameter formats:
  # 1. (selecto, lateral_source, opts) - where opts contains :as and :type
  # 2. (selecto, join_type, subquery_builder_or_function, alias_name, opts)
  def lateral_join(selecto, arg2, arg3 \\ [], arg4 \\ nil, arg5 \\ []) do
    # Determine which parameter format is being used
    {join_type, subquery_builder_or_function, alias_name, opts} =
      case {arg2, arg3, arg4, arg5} do
        # Format 1: (selecto, lateral_source, opts)
        {lateral_source, opts, nil, []} when is_list(opts) ->
          alias_name = Keyword.fetch!(opts, :as)
          join_type = Keyword.get(opts, :type, :left)
          {join_type, lateral_source, alias_name, opts}

        # Format 2: (selecto, join_type, subquery_builder_or_function, alias_name, opts)
        {join_type, subquery_builder_or_function, alias_name, opts} ->
          {join_type, subquery_builder_or_function, alias_name, opts}
      end

    # Handle different lateral source types
    lateral_spec =
      case subquery_builder_or_function do
        # Handle inline spec format from old version
        {:function, func_name, args, returns: returns} ->
          %{
            type: :table_function,
            function: func_name,
            arguments: args,
            returns: returns,
            alias: alias_name,
            join_type: join_type
          }

        # For other cases, use Advanced.LateralJoin
        _ ->
          Selecto.Advanced.LateralJoin.create_lateral_join(
            join_type,
            subquery_builder_or_function,
            alias_name,
            opts
          )
      end

    # Validate correlations
    case Selecto.Advanced.LateralJoin.validate_correlations(lateral_spec, selecto) do
      {:ok, validated_spec} ->
        # Add to selecto set
        current_lateral_joins = Map.get(selecto.set, :lateral_joins, [])
        updated_lateral_joins = current_lateral_joins ++ [validated_spec]

        put_in(selecto.set[:lateral_joins], updated_lateral_joins)

      {:error, correlation_error} ->
        raise correlation_error
    end
  end

  @doc """
  Add a VALUES clause to create an inline table from literal data.

  VALUES clauses allow creating inline tables from literal values, useful for
  data transformations, lookup tables, and testing scenarios.

  ## Parameters

  - `selecto` - The Selecto struct
  - `data` - List of data rows (lists or maps)
  - `opts` - Options including `:columns` (explicit column names), `:as` (table alias),
    and optional `:join` configuration to auto-join the VALUES table

  ## Examples

      # Basic VALUES table with explicit columns
      selecto
      |> Selecto.with_values([
          ["PG", "Family Friendly", 1],
          ["PG-13", "Teen", 2],
          ["R", "Adult", 3]
        ],
        columns: ["rating_code", "description", "sort_order"],
        as: "rating_lookup"
      )

      # Map-based VALUES (columns inferred from keys)
      selecto
      |> Selecto.with_values([
          %{month: 1, name: "January", days: 31},
          %{month: 2, name: "February", days: 28},
          %{month: 3, name: "March", days: 31}
        ], as: "months")

      # VALUES with auto-join (fields inferred from VALUES columns)
      selecto
      |> Selecto.with_values([
          ["processing", "In Progress"],
          ["shipped", "In Transit"]
        ],
        columns: ["status", "status_label"],
        as: "status_labels",
        join: [owner_key: :status, related_key: :status]
      )
      |> Selecto.select(["order_number", "status_labels.status_label"])

      # Generated SQL:
      # WITH rating_lookup (rating_code, description, sort_order) AS (
      #   VALUES ('PG', 'Family Friendly', 1),
      #          ('PG-13', 'Teen', 2),
      #          ('R', 'Adult', 3)
      # )
  """
  def with_values(selecto, data, opts \\ []) do
    # Create VALUES clause specification
    values_spec = Selecto.Advanced.ValuesClause.create_values_clause(data, opts)

    # Add to selecto set
    current_values_clauses = Map.get(selecto.set, :values_clauses, [])
    updated_values_clauses = current_values_clauses ++ [values_spec]

    selecto
    |> put_in([Access.key(:set), :values_clauses], updated_values_clauses)
    |> maybe_join_values_clause(values_spec, Keyword.get(opts, :join))
  end

  defp maybe_join_values_clause(selecto, _values_spec, join_opts)
       when join_opts in [nil, false],
       do: selecto

  defp maybe_join_values_clause(selecto, values_spec, join_opts) do
    {join_id, join_options} = build_values_join_options(values_spec, join_opts)
    Selecto.join(selecto, join_id, join_options)
  end

  defp build_values_join_options(values_spec, true),
    do: build_values_join_options(values_spec, [])

  defp build_values_join_options(values_spec, join_opts) when is_map(join_opts) do
    values_spec
    |> build_values_join_options(normalize_values_join_opts(join_opts))
  end

  defp build_values_join_options(values_spec, join_opts) when is_list(join_opts) do
    normalized_join_opts = normalize_values_join_opts(join_opts)
    default_join_key = default_values_join_key(values_spec)

    owner_key = Keyword.get(normalized_join_opts, :owner_key, default_join_key)
    related_key = Keyword.get(normalized_join_opts, :related_key, owner_key)

    join_id =
      normalized_join_opts
      |> Keyword.get(:id, values_spec.alias)
      |> normalize_values_join_id()

    join_options =
      normalized_join_opts
      |> Keyword.delete(:id)
      |> Keyword.put_new(:source, values_spec.alias)
      |> Keyword.put_new(:type, :left)
      |> Keyword.put_new(:owner_key, owner_key)
      |> Keyword.put_new(:related_key, related_key)
      |> Keyword.put_new(:fields, inferred_values_join_fields(values_spec))

    {join_id, join_options}
  end

  defp build_values_join_options(_values_spec, invalid_opts) do
    raise ArgumentError,
          ":join option for with_values/3 must be true, false, nil, a keyword list, or a map. Got: #{inspect(invalid_opts)}"
  end

  defp normalize_values_join_opts(join_opts) when is_map(join_opts),
    do: join_opts |> Map.to_list() |> normalize_values_join_opts()

  defp normalize_values_join_opts(join_opts) when is_list(join_opts) do
    Enum.map(join_opts, fn
      {key, value} -> {normalize_values_join_key(key), value}
      other -> other
    end)
  end

  defp normalize_values_join_key(key) when is_atom(key), do: key

  defp normalize_values_join_key(key) when is_binary(key) do
    case key do
      "id" -> :id
      "source" -> :source
      "type" -> :type
      "owner_key" -> :owner_key
      "related_key" -> :related_key
      "on" -> :on
      "fields" -> :fields
      other -> other
    end
  end

  defp normalize_values_join_key(key), do: key

  defp normalize_values_join_id(join_id) when is_atom(join_id), do: join_id
  defp normalize_values_join_id(join_id) when is_binary(join_id), do: String.to_atom(join_id)

  defp normalize_values_join_id(join_id) do
    raise ArgumentError,
          "VALUES auto-join id must be an atom or string. Got: #{inspect(join_id)}"
  end

  defp default_values_join_key(%{columns: [first_column | _]}), do: first_column

  defp default_values_join_key(%{columns: []}) do
    raise ArgumentError,
          "Cannot infer VALUES join key from empty columns. Provide join: [owner_key: ..., related_key: ...]."
  end

  defp inferred_values_join_fields(values_spec) do
    Enum.reduce(values_spec.columns, %{}, fn column, acc ->
      type = Map.get(values_spec.column_types, column, :string)
      Map.put(acc, column, %{type: type})
    end)
  end

  @doc """
  Add JSON operations to SELECT clauses for PostgreSQL JSON/JSONB functionality.

  Supports JSON path extraction, aggregation, construction, and manipulation operations.

  ## Parameters

  - `selecto` - The Selecto instance
  - `json_operations` - List of JSON operation tuples or single operation
  - `opts` - Options (reserved for future use)

  ## Examples

      # JSON path extraction
      selecto
      |> Selecto.json_select([
          {:json_extract, "metadata", "$.category", as: "category"},
          {:json_extract, "metadata", "$.specs.weight", as: "weight"}
        ])

      # JSON aggregation
      selecto
      |> Selecto.json_select([
          {:json_agg, "product_name", as: "products"},
          {:json_object_agg, "product_id", "price", as: "price_map"}
        ])
      |> Selecto.group_by(["category"])

      # Single JSON operation
      selecto
      |> Selecto.json_select({:json_extract, "data", "$.status", as: "status"})
  """
  def json_select(selecto, json_operations, opts \\ [])

  def json_select(selecto, json_operations, _opts) when is_list(json_operations) do
    # Create JSON operation specifications
    json_specs =
      json_operations
      |> Enum.map(fn
        {operation, column, path_or_opts} when is_binary(path_or_opts) ->
          Selecto.Advanced.JsonOperations.create_json_operation(operation, column,
            path: path_or_opts
          )

        {operation, column, path, opts} when is_binary(path) ->
          Selecto.Advanced.JsonOperations.create_json_operation(
            operation,
            column,
            [path: path] ++ opts
          )

        {operation, column, opts} when is_list(opts) ->
          Selecto.Advanced.JsonOperations.create_json_operation(operation, column, opts)

        {operation, column} ->
          Selecto.Advanced.JsonOperations.create_json_operation(operation, column)
      end)

    # Add to selecto set
    current_json_selects = Map.get(selecto.set, :json_selects, [])
    updated_json_selects = current_json_selects ++ json_specs

    put_in(selecto.set[:json_selects], updated_json_selects)
  end

  def json_select(selecto, json_operation, opts) do
    json_select(selecto, [json_operation], opts)
  end

  @doc """
  Add JSON operations to WHERE clauses for filtering with PostgreSQL JSON/JSONB functionality.

  Supports JSON containment, existence, and comparison operations.

  ## Parameters

  - `selecto` - The Selecto instance
  - `json_filters` - List of JSON filter tuples or single filter
  - `opts` - Options (reserved for future use)

  ## Examples

      # JSON containment and existence
      selecto
      |> Selecto.json_filter([
          {:json_contains, "metadata", %{"category" => "electronics"}},
          {:json_path_exists, "metadata", "$.specs.warranty"}
        ])

      # JSON path comparison
      selecto
      |> Selecto.json_filter([
          {:json_extract_text, "settings", "$.theme", {:=, "dark"}},
          {:json_extract, "data", "$.priority", {:>, 5}}
        ])

      # Single JSON filter
      selecto
      |> Selecto.json_filter({:json_exists, "tags", "electronics"})
  """
  def json_filter(selecto, json_filters, opts \\ [])

  def json_filter(selecto, json_filters, _opts) when is_list(json_filters) do
    # Create JSON filter specifications
    json_specs =
      json_filters
      |> Enum.map(fn
        {operation, column, path_or_value, comparison} when is_binary(path_or_value) ->
          Selecto.Advanced.JsonOperations.create_json_operation(operation, column,
            path: path_or_value,
            comparison: comparison
          )

        {operation, column, value} ->
          Selecto.Advanced.JsonOperations.create_json_operation(operation, column, value: value)

        {operation, column} ->
          Selecto.Advanced.JsonOperations.create_json_operation(operation, column)
      end)

    # Add to selecto set
    current_json_filters = Map.get(selecto.set, :json_filters, [])
    updated_json_filters = current_json_filters ++ json_specs

    put_in(selecto.set[:json_filters], updated_json_filters)
  end

  def json_filter(selecto, json_filter, opts) do
    json_filter(selecto, [json_filter], opts)
  end

  @doc """
  Add JSON operations to ORDER BY clauses for sorting with PostgreSQL JSON/JSONB functionality.

  ## Parameters

  - `selecto` - The Selecto instance
  - `json_sorts` - List of JSON sort tuples or single sort
  - `opts` - Options (reserved for future use)

  ## Examples

      # Sort by JSON path values
      selecto
      |> Selecto.json_order_by([
          {:json_extract, "metadata", "$.priority", :desc},
          {:json_extract_text, "data", "$.created_at", :asc}
        ])

      # Single JSON sort
      selecto
      |> Selecto.json_order_by({:json_extract, "settings", "$.sort_order"})
  """
  def json_order_by(selecto, json_sorts, opts \\ [])

  def json_order_by(selecto, json_sorts, _opts) when is_list(json_sorts) do
    # Create JSON sort specifications
    json_specs =
      json_sorts
      |> Enum.map(fn
        {operation, column, path, direction} when is_binary(path) ->
          spec =
            Selecto.Advanced.JsonOperations.create_json_operation(operation, column, path: path)

          {spec, direction || :asc}

        {operation, column, direction} when direction in [:asc, :desc] ->
          spec = Selecto.Advanced.JsonOperations.create_json_operation(operation, column)
          {spec, direction}

        {operation, column, path} when is_binary(path) ->
          spec =
            Selecto.Advanced.JsonOperations.create_json_operation(operation, column, path: path)

          {spec, :asc}

        {operation, column} ->
          spec = Selecto.Advanced.JsonOperations.create_json_operation(operation, column)
          {spec, :asc}
      end)

    # Add to selecto set
    current_json_sorts = Map.get(selecto.set, :json_order_by, [])
    updated_json_sorts = current_json_sorts ++ json_specs

    put_in(selecto.set[:json_order_by], updated_json_sorts)
  end

  def json_order_by(selecto, json_sort, opts) do
    json_order_by(selecto, [json_sort], opts)
  end

  @doc """
  Add a Common Table Expression (CTE) to the query using WITH clause.

  CTEs provide a way to create temporary named result sets that can be
  referenced within the main query, enabling query modularity and readability.

  ## Parameters

  - `selecto` - The Selecto instance
  - `name` - CTE name (must be valid SQL identifier)
  - `query_builder` - Function that returns a Selecto query for the CTE
  - `opts` - Options including :columns, :dependencies, and optional :join

  Join shortcut options (`:join`):
  - `true` - infer join keys from first declared CTE column
  - keyword/map - any `Selecto.join/3` options (`:type`, `:owner_key`, `:related_key`, `:on`, `:fields`)
  - `fields: :infer` - infer join fields from declared CTE columns

  ## Examples

      # Simple CTE for filtering
      selecto
      |> Selecto.with_cte("active_customers", fn ->
          Selecto.configure(customer_domain, connection)
          |> Selecto.filter([{"active", true}])
        end,
        join: [owner_key: :customer_id, related_key: :customer_id]
      )
      |> Selecto.select(["film.title", "active_customers.first_name"])

      # CTE with explicit columns
      selecto
      |> Selecto.with_cte("customer_stats",
          fn ->
            Selecto.configure(customer_domain, connection)
            |> Selecto.select(["customer_id", {:func, "COUNT", ["rental_id"], as: "rental_count"}])
            |> Selecto.join(:left, "rental", on: "customer.customer_id = rental.customer_id")
            |> Selecto.group_by(["customer_id"])
          end,
          columns: ["customer_id", "rental_count"]
        )

      # Generated SQL:
      # WITH active_customers AS (
      #   SELECT * FROM customer WHERE active = true
      # )
      # SELECT film.title, active_customers.first_name
      # FROM film
      # INNER JOIN active_customers ON rental.customer_id = active_customers.customer_id
  """
  def with_cte(selecto, name, query_builder, opts \\ []) do
    join_opts = Keyword.get(opts, :join)
    cte_opts = Keyword.delete(opts, :join)

    # Create CTE specification
    cte_spec = Selecto.Advanced.CTE.create_cte(name, query_builder, cte_opts)

    selecto
    |> append_cte_spec(cte_spec)
    |> maybe_join_cte_spec(cte_spec, join_opts)
  end

  @doc """
  Add a recursive Common Table Expression (CTE) to the query.

  Recursive CTEs enable hierarchical queries by combining an anchor query
  with a recursive query that references the CTE itself.

  ## Parameters

  - `selecto` - The Selecto instance
  - `name` - CTE name (must be valid SQL identifier)
  - `opts` - Options with :base_query, :recursive_query, and optional :join

  Join shortcut options (`:join`) follow `with_cte/4`.

  ## Examples

      # Hierarchical employee structure
      selecto
      |> Selecto.with_recursive_cte("employee_hierarchy",
          base_query: fn ->
            # Anchor: top-level managers
            Selecto.configure(employee_domain, connection)
            |> Selecto.select(["employee_id", "name", "manager_id", {:literal, 0, as: "level"}])
            |> Selecto.filter([{"manager_id", nil}])
          end,
          recursive_query: fn cte_ref ->
            # Recursive: subordinates
            Selecto.configure(employee_domain, connection)
            |> Selecto.select(["employee.employee_id", "employee.name", "employee.manager_id",
                              {:func, "employee_hierarchy.level + 1", as: "level"}])
            |> Selecto.join(:inner, cte_ref, on: "employee.manager_id = employee_hierarchy.employee_id")
          end,
          join: [owner_key: :employee_id, related_key: :employee_id]
        )
      |> Selecto.select([
          "employee_hierarchy.employee_id",
          "employee_hierarchy.name",
          "employee_hierarchy.level"
        ])
      |> Selecto.order_by([{"employee_hierarchy.level", :asc}, {"employee_hierarchy.name", :asc}])

      # Generated SQL:
      # WITH RECURSIVE employee_hierarchy AS (
      #   SELECT employee_id, name, manager_id, 0 as level
      #   FROM employee
      #   WHERE manager_id IS NULL
      #   UNION ALL
      #   SELECT employee.employee_id, employee.name, employee.manager_id, employee_hierarchy.level + 1
      #   FROM employee
      #   INNER JOIN employee_hierarchy ON employee.manager_id = employee_hierarchy.employee_id
      # )
      # SELECT employee_hierarchy.employee_id, employee_hierarchy.name, employee_hierarchy.level
      # FROM employee
      # INNER JOIN employee_hierarchy ON employee.employee_id = employee_hierarchy.employee_id
      # ORDER BY employee_hierarchy.level ASC, employee_hierarchy.name ASC
  """
  # Consolidated version that handles both parameter formats:
  # 1. (selecto, cte_name, base_fn, recursive_fn, opts) - original inline format
  # 2. (selecto, name, opts) - newer format using Advanced.CTE
  def with_recursive_cte(selecto, arg2, arg3, arg4 \\ nil, arg5 \\ []) do
    {cte_spec, join_opts} =
      case {arg2, arg3, arg4, arg5} do
        # Format 1: (selecto, cte_name, base_fn, recursive_fn, opts)
        {cte_name, base_fn, recursive_fn, opts}
        when is_function(base_fn) and is_function(recursive_fn) ->
          normalized_opts = normalize_cte_option_list(opts)
          join_opts = Keyword.get(normalized_opts, :join)
          cte_opts = Keyword.delete(normalized_opts, :join)

          # Inline spec format
          {%{
             name: cte_name,
             type: :recursive,
             base_query: base_fn,
             recursive_query: recursive_fn,
             max_depth: Keyword.get(cte_opts, :max_depth),
             cycle_detection: Keyword.get(cte_opts, :cycle_detection, false),
             columns: Keyword.get(cte_opts, :columns)
           }, join_opts}

        # Format 2: (selecto, name, opts)
        {name, opts, nil, []} when is_list(opts) or is_map(opts) ->
          normalized_opts = normalize_cte_option_list(opts)
          join_opts = Keyword.get(normalized_opts, :join)
          cte_opts = Keyword.delete(normalized_opts, :join)

          # Use Advanced.CTE module
          {Selecto.Advanced.CTE.create_recursive_cte(name, cte_opts), join_opts}
      end

    selecto
    |> append_cte_spec(cte_spec)
    |> maybe_join_cte_spec(cte_spec, join_opts)
  end

  @doc """
  Add multiple CTEs to the query in a single operation.

  Useful for complex queries that require multiple temporary result sets.
  CTEs will be automatically ordered based on their dependencies.

  ## Parameters

  - `selecto` - The Selecto instance
  - `cte_specs` - List of CTE specifications created with create_cte/3
  - `opts` - Options including :joins for auto-joining one or more CTEs

  Batch join shortcut options (`:joins`):
  - `true` - auto-join every provided CTE (default key inference)
  - list of entries where each entry is one of:
    - cte name (`"my_cte"` or `:my_cte`) to use inferred defaults
    - `{name, join_opts}` tuple
    - keyword/map with `:name` plus join options

  ## Examples

      # Multiple related CTEs
      active_customers_cte = Selecto.Advanced.CTE.create_cte("active_customers", fn ->
        Selecto.configure(customer_domain, connection)
        |> Selecto.filter([{"active", true}])
      end)

      high_value_cte = Selecto.Advanced.CTE.create_cte("high_value_customers", fn ->
        Selecto.configure(customer_domain, connection)
        |> Selecto.aggregate([{"payment.amount", :sum, as: "total_spent"}])
        |> Selecto.join(:inner, "payment", on: "customer.customer_id = payment.customer_id")
        |> Selecto.group_by(["customer.customer_id"])
        |> Selecto.having([{"total_spent", {:>, 100}}])
      end, dependencies: ["active_customers"])

      selecto
      |> Selecto.with_ctes([active_customers_cte, high_value_cte],
        joins: [
          [name: "active_customers", owner_key: :customer_id, related_key: :customer_id],
          [name: "high_value_customers", owner_key: :customer_id, related_key: :customer_id]
        ]
      )
      |> Selecto.select(["film.title", "high_value_customers.total_spent"])
  """
  def with_ctes(selecto, cte_specs, opts \\ []) when is_list(cte_specs) do
    selecto_with_ctes = append_cte_specs(selecto, cte_specs)
    apply_with_ctes_joins(selecto_with_ctes, cte_specs, Keyword.get(opts, :joins))
  end

  defp append_cte_spec(selecto, cte_spec), do: append_cte_specs(selecto, [cte_spec])

  defp append_cte_specs(selecto, cte_specs) when is_list(cte_specs) do
    current_ctes = Map.get(selecto.set, :ctes, [])
    updated_ctes = current_ctes ++ cte_specs

    put_in(selecto.set[:ctes], updated_ctes)
  end

  defp maybe_join_cte_spec(selecto, _cte_spec, join_opts) when join_opts in [nil, false],
    do: selecto

  defp maybe_join_cte_spec(selecto, cte_spec, join_opts) do
    {join_id, join_options} = build_cte_join_options(cte_spec, join_opts)
    Selecto.join(selecto, join_id, join_options)
  end

  defp build_cte_join_options(cte_spec, true), do: build_cte_join_options(cte_spec, [])

  defp build_cte_join_options(cte_spec, join_opts) when is_map(join_opts) do
    cte_spec
    |> build_cte_join_options(normalize_cte_join_opts(join_opts))
  end

  defp build_cte_join_options(cte_spec, join_opts) when is_list(join_opts) do
    cte_name = cte_spec_name!(cte_spec)
    normalized_join_opts = normalize_cte_join_opts(join_opts)

    join_id =
      normalized_join_opts
      |> Keyword.get(:id, cte_name)
      |> normalize_values_join_id()

    join_options =
      normalized_join_opts
      |> Keyword.delete(:id)
      |> Keyword.delete(:name)
      |> Keyword.put_new(:source, cte_name)
      |> Keyword.put_new(:type, :left)
      |> maybe_add_default_cte_join_keys(cte_spec)
      |> maybe_add_default_cte_join_fields(cte_spec)

    {join_id, join_options}
  end

  defp build_cte_join_options(_cte_spec, invalid_opts) do
    raise ArgumentError,
          ":join option for CTE helpers must be true, false, nil, a keyword list, or a map. Got: #{inspect(invalid_opts)}"
  end

  defp maybe_add_default_cte_join_keys(join_options, cte_spec) do
    if Keyword.has_key?(join_options, :on) do
      join_options
    else
      case {Keyword.get(join_options, :owner_key), Keyword.get(join_options, :related_key)} do
        {owner_key, related_key} when not is_nil(owner_key) and not is_nil(related_key) ->
          join_options

        {owner_key, nil} when not is_nil(owner_key) ->
          Keyword.put(join_options, :related_key, owner_key)

        _ ->
          default_join_key = default_cte_join_key(cte_spec)
          owner_key = Keyword.get(join_options, :owner_key, default_join_key)
          related_key = Keyword.get(join_options, :related_key, owner_key)

          join_options
          |> Keyword.put_new(:owner_key, owner_key)
          |> Keyword.put_new(:related_key, related_key)
      end
    end
  end

  defp maybe_add_default_cte_join_fields(join_options, cte_spec) do
    case Keyword.get(join_options, :fields, :__missing__) do
      :infer ->
        Keyword.put(join_options, :fields, inferred_cte_join_fields!(cte_spec))

      :__missing__ ->
        inferred_fields = inferred_cte_join_fields(cte_spec)

        if inferred_fields == %{} do
          join_options
        else
          Keyword.put(join_options, :fields, inferred_fields)
        end

      _ ->
        join_options
    end
  end

  defp default_cte_join_key(cte_spec) do
    case cte_columns(cte_spec) do
      [first_column | _] ->
        first_column

      [] ->
        cte_name = cte_spec_name!(cte_spec)

        raise ArgumentError,
              "Cannot infer join keys for CTE '#{cte_name}'. Declare CTE columns or pass :owner_key/:related_key (or :on) in :join options."
    end
  end

  defp inferred_cte_join_fields(cte_spec) do
    Enum.reduce(cte_columns(cte_spec), %{}, fn column, acc ->
      Map.put(acc, String.to_atom(column), %{type: :any})
    end)
  end

  defp inferred_cte_join_fields!(cte_spec) do
    inferred_fields = inferred_cte_join_fields(cte_spec)

    if inferred_fields == %{} do
      cte_name = cte_spec_name!(cte_spec)

      raise ArgumentError,
            "Cannot infer fields for CTE '#{cte_name}' because it has no declared columns. Provide fields explicitly or declare CTE columns."
    else
      inferred_fields
    end
  end

  defp cte_columns(cte_spec) do
    cte_spec
    |> Map.get(:columns, [])
    |> List.wrap()
    |> Enum.map(&to_string/1)
  end

  defp cte_spec_name!(%{name: name}) when is_binary(name), do: name
  defp cte_spec_name!(%{name: name}) when is_atom(name), do: Atom.to_string(name)

  defp cte_spec_name!(cte_spec) do
    raise ArgumentError,
          "CTE spec must include a string or atom :name. Got: #{inspect(cte_spec)}"
  end

  defp apply_with_ctes_joins(selecto, _cte_specs, joins) when joins in [nil, false, []],
    do: selecto

  defp apply_with_ctes_joins(selecto, cte_specs, true) do
    Enum.reduce(cte_specs, selecto, fn cte_spec, acc ->
      maybe_join_cte_spec(acc, cte_spec, true)
    end)
  end

  defp apply_with_ctes_joins(selecto, cte_specs, joins) when is_list(joins) do
    Enum.reduce(joins, selecto, fn join_entry, acc ->
      {cte_spec, join_opts} = resolve_with_ctes_join_entry(cte_specs, join_entry)
      maybe_join_cte_spec(acc, cte_spec, join_opts)
    end)
  end

  defp apply_with_ctes_joins(_selecto, _cte_specs, invalid_joins) do
    raise ArgumentError,
          ":joins option for with_ctes/3 must be true, false, nil, or a list. Got: #{inspect(invalid_joins)}"
  end

  defp resolve_with_ctes_join_entry(cte_specs, entry) when is_binary(entry) or is_atom(entry) do
    {find_cte_spec!(cte_specs, entry), true}
  end

  defp resolve_with_ctes_join_entry(cte_specs, {name, join_opts}) do
    {find_cte_spec!(cte_specs, name), join_opts}
  end

  defp resolve_with_ctes_join_entry(cte_specs, join_entry) when is_map(join_entry) do
    resolve_with_ctes_join_entry(cte_specs, Map.to_list(join_entry))
  end

  defp resolve_with_ctes_join_entry(cte_specs, join_entry) when is_list(join_entry) do
    normalized_entry = normalize_cte_join_opts(join_entry)
    cte_name = Keyword.get(normalized_entry, :name)

    if is_nil(cte_name) do
      raise ArgumentError,
            "Each entry in :joins for with_ctes/3 must include :name, be a cte name, or be a {name, opts} tuple. Got: #{inspect(join_entry)}"
    end

    join_opts = Keyword.delete(normalized_entry, :name)
    {find_cte_spec!(cte_specs, cte_name), join_opts}
  end

  defp resolve_with_ctes_join_entry(_cte_specs, invalid_entry) do
    raise ArgumentError,
          "Invalid :joins entry for with_ctes/3: #{inspect(invalid_entry)}"
  end

  defp find_cte_spec!(cte_specs, cte_name) do
    cte_name_string = to_string(cte_name)

    Enum.find(cte_specs, fn cte_spec ->
      cte_spec_name!(cte_spec) == cte_name_string
    end) ||
      raise ArgumentError,
            "CTE named '#{cte_name_string}' was not found in with_ctes/3 input"
  end

  defp normalize_cte_option_list(opts) when is_map(opts),
    do: opts |> Map.to_list() |> normalize_cte_option_list()

  defp normalize_cte_option_list(opts) when is_list(opts) do
    Enum.map(opts, fn
      {key, value} -> {normalize_cte_option_key(key), value}
      other -> other
    end)
  end

  defp normalize_cte_option_key(key) when is_atom(key), do: key

  defp normalize_cte_option_key(key) when is_binary(key) do
    case key do
      "base_query" -> :base_query
      "recursive_query" -> :recursive_query
      "columns" -> :columns
      "dependencies" -> :dependencies
      "join" -> :join
      "joins" -> :joins
      "max_depth" -> :max_depth
      "cycle_detection" -> :cycle_detection
      other -> other
    end
  end

  defp normalize_cte_option_key(key), do: key

  defp normalize_cte_join_opts(join_opts) when is_map(join_opts),
    do: join_opts |> Map.to_list() |> normalize_cte_join_opts()

  defp normalize_cte_join_opts(join_opts) when is_list(join_opts) do
    Enum.map(join_opts, fn
      {key, value} -> {normalize_cte_join_key(key), value}
      other -> other
    end)
  end

  defp normalize_cte_join_key(key) when is_atom(key), do: key

  defp normalize_cte_join_key(key) when is_binary(key) do
    case key do
      "name" -> :name
      "id" -> :id
      "source" -> :source
      "type" -> :type
      "owner_key" -> :owner_key
      "related_key" -> :related_key
      "on" -> :on
      "fields" -> :fields
      other -> other
    end
  end

  defp normalize_cte_join_key(key), do: key

  @doc """
  Add a simple CASE expression to the select fields.

  Simple CASE expressions test a column against specific values and return
  corresponding results. This is useful for data transformation and categorization.

  ## Parameters

  - `selecto` - The Selecto instance
  - `column` - Column to test against
  - `when_clauses` - List of {value, result} tuples for WHEN conditions
  - `opts` - Options including :else and :as

  ## Examples

      # Simple CASE for film ratings
      selecto
      |> Selecto.case_select("film.rating", [
          {"G", "General Audience"},
          {"PG", "Parental Guidance"},
          {"PG-13", "Parents Strongly Cautioned"},
          {"R", "Restricted"}
        ], else: "Not Rated", as: "rating_description")
      |> Selecto.select(["film.title", "rating_description"])

      # Generated SQL:
      # SELECT film.title,
      #        CASE film.rating
      #          WHEN 'G' THEN 'General Audience'
      #          WHEN 'PG' THEN 'Parental Guidance'
      #          WHEN 'PG-13' THEN 'Parents Strongly Cautioned'
      #          WHEN 'R' THEN 'Restricted'
      #          ELSE 'Not Rated'
      #        END AS rating_description
  """
  def case_select(selecto, column, when_clauses, opts \\ []) do
    # Create CASE specification
    case_spec = Selecto.Advanced.CaseExpression.create_simple_case(column, when_clauses, opts)

    # Add to select fields
    case_field = {:case, case_spec}
    select(selecto, case_field)
  end

  @doc """
  Add a searched CASE expression to the select fields.

  Searched CASE expressions evaluate multiple conditions and return results
  based on the first true condition. This enables complex conditional logic.

  ## Parameters

  - `selecto` - The Selecto instance
  - `when_clauses` - List of {conditions, result} tuples
  - `opts` - Options including :else and :as

  ## Examples

      # Customer tier based on payment totals
      selecto
      |> Selecto.case_when_select([
          {[{"payment_total", {:>, 100}}], "Premium"},
          {[{"payment_total", {:between, 50, 100}}], "Standard"},
          {[{"payment_total", {:>, 0}}], "Basic"}
        ], else: "No Purchases", as: "customer_tier")
      |> Selecto.select(["customer.first_name", "customer_tier"])

      # Multiple conditions per WHEN clause
      selecto
      |> Selecto.case_when_select([
          {[{"film.rating", "R"}, {"film.length", {:>, 120}}], "Long Adult Film"},
          {[{"film.rating", "G"}, {"film.special_features", {:like, "%Family%"}}], "Family Film"}
        ], else: "Regular Film", as: "film_category")

      # Generated SQL:
      # SELECT customer.first_name,
      #        CASE
      #          WHEN payment_total > $1 THEN $2
      #          WHEN payment_total BETWEEN $3 AND $4 THEN $5
      #          WHEN payment_total > $6 THEN $7
      #          ELSE $8
      #        END AS customer_tier
  """
  def case_when_select(selecto, when_clauses, opts \\ []) do
    # Create CASE specification
    case_spec = Selecto.Advanced.CaseExpression.create_searched_case(when_clauses, opts)

    # Add to select fields
    case_field = {:case_when, case_spec}
    select(selecto, case_field)
  end

  @doc """
  Add array aggregation operations to select fields.

  Supports ARRAY_AGG, STRING_AGG, and other array aggregation functions
  with optional DISTINCT, ORDER BY, and filtering.

  ## Parameters

  - `selecto` - The Selecto instance
  - `array_operations` - List of array operation tuples or single operation
  - `opts` - Additional options

  ## Examples

      # Simple array aggregation
      selecto
      |> Selecto.array_select({:array_agg, "film.title", as: "film_titles"})

      # Array aggregation with DISTINCT and ORDER BY
      selecto
      |> Selecto.array_select({:array_agg, "actor.name",
          distinct: true,
          order_by: [{"actor.last_name", :asc}],
          as: "unique_actors"})

      # String aggregation with custom delimiter
      selecto
      |> Selecto.array_select({:string_agg, "tag.name",
          delimiter: ", ",
          as: "tag_list"})

      # Array length operation
      selecto
      |> Selecto.array_select({:array_length, "tags", 1, as: "tag_count"})
  """
  def array_select(selecto, array_operations, opts \\ [])

  def array_select(selecto, array_operations, _opts) when is_list(array_operations) do
    # Create array operation specifications
    array_specs =
      array_operations
      |> Enum.map(fn
        # Aggregation operations
        {:array_agg, column, opts} ->
          Selecto.Advanced.ArrayOperations.create_array_operation(:array_agg, column, opts)

        {:array_agg_distinct, column, opts} ->
          Selecto.Advanced.ArrayOperations.create_array_operation(
            :array_agg_distinct,
            column,
            opts
          )

        {:string_agg, column, opts} ->
          Selecto.Advanced.ArrayOperations.create_array_operation(:string_agg, column, opts)

        # Size operations with dimension
        {:array_length, column, dimension, opts} ->
          Selecto.Advanced.ArrayOperations.create_array_size(
            :array_length,
            column,
            dimension,
            opts
          )

        # Size operations without dimension
        {:cardinality, column, opts} ->
          Selecto.Advanced.ArrayOperations.create_array_size(:cardinality, column, nil, opts)

        {:array_ndims, column, opts} ->
          Selecto.Advanced.ArrayOperations.create_array_size(:array_ndims, column, nil, opts)

        {:array_dims, column, opts} ->
          Selecto.Advanced.ArrayOperations.create_array_size(:array_dims, column, nil, opts)

        # Array construction/manipulation with value
        {:array_append, column, value, opts} ->
          spec_opts = Keyword.put(opts, :value, value)

          Selecto.Advanced.ArrayOperations.create_array_operation(
            :array_append,
            column,
            spec_opts
          )

        {:array_prepend, column, value, opts} ->
          spec_opts = Keyword.put(opts, :value, value)

          Selecto.Advanced.ArrayOperations.create_array_operation(
            :array_prepend,
            column,
            spec_opts
          )

        {:array_remove, column, value, opts} ->
          spec_opts = Keyword.put(opts, :value, value)

          Selecto.Advanced.ArrayOperations.create_array_operation(
            :array_remove,
            column,
            spec_opts
          )

        {:array_replace, column, old_value, new_value, opts} ->
          spec_opts = opts |> Keyword.put(:value, old_value) |> Keyword.put(:new_value, new_value)

          Selecto.Advanced.ArrayOperations.create_array_operation(
            :array_replace,
            column,
            spec_opts
          )

        {:array_cat, column, value, opts} ->
          spec_opts = Keyword.put(opts, :value, value)
          Selecto.Advanced.ArrayOperations.create_array_operation(:array_cat, column, spec_opts)

        {:array_position, column, value, opts} ->
          spec_opts = Keyword.put(opts, :value, value)

          Selecto.Advanced.ArrayOperations.create_array_operation(
            :array_position,
            column,
            spec_opts
          )

        {:array_positions, column, value, opts} ->
          spec_opts = Keyword.put(opts, :value, value)

          Selecto.Advanced.ArrayOperations.create_array_operation(
            :array_positions,
            column,
            spec_opts
          )

        # Array transformation operations
        {:array_to_string, column, delimiter, opts} ->
          spec_opts = Keyword.put(opts, :value, delimiter)

          Selecto.Advanced.ArrayOperations.create_array_operation(
            :array_to_string,
            column,
            spec_opts
          )

        {:string_to_array, column, delimiter, opts} ->
          spec_opts = Keyword.put(opts, :value, delimiter)

          Selecto.Advanced.ArrayOperations.create_array_operation(
            :string_to_array,
            column,
            spec_opts
          )

        # Array constructor (no column)
        {:array, elements, opts} ->
          spec_opts = Keyword.put(opts, :value, elements)
          Selecto.Advanced.ArrayOperations.create_array_operation(:array, nil, spec_opts)

        # Generic pattern for operations with column and options
        {operation, column, opts} when is_atom(operation) and is_list(opts) ->
          Selecto.Advanced.ArrayOperations.create_array_operation(operation, column, opts)

        _ = spec ->
          spec
      end)

    # Add to selecto set
    current_array_ops = Map.get(selecto.set, :array_operations, [])
    updated_array_ops = current_array_ops ++ array_specs

    put_in(selecto.set[:array_operations], updated_array_ops)
  end

  def array_select(selecto, array_operation, opts) do
    array_select(selecto, [array_operation], opts)
  end

  @doc """
  Add array filtering operations to WHERE clauses.

  Supports array containment, overlap, and equality operations.

  ## Parameters

  - `selecto` - The Selecto instance
  - `array_filters` - List of array filter tuples or single filter
  - `opts` - Additional options

  ## Examples

      # Array contains
      selecto
      |> Selecto.array_filter({:array_contains, "tags", ["featured", "new"]})

      # Array overlap (has any of the elements)
      selecto
      |> Selecto.array_filter({:array_overlap, "categories", ["electronics", "computers"]})

      # Array contained by
      selecto
      |> Selecto.array_filter({:array_contained, "permissions", ["read", "write", "admin"]})

      # Multiple filters
      selecto
      |> Selecto.array_filter([
          {:array_contains, "special_features", ["Trailers"]},
          {:array_overlap, "languages", ["English", "Spanish"]}
        ])
  """
  def array_filter(selecto, array_filters, opts \\ [])

  def array_filter(selecto, array_filters, _opts) when is_list(array_filters) do
    # Create array filter specifications
    array_specs =
      array_filters
      |> Enum.map(fn
        {operation, column, value} ->
          Selecto.Advanced.ArrayOperations.create_array_filter(operation, column, value)

        _ = spec ->
          spec
      end)

    # Add to selecto set filters
    current_filters = Map.get(selecto.set, :array_filters, [])
    updated_filters = current_filters ++ array_specs

    put_in(selecto.set[:array_filters], updated_filters)
  end

  def array_filter(selecto, array_filter, opts) do
    array_filter(selecto, [array_filter], opts)
  end

  @doc """
  Add array manipulation operations to select fields.

  Supports array construction, modification, and transformation operations.

  ## Parameters

  - `selecto` - The Selecto instance
  - `array_operations` - List of array manipulation operations
  - `opts` - Additional options

  ## Examples

      # Array append
      selecto
      |> Selecto.array_manipulate({:array_append, "tags", "new-tag", as: "updated_tags"})

      # Array remove
      selecto
      |> Selecto.array_manipulate({:array_remove, "tags", "deprecated", as: "cleaned_tags"})

      # Array to string
      selecto
      |> Selecto.array_manipulate({:array_to_string, "tags", ", ", as: "tag_string"})
  """
  def array_manipulate(selecto, array_operations, opts \\ [])

  def array_manipulate(selecto, array_operations, _opts) when is_list(array_operations) do
    # Create array operation specifications
    array_specs =
      array_operations
      |> Enum.map(fn
        {:array_append, column, value, opts} ->
          Selecto.Advanced.ArrayOperations.create_array_operation(
            :array_append,
            column,
            Keyword.put(opts, :value, value)
          )

        {:array_prepend, column, value, opts} ->
          Selecto.Advanced.ArrayOperations.create_array_operation(
            :array_prepend,
            column,
            Keyword.put(opts, :value, value)
          )

        {:array_remove, column, value, opts} ->
          Selecto.Advanced.ArrayOperations.create_array_operation(
            :array_remove,
            column,
            Keyword.put(opts, :value, value)
          )

        {:array_replace, column, old_value, new_value, opts} ->
          Selecto.Advanced.ArrayOperations.create_array_operation(
            :array_replace,
            column,
            opts |> Keyword.put(:value, old_value) |> Keyword.put(:new_value, new_value)
          )

        {:array_to_string, column, delimiter, opts} ->
          Selecto.Advanced.ArrayOperations.create_array_operation(
            :array_to_string,
            column,
            Keyword.put(opts, :value, delimiter)
          )

        {:string_to_array, column, delimiter, opts} ->
          Selecto.Advanced.ArrayOperations.create_array_operation(
            :string_to_array,
            column,
            Keyword.put(opts, :value, delimiter)
          )

        {operation, column, opts} when is_atom(operation) ->
          Selecto.Advanced.ArrayOperations.create_array_operation(operation, column, opts)

        _ = spec ->
          spec
      end)

    # Add to selecto set
    current_array_ops = Map.get(selecto.set, :array_operations, [])
    updated_array_ops = current_array_ops ++ array_specs

    put_in(selecto.set[:array_operations], updated_array_ops)
  end

  def array_manipulate(selecto, array_operation, opts) do
    array_manipulate(selecto, [array_operation], opts)
  end
end
