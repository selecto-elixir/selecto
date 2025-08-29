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
  #  @spec configure(Selecto.Types.domain(), Postgrex.conn(), Selecto.Types.configure_options()) :: t()
  def configure(domain, postgrex_opts, opts \\ []) do
    validate? = Keyword.get(opts, :validate, true)
    use_pool? = Keyword.get(opts, :pool, false)
    pool_options = Keyword.get(opts, :pool_options, [])

    if validate? do
      Selecto.DomainValidator.validate_domain!(domain)
    end

    # Handle connection pooling
    final_postgrex_opts =
      if use_pool? and not match?({:pool, _}, postgrex_opts) do
        case Selecto.ConnectionPool.start_pool(postgrex_opts, pool_options) do
          {:ok, pool_ref} ->
            {:pool, pool_ref}

          {:error, reason} ->
            require Logger

            Logger.warning(
              "Failed to start connection pool: #{inspect(reason)}. Falling back to direct connection."
            )

            postgrex_opts
        end
      else
        postgrex_opts
      end

    %Selecto{
      postgrex_opts: final_postgrex_opts,
      domain: domain,
      config: configure_domain(domain),
      set: %{
        selected: Map.get(domain, :required_selected, []),
        filtered: [],
        post_pivot_filters: [],
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
    # Try enhanced field resolution first
    case Selecto.FieldResolver.resolve_field(selecto_struct, field) do
      {:ok, field_info} ->
        # Convert field_info to legacy format for backward compatibility
        %{
          name: field_info.name,
          # Add for backward compatibility - use the actual database field name
          field: field_info.field || field_info.name,
          type: field_info.type,
          requires_join: field_info.source_join,
          qualified_name: field_info.qualified_name,
          alias: field_info.alias
        }

      {:error, _} ->
        # Fallback to legacy field resolution
        fallback_result = selecto_struct.config.columns[field] || selecto_struct.config.columns[String.to_atom(field)]

        if fallback_result do
          # Ensure the field property contains the database field name
          database_field = case Map.get(fallback_result, :field) do
            atom when is_atom(atom) -> Atom.to_string(atom)
            string when is_binary(string) -> string
            nil ->
              # Extract field name from colid if available, otherwise use the field parameter
              case Map.get(fallback_result, :colid) do
                colid when is_binary(colid) ->
                  case Regex.run(~r/\[([^\]]+)\]$/, colid) do
                    [_, field_name] -> field_name
                    nil -> Atom.to_string(field)
                  end
                _ -> Atom.to_string(field)
              end
          end
          Map.put(fallback_result, :field, database_field)
        else
          fallback_result
        end
    end
  end

  @doc """
  Enhanced field resolution with disambiguation and error handling.

  Provides detailed field information and helpful error messages.
  """
  def resolve_field(selecto_struct, field) do
    Selecto.FieldResolver.resolve_field(selecto_struct, field)
  end

  @doc """
  Get all available fields across all joins and the source table.
  """
  def available_fields(selecto_struct) do
    Selecto.FieldResolver.get_available_fields(selecto_struct)
  end

  @doc """
  Get field suggestions for autocomplete or error recovery.
  """
  def field_suggestions(selecto_struct, partial_name) do
    Selecto.FieldResolver.suggest_fields(selecto_struct, partial_name)
  end

  #  @spec set(t()) :: query_set()
  def set(selecto_struct) do
    selecto_struct.set
  end

  #### TODO join stuff, CTE stuff
  ### options:
  ### paramterize: value -- will cause a special case of this join with the indicated parameter, and fields/filters to be made available
  ### inner: true -- change the default

  # def join(selecto_struct, join_id, options \\ []) do
  # end

  ### returns a key to use to add filters, selects, etc from this join
  # def join_paramterize(selecto_struct, join_id, parameter, options) do
  # end

  # def join(selecto_struct, join_id, join_selecto, options \\ []) do
  # end

  ### CTEs. once a CTE is entered, further CTEs can reference it. CTEs are meant to be added as configuration not dynamically!
  # def with(selecto_struct, cte_name, cte, params, options \\ []) do
  # end
  # def with(selecto_struct, cte_name, cte_selecto, options \\ []) do
  # end

  ### Modify an existing CTE
  # def on_with(selecto_struct, cte_name, fn selecto, cte_selecto -> selecto end, options \\ [])
  # end

  @doc """
    add a field to the Select list. Send in one or a list of field names or selectable tuples
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
    # Track whether this filter is applied before or after pivot
    has_pivot = Selecto.Pivot.has_pivot?(selecto)
    pivot_config = Selecto.Pivot.get_pivot_config(selecto)

    # Separate filters into pre-pivot and post-pivot
    {pre_pivot_filters, post_pivot_filters} = case {has_pivot, pivot_config} do
      {false, _} ->
        # No pivot yet, all filters are pre-pivot
        {selecto.set.filtered ++ filters, []}
      {true, _} ->
        # Pivot exists, new filters are post-pivot
        {selecto.set.filtered, filters}
    end

    # Update the set with new filter lists
    updated_set = selecto.set
    |> Map.put(:filtered, pre_pivot_filters)
    |> Map.put(:post_pivot_filters, post_pivot_filters)

    %{selecto | set: updated_set}
  end

  #  @spec filter(t(), filter()) :: t()
  def filter(selecto, filter) do
    Selecto.filter(selecto, [filter])
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
      |> Selecto.select(["attendee[name]"])
      |> Selecto.subselect(["order[product_name]", "order[quantity]"])

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

  #  @spec gen_sql(t(), sql_generation_options()) :: {String.t(), %{String.t() => String.t()}, sql_params()}
  def gen_sql(selecto, opts) do
    # Support both old and new query generation approaches
    if Keyword.get(opts, :use_new_generator, false) do
      Selecto.QueryGenerator.generate_sql(selecto, opts)
    else
      # Keep existing implementation for backward compatibility
      Selecto.Builder.Sql.build(selecto, opts)
    end
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
    # Delegate to the extracted Executor module
    Selecto.Executor.execute(selecto, opts)
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
    # Delegate to the extracted Executor module
    Selecto.Executor.execute_one(selecto, opts)
  end

  @doc """
    Generate SQL without executing - useful for debugging and caching
  """
  #  @spec to_sql(t(), sql_generation_options()) :: sql_result()
  def to_sql(selecto, opts \\ []) do
    {query, _aliases, params} = gen_sql(selecto, opts)
    {query, params}
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
  def lateral_join(selecto, join_type, subquery_builder_or_function, alias_name, opts \\ []) do
    # Create LATERAL join specification
    lateral_spec = Selecto.Advanced.LateralJoin.create_lateral_join(
      join_type, 
      subquery_builder_or_function, 
      alias_name, 
      opts
    )
    
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
  - `opts` - Options including `:columns` (explicit column names) and `:as` (table alias)
  
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
    
    put_in(selecto.set[:values_clauses], updated_values_clauses)
  end
end
