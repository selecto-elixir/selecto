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

  """

  @doc """
    Generate a selecto structure from this Repo following
    the instructions in Domain map.
    
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
    Generate and run the query, returning list of lists, db produces column headers, and provides aliases
  """
#  @spec execute(t(), execute_options()) :: execute_result()
  def execute(selecto, opts \\ []) do
    # IO.puts("Execute Query")

    {query, aliases, params} = gen_sql(selecto, opts)
    # IO.inspect(query, label: "Exe")

    result = Postgrex.query!(selecto.postgrex_opts, query, params)
    # |> IO.inspect(label: "Results")

    {result.rows, result.columns, aliases}
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
