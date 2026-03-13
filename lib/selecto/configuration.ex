defmodule Selecto.Configuration do
  @moduledoc """
  Domain configuration and initialization for Selecto.

  This module handles the setup and configuration of Selecto instances,
  including domain validation, connection pooling, and adapter initialization.
  """

  require Logger

  @doc """
  Generate a selecto structure from a domain configuration and connection input.

  ## Parameters

  - `domain` - Domain configuration map (see domain configuration docs)
  - `postgrex_opts` - Connection input retained for backward compatibility.
    This may be adapter-specific connection options, an Ecto repo, a live
    connection pid/name, or a pooled connection reference.
  - `opts` - Configuration options

  ## Options

  - `:validate` - (boolean, default: true) Whether to validate the domain configuration
  - `:pool` - (boolean, default: false) Whether to enable connection pooling
  - `:pool_options` - Connection pool configuration options
  - `:adapter` - (module, default: `SelectoDBPostgreSQL.Adapter`) Database adapter module
  - `:rollup_sort_fix` - (`true | false | :auto`, default: `:auto`) whether to
    wrap `GROUP BY ROLLUP ... ORDER BY` queries in a compatibility subquery;
    `:auto` disables the wrapper on PostgreSQL 18+

  ## Examples

      # Basic usage (validation enabled by default)
      selecto = Selecto.Configuration.configure(domain, postgrex_opts)

      # With connection pooling
      selecto = Selecto.Configuration.configure(domain, postgrex_opts, pool: true)

      # Disable validation for performance
      selecto = Selecto.Configuration.configure(domain, postgrex_opts, validate: false)
  """
  @spec configure(Selecto.Types.domain(), term(), keyword()) :: Selecto.Types.t()
  def configure(domain, postgrex_opts, opts \\ []) do
    Selecto.OptionsValidator.validate_configure_opts!(opts)

    validate? = Keyword.get(opts, :validate, true)
    use_pool? = Keyword.get(opts, :pool, false)
    adapter = Keyword.get(opts, :adapter, Selecto.AdapterSupport.default_adapter())
    pool_options = opts |> Keyword.get(:pool_options, []) |> Keyword.put_new(:adapter, adapter)

    extension_specs = Selecto.Extensions.from_domain(domain)
    domain = Selecto.Extensions.merge_domain_extensions(domain, extension_specs)

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
            Logger.warning(
              "Failed to start connection pool: #{inspect(reason)}. Falling back to direct connection."
            )

            postgrex_opts
        end
      else
        postgrex_opts
      end

    # Initialize connection based on adapter
    connection =
      if Selecto.AdapterSupport.postgresql_adapter?(adapter) do
        # Backward compatibility: use postgrex_opts directly for PostgreSQL
        final_postgrex_opts
      else
        # For non-PostgreSQL adapters, reuse pooled adapter reference when available.
        case final_postgrex_opts do
          {:pool, %{adapter: ^adapter} = pool_ref} ->
            pool_ref

          _ ->
            # Otherwise let the adapter establish its own connection.
            case adapter.connect(postgrex_opts) do
              {:ok, conn} ->
                conn

              {:error, reason} ->
                raise "Failed to connect with adapter #{inspect(adapter)}: #{inspect(reason)}"
            end
        end
      end

    rollup_sort_fix = resolve_rollup_sort_fix(adapter, connection, opts)

    %Selecto{
      # Keep for backward compatibility
      postgrex_opts: final_postgrex_opts,
      adapter: adapter,
      connection: connection,
      domain: domain,
      config:
        configure_domain(domain, extension_specs) |> Map.put(:rollup_sort_fix, rollup_sort_fix),
      extensions: extension_specs,
      set: %{
        selected: Map.get(domain, :required_selected, []),
        filtered: [],
        required_filters: Map.get(domain, :required_filters, []),
        post_pivot_filters: [],
        order_by: Map.get(domain, :required_order_by, []),
        group_by: Map.get(domain, :required_group_by, [])
      }
    }
  end

  defp resolve_rollup_sort_fix(adapter, connection, opts) do
    case Keyword.get(opts, :rollup_sort_fix, :auto) do
      value when value in [true, false] ->
        value

      _auto_or_invalid ->
        auto_rollup_sort_fix(adapter, connection)
    end
  end

  defp auto_rollup_sort_fix(adapter, connection) do
    case detect_postgres_major_version(adapter, connection) do
      major when is_integer(major) and major >= 18 -> false
      _ -> true
    end
  end

  defp detect_postgres_major_version(adapter, connection) do
    cond do
      not Selecto.AdapterSupport.postgresql_adapter?(adapter) ->
        nil

      function_exported?(adapter, :server_version_major, 1) ->
        case adapter.server_version_major(connection) do
          {:ok, major} when is_integer(major) -> major
          _ -> nil
        end

      true ->
        nil
    end
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
      selecto = Selecto.Configuration.from_ecto(MyApp.Repo, MyApp.User)

      # With joins and options
      selecto = Selecto.Configuration.from_ecto(MyApp.Repo, MyApp.User,
        joins: [:posts, :profile],
        redact_fields: [:password_hash]
      )
  """
  @spec from_ecto(module(), module(), keyword()) :: Selecto.Types.t()
  def from_ecto(repo, schema, opts \\ []) do
    Selecto.EctoAdapter.configure(repo, schema, opts)
  end

  @doc """
  Generate the selecto configuration from a domain map.

  Processes domain configuration to extract fields, joins, and filters.
  This is called internally during configure/3.
  """
  @spec configure_domain(Selecto.Types.domain()) :: Selecto.Types.processed_config()
  def configure_domain(%{source: _source} = domain) do
    configure_domain(domain, Selecto.Extensions.from_domain(domain))
  end

  @spec configure_domain(Selecto.Types.domain(), [{module(), keyword()}]) ::
          Selecto.Types.processed_config()
  def configure_domain(%{source: source} = domain, extension_specs)
      when is_list(extension_specs) do
    primary_key = source.primary_key

    fields =
      Selecto.Schema.Column.configure_columns(
        :selecto_root,
        source.fields -- Map.get(source, :redact_fields, []),
        source,
        domain
      )

    joins = Selecto.Schema.Join.recurse_joins(source, domain)

    # Combine fields from Joins into fields list
    fields =
      List.flatten([fields | Enum.map(Map.values(joins), fn e -> e.fields end)])
      |> Enum.reduce(%{}, fn m, acc -> Map.merge(m, acc) end)

    # Extra filters (all normal fields can be a filter)
    # These are custom filters passed to Selecto Components
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
      domain_data: Map.get(domain, :domain_data),
      extensions: extension_specs
    }
  end
end
