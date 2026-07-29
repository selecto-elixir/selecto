defmodule Selecto.Tenant do
  @moduledoc """
  Multi-tenant helpers for Selecto query state.

  This module provides a lightweight tenant context contract for read-path
  queries. Tenant scope is enforced by appending tenant constraints to the
  query's required filter bucket (`set.required_filters`).
  """

  @default_tenant_field "tenant_id"
  @default_namespace "tenant"

  @type tenant_context :: %{
          optional(:tenant_id) => term(),
          optional(:tenant_mode) => atom() | String.t(),
          optional(:tenant_field) => atom() | String.t(),
          optional(:prefix) => String.t(),
          optional(:namespace) => String.t(),
          optional(:required) => boolean(),
          optional(:required_filters) => [Selecto.Types.filter()]
        }

  @doc """
  Attach tenant context to a Selecto query.
  """
  @spec with_tenant(Selecto.Types.t(), tenant_context() | keyword() | String.t() | atom() | nil) ::
          Selecto.Types.t()
  def with_tenant(selecto, tenant_context) do
    %{selecto | tenant: normalize_context(tenant_context)}
  end

  @doc """
  Read the tenant context from a Selecto query.
  """
  @spec tenant(Selecto.Types.t()) :: tenant_context() | nil
  def tenant(%{tenant: tenant_context}), do: tenant_context
  def tenant(_), do: nil

  @doc """
  Apply tenant scope to a query's required filter bucket.

  Options:

  - `:tenant` - explicit tenant context override
  - `:tenant_id` - explicit tenant id override
  - `:tenant_field` - explicit tenant field override
  - `:required_filters` - additional required filters
  """
  @spec apply_tenant_scope(Selecto.Types.t(), keyword()) :: Selecto.Types.t()
  def apply_tenant_scope(selecto, opts \\ []) do
    tenant_context =
      opts
      |> Keyword.get(:tenant, tenant(selecto))
      |> normalize_context()

    case tenant_context do
      nil ->
        selecto

      normalized_context ->
        selecto = %{selecto | tenant: normalized_context}

        additional_required =
          List.wrap(Map.get(normalized_context, :required_filters, [])) ++
            List.wrap(Keyword.get(opts, :required_filters, []))

        selecto =
          Enum.reduce(additional_required, selecto, fn filter, acc ->
            require_tenant_filter(acc, filter)
          end)

        tenant_field =
          Keyword.get(
            opts,
            :tenant_field,
            Map.get(normalized_context, :tenant_field, @default_tenant_field)
          )

        tenant_id = Keyword.get(opts, :tenant_id, Map.get(normalized_context, :tenant_id))

        if is_nil(tenant_id) do
          selecto
        else
          require_tenant_filter(selecto, tenant_field, tenant_id)
        end
    end
  end

  @doc """
  Append a required filter to the query set.
  """
  @spec require_tenant_filter(Selecto.Types.t(), Selecto.Types.filter()) :: Selecto.Types.t()
  def require_tenant_filter(selecto, filter) do
    set = Map.get(selecto, :set, %{})
    current = Map.get(set, :required_filters, [])
    updated = uniq_filters(current ++ [filter])
    %{selecto | set: Map.put(set, :required_filters, updated)}
  end

  @doc """
  Append a required tenant filter from field + value.
  """
  @spec require_tenant_filter(Selecto.Types.t(), atom() | String.t(), term()) :: Selecto.Types.t()
  def require_tenant_filter(selecto, tenant_field, tenant_id) do
    require_tenant_filter(selecto, {normalize_field(tenant_field), tenant_id})
  end

  @doc """
  Return whether tenant scope is required for this query.

  Precedence:

  1. `opts[:require_tenant]`
  2. tenant context `:required` / `:require_tenant`
  3. domain `:tenant_required` / `:require_tenant`
  4. inferred true for `tenant_mode` in shared-column/shared-rls/schema modes
  """
  @spec tenant_required?(Selecto.Types.t(), keyword()) :: boolean()
  def tenant_required?(selecto, opts \\ []) do
    context = tenant(selecto) || %{}
    domain = Map.get(selecto, :domain, %{})

    cond do
      is_boolean(Keyword.get(opts, :require_tenant)) ->
        Keyword.get(opts, :require_tenant)

      is_boolean(map_get(context, :required)) ->
        map_get(context, :required)

      is_boolean(map_get(context, :require_tenant)) ->
        map_get(context, :require_tenant)

      is_boolean(map_get(domain, :tenant_required)) ->
        map_get(domain, :tenant_required)

      is_boolean(map_get(domain, :require_tenant)) ->
        map_get(domain, :require_tenant)

      true ->
        tenant_mode_requires_scope?(map_get(context, :tenant_mode))
    end
  end

  @doc """
  Validate tenant scope requirements for read and derivation paths.

  Returns `:ok` when tenant scope is optional or present. Returns structured
  validation error when tenant scope is required but missing.
  """
  @spec validate_scope(Selecto.Types.t(), keyword()) :: :ok | {:error, Selecto.Error.t()}
  def validate_scope(selecto, opts \\ []) do
    if tenant_required?(selecto, opts) and not has_tenant_scope?(selecto, opts) do
      tenant_context = tenant(selecto) || %{}

      {:error,
       Selecto.Error.validation_error("Tenant scope is required but missing", %{
         tenant_mode: map_get(tenant_context, :tenant_mode),
         tenant_field: tenant_field(selecto, opts),
         tenant_id: map_get(tenant_context, :tenant_id),
         prefix: map_get(tenant_context, :prefix)
       })}
    else
      :ok
    end
  end

  @doc """
  Raise when tenant scope is required and missing.
  """
  @spec ensure_scope!(Selecto.Types.t(), keyword()) :: :ok
  def ensure_scope!(selecto, opts \\ []) do
    case validate_scope(selecto, opts) do
      :ok -> :ok
      {:error, error} -> raise Selecto.Error.to_exception(error)
    end
  end

  @doc """
  Merge execution options with tenant-derived defaults.

  If no `:prefix` option is provided explicitly and tenant context includes a
  `:prefix`, the prefix is injected into execution opts.
  """
  @spec merge_execution_opts(Selecto.Types.t(), keyword()) :: keyword()
  def merge_execution_opts(selecto, opts \\ []) do
    if Keyword.has_key?(opts, :prefix) do
      opts
    else
      case tenant(selecto) do
        %{prefix: prefix} when is_binary(prefix) and byte_size(prefix) > 0 ->
          Keyword.put(opts, :prefix, prefix)

        _ ->
          opts
      end
    end
  end

  @doc """
  Normalize tenant context input into a map with atom keys.
  """
  @spec normalize_context(tenant_context() | keyword() | String.t() | atom() | nil) ::
          tenant_context() | nil
  def normalize_context(nil), do: nil

  def normalize_context(tenant_id) when is_binary(tenant_id) or is_atom(tenant_id) do
    %{
      tenant_id: to_string(tenant_id),
      tenant_field: @default_tenant_field,
      namespace: @default_namespace,
      required_filters: []
    }
  end

  def normalize_context(tenant_context) when is_list(tenant_context) do
    tenant_context
    |> Enum.into(%{})
    |> normalize_context()
  end

  def normalize_context(tenant_context) when is_map(tenant_context) do
    tenant_id = map_get(tenant_context, :tenant_id) || map_get(tenant_context, :id)

    tenant_field =
      tenant_context
      |> map_get(:tenant_field)
      |> normalize_field()

    %{
      tenant_id: tenant_id,
      tenant_mode: map_get(tenant_context, :tenant_mode),
      tenant_field: tenant_field,
      prefix: map_get(tenant_context, :prefix),
      namespace: map_get(tenant_context, :namespace) || @default_namespace,
      required:
        case map_get(tenant_context, :required) do
          nil -> map_get(tenant_context, :require_tenant)
          value -> value
        end,
      required_filters: List.wrap(map_get(tenant_context, :required_filters) || [])
    }
  end

  def normalize_context(_), do: nil

  defp map_get(map, key) when is_map(map) and is_atom(key) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key))
  end

  defp normalize_field(nil), do: @default_tenant_field
  defp normalize_field(field) when is_atom(field), do: Atom.to_string(field)
  defp normalize_field(field) when is_binary(field), do: field
  defp normalize_field(field), do: to_string(field)

  defp tenant_mode_requires_scope?(mode) do
    normalized = mode |> to_string() |> String.downcase()
    normalized in ["shared_column", "shared_rls", "schema"]
  end

  defp has_tenant_scope?(selecto, opts) do
    tenant_context = tenant(selecto) || %{}
    field = tenant_field(selecto, opts)
    required = required_filter_scope?(selecto, field)
    has_prefix = map_get(tenant_context, :prefix) |> present_string?()
    # A context id is intent, not proof that row scope reached the query.
    # `apply_tenant_scope/2` turns that id into a required filter. Accepting the
    # id alone lets validation pass while generated SQL remains unscoped.
    required or has_prefix
  end

  defp tenant_field(selecto, opts) do
    tenant_context = tenant(selecto) || %{}

    opts
    |> Keyword.get(:tenant_field, map_get(tenant_context, :tenant_field) || @default_tenant_field)
    |> normalize_field()
  end

  defp required_filter_scope?(selecto, tenant_field) do
    set_required =
      selecto
      |> Map.get(:set, %{})
      |> Map.get(:required_filters, [])

    domain_required =
      selecto
      |> Map.get(:domain, %{})
      |> Map.get(:required_filters, [])

    (set_required ++ domain_required)
    |> Enum.any?(fn
      {field, value} -> normalize_field(field) == tenant_field and not is_nil(value)
      _ -> false
    end)
  end

  defp present_string?(value) when is_binary(value), do: byte_size(value) > 0
  defp present_string?(_), do: false

  defp uniq_filters(filters) do
    Enum.reduce(filters, [], fn filter, acc ->
      if filter in acc do
        acc
      else
        acc ++ [filter]
      end
    end)
  end
end
