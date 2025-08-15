defmodule Selecto.OptionProvider do
  @moduledoc """
  Handles loading and processing of options for select-based filters.
  
  This module provides a unified interface for loading options from various sources:
  - Static lists
  - Other Selecto domains
  - Ecto enum schemas
  - Custom SQL queries
  """

  alias Selecto.Types

  @type option :: {value :: term(), display :: String.t()}
  @type options :: [option()]
  @type load_result :: {:ok, options()} | {:error, term()}

  @doc """
  Load options from an option provider configuration.
  
  ## Parameters
  
  - `provider` - Option provider configuration
  - `selecto` - Current Selecto instance (for domain queries)
  - `opts` - Additional options like search terms, limits
  
  ## Examples
  
      # Static options
      provider = %{type: :static, values: ["active", "inactive"]}
      {:ok, options} = load_options(provider, selecto)
      
      # Domain-based options
      provider = %{
        type: :domain, 
        domain: :categories_domain,
        value_field: :id,
        display_field: :name
      }
      {:ok, options} = load_options(provider, selecto)
  """
  @spec load_options(Types.option_provider(), Selecto.t(), keyword()) :: load_result()
  def load_options(provider, selecto \\ nil, opts \\ [])

  def load_options(%{type: :static, values: values}, _selecto, _opts) do
    options = Enum.map(values, fn value ->
      {value, to_string(value)}
    end)
    {:ok, options}
  end

  def load_options(%{type: :domain} = provider, selecto, opts) when not is_nil(selecto) do
    load_domain_options(provider, selecto, opts)
  end

  def load_options(%{type: :enum} = provider, _selecto, _opts) do
    load_enum_options(provider)
  end

  def load_options(%{type: :query} = provider, selecto, opts) when not is_nil(selecto) do
    load_query_options(provider, selecto, opts)
  end

  def load_options(_provider, _selecto, _opts) do
    {:error, :invalid_provider_configuration}
  end

  @doc """
  Load options from another Selecto domain.
  
  Executes a query against the specified domain to get value/display pairs.
  """
  @spec load_domain_options(Types.domain_option_provider(), Selecto.t(), keyword()) :: load_result()
  defp load_domain_options(provider, selecto, opts) do
    %{
      domain: domain_name,
      value_field: value_field,
      display_field: display_field
    } = provider
    
    # Get domain configuration
    domain_config = get_domain_config(domain_name, selecto)
    
    if domain_config do
      # Create a new Selecto for the domain query
      domain_selecto = Selecto.configure(domain_config, selecto.postgrex_opts)
      
      # Build the query
      selected = [Atom.to_string(value_field), Atom.to_string(display_field)]
      
      domain_selecto = domain_selecto
      |> Selecto.select(selected)
      |> apply_domain_filters(provider)
      |> apply_domain_ordering(provider)
      |> apply_search_filter(opts[:search], display_field)
      |> apply_limit(opts[:limit] || 100)
      
      case Selecto.execute(domain_selecto) do
        {:ok, {rows, _columns, _aliases}} ->
          options = Enum.map(rows, fn [value, display] ->
            {value, to_string(display)}
          end)
          {:ok, options}
          
        {:error, reason} ->
          {:error, {:domain_query_failed, reason}}
      end
    else
      {:error, {:domain_not_found, domain_name}}
    end
  end

  @doc """
  Load options from an Ecto enum field.
  
  Extracts the enum values from the schema module and field definition.
  """
  @spec load_enum_options(Types.enum_option_provider()) :: load_result()
  defp load_enum_options(%{schema: schema_module, field: field}) do
    try do
      case schema_module.__schema__(:type, field) do
        {:parameterized, {Ecto.Enum, %{mappings: mappings}}} ->
          options = Enum.map(mappings, fn {key, value} ->
            {value, to_string(key)}
          end)
          {:ok, options}

        {:parameterized, Ecto.Enum, %{mappings: mappings}} ->
          options = Enum.map(mappings, fn {key, value} ->
            {value, to_string(key)}
          end)
          {:ok, options}
          
        other_type ->
          {:error, {:not_enum_field, field, other_type}}
      end
    rescue
      error ->
        {:error, {:schema_introspection_failed, error}}
    end
  end

  @doc """
  Load options from a custom SQL query.
  
  Executes the provided SQL query and expects results in [value, display] format.
  """
  @spec load_query_options(Types.query_option_provider(), Selecto.t(), keyword()) :: load_result()
  defp load_query_options(%{query: query, params: params}, selecto, opts) do
    # Apply search filter if provided
    final_query = if search = opts[:search] do
      add_search_to_query(query, search)
    else
      query
    end
    
    # Apply limit
    final_query = add_limit_to_query(final_query, opts[:limit] || 100)
    
    case Postgrex.query(selecto.postgrex_opts, final_query, params) do
      {:ok, %{rows: rows}} ->
        options = Enum.map(rows, fn
          [value, display] -> {value, to_string(display)}
          [value] -> {value, to_string(value)}
          row -> {:error, {:invalid_query_result, row}}
        end)
        
        case Enum.find(options, &match?({:error, _}, &1)) do
          nil -> {:ok, options}
          {:error, reason} -> {:error, reason}
        end
        
      {:error, reason} ->
        {:error, {:query_execution_failed, reason}}
    end
  end

  # Helper functions

  defp get_domain_config(domain_name, selecto) do
    # This would need to be implemented based on how domains are registered
    # For now, we'll assume domains are stored somewhere accessible
    case domain_name do
      :actors_domain -> SelectoTest.PagilaDomain.actors_domain()
      _ -> nil
    end
  end

  defp apply_domain_filters(selecto, %{filters: filters}) when is_list(filters) do
    Enum.reduce(filters, selecto, fn filter, acc ->
      Selecto.filter(acc, filter)
    end)
  end
  defp apply_domain_filters(selecto, _), do: selecto

  defp apply_domain_ordering(selecto, %{order_by: order_by}) when is_list(order_by) do
    Selecto.order_by(selecto, order_by)
  end
  defp apply_domain_ordering(selecto, _), do: selecto

  defp apply_search_filter(selecto, nil, _field), do: selecto
  defp apply_search_filter(selecto, search, field) when is_binary(search) do
    Selecto.filter(selecto, {Atom.to_string(field), {:like, "%#{search}%"}})
  end

  defp apply_limit(selecto, limit) when is_integer(limit) and limit > 0 do
    Selecto.limit(selecto, limit)
  end
  defp apply_limit(selecto, _), do: selecto

  defp add_search_to_query(query, search) do
    # This is a simple implementation - in practice you'd want more sophisticated search
    if String.contains?(query, "WHERE") do
      query <> " AND (column_name ILIKE '%#{search}%')"
    else
      query <> " WHERE (column_name ILIKE '%#{search}%')"
    end
  end

  defp add_limit_to_query(query, limit) do
    query <> " LIMIT #{limit}"
  end

  @doc """
  Validate an option provider configuration.
  
  Checks that all required fields are present and have valid types.
  """
  @spec validate_provider(Types.option_provider()) :: :ok | {:error, term()}
  def validate_provider(%{type: :static, values: values}) when is_list(values) do
    :ok
  end

  def validate_provider(%{type: :domain} = provider) do
    required_fields = [:domain, :value_field, :display_field]
    case check_required_fields(provider, required_fields) do
      :ok -> :ok
      error -> error
    end
  end

  def validate_provider(%{type: :enum} = provider) do
    required_fields = [:schema, :field]
    case check_required_fields(provider, required_fields) do
      :ok ->
        if is_atom(provider.schema) and is_atom(provider.field) do
          :ok
        else
          {:error, :invalid_schema_or_field_type}
        end
      error -> error
    end
  end

  def validate_provider(%{type: :query} = provider) do
    required_fields = [:query, :params]
    case check_required_fields(provider, required_fields) do
      :ok ->
        if is_binary(provider.query) and is_list(provider.params) do
          :ok
        else
          {:error, :invalid_query_or_params_type}
        end
      error -> error
    end
  end

  def validate_provider(_provider) do
    {:error, :unknown_provider_type}
  end

  defp check_required_fields(provider, required_fields) do
    missing_fields = Enum.filter(required_fields, fn field ->
      not Map.has_key?(provider, field)
    end)
    
    case missing_fields do
      [] -> :ok
      fields -> {:error, {:missing_required_fields, fields}}
    end
  end
end