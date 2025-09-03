defmodule Selecto.Database.Registry do
  @moduledoc """
  Registry for database adapters.
  
  Manages registration and discovery of database adapters, both built-in and external.
  Adapters can register themselves at compile-time or runtime.
  """

  use GenServer

  @type adapter_info :: %{
    module: module(),
    name: String.t(),
    version: String.t() | nil,
    capabilities: map()
  }

  # Client API

  @doc """
  Starts the adapter registry.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Registers an adapter with the registry.
  """
  @spec register(module(), keyword()) :: :ok | {:error, term()}
  def register(adapter_module, opts \\ []) do
    GenServer.call(__MODULE__, {:register, adapter_module, opts})
  end

  @doc """
  Retrieves information about a registered adapter.
  """
  @spec get_adapter(module() | String.t()) :: {:ok, adapter_info()} | {:error, :not_found}
  def get_adapter(adapter) do
    GenServer.call(__MODULE__, {:get_adapter, adapter})
  end

  @doc """
  Lists all registered adapters.
  """
  @spec list_adapters() :: [adapter_info()]
  def list_adapters do
    GenServer.call(__MODULE__, :list_adapters)
  end

  @doc """
  Discovers and loads adapters from installed packages.
  """
  @spec discover_adapters() :: {:ok, [module()]} | {:error, term()}
  def discover_adapters do
    GenServer.call(__MODULE__, :discover_adapters)
  end

  @doc """
  Checks if an adapter module implements the required behavior.
  """
  @spec valid_adapter?(module()) :: boolean()
  def valid_adapter?(module) do
    Code.ensure_loaded?(module) and
      function_exported?(module, :__info__, 1) and
      Selecto.Database.Adapter in (module.__info__(:attributes)[:behaviour] || [])
  end

  # Server Callbacks

  @impl true
  def init(_opts) do
    state = %{
      adapters: %{},
      discovered: false
    }
    
    # Register built-in PostgreSQL adapter if available
    if Code.ensure_loaded?(Selecto.Adapters.PostgreSQL) do
      register_internal(Selecto.Adapters.PostgreSQL, state)
    end
    
    {:ok, state}
  end

  @impl true
  def handle_call({:register, adapter_module, opts}, _from, state) do
    case validate_and_register(adapter_module, opts, state) do
      {:ok, new_state} ->
        {:reply, :ok, new_state}
      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:get_adapter, adapter}, _from, state) do
    result = case adapter do
      module when is_atom(module) ->
        Map.fetch(state.adapters, module)
      name when is_binary(name) ->
        Enum.find_value(state.adapters, {:error, :not_found}, fn {_mod, info} ->
          if info.name == name, do: {:ok, info}, else: nil
        end)
    end
    
    {:reply, result, state}
  end

  @impl true
  def handle_call(:list_adapters, _from, state) do
    adapters = Map.values(state.adapters)
    {:reply, adapters, state}
  end

  @impl true
  def handle_call(:discover_adapters, _from, state) do
    if state.discovered do
      {:reply, {:ok, Map.keys(state.adapters)}, state}
    else
      case discover_and_load() do
        {:ok, modules} ->
          new_state = Enum.reduce(modules, state, fn module, acc_state ->
            case validate_and_register(module, [], acc_state) do
              {:ok, updated_state} -> updated_state
              _ -> acc_state
            end
          end)
          
          {:reply, {:ok, modules}, %{new_state | discovered: true}}
        
        error ->
          {:reply, error, state}
      end
    end
  end

  # Private Functions

  defp validate_and_register(adapter_module, opts, state) do
    with :ok <- ensure_loaded(adapter_module),
         true <- valid_adapter?(adapter_module) do
      
      info = %{
        module: adapter_module,
        name: Keyword.get(opts, :name, module_name(adapter_module)),
        version: get_adapter_version(adapter_module),
        capabilities: get_adapter_capabilities(adapter_module)
      }
      
      new_state = %{state | adapters: Map.put(state.adapters, adapter_module, info)}
      {:ok, new_state}
    else
      false ->
        {:error, {:invalid_adapter, adapter_module}}
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp register_internal(adapter_module, state) do
    case validate_and_register(adapter_module, [name: "PostgreSQL"], state) do
      {:ok, new_state} -> new_state
      _ -> state
    end
  end

  defp ensure_loaded(module) do
    case Code.ensure_loaded(module) do
      {:module, _} -> :ok
      {:error, reason} -> {:error, {:cannot_load, module, reason}}
    end
  end

  defp module_name(module) do
    module
    |> Module.split()
    |> List.last()
  end

  defp get_adapter_version(module) do
    if function_exported?(module, :version, 0) do
      module.version()
    else
      nil
    end
  end

  defp get_adapter_capabilities(module) do
    if function_exported?(module, :capabilities, 0) do
      module.capabilities()
    else
      %{}
    end
  end

  defp discover_and_load do
    try do
      # Look for modules that might be database adapters
      # Pattern: Selecto.DB.* or Selecto.Adapters.*
      applications = :application.loaded_applications()
      
      modules = Enum.flat_map(applications, fn {app, _, _} ->
        case :application.get_key(app, :modules) do
          {:ok, app_modules} ->
            Enum.filter(app_modules, &potential_adapter?/1)
          _ ->
            []
        end
      end)
      
      {:ok, modules}
    rescue
      error ->
        {:error, error}
    end
  end

  defp potential_adapter?(module) do
    module_str = to_string(module)
    
    (String.starts_with?(module_str, "Elixir.Selecto.DB.") or
     String.starts_with?(module_str, "Elixir.Selecto.Adapters.")) and
     Code.ensure_loaded?(module) and
     valid_adapter?(module)
  end
end