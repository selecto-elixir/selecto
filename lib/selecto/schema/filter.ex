defmodule Selecto.Schema.Filter do
  @moduledoc """
  Handles filter configuration and processing for Selecto domains.
  
  This module processes filter configurations and integrates option providers
  for select-based filtering.
  """

  alias Selecto.OptionProvider

  def configure_filters(filters, _dep) do
    # Process filters and add option provider support
    filters
    |> Enum.map(&process_filter/1)
  end

  @doc """
  Process filter form data to create appropriate filter clauses for Selecto.
  
  This function handles different filter types including the new :select_options type.
  """
  def process_filter_form_data(selecto, filter_data) do
    filter_field = filter_data["filter"]
    column_def = Selecto.columns(selecto)[filter_field]
    
    case Map.get(column_def, :type) do
      :select_options ->
        process_select_options_filter(filter_data, column_def)
        
      other_type ->
        # Delegate to existing filter processing for other types
        process_standard_filter(filter_data, other_type)
    end
  end

  # Private functions

  defp process_filter(filter) when is_map(filter) do
    # Add option provider validation if present
    case Map.get(filter, :option_provider) do
      nil -> filter
      provider -> 
        case OptionProvider.validate_provider(provider) do
          :ok -> Map.put(filter, :validated_provider, true)
          {:error, reason} -> 
            # Log warning but don't fail - could be runtime issue
            require Logger
            Logger.warning("Invalid option provider in filter: #{inspect(reason)}")
            filter
        end
    end
  end
  defp process_filter(filter), do: filter

  defp process_select_options_filter(filter_data, column_def) do
    field = filter_data["filter"]
    selected_values = List.wrap(filter_data["value"] || [])
    
    # Handle empty selection
    if Enum.empty?(selected_values) do
      nil
    else
      # Validate selected values against option provider if available
      case Map.get(column_def, :option_provider) do
        nil ->
          # No validation available, proceed with selection
          create_selection_filter(field, selected_values)
          
        _provider ->
          # TODO: Add runtime validation against option provider
          # For now, proceed with selection
          create_selection_filter(field, selected_values)
      end
    end
  end

  defp create_selection_filter(field, [single_value]) do
    {field, single_value}
  end
  defp create_selection_filter(field, multiple_values) do
    {field, {:in, multiple_values}}
  end

  defp process_standard_filter(filter_data, _type) do
    # This would delegate to the existing filter processing logic
    # For now, return basic filter structure
    field = filter_data["filter"]
    value = filter_data["value"]
    {field, value}
  end

  @doc """
  Load options for a select filter field.
  
  This function is used by UI components to populate select dropdowns.
  """
  def load_filter_options(selecto, field_name, opts \\ []) do
    case Selecto.columns(selecto)[field_name] do
      %{type: :select_options, option_provider: provider} ->
        OptionProvider.load_options(provider, selecto, opts)
        
      _other ->
        {:error, :not_select_options_field}
    end
  end

  @doc """
  Get filter configuration for a field including option provider info.
  """
  def get_filter_config(selecto, field_name) do
    case Selecto.columns(selecto)[field_name] do
      %{type: :select_options} = column_def ->
        base_config = %{
          type: :select_options,
          multiple: Map.get(column_def, :multiple, false),
          searchable: Map.get(column_def, :searchable, true)
        }
        
        case Map.get(column_def, :option_provider) do
          nil -> base_config
          provider -> Map.put(base_config, :option_provider, provider)
        end
        
      %{type: other_type} ->
        %{type: other_type}
        
      nil ->
        {:error, :field_not_found}
    end
  end
end
