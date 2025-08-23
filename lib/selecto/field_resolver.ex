defmodule Selecto.FieldResolver do
  @moduledoc """
  Enhanced field resolution system for Selecto with disambiguation, aliasing, and dynamic resolution.
  
  This module provides advanced field resolution capabilities including:
  - Field disambiguation when multiple tables have same field names
  - Dynamic field resolution at runtime
  - Field aliasing for better naming and conflict resolution
  - Smart error messages with suggestions
  - Support for qualified field references
  - Join-aware field validation
  
  ## Field Reference Formats
  
  ### Basic Field References
  - `"field_name"` - Field from source table
  - `"join.field_name"` - Qualified field from specific join
  - `"table_alias.field_name"` - Field using table alias
  
  ### Advanced Field References  
  - `{:field, "field_name", alias: "custom_alias"}` - Field with custom alias
  - `{:qualified_field, "join.field_name"}` - Explicitly qualified field
  - `{:disambiguated_field, "field_name", from: "join"}` - Disambiguated field reference
  
  ## Usage Examples
  
      # Basic resolution
      {:ok, field_info} = FieldResolver.resolve_field(selecto, "user_name")
      
      # Qualified resolution
      {:ok, field_info} = FieldResolver.resolve_field(selecto, "users.name")
      
      # With disambiguation
      {:ok, field_info} = FieldResolver.resolve_field(selecto, {:disambiguated_field, "name", from: "users"})
      
      # Get all available fields
      all_fields = FieldResolver.get_available_fields(selecto)
      
      # Find field suggestions
      suggestions = FieldResolver.suggest_fields(selecto, "nam")
  """
  
  alias Selecto.Error
  require Logger
  
  @type field_reference :: String.t() | atom() | tuple()
  @type field_info :: %{
    name: String.t(),
    qualified_name: String.t(),
    source_join: atom(),
    type: atom(),
    alias: String.t() | nil,
    table_alias: String.t() | nil
  }
  @type resolution_result :: {:ok, field_info()} | {:error, term()}
  
  @doc """
  Resolve a field reference to detailed field information.
  
  Handles various field reference formats and provides disambiguation when needed.
  """
  @spec resolve_field(Selecto.t(), field_reference()) :: resolution_result()
  def resolve_field(selecto, field_ref) do
    case parse_field_reference(field_ref) do
      {:ok, parsed_ref} ->
        do_resolve_field(selecto, parsed_ref)
      {:error, reason} ->
        {:error, Error.field_resolution_error("Invalid field reference format", field_ref, %{reason: reason})}
    end
  end
  
  @doc """
  Get all available fields from all joins and the source table.
  
  Returns a map with field names as keys and field info as values.
  """
  @spec get_available_fields(Selecto.t()) :: %{String.t() => field_info()}
  def get_available_fields(selecto) do
    source_fields = get_source_fields(selecto)
    join_fields = get_join_fields(selecto)
    
    Map.merge(source_fields, join_fields)
  end
  
  @doc """
  Find field suggestions for a partial field name.
  
  Useful for autocomplete and error messages with suggestions.
  """
  @spec suggest_fields(Selecto.t(), String.t()) :: [String.t()]
  def suggest_fields(selecto, partial_name) do
    available_fields = get_available_fields(selecto)
    
    available_fields
    |> Map.keys()
    |> Enum.filter(&String.contains?(&1, partial_name))
    |> Enum.sort_by(&String.jaro_distance(&1, partial_name), :desc)
    |> Enum.take(5)
  end
  
  @doc """
  Check if a field reference is ambiguous (exists in multiple tables).
  """
  @spec is_ambiguous_field?(Selecto.t(), String.t()) :: boolean()
  def is_ambiguous_field?(selecto, field_name) do
    available_fields = get_available_fields(selecto)
    
    qualified_fields = available_fields
    |> Enum.filter(fn {qualified_name, _info} ->
      String.ends_with?(qualified_name, ".#{field_name}") or qualified_name == field_name
    end)
    
    length(qualified_fields) > 1
  end
  
  @doc """
  Get disambiguation options for an ambiguous field.
  """
  @spec get_disambiguation_options(Selecto.t(), String.t()) :: [field_info()]
  def get_disambiguation_options(selecto, field_name) do
    available_fields = get_available_fields(selecto)
    
    available_fields
    |> Enum.filter(fn {qualified_name, _info} ->
      String.ends_with?(qualified_name, ".#{field_name}") or qualified_name == field_name
    end)
    |> Enum.map(fn {_qualified_name, info} -> info end)
  end
  
  @doc """
  Validate that all field references in a list are resolvable.
  """
  @spec validate_field_references(Selecto.t(), [field_reference()]) :: :ok | {:error, [term()]}
  def validate_field_references(selecto, field_refs) do
    errors = Enum.reduce(field_refs, [], fn field_ref, acc ->
      case resolve_field(selecto, field_ref) do
        {:ok, _field_info} -> acc
        {:error, error} -> [error | acc]
      end
    end)
    
    case errors do
      [] -> :ok
      errors -> {:error, Enum.reverse(errors)}
    end
  end
  
  # Private Implementation
  
  defp parse_field_reference(field_ref) when is_binary(field_ref) do
    case String.split(field_ref, ".", parts: 2) do
      [field_name] -> 
        {:ok, %{type: :simple, field: field_name}}
      [join_name, field_name] -> 
        {:ok, %{type: :qualified, join: join_name, field: field_name}}
    end
  end
  
  defp parse_field_reference(field_ref) when is_atom(field_ref) do
    parse_field_reference(Atom.to_string(field_ref))
  end
  
  defp parse_field_reference({:field, field_name, opts}) when is_list(opts) do
    alias_name = Keyword.get(opts, :alias)
    {:ok, %{type: :aliased, field: field_name, alias: alias_name}}
  end
  
  defp parse_field_reference({:qualified_field, qualified_name}) do
    parse_field_reference(qualified_name)
    |> case do
      {:ok, parsed} -> {:ok, Map.put(parsed, :type, :explicitly_qualified)}
      error -> error
    end
  end
  
  defp parse_field_reference({:disambiguated_field, field_name, opts}) when is_list(opts) do
    from_join = Keyword.get(opts, :from)
    {:ok, %{type: :disambiguated, field: field_name, from_join: from_join}}
  end
  
  defp parse_field_reference(field_ref) do
    {:error, "Unsupported field reference format: #{inspect(field_ref)}"}
  end
  
  defp do_resolve_field(selecto, %{type: :simple, field: field_name}) do
    available_fields = get_available_fields(selecto)
    
    # Try direct field name first
    case Map.get(available_fields, field_name) do
      nil ->
        # Check if it's an ambiguous field
        if is_ambiguous_field?(selecto, field_name) do
          options = get_disambiguation_options(selecto, field_name)
          qualified_names = Enum.map(options, & &1.qualified_name)
          {:error, Error.field_resolution_error(
            "Ambiguous field reference '#{field_name}'. Please qualify with table name.", 
            field_name, 
            %{available_options: qualified_names}
          )}
        else
          suggestions = suggest_fields(selecto, field_name)
          {:error, Error.field_resolution_error(
            "Field '#{field_name}' not found", 
            field_name, 
            %{suggestions: suggestions, available_fields: Map.keys(available_fields)}
          )}
        end
      field_info ->
        {:ok, field_info}
    end
  end
  
  defp do_resolve_field(selecto, %{type: :qualified, join: join_name, field: field_name}) do
    available_fields = get_available_fields(selecto)
    qualified_name = "#{join_name}.#{field_name}"
    
    case Map.get(available_fields, qualified_name) do
      nil ->
        # Check if the join exists
        if Map.has_key?(selecto.config.joins, String.to_atom(join_name)) do
          join_atom = String.to_atom(join_name)
          join_info = selecto.config.joins[join_atom]
          available_join_fields = Map.keys(join_info.fields || %{})
          {:error, Error.field_resolution_error(
            "Field '#{field_name}' not found in join '#{join_name}'", 
            qualified_name, 
            %{available_fields_in_join: available_join_fields}
          )}
        else
          available_joins = Map.keys(selecto.config.joins)
          {:error, Error.field_resolution_error(
            "Join '#{join_name}' not found", 
            qualified_name, 
            %{available_joins: available_joins}
          )}
        end
      field_info ->
        {:ok, field_info}
    end
  end
  
  defp do_resolve_field(selecto, %{type: :aliased, field: field_name, alias: alias_name}) do
    case do_resolve_field(selecto, %{type: :simple, field: field_name}) do
      {:ok, field_info} ->
        {:ok, Map.put(field_info, :alias, alias_name)}
      error ->
        error
    end
  end
  
  defp do_resolve_field(selecto, %{type: :disambiguated, field: field_name, from_join: from_join}) do
    _qualified_name = "#{from_join}.#{field_name}"
    do_resolve_field(selecto, %{type: :qualified, join: from_join, field: field_name})
  end
  
  defp do_resolve_field(selecto, %{type: :explicitly_qualified} = parsed_ref) do
    # Remove the :explicitly_qualified type and treat as regular qualified
    parsed_ref = Map.put(parsed_ref, :type, :qualified)
    do_resolve_field(selecto, parsed_ref)
  end
  
  defp get_source_fields(selecto) do
    source = selecto.config.source
    
    source.fields
    |> Enum.filter(fn field -> field not in source.redact_fields end)
    |> Enum.into(%{}, fn field ->
      field_str = Atom.to_string(field)
      field_info = %{
        name: field_str,
        qualified_name: field_str,
        source_join: :selecto_root,
        type: get_field_type(source.columns, field),
        alias: nil,
        table_alias: "selecto_root"
      }
      {field_str, field_info}
    end)
  end
  
  defp get_join_fields(selecto) do
    selecto.config.joins
    |> Enum.flat_map(fn {join_name, join_config} ->
      join_fields = join_config.fields || %{}
      
      Enum.map(join_fields, fn {field_key, field_config} ->
        field_name = extract_field_name(field_key)
        qualified_name = "#{join_name}.#{field_name}"
        
        field_info = %{
          name: field_name,
          qualified_name: qualified_name,
          source_join: join_name,
          type: field_config[:type] || :string,
          alias: field_config[:alias],
          table_alias: Atom.to_string(join_name)
        }
        
        {qualified_name, field_info}
      end)
    end)
    |> Enum.into(%{})
  end
  
  defp extract_field_name(field_key) when is_binary(field_key) do
    # Handle formats like "join[field]" -> "field"
    case Regex.run(~r/\[([^\]]+)\]$/, field_key) do
      [_, field_name] -> field_name
      nil -> field_key
    end
  end
  
  defp extract_field_name(field_key) when is_atom(field_key) do
    Atom.to_string(field_key)
  end
  
  defp extract_field_name(field_key) do
    to_string(field_key)
  end
  
  defp get_field_type(columns, field) do
    case Map.get(columns, field) do
      %{type: type} -> type
      _ -> :string
    end
  end
end