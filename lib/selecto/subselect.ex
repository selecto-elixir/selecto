defmodule Selecto.Subselect do
  @moduledoc """
  Subselect functionality for array-based data aggregation from related tables.

  The Subselect feature enables returning related data as arrays or JSON objects,
  preventing result set denormalization while maintaining relational context.

  ## Examples

      # Basic subselect - get orders as JSON array for each attendee
      selecto
      |> Selecto.select(["attendee[name]"])
      |> Selecto.subselect([
           "order[product_name]", 
           "order[quantity]"
         ])
      |> Selecto.filter([{"event_id", 123}])

      # This generates SQL like:
      # SELECT 
      #   a.name,
      #   (SELECT json_agg(json_build_object(
      #     'product_name', o.product_name,
      #     'quantity', o.quantity
      #   )) FROM orders o WHERE o.attendee_id = a.attendee_id) as orders
      # FROM attendees a
      # WHERE a.event_id = 123

  ## Aggregation Formats

  - `:json_agg` - Returns JSON array of objects (default)
  - `:array_agg` - Returns PostgreSQL array
  - `:string_agg` - Returns delimited string
  - `:count` - Returns count of related records
  """

  alias Selecto.Types

  @doc """
  Add subselect fields to return related data as aggregated arrays.

  ## Parameters

  - `selecto` - The Selecto struct
  - `field_specs` - List of field specifications with optional configuration
  - `opts` - Global options for subselects

  ## Field Specification Formats

      # Simple field list (uses defaults)
      ["order[product_name]", "order[quantity]"]

      # With custom configuration
      [
        %{
          fields: ["product_name", "quantity"],
          target_schema: :order,
          format: :json_agg,
          alias: "order_items"
        }
      ]

  ## Options

  - `:format` - Default aggregation format (`:json_agg`, `:array_agg`, `:string_agg`, `:count`)
  - `:alias_prefix` - Prefix for generated field aliases
  - `:order_by` - Default ordering for aggregated results

  ## Returns

  Updated Selecto struct with subselect configuration applied.
  """
  @spec subselect(Types.t(), [String.t() | Types.subselect_selector()], keyword()) :: Types.t()
  def subselect(selecto, field_specs, opts \\ []) do
    subselect_configs = normalize_field_specs(field_specs, opts)
    
    # Validate all subselect configurations
    Enum.each(subselect_configs, &validate_subselect_config(selecto, &1))
    
    # Add to selecto state
    current_subselects = Map.get(selecto.set, :subselected, [])
    updated_subselects = current_subselects ++ subselect_configs
    
    put_in(selecto.set[:subselected], updated_subselects)
  end

  @doc """
  Validate that a subselect configuration is valid for the given domain.
  """
  @spec validate_subselect_config(Types.t(), Types.subselect_selector()) :: :ok | {:error, String.t()}
  def validate_subselect_config(selecto, subselect_config) do
    with :ok <- validate_target_schema(selecto, subselect_config.target_schema),
         :ok <- validate_fields_exist(selecto, subselect_config),
         :ok <- validate_relationship_path(selecto, subselect_config) do
      :ok
    else
      {:error, reason} -> raise ArgumentError, "Invalid subselect configuration: #{reason}"
    end
  end

  @doc """
  Group subselects by their target table for efficient SQL generation.
  """
  @spec group_subselects_by_table(Types.t()) :: %{atom() => [Types.subselect_selector()]}
  def group_subselects_by_table(selecto) do
    subselects = Map.get(selecto.set, :subselected, [])
    
    Enum.group_by(subselects, fn config ->
      config.target_schema
    end)
  end

  @doc """
  Check if a Selecto query has subselect configuration applied.
  """
  @spec has_subselects?(Types.t()) :: boolean()
  def has_subselects?(selecto) do
    subselects = Map.get(selecto.set, :subselected, [])
    length(subselects) > 0
  end

  @doc """
  Get all subselect configurations from a Selecto query.
  """
  @spec get_subselect_configs(Types.t()) :: [Types.subselect_selector()]
  def get_subselect_configs(selecto) do
    Map.get(selecto.set, :subselected, [])
  end

  @doc """
  Clear all subselect configurations from a Selecto query.
  """
  @spec clear_subselects(Types.t()) :: Types.t()
  def clear_subselects(selecto) do
    updated_set = Map.delete(selecto.set, :subselected)
    %{selecto | set: updated_set}
  end

  @doc """
  Resolve the join path needed to reach a target schema from the source.
  """
  @spec resolve_join_path(Types.t(), atom()) :: {:ok, [atom()]} | {:error, String.t()}
  def resolve_join_path(selecto, target_schema) do
    # Reuse the path-finding logic from Pivot module
    Selecto.Pivot.calculate_join_path(selecto, target_schema)
  end

  # Private helper functions

  defp normalize_field_specs(field_specs, opts) do
    default_format = Keyword.get(opts, :format, :json_agg)
    alias_prefix = Keyword.get(opts, :alias_prefix, "")
    default_order_by = Keyword.get(opts, :order_by, [])
    
    Enum.map(field_specs, fn spec ->
      case spec do
        field when is_binary(field) ->
          parse_field_string(field, default_format, alias_prefix, default_order_by)
          
        %{} = config ->
          normalize_config_map(config, default_format, alias_prefix, default_order_by)
          
        _ ->
          raise ArgumentError, "Invalid field specification: #{inspect(spec)}"
      end
    end)
  end

  defp parse_field_string(field_string, default_format, alias_prefix, default_order_by) do
    # Parse "table[field]" format (legacy) or "table.field" format (dot notation)
    cond do
      # Try bracket notation first for backward compatibility
      match = Regex.run(~r/^([^[]+)\[([^]]+)\]$/, field_string) ->
        [_, table_part, field_part] = match
        target_schema = String.to_atom(table_part)
        fields = String.split(field_part, ",") |> Enum.map(&String.trim/1)
        
        %{
          fields: fields,
          target_schema: target_schema,
          format: default_format,
          alias: generate_alias(target_schema, alias_prefix),
          order_by: default_order_by,
          filters: []
        }
      
      # Try dot notation
      match = Regex.run(~r/^([^.]+)\.([^.]+)$/, field_string) ->
        [_, table_part, field_part] = match
        target_schema = String.to_atom(table_part)
        fields = [field_part]
        
        %{
          fields: fields,
          target_schema: target_schema,
          format: default_format,
          alias: generate_alias(target_schema, alias_prefix),
          order_by: default_order_by,
          filters: []
        }
      
      true ->
        raise ArgumentError, "Invalid field format: #{field_string}. Expected 'table[field]' or 'table.field' format."
    end
  end

  defp normalize_config_map(config, default_format, alias_prefix, default_order_by) do
    %{
      fields: Map.fetch!(config, :fields),
      target_schema: Map.fetch!(config, :target_schema),
      format: Map.get(config, :format, default_format),
      alias: Map.get(config, :alias, generate_alias(config.target_schema, alias_prefix)),
      separator: Map.get(config, :separator, ","),
      order_by: Map.get(config, :order_by, default_order_by),
      filters: Map.get(config, :filters, [])
    }
  end

  defp generate_alias(target_schema, prefix) do
    base_name = Atom.to_string(target_schema)
    if prefix != "", do: "#{prefix}_#{base_name}", else: base_name
  end

  defp validate_target_schema(selecto, target_schema) do
    case Map.get(selecto.domain.schemas, target_schema) do
      nil -> {:error, "Target schema #{target_schema} not found in domain"}
      _ -> :ok
    end
  end

  defp validate_fields_exist(selecto, subselect_config) do
    target_schema_config = Map.get(selecto.domain.schemas, subselect_config.target_schema)
    
    invalid_fields = Enum.filter(subselect_config.fields, fn field_name ->
      field_atom = if is_binary(field_name), do: String.to_atom(field_name), else: field_name
      not Enum.member?(target_schema_config.fields, field_atom)
    end)
    
    case invalid_fields do
      [] -> :ok
      fields -> {:error, "Fields #{inspect(fields)} not found in schema #{subselect_config.target_schema}"}
    end
  end

  defp validate_relationship_path(selecto, subselect_config) do
    case resolve_join_path(selecto, subselect_config.target_schema) do
      {:ok, _path} -> :ok
      {:error, reason} -> {:error, "Cannot reach target schema: #{reason}"}
    end
  end
end