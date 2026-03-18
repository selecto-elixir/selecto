defmodule Selecto.Subselect do
  @moduledoc """
  Subselect functionality for array-based data aggregation from related tables.

  The Subselect feature enables returning related data as arrays or JSON objects,
  preventing result set denormalization while maintaining relational context.

  ## Examples

      # Basic subselect - get orders as JSON array for each attendee
      selecto
      |> Selecto.select(["attendee.name"])
      |> Selecto.subselect([
           "order.product_name", 
           "order.quantity"
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
      ["order.product_name", "order.quantity"]

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
  @spec validate_subselect_config(Types.t(), Types.subselect_selector()) ::
          :ok | {:error, String.t()}
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
  Resolve the join path needed to reach a target schema from the current context.

  If we're in a retargeted context, the path is calculated from the retarget target.
  Otherwise, the path is calculated from the source.
  """
  @spec resolve_join_path(Types.t(), atom()) :: {:ok, [atom()]} | {:error, String.t()}
  def resolve_join_path(selecto, target_schema) do
    if Selecto.Retarget.has_retarget?(selecto) do
      # In retarget context - calculate path from retarget target to target schema
      retarget_config = Selecto.Retarget.get_retarget_config(selecto)
      retarget_target = retarget_config.target_schema

      # Use the path-finding logic starting from the retarget target
      case find_join_path_from_schema(selecto.domain, retarget_target, target_schema, []) do
        {:ok, path} -> {:ok, path}
        :not_found -> {:error, "No join path found from #{retarget_target} to #{target_schema}"}
      end
    else
      # Normal context - use standard path calculation from source
      Selecto.Retarget.calculate_join_path(selecto, target_schema)
    end
  end

  # Find a join path starting from a specific schema (not just source)
  defp find_join_path_from_schema(domain, from_schema, to_schema, visited) do
    cond do
      from_schema == to_schema && from_schema not in visited && length(visited) == 0 ->
        # Self-referential case ONLY when this is the initial call (visited is empty)
        # For example: film → film_actors → film (when asking for film from film)
        # NOT when we've traversed from another schema and arrived at the target
        from_schema_config = Map.get(domain.schemas, from_schema)

        if from_schema_config && from_schema_config.associations do
          # Try to find a path that goes through another table and back
          case find_self_referential_path(
                 domain,
                 from_schema_config.associations,
                 to_schema,
                 [from_schema]
               ) do
            {:ok, path} -> {:ok, path}
            # Fallback to direct (same table)
            :not_found -> {:ok, []}
          end
        else
          {:ok, []}
        end

      from_schema == to_schema ->
        # Reached target - return empty path (we're already there)
        {:ok, []}

      from_schema in visited ->
        # Cycle detected
        :not_found

      true ->
        # Get the schema config (could be source or a schema in schemas map)
        from_schema_config = Map.get(domain.schemas, from_schema)

        if from_schema_config && from_schema_config.associations do
          find_path_through_associations(
            domain,
            from_schema_config.associations,
            to_schema,
            [from_schema | visited]
          )
        else
          :not_found
        end
    end
  end

  # Find a self-referential path (e.g., film → film_actors → film)
  defp find_self_referential_path(domain, associations, target_schema, _visited) do
    # First try: Look through direct associations
    case find_through_direct_associations(domain, associations, target_schema) do
      {:ok, path} ->
        {:ok, path}

      :not_found ->
        # Second try: Search all schemas for junction tables that connect to target
        find_through_junction_inference(domain, target_schema)
    end
  end

  defp find_through_direct_associations(domain, associations, target_schema) do
    associations
    |> Enum.reduce_while(:not_found, fn {assoc_name, assoc_config}, _acc ->
      queryable_schema = assoc_config.queryable

      # Check if this is a direct self-reference (e.g., categories.parent_category → categories)
      if queryable_schema == target_schema do
        # Direct self-join, return just the association name
        {:halt, {:ok, [assoc_name]}}
      else
        # Check if this could be a junction table
        junction_config = Map.get(domain.schemas, queryable_schema)

        if junction_config && junction_config.associations do
          back_assoc =
            Enum.find(junction_config.associations, fn {_name, assoc} ->
              assoc.queryable == target_schema
            end)

          if back_assoc do
            # Found a path: target → junction → target (many-to-many)
            {:halt, {:ok, [assoc_name, target_schema]}}
          else
            {:cont, :not_found}
          end
        else
          {:cont, :not_found}
        end
      end
    end)
  end

  # Infer self-referential junction by searching all schemas
  defp find_through_junction_inference(domain, target_schema) do
    target_config = Map.get(domain.schemas, target_schema)
    if !target_config, do: :not_found

    target_table = target_config.source_table

    # Search all schemas for junction tables that connect to our target twice
    Enum.reduce_while(domain.schemas, :not_found, fn {schema_name, schema_config}, _acc ->
      # Check if this schema's associations point to our target
      target_associations =
        Enum.filter(schema_config.associations, fn {_name, assoc} ->
          target_sch = Map.get(domain.schemas, assoc.queryable)
          target_sch && target_sch.source_table == target_table
        end)

      # If we found at least one association to our target, this could be a junction
      if length(target_associations) >= 1 do
        # This schema connects to our target - use it as junction
        {:halt, {:ok, [schema_name, target_schema]}}
      else
        {:cont, :not_found}
      end
    end)
  end

  # Helper to search through associations for a path
  defp find_path_through_associations(domain, associations, target, visited) do
    associations
    |> Enum.reduce_while(:not_found, fn {assoc_name, assoc_config}, _acc ->
      next_schema = assoc_config.queryable

      case find_join_path_from_schema(domain, next_schema, target, visited) do
        {:ok, path} -> {:halt, {:ok, [assoc_name | path]}}
        :not_found -> {:cont, :not_found}
      end
    end)
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
    # Parse dot notation: "table.field" or "table.field1,field2"
    cond do
      match = Regex.run(~r/^([^.]+)\.([^.]+(?:\s*,\s*[^.]+)*)$/, field_string) ->
        [_, table_part, field_part] = match
        target_schema = normalize_schema_reference(table_part)
        fields = String.split(field_part, ",") |> Enum.map(&String.trim/1)

        %{
          fields: fields,
          target_schema: target_schema,
          format: default_format,
          alias: generate_alias(target_schema, alias_prefix),
          order_by: default_order_by,
          filters: []
        }

      true ->
        raise ArgumentError,
              "Invalid field format: #{field_string}. Expected 'table.field' or 'table.field1,field2' format."
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
    base_name = to_string(target_schema)
    if prefix != "", do: "#{prefix}_#{base_name}", else: base_name
  end

  defp validate_target_schema(selecto, target_schema) do
    case fetch_schema_config(selecto.domain.schemas, target_schema) do
      nil -> {:error, "Target schema #{target_schema} not found in domain"}
      _ -> :ok
    end
  end

  defp validate_fields_exist(selecto, subselect_config) do
    target_schema_config =
      fetch_schema_config(selecto.domain.schemas, subselect_config.target_schema)

    invalid_fields =
      Enum.filter(subselect_config.fields, fn field_name ->
        field_name_string = to_string(field_name)

        not Enum.any?(target_schema_config.fields, fn existing_field ->
          to_string(existing_field) == field_name_string
        end)
      end)

    case invalid_fields do
      [] ->
        :ok

      fields ->
        {:error,
         "Fields #{inspect(fields)} not found in schema #{subselect_config.target_schema}"}
    end
  end

  defp validate_relationship_path(selecto, subselect_config) do
    case resolve_join_path(selecto, subselect_config.target_schema) do
      {:ok, _path} -> :ok
      {:error, reason} -> {:error, "Cannot reach target schema: #{reason}"}
    end
  end

  defp normalize_schema_reference(schema) when is_atom(schema), do: schema

  defp normalize_schema_reference(schema) when is_binary(schema) do
    try do
      String.to_existing_atom(schema)
    rescue
      ArgumentError -> schema
    end
  end

  defp fetch_schema_config(schemas, schema_key) when is_map(schemas) do
    normalized_key = normalize_schema_reference(to_string(schema_key))
    Map.get(schemas, schema_key) || Map.get(schemas, normalized_key)
  end
end
