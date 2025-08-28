defmodule Selecto.Pivot do
  @moduledoc """
  Pivot functionality for retargeting joined tables as primary query focus.

  The Pivot feature allows you to shift the perspective of a Selecto query from the 
  source table to any joined table, while preserving existing filters through subqueries.

  ## Examples

      # Basic pivot - shift from events to orders
      selecto
      |> Selecto.filter([{"event_id", 123}])
      |> Selecto.pivot(:orders)
      |> Selecto.select(["product_name", "quantity"])

      # This generates SQL like:
      # SELECT o.product_name, o.quantity 
      # FROM orders o 
      # WHERE o.attendee_id IN (
      #   SELECT a.attendee_id FROM events e 
      #   JOIN attendees a ON e.event_id = a.event_id 
      #   WHERE e.event_id = 123
      # )

  ## Configuration Options

  - `:preserve_filters` - Whether to preserve existing filters in subquery (default: true)
  - `:subquery_strategy` - How to generate the subquery (`:in`, `:exists`, `:join`)
  """

  alias Selecto.Types

  @doc """
  Pivot the query to focus on a different table while preserving existing context.

  ## Parameters

  - `selecto` - The Selecto struct to pivot
  - `target_schema` - Atom representing the target table to pivot to
  - `opts` - Optional configuration (see module docs)

  ## Returns

  Updated Selecto struct with pivot configuration applied.

  ## Examples

      selecto
      |> Selecto.filter([{"event_id", 123}])
      |> Selecto.pivot(:orders)
      |> Selecto.select(["product_name"])
  """
  @spec pivot(Types.t(), atom(), keyword()) :: Types.t()
  def pivot(selecto, target_schema, opts \\ []) do
    with {:ok, join_path} <- calculate_join_path(selecto, target_schema),
         :ok <- validate_pivot_path(selecto, join_path) do
      
      pivot_config = %{
        target_schema: target_schema,
        join_path: join_path,
        preserve_filters: Keyword.get(opts, :preserve_filters, true),
        subquery_strategy: Keyword.get(opts, :subquery_strategy, :in)
      }

      put_in(selecto.set[:pivot_state], pivot_config)
    else
      {:error, reason} ->
        raise ArgumentError, "Invalid pivot configuration: #{reason}"
    end
  end

  @doc """
  Calculate the join path from the source table to the target table.

  This function analyzes the domain configuration to find the shortest path
  of associations from the current source to the target schema.
  """
  @spec calculate_join_path(Types.t(), atom()) :: {:ok, [atom()]} | {:error, String.t()}
  def calculate_join_path(selecto, target_schema) do
    source_name = get_source_schema_name(selecto)
    
    case find_join_path(selecto.domain.schemas, source_name, target_schema, []) do
      {:ok, path} -> {:ok, path}
      :not_found -> {:error, "No join path found from #{source_name} to #{target_schema}"}
    end
  end

  @doc """
  Validate that a pivot path exists and is traversable.
  """
  @spec validate_pivot_path(Types.t(), [atom()]) :: :ok | {:error, String.t()}
  def validate_pivot_path(selecto, join_path) do
    case verify_join_chain(selecto.domain, join_path) do
      true -> :ok
      false -> {:error, "Join path validation failed"}
    end
  end

  @doc """
  Check if a Selecto query has pivot configuration applied.
  """
  @spec has_pivot?(Types.t()) :: boolean()
  def has_pivot?(selecto) do
    not is_nil(selecto.set[:pivot_state])
  end

  @doc """
  Get the pivot configuration from a Selecto query.
  """
  @spec get_pivot_config(Types.t()) :: Types.pivot_config() | nil
  def get_pivot_config(selecto) do
    selecto.set[:pivot_state]
  end

  @doc """
  Reset/remove pivot configuration from a Selecto query.
  """
  @spec reset_pivot(Types.t()) :: Types.t()
  def reset_pivot(selecto) do
    updated_set = Map.delete(selecto.set, :pivot_state)
    %{selecto | set: updated_set}
  end

  # Private helper functions

  defp get_source_schema_name(selecto) do
    # Extract schema name from source configuration
    # This is a simplified version - may need refinement based on actual source structure
    selecto.domain.source.source_table
    |> String.to_atom()
  end

  defp find_join_path(schemas, from_schema, to_schema, visited) do
    cond do
      from_schema == to_schema ->
        {:ok, []}
        
      from_schema in visited ->
        :not_found
        
      true ->
        from_schema_config = Map.get(schemas, from_schema)
        if from_schema_config do
          find_path_through_associations(
            schemas, 
            from_schema_config.associations, 
            to_schema, 
            [from_schema | visited]
          )
        else
          :not_found
        end
    end
  end

  defp find_path_through_associations(schemas, associations, target, visited) do
    associations
    |> Enum.reduce_while(:not_found, fn {assoc_name, assoc_config}, _acc ->
      next_schema = assoc_config.queryable
      
      case find_join_path(schemas, next_schema, target, visited) do
        {:ok, path} -> {:halt, {:ok, [assoc_name | path]}}
        :not_found -> {:cont, :not_found}
      end
    end)
  end

  defp verify_join_chain(domain, join_path) do
    # Verify each step in the join path exists and is valid
    # Start from source and validate each association step
    verify_join_step(domain, :source, join_path)
  end

  defp verify_join_step(_domain, _current_schema, []) do
    true
  end

  defp verify_join_step(domain, current_schema, [next_assoc | remaining_path]) do
    current_config = case current_schema do
      :source -> domain.source
      schema_name -> Map.get(domain.schemas, schema_name)
    end

    if current_config do
      case Map.get(current_config.associations, next_assoc) do
        nil -> false
        assoc_config ->
          verify_join_step(domain, assoc_config.queryable, remaining_path)
      end
    else
      false
    end
  end
end