defmodule Selecto.Retarget do
  @moduledoc """
  Retarget functionality for retargeting joined tables as primary query focus.

  The Retarget feature allows you to shift the perspective of a Selecto query from the 
  source table to any joined table, while preserving existing filters through subqueries.

  ## Examples

      # Basic retarget - shift from events to orders
      selecto
      |> Selecto.filter([{"event_id", 123}])
      |> Selecto.retarget(:orders)
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
  Retarget the query to focus on a different table while preserving existing context.

  ## Parameters

  - `selecto` - The Selecto struct to retarget
  - `target_schema` - Atom representing the target table to retarget to
  - `opts` - Optional configuration (see module docs)

  ## Returns

  Updated Selecto struct with retarget configuration applied.

  ## Examples

      selecto
      |> Selecto.filter([{"event_id", 123}])
      |> Selecto.retarget(:orders)
      |> Selecto.select(["product_name"])
  """
  @spec retarget(Types.t(), atom(), keyword()) :: Types.t()
  def retarget(selecto, target_schema, opts \\ []) do
    with {:ok, join_path} <- calculate_join_path(selecto, target_schema),
         :ok <- validate_retarget_path(selecto, join_path) do
      pivot_config = %{
        target_schema: target_schema,
        join_path: join_path,
        preserve_filters: Keyword.get(opts, :preserve_filters, true),
        subquery_strategy: Keyword.get(opts, :subquery_strategy, :in),
        # :subquery or :cte
        strategy: Keyword.get(opts, :strategy, :subquery)
      }

      put_in(selecto.set[:retarget_state], pivot_config)
    else
      {:error, reason} ->
        raise ArgumentError, "Invalid retarget configuration: #{reason}"
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

    case find_join_path(selecto.domain, source_name, target_schema, []) do
      {:ok, path} -> {:ok, path}
      :not_found -> {:error, "No join path found from #{source_name} to #{target_schema}"}
    end
  end

  @doc """
  Validate that a retarget path exists and is traversable.
  """
  @spec validate_retarget_path(Types.t(), [atom()]) :: :ok | {:error, String.t()}
  def validate_retarget_path(selecto, join_path) do
    case verify_join_chain(selecto.domain, join_path) do
      true -> :ok
      false -> {:error, "Join path validation failed"}
    end
  end

  @doc """
  Check if a Selecto query has retarget configuration applied.
  """
  @spec has_retarget?(Types.t()) :: boolean()
  def has_retarget?(selecto) do
    not is_nil(selecto.set[:retarget_state] || selecto.set[:pivot_state])
  end

  @doc """
  Get the retarget configuration from a Selecto query.
  """
  @spec get_retarget_config(Types.t()) :: Types.retarget_config() | nil
  def get_retarget_config(selecto) do
    selecto.set[:retarget_state] || selecto.set[:pivot_state]
  end

  @doc """
  Reset/remove retarget configuration from a Selecto query.
  """
  @spec reset_retarget(Types.t()) :: Types.t()
  def reset_retarget(selecto) do
    updated_set = selecto.set |> Map.delete(:retarget_state) |> Map.delete(:pivot_state)
    %{selecto | set: updated_set}
  end

  # Private helper functions

  defp get_source_schema_name(_selecto) do
    # The source is always the starting point for path finding
    :source
  end

  defp find_join_path(domain, from_schema, to_schema, visited) do
    cond do
      from_schema == to_schema ->
        {:ok, []}

      from_schema in visited ->
        :not_found

      true ->
        # First, try looking in the hierarchical joins structure
        case find_path_in_joins_hierarchy(domain, to_schema) do
          {:ok, path} ->
            {:ok, path}

          :not_found ->
            # Fall back to the association-based search
            from_schema_config =
              case from_schema do
                :source -> domain.source
                schema_name -> Map.get(domain.schemas, schema_name)
              end

            if from_schema_config do
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
  end

  defp find_path_in_joins_hierarchy(domain, target) do
    # Search the joins structure hierarchically
    joins = Map.get(domain, :joins, %{})
    find_in_joins_tree(joins, target, [])
  end

  defp find_in_joins_tree(joins, target, path) when is_map(joins) do
    Enum.reduce_while(joins, :not_found, fn {join_name, join_config}, _acc ->
      if join_name == target do
        # Found it at this level
        {:halt, {:ok, path ++ [join_name]}}
      else
        # Check nested joins
        case Map.get(join_config, :joins) do
          nil ->
            {:cont, :not_found}

          nested_joins ->
            case find_in_joins_tree(nested_joins, target, path ++ [join_name]) do
              {:ok, found_path} -> {:halt, {:ok, found_path}}
              :not_found -> {:cont, :not_found}
            end
        end
      end
    end)
  end

  defp find_in_joins_tree(_, _, _), do: :not_found

  defp find_path_through_associations(domain, associations, target, visited) do
    associations
    |> Enum.reduce_while(:not_found, fn {assoc_name, assoc_config}, _acc ->
      next_schema = assoc_config.queryable

      case find_join_path(domain, next_schema, target, visited) do
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
    current_config =
      case current_schema do
        :source -> domain.source
        schema_name -> Map.get(domain.schemas, schema_name)
      end

    if current_config do
      case Map.get(current_config.associations, next_assoc) do
        nil ->
          false

        assoc_config ->
          verify_join_step(domain, assoc_config.queryable, remaining_path)
      end
    else
      false
    end
  end
end
