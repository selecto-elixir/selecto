defmodule Selecto.Builder.Window do
  @moduledoc """
  Builds SQL for window functions.

  This module handles the generation of SQL for window functions including:
  - Ranking functions (ROW_NUMBER, RANK, DENSE_RANK)
  - Offset functions (LAG, LEAD, FIRST_VALUE, LAST_VALUE)  
  - Aggregate functions with OVER clause
  - Window frame specifications
  """

  alias Selecto.Window.{Spec, Frame}

  @doc """
  Build window function SQL for SELECT clause.

  Returns iodata for window functions that can be appended to the SELECT clause.
  """
  @spec build_window_functions(Selecto.Types.t()) :: {[String.t()], iolist(), [any()]}
  def build_window_functions(selecto) do
    window_functions = Map.get(selecto.set, :window_functions, [])

    case window_functions do
      [] ->
        {[], [], []}

      functions ->
        {joins, iodata, params} =
          functions
          |> Enum.map(&build_single_window_function(selecto, &1))
          |> Enum.reduce({[], [], []}, fn {j, i, p}, {acc_j, acc_i, acc_p} ->
            {acc_j ++ j, acc_i ++ [", ", i], acc_p ++ p}
          end)

        # Remove the leading comma from iodata
        final_iodata =
          case iodata do
            [", " | rest] -> rest
            other -> other
          end

        {joins, final_iodata, params}
    end
  end

  # Build SQL for a single window function
  defp build_single_window_function(selecto, %Spec{} = window_spec) do
    {function_iodata, function_params} = build_function_call(selecto, window_spec)
    {over_iodata, over_params} = build_over_clause(selecto, window_spec)

    alias_part = if window_spec.alias, do: [" AS ", window_spec.alias], else: []

    iodata = [function_iodata, " OVER (", over_iodata, ")", alias_part]
    params = function_params ++ over_params

    # Window functions might require joins if they reference related fields
    joins = extract_required_joins(selecto, window_spec)

    {joins, iodata, params}
  end

  # Build the function call part (e.g., "ROW_NUMBER()", "SUM(sales_amount)")
  defp build_function_call(selecto, %Spec{function: function, arguments: arguments}) do
    case function do
      # Ranking functions - no arguments
      func when func in [:row_number, :rank, :dense_rank, :percent_rank] ->
        {[String.upcase(to_string(func)), "()"], []}

      # NTILE function - takes bucket count
      :ntile ->
        bucket_count = get_ntile_bucket_count(arguments)
        {["NTILE(", {:param, bucket_count}, ")"], [bucket_count]}

      # Cumulative distribution
      :cume_dist ->
        {["CUME_DIST()"], []}

      # Offset functions - field and optional offset
      func when func in [:lag, :lead] ->
        build_offset_function(selecto, func, arguments)

      # Value functions - field argument
      func when func in [:first_value, :last_value, :nth_value] ->
        build_value_function(selecto, func, arguments)

      # Aggregate functions - field argument
      func when func in [:sum, :avg, :count, :min, :max, :stddev, :variance] ->
        build_aggregate_function(selecto, func, arguments)
    end
  end

  # Build NTILE function - get bucket count from arguments
  # Default to quartiles
  defp get_ntile_bucket_count(nil), do: 4
  defp get_ntile_bucket_count([]), do: 4
  defp get_ntile_bucket_count([count | _]) when is_integer(count), do: count
  defp get_ntile_bucket_count(_), do: 4

  # Build offset functions (LAG, LEAD)
  defp build_offset_function(selecto, function, arguments) do
    {field, offset} = parse_offset_arguments(arguments)
    resolved_field = resolve_field_reference(selecto, field)

    func_name = String.upcase(to_string(function))

    case offset do
      1 ->
        {[func_name, "(", resolved_field, ")"], []}

      offset when is_integer(offset) ->
        {[func_name, "(", resolved_field, ", ", {:param, offset}, ")"], [offset]}
    end
  end

  # Parse arguments for LAG/LEAD functions  
  defp parse_offset_arguments([field]), do: {field, 1}
  defp parse_offset_arguments([field, offset]) when is_integer(offset), do: {field, offset}
  defp parse_offset_arguments([field | _]), do: {field, 1}
  defp parse_offset_arguments(_), do: {nil, 1}

  # Build value functions (FIRST_VALUE, LAST_VALUE, NTH_VALUE)
  defp build_value_function(selecto, function, arguments) do
    case function do
      :nth_value ->
        # NTH_VALUE takes field and position
        case arguments do
          [field, n] when is_integer(n) ->
            resolved_field = resolve_field_reference(selecto, field)
            {["NTH_VALUE(", resolved_field, ", ", {:param, n}, ")"], [n]}

          _ ->
            raise ArgumentError, "NTH_VALUE requires field and position arguments"
        end

      func ->
        # FIRST_VALUE and LAST_VALUE take only field
        field = get_first_argument(arguments)
        resolved_field = resolve_field_reference(selecto, field)
        func_name = String.upcase(to_string(func))
        {[func_name, "(", resolved_field, ")"], []}
    end
  end

  # Build aggregate functions with OVER
  defp build_aggregate_function(selecto, function, arguments) do
    field = get_first_argument(arguments)

    case {function, field} do
      {:count, "*"} ->
        {["COUNT(*)"], []}

      {:count, nil} ->
        {["COUNT(*)"], []}

      {func, field} when not is_nil(field) ->
        resolved_field = resolve_field_reference(selecto, field)
        func_name = String.upcase(to_string(func))
        {[func_name, "(", resolved_field, ")"], []}
    end
  end

  # Get first argument from arguments list
  defp get_first_argument(nil), do: nil
  defp get_first_argument([]), do: nil
  defp get_first_argument([first | _]), do: first

  # Build OVER clause
  defp build_over_clause(selecto, %Spec{
         partition_by: partition_by,
         order_by: order_by,
         frame: frame
       }) do
    {partition_iodata, partition_params} = build_partition_by(selecto, partition_by)
    {order_iodata, order_params} = build_order_by(selecto, order_by)
    {frame_iodata, frame_params} = build_frame_clause(frame)

    # Combine clauses with appropriate spacing
    clauses =
      [partition_iodata, order_iodata, frame_iodata]
      |> Enum.reject(&is_empty_iodata/1)
      |> Enum.intersperse(" ")

    params = partition_params ++ order_params ++ frame_params

    {clauses, params}
  end

  # Build PARTITION BY clause
  defp build_partition_by(_selecto, nil), do: {[], []}
  defp build_partition_by(_selecto, []), do: {[], []}

  defp build_partition_by(selecto, fields) do
    resolved_fields =
      fields
      |> Enum.map(&resolve_field_reference(selecto, &1))
      |> Enum.intersperse(", ")

    {["PARTITION BY ", resolved_fields], []}
  end

  # Build ORDER BY clause for window
  defp build_order_by(_selecto, nil), do: {[], []}
  defp build_order_by(_selecto, []), do: {[], []}

  defp build_order_by(selecto, order_specs) do
    order_items =
      order_specs
      |> Enum.map(fn {field, direction} ->
        resolved_field = resolve_field_reference(selecto, field)
        direction_str = String.upcase(to_string(direction))
        [resolved_field, " ", direction_str]
      end)
      |> Enum.intersperse(", ")

    {["ORDER BY ", order_items], []}
  end

  # Build window frame clause
  defp build_frame_clause(nil), do: {[], []}

  defp build_frame_clause(%Frame{type: type, start: start_bound, end: end_bound}) do
    type_str = String.upcase(to_string(type))
    start_str = build_frame_boundary(start_bound)
    end_str = build_frame_boundary(end_bound)

    iodata = [type_str, " BETWEEN ", start_str, " AND ", end_str]

    {iodata, []}
  end

  # Build individual frame boundary
  defp build_frame_boundary(:unbounded_preceding), do: "UNBOUNDED PRECEDING"
  defp build_frame_boundary(:current_row), do: "CURRENT ROW"
  defp build_frame_boundary(:unbounded_following), do: "UNBOUNDED FOLLOWING"
  defp build_frame_boundary({:preceding, n}), do: "#{n} PRECEDING"
  defp build_frame_boundary({:following, n}), do: "#{n} FOLLOWING"
  defp build_frame_boundary({:interval, interval}), do: "INTERVAL '#{interval}' PRECEDING"

  # Resolve field references (handle joins if needed)
  defp resolve_field_reference(_selecto, field) when is_binary(field) do
    # Simple implementation - use field resolver or just return field
    # This should integrate with Selecto's field resolution system
    if String.contains?(field, ".") do
      # Already qualified
      field
    else
      # Root fields should reference the query alias used in FROM.
      "selecto_root.#{field}"
    end
  end

  defp resolve_field_reference(_selecto, field), do: to_string(field)

  # Extract joins required for window function fields
  defp extract_required_joins(selecto, %Spec{
         partition_by: partition_by,
         order_by: order_by,
         arguments: arguments
       }) do
    # Collect all field references from the window spec
    all_fields = collect_field_references(partition_by, order_by, arguments)

    # Find which joins are required for each field
    all_fields
    |> Enum.map(&find_join_for_field(selecto, &1))
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  # Collect all field references from window spec components
  defp collect_field_references(partition_by, order_by, arguments) do
    partition_fields = partition_by || []

    order_fields =
      case order_by do
        nil ->
          []

        specs when is_list(specs) ->
          Enum.map(specs, fn
            {field, _direction} -> field
            field -> field
          end)
      end

    argument_fields =
      case arguments do
        nil ->
          []

        args when is_list(args) ->
          Enum.filter(args, &is_binary/1)
      end

    partition_fields ++ order_fields ++ argument_fields
  end

  # Find the join required for a field reference
  defp find_join_for_field(selecto, field) when is_binary(field) do
    # Check if field is qualified with a join prefix (e.g., "category.name")
    case String.split(field, ".", parts: 2) do
      [prefix, _field_name] ->
        # Check if prefix matches a join
        joins = Selecto.joins(selecto)
        join_key = String.to_atom(prefix)

        if Map.has_key?(joins, join_key) do
          join_key
        else
          # Check columns for this field
          find_join_from_columns(selecto, field)
        end

      [_field_only] ->
        # Unqualified field - check if it needs a join
        find_join_from_columns(selecto, field)
    end
  end

  defp find_join_for_field(_selecto, _field), do: nil

  # Look up join requirement from column configuration
  defp find_join_from_columns(selecto, field) do
    columns = Selecto.columns(selecto)

    case Map.get(columns, field) do
      nil ->
        nil

      column_config ->
        case Map.get(column_config, :requires_join) do
          :selecto_root -> nil
          nil -> nil
          join -> join
        end
    end
  end

  # Check if iodata is effectively empty
  defp is_empty_iodata([]), do: true
  defp is_empty_iodata(""), do: true

  defp is_empty_iodata(iodata) when is_list(iodata) do
    Enum.all?(iodata, &is_empty_iodata/1)
  end

  defp is_empty_iodata(_), do: false
end
