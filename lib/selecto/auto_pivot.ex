defmodule Selecto.AutoPivot do
  @moduledoc """
  Automatic pivot detection and application for Selecto queries.

  When querying with fields from joined tables only (no source table fields),
  this module can automatically pivot the query to use the joined table as the
  source, which can be more efficient.

  Auto-pivot is typically used with `prevent_denormalization` mode in detail views.
  Aggregate views handle joins naturally through GROUP BY and don't need pivoting.
  """

  require Logger

  @doc """
  Check if a selecto query should be automatically pivoted based on selected columns.

  Returns the pivoted selecto if pivot is needed, otherwise returns the original.

  ## Options

  - `:view_mode` - View type ("detail", "aggregate", "graph"). Only "detail" allows pivoting.

  ## Examples

      # Will pivot if only category fields are selected
      selecto = Selecto.configure(domain, conn)
      pivoted = Selecto.AutoPivot.maybe_apply(selecto, selected: ["category.name"], view_mode: "detail")
  """
  @spec maybe_apply(Selecto.Types.t(), keyword()) :: Selecto.Types.t()
  def maybe_apply(selecto, opts \\ []) do
    view_mode = Keyword.get(opts, :view_mode, "detail")
    selected_columns = Keyword.get(opts, :selected, [])

    Logger.debug("=== AUTO_PIVOT: Checking if pivot needed ===")
    Logger.debug("View mode: #{view_mode}, Selected columns: #{inspect(selected_columns)}")

    # Auto-pivot should only be used for detail views, not aggregate views
    # Aggregate views handle joins naturally through GROUP BY
    if view_mode != "aggregate" do
      if should_pivot?(selecto, selected_columns) do
        target_table = find_pivot_target(selecto, selected_columns)

        if target_table do
          Logger.debug("Applying pivot to target: #{inspect(target_table)}")

          # Find the join path to the target table
          join_path = find_join_path(selecto.domain, target_table)

          if join_path do
            apply_pivot(selecto, target_table, join_path)
          else
            Logger.warning("Could not find join path to #{target_table}, skipping pivot")
            selecto
          end
        else
          selecto
        end
      else
        selecto
      end
    else
      Logger.debug("Skipping auto-pivot for aggregate view")
      selecto
    end
  end

  @doc """
  Determine if a query should be pivoted based on selected columns.

  Pivoting is beneficial when:
  1. There are qualified columns from other tables
  2. NO source table columns are selected (they wouldn't be available after pivot)
  3. All qualified columns are from tables accessible from a single pivot target
  """
  @spec should_pivot?(Selecto.Types.t(), [String.t() | atom()]) :: boolean()
  def should_pivot?(selecto, selected_columns) do
    source_columns = get_source_columns(selecto)

    # Categorize columns into source vs joined tables
    {source_cols, qualified_cols_by_table} = categorize_columns(selected_columns, source_columns)

    # Only pivot if there are joined table columns and no source columns
    case Map.keys(qualified_cols_by_table) do
      [] ->
        false
      _table_names ->
        source_cols == []
    end
  end

  @doc """
  Find the best table to pivot to based on selected columns.

  Returns the table name (as atom) that should become the new source table.
  """
  @spec find_pivot_target(Selecto.Types.t(), [String.t() | atom()]) :: atom() | nil
  def find_pivot_target(selecto, selected_columns) do
    source_columns = get_source_columns(selecto)
    {_source_cols, qualified_cols_by_table} = categorize_columns(selected_columns, source_columns)

    case Map.keys(qualified_cols_by_table) do
      [] -> nil
      [single_table] -> String.to_atom(single_table)
      [first_table | _rest] ->
        # For multiple tables, pivot to the first one
        # TODO: Could be smarter about choosing the "root" table
        String.to_atom(first_table)
    end
  end

  # Private functions

  defp categorize_columns(selected_columns, source_columns) do
    Enum.reduce(selected_columns, {[], %{}}, fn col, {src, qualified} ->
      col_str = to_string(col)

      if String.contains?(col_str, ".") do
        # Qualified column like "category.name"
        [table_name, _column_name] = String.split(col_str, ".", parts: 2)

        if table_name in ["selecto_root", ""] do
          # It's a source column with qualification
          {[col_str | src], qualified}
        else
          # Group by table name
          current = Map.get(qualified, table_name, [])
          {src, Map.put(qualified, table_name, [col_str | current])}
        end
      else
        # Unqualified column - check if it's from source
        if column_exists_in_source?(col, source_columns) do
          {[col_str | src], qualified}
        else
          # Unknown origin, can't determine
          {src, qualified}
        end
      end
    end)
  end

  defp get_source_columns(selecto) do
    source_config = selecto.domain.source
    Map.keys(source_config.columns || %{})
  end

  defp column_exists_in_source?(column_name, source_columns) do
    col_atom = if is_binary(column_name), do: String.to_existing_atom(column_name), else: column_name
    col_string = if is_atom(column_name), do: Atom.to_string(column_name), else: column_name

    Enum.any?(source_columns, fn source_col ->
      source_col == col_atom or source_col == col_string or
      Atom.to_string(source_col) == col_string or
      String.to_atom(to_string(source_col)) == col_atom
    end)
  rescue
    ArgumentError -> false
  end

  defp find_join_path(domain, target_table) do
    target_name = to_string(target_table)
    joins = domain.source.joins || %{}
    search_joins_recursive(joins, target_name, [])
  end

  defp search_joins_recursive(joins, target, path) when is_map(joins) do
    Enum.find_value(joins, fn {join_name, join_config} ->
      cond do
        # Direct match
        to_string(join_name) == target ->
          Enum.reverse([join_name | path])

        # Check nested joins
        nested_joins = Map.get(join_config, :joins, %{}) ->
          search_joins_recursive(nested_joins, target, [join_name | path])

        true ->
          nil
      end
    end)
  end
  defp search_joins_recursive(_, _, _), do: nil

  defp apply_pivot(selecto, target_table, join_path) do
    Logger.debug("Applying pivot: #{inspect(join_path)} -> #{target_table}")

    # Use Selecto's built-in pivot functionality
    Selecto.pivot(selecto, target_table, join: join_path)
  end
end