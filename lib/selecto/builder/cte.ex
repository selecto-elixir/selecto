defmodule Selecto.Builder.CteSql do
  @moduledoc """
  SQL generation for PostgreSQL Common Table Expressions (CTEs).

  Generates SQL for both non-recursive and recursive WITH clauses,
  handling dependency ordering, column specifications, and proper
  PostgreSQL CTE syntax.
  """

  alias Selecto.Advanced.CTE.Spec
  alias Selecto.Builder.Sql

  @doc """
  Build WITH clause SQL from a list of CTE specifications.

  Returns {with_clause_iodata, parameters} tuple with properly ordered
  CTEs and parameter bindings.
  """
  def build_with_clause(ctes) when is_list(ctes) and length(ctes) > 0 do
    {structured_ctes, raw_ctes, invalid_entries} = partition_ctes(ctes)

    if invalid_entries != [] do
      raise ArgumentError, "Unsupported CTE entries: #{inspect(invalid_entries)}"
    end

    ordered_structured_ctes =
      case structured_ctes do
        [] ->
          []

        list ->
          case Selecto.Advanced.CTE.detect_circular_dependencies(list) do
            {:ok, ordered_ctes} ->
              ordered_ctes

            {:error, validation_error} ->
              raise validation_error
          end
      end

    build_ordered_with_clause(ordered_structured_ctes, raw_ctes)
  end

  def build_with_clause([]), do: {[], []}

  @doc """
  Build a single CTE definition SQL.

  Returns {cte_definition_iodata, parameters} for a single CTE.
  """
  def build_cte_definition(%Spec{} = spec) do
    case spec.validated do
      false ->
        raise ArgumentError, "CTE specification must be validated before SQL generation"

      true ->
        generate_cte_sql(spec)
    end
  end

  # Build WITH clause from ordered CTEs
  defp build_ordered_with_clause(ordered_structured_ctes, raw_ctes) do
    # Check if any CTE is recursive
    has_recursive =
      Enum.any?(ordered_structured_ctes, &(&1.type == :recursive)) or
        Enum.any?(raw_ctes, fn
          {:raw_recursive_cte, _, _} -> true
          _ -> false
        end)

    # Build structured CTE definitions
    {structured_definitions, structured_params} =
      ordered_structured_ctes
      |> Enum.map(&build_cte_definition/1)
      |> Enum.unzip()

    # Raw CTE entries are already complete CTE definitions.
    {raw_definitions, raw_params} =
      raw_ctes
      |> Enum.map(fn
        {:raw_cte, cte_definition, params} -> {cte_definition, params}
        {:raw_recursive_cte, cte_definition, params} -> {cte_definition, params}
      end)
      |> Enum.unzip()

    # Combine with proper WITH syntax
    with_keyword = if has_recursive, do: "WITH RECURSIVE ", else: "WITH "
    cte_definitions = raw_definitions ++ structured_definitions
    cte_list = Enum.intersperse(cte_definitions, ",\n    ")

    with_clause = [with_keyword | cte_list]
    combined_params = List.flatten(raw_params) ++ List.flatten(structured_params)

    {with_clause, combined_params}
  end

  defp partition_ctes(ctes) do
    Enum.reduce(ctes, {[], [], []}, fn
      %Spec{} = cte, {structured, raw, invalid} ->
        {structured ++ [cte], raw, invalid}

      {:raw_cte, cte_definition, params}, {structured, raw, invalid} ->
        {structured, raw ++ [{:raw_cte, cte_definition, params}], invalid}

      {:raw_recursive_cte, cte_definition, params}, {structured, raw, invalid} ->
        {structured, raw ++ [{:raw_recursive_cte, cte_definition, params}], invalid}

      entry, {structured, raw, invalid} ->
        {structured, raw, invalid ++ [entry]}
    end)
  end

  # Generate SQL for individual CTE
  defp generate_cte_sql(%Spec{type: :normal} = spec) do
    # Execute the query builder to get the Selecto query
    selecto_query = spec.query_builder.()

    # Generate SQL from the Selecto query
    {sql, _aliases, params} = Sql.build(selecto_query, [])

    # Convert SQL string back to iodata with param markers
    sql_iodata = convert_sql_to_iodata(sql, params)

    # Build CTE definition
    cte_name = escape_identifier(spec.name)

    cte_definition =
      case spec.columns do
        nil ->
          [cte_name, " AS (\n    ", sql_iodata, "\n)"]

        columns when is_list(columns) ->
          column_list = columns |> Enum.map(&escape_identifier/1) |> Enum.join(", ")
          [cte_name, " (", column_list, ") AS (\n    ", sql_iodata, "\n)"]
      end

    {cte_definition, params}
  end

  defp generate_cte_sql(%Spec{type: :recursive} = spec) do
    # For recursive CTEs, we need special handling of the CTE reference
    cte_ref = create_cte_reference(spec.name)

    # Execute base query
    base_selecto = spec.base_query.()
    {base_sql, _base_aliases, base_params} = Sql.build(base_selecto, [])

    # Execute recursive query with CTE reference
    recursive_selecto = spec.recursive_query.(cte_ref)
    {recursive_sql, _recursive_aliases, recursive_params} = Sql.build(recursive_selecto, [])

    # Convert SQL strings back to iodata with param markers
    base_sql_iodata = convert_sql_to_iodata(base_sql, base_params)
    # Adjust param indices for recursive part
    recursive_sql_iodata =
      convert_sql_to_iodata_with_offset(recursive_sql, recursive_params, length(base_params))

    # Build recursive CTE definition
    cte_name = escape_identifier(spec.name)

    cte_definition =
      case spec.columns do
        nil ->
          [
            cte_name,
            " AS (\n    ",
            base_sql_iodata,
            "\n    UNION ALL\n    ",
            recursive_sql_iodata,
            "\n)"
          ]

        columns when is_list(columns) ->
          column_list = columns |> Enum.map(&escape_identifier/1) |> Enum.join(", ")

          [
            cte_name,
            " (",
            column_list,
            ") AS (\n    ",
            base_sql_iodata,
            "\n    UNION ALL\n    ",
            recursive_sql_iodata,
            "\n)"
          ]
      end

    combined_params = base_params ++ recursive_params
    {cte_definition, combined_params}
  end

  @doc """
  Create a CTE reference that can be used in joins and queries.

  Returns a structure that represents the CTE as a queryable table.
  """
  def create_cte_reference(cte_name) when is_binary(cte_name) do
    # Return a simple reference structure
    # This would be used in the recursive query function
    %{
      __cte_reference__: true,
      name: cte_name,
      source: cte_name,
      type: :cte
    }
  end

  @doc """
  Integrate CTEs with a main query, combining the WITH clause with the query.

  Returns the complete SQL with CTEs at the top.
  """
  def integrate_ctes_with_query(ctes, query_iodata, query_params) when is_list(ctes) do
    case build_with_clause(ctes) do
      {[], []} ->
        # No CTEs, return query as-is
        {query_iodata, query_params}

      {with_clause, cte_params} ->
        # Combine WITH clause with main query
        combined_iodata = [with_clause, "\n", query_iodata]
        combined_params = cte_params ++ query_params
        {combined_iodata, combined_params}
    end
  end

  # Escape SQL identifier (table names, column names)
  defp escape_identifier(identifier) when is_binary(identifier) do
    # Simple identifier escaping - quote if contains special characters
    if String.match?(identifier, ~r/^[a-zA-Z_][a-zA-Z0-9_]*$/) and
         not String.match?(
           identifier,
           ~r/^(select|from|where|order|group|having|with|recursive)$/i
         ) do
      identifier
    else
      "\"#{String.replace(identifier, "\"", "\"\"")}\""
    end
  end

  # Convert SQL string with $1, $2 placeholders back to iodata with {:param, value} markers
  defp convert_sql_to_iodata(sql, params) do
    convert_sql_to_iodata_with_offset(sql, params, 0)
  end

  defp convert_sql_to_iodata_with_offset(sql, params, _offset) do
    params
    |> Enum.with_index(1)
    |> Enum.reduce([sql], fn {value, idx}, acc ->
      placeholder = "$#{idx}"

      Enum.flat_map(acc, fn
        s when is_binary(s) ->
          case String.split(s, placeholder, parts: 2) do
            [before, after_str] ->
              [before, {:param, value}, after_str]

            [unchanged] ->
              [unchanged]
          end

        other ->
          [other]
      end)
    end)
  end
end
