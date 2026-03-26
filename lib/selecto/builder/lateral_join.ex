defmodule Selecto.Builder.LateralJoin do
  @moduledoc """
  SQL generation for LATERAL joins.

  This module handles the conversion of LATERAL join specifications into
  proper PostgreSQL LATERAL JOIN SQL syntax.
  """

  alias Selecto.Advanced.LateralJoin.Spec
  alias Selecto.AdapterSupport
  alias Selecto.Error
  # alias Selecto.Builder.SQL

  @doc """
  Build LATERAL JOIN SQL clauses from specifications.

  Takes a list of LATERAL join specifications and generates the corresponding
  SQL JOIN clauses with LATERAL keyword and proper correlation handling.

  ## Examples

      iex> build_lateral_joins([lateral_spec])
      {["LEFT JOIN LATERAL (SELECT ...) AS recent_rentals ON true"], [param1, param2]}
  """
  def build_lateral_joins(lateral_specs, opts \\ [])

  def build_lateral_joins([], _opts), do: {[], []}

  def build_lateral_joins(lateral_specs, opts) when is_list(lateral_specs) do
    lateral_specs
    |> Enum.map(&build_lateral_join(&1, opts))
    |> Enum.reduce({[], []}, fn {sql, params}, {acc_sql, acc_params} ->
      {[sql | acc_sql], Enum.reverse(params, acc_params)}
    end)
    |> then(fn {sql, params} -> {Enum.reverse(sql), Enum.reverse(params)} end)
  end

  @doc """
  Build a single LATERAL JOIN SQL clause.
  """
  def build_lateral_join(%Spec{} = spec, opts \\ []) do
    adapter = Keyword.get(opts, :adapter)

    case spec.subquery_builder do
      nil ->
        # Table function LATERAL join
        build_table_function_lateral_join(spec, adapter)

      subquery_builder when is_function(subquery_builder) ->
        join_syntax = join_syntax(spec, adapter)

        # Subquery LATERAL join
        build_subquery_lateral_join(spec, join_syntax)
    end
  end

  # Build LATERAL join with table function
  defp build_table_function_lateral_join(
         %Spec{table_function: {:json_table, _, _, _}} = spec,
         adapter
       ) do
    build_json_table_join(spec, adapter)
  end

  defp build_table_function_lateral_join(
         %Spec{table_function: {function_name, _, _}} = spec,
         adapter
       )
       when function_name in [:json_each, :json_tree] do
    build_sqlite_json_rowset_join(spec, adapter)
  end

  defp build_table_function_lateral_join(%Spec{} = spec, adapter) do
    join_syntax = join_syntax(spec, adapter)
    {function_sql, params} = build_table_function_sql(spec.table_function)

    sql =
      case join_syntax do
        {:lateral, join_type_sql} ->
          [join_type_sql, " JOIN LATERAL ", function_sql, " AS ", spec.alias, " ON true"]

        {:apply, apply_sql} ->
          [apply_sql, " ", function_sql, " AS ", spec.alias]
      end

    {sql, params}
  end

  defp build_json_table_join(%Spec{} = spec, adapter) do
    cond do
      not AdapterSupport.supports_feature?(adapter, :json_table) ->
        adapter_name = AdapterSupport.adapter_name(adapter) || adapter

        error =
          Error.validation_error("Adapter does not support JSON_TABLE joins", %{
            adapter: adapter_name,
            unsupported_feature: :json_table
          })

        raise Error.to_exception(error)

      spec.join_type not in [:inner, :left] ->
        error =
          Error.validation_error("MySQL JSON_TABLE joins only support :inner and :left", %{
            adapter: AdapterSupport.adapter_name(adapter) || adapter,
            join_type: spec.join_type,
            supported_join_types: [:inner, :left],
            unsupported_feature: :json_table
          })

        raise Error.to_exception(error)

      true ->
        {function_sql, params} = build_table_function_sql(spec.table_function)
        join_sql = if spec.join_type == :left, do: "LEFT JOIN", else: "INNER JOIN"
        {[join_sql, " ", function_sql, " AS ", spec.alias, " ON true"], params}
    end
  end

  defp build_sqlite_json_rowset_join(%Spec{} = spec, adapter) do
    cond do
      not AdapterSupport.supports_feature?(adapter, :json_rowset) ->
        adapter_name = AdapterSupport.adapter_name(adapter) || adapter

        error =
          Error.validation_error("Adapter does not support SQLite JSON rowset joins", %{
            adapter: adapter_name,
            unsupported_feature: :json_rowset
          })

        raise Error.to_exception(error)

      spec.join_type not in [:inner, :left] ->
        error =
          Error.validation_error("SQLite JSON rowset joins only support :inner and :left", %{
            adapter: AdapterSupport.adapter_name(adapter) || adapter,
            join_type: spec.join_type,
            supported_join_types: [:inner, :left],
            unsupported_feature: :json_rowset
          })

        raise Error.to_exception(error)

      true ->
        {function_sql, params} = build_table_function_sql(spec.table_function)
        join_sql = if spec.join_type == :left, do: "LEFT JOIN", else: "INNER JOIN"
        {[join_sql, " ", function_sql, " AS ", spec.alias, " ON true"], params}
    end
  end

  # Build LATERAL join with correlated subquery
  defp build_subquery_lateral_join(%Spec{} = spec, join_syntax) do
    # Build the subquery - we need to pass a dummy base query since
    # the actual correlation will be resolved at SQL generation time
    dummy_base = %Selecto{domain: %{}, postgrex_opts: [], set: %{}}
    subquery = spec.subquery_builder.(dummy_base)

    # Generate SQL for the subquery
    {subquery_sql, params} = Selecto.to_sql(subquery)

    subquery_iodata =
      subquery_sql
      |> rewrite_subquery_root_alias(build_subquery_root_alias(subquery))
      |> restore_outer_ref_alias()
      |> convert_sql_placeholders_to_iodata(params)

    sql =
      case join_syntax do
        {:lateral, join_type_sql} ->
          [join_type_sql, " JOIN LATERAL (", subquery_iodata, ") AS ", spec.alias, " ON true"]

        {:apply, apply_sql} ->
          [apply_sql, " (", subquery_iodata, ") AS ", spec.alias]
      end

    # Params are now embedded as {:param, value} markers in subquery_iodata.
    {sql, []}
  end

  # Build table function SQL
  defp build_table_function_sql({:unnest, column_ref}) do
    {["UNNEST(", column_ref, ")"], []}
  end

  defp build_table_function_sql({:function, func_name, args}) do
    arg_sql = build_function_args(args)

    function_sql = [String.upcase(to_string(func_name)), "(", arg_sql, ")"]
    {function_sql, []}
  end

  defp build_table_function_sql({:json_table, source_ref, path, columns}) do
    column_sql =
      columns
      |> Enum.map(&build_json_table_column_sql/1)
      |> Enum.intersperse(", ")

    {[
       "JSON_TABLE(",
       source_ref,
       ", '",
       escape_sql_literal(path),
       "' COLUMNS (",
       column_sql,
       "))"
     ], []}
  end

  defp build_table_function_sql({function_name, source_ref, path})
       when function_name in [:json_each, :json_tree] do
    function_sql = String.upcase(to_string(function_name))

    args =
      case path do
        nil -> [source_ref]
        value -> [source_ref, ", ", "'", escape_sql_literal(value), "'"]
      end

    {[function_sql, "(", args, ")"], []}
  end

  defp build_table_function_sql(unknown) do
    raise ArgumentError, "Unknown table function specification: #{inspect(unknown)}"
  end

  # Build function arguments with parameter binding
  defp build_function_args(args) do
    args
    |> Enum.map(&build_function_arg/1)
    |> Enum.intersperse(", ")
  end

  # Build individual function argument
  defp build_function_arg({:ref, field}) do
    # Correlation reference - no parameters
    field
  end

  defp build_function_arg(value) when is_binary(value) do
    if String.contains?(value, ".") do
      # Column reference
      value
    else
      # Literal string value
      {:param, value}
    end
  end

  defp build_function_arg(value) when is_number(value) or is_boolean(value) do
    {:param, value}
  end

  defp build_function_arg({:literal, value}) do
    {:param, value}
  end

  defp build_function_arg(unknown) do
    # Fallback - treat as parameter
    {:param, unknown}
  end

  defp build_json_table_column_sql(%{name: name, for_ordinality: true}) do
    [to_string(name), " FOR ORDINALITY"]
  end

  defp build_json_table_column_sql(%{name: name} = column) do
    path = Map.get(column, :path, "$")
    type = mysql_json_table_type(Map.get(column, :type, :string))

    [
      to_string(name),
      " ",
      type,
      " PATH '",
      escape_sql_literal(path),
      "'"
    ]
  end

  defp mysql_json_table_type(:integer), do: "INTEGER"
  defp mysql_json_table_type(:decimal), do: "DECIMAL(38, 10)"
  defp mysql_json_table_type(:float), do: "DOUBLE"
  defp mysql_json_table_type(:boolean), do: "BOOLEAN"
  defp mysql_json_table_type(:date), do: "DATE"
  defp mysql_json_table_type(:naive_datetime), do: "DATETIME"
  defp mysql_json_table_type(:utc_datetime), do: "DATETIME"
  defp mysql_json_table_type(:json), do: "JSON"
  defp mysql_json_table_type(_), do: "VARCHAR(255)"

  defp escape_sql_literal(value) when is_binary(value), do: String.replace(value, "'", "''")
  defp escape_sql_literal(value), do: to_string(value)

  # Build JOIN type SQL
  defp build_join_type(:left), do: "LEFT"
  defp build_join_type(:inner), do: "INNER"
  defp build_join_type(:right), do: "RIGHT"
  defp build_join_type(:full), do: "FULL"

  defp build_join_type(unknown),
    do: raise(ArgumentError, "Unknown LATERAL join type: #{inspect(unknown)}")

  defp build_apply_join_type(:inner), do: "CROSS APPLY"
  defp build_apply_join_type(:left), do: "OUTER APPLY"

  defp build_apply_join_type(unknown) do
    error =
      Error.validation_error("MSSQL APPLY only supports :inner and :left lateral joins", %{
        adapter: :mssql,
        join_type: unknown,
        supported_join_types: [:inner, :left],
        unsupported_feature: :lateral_join
      })

    raise Error.to_exception(error)
  end

  defp join_syntax(%Spec{} = spec, adapter) do
    cond do
      adapter in [nil, ""] ->
        {:lateral, build_join_type(spec.join_type)}

      AdapterSupport.supports_feature?(adapter, :apply_join) ->
        {:apply, build_apply_join_type(spec.join_type)}

      AdapterSupport.supports_feature?(adapter, :lateral_join) ->
        {:lateral, build_join_type(spec.join_type)}

      true ->
        adapter_name = AdapterSupport.adapter_name(adapter) || adapter

        error =
          Error.validation_error("Adapter does not support lateral/apply joins", %{
            adapter: adapter_name,
            unsupported_feature: :lateral_join
          })

        raise Error.to_exception(error)
    end
  end

  @doc """
  Integrate LATERAL joins into the main SQL generation pipeline.

  This function is called by the main SQL builder to include LATERAL JOIN
  clauses in the generated SQL.
  """
  def integrate_lateral_joins_sql(base_sql_parts, lateral_specs) when is_list(lateral_specs) do
    case build_lateral_joins(lateral_specs) do
      {[], []} ->
        {base_sql_parts, []}

      {lateral_sql_parts, lateral_params} ->
        # Insert LATERAL JOINs after regular JOINs in the SQL
        updated_sql = insert_lateral_joins(base_sql_parts, lateral_sql_parts)
        {updated_sql, lateral_params}
    end
  end

  # Insert LATERAL JOIN clauses into the SQL structure
  defp insert_lateral_joins(base_sql_parts, lateral_sql_parts) do
    # Find the position after regular JOINs and before WHERE clause
    insertion_point = find_lateral_insertion_point(base_sql_parts)

    case insertion_point do
      nil ->
        # No specific insertion point found, append after FROM
        base_sql_parts ++ [" "] ++ lateral_sql_parts

      index ->
        # Insert at specific position
        {before_parts, after_parts} = Enum.split(base_sql_parts, index)
        before_parts ++ [" "] ++ lateral_sql_parts ++ [" "] ++ after_parts
    end
  end

  # Find the appropriate insertion point for LATERAL JOINs
  defp find_lateral_insertion_point(sql_parts) do
    sql_parts
    |> Enum.with_index()
    |> Enum.find_value(fn {part, index} ->
      cond do
        String.contains?(to_string(part), "WHERE") -> index
        String.contains?(to_string(part), "GROUP BY") -> index
        String.contains?(to_string(part), "HAVING") -> index
        String.contains?(to_string(part), "ORDER BY") -> index
        String.contains?(to_string(part), "LIMIT") -> index
        true -> nil
      end
    end)
  end

  # Convert SQL with $1-style placeholders to iodata markers that participate
  # in global parameter numbering.
  defp convert_sql_placeholders_to_iodata(sql, params) do
    values_by_index =
      params
      |> Enum.with_index(1)
      |> Map.new(fn {value, idx} -> {idx, value} end)

    cond do
      String.contains?(sql, "$") ->
        convert_numbered_placeholders(sql, values_by_index, ~r/(\$\d+)/, ~r/^\$(\d+)$/)

      String.contains?(sql, "@p") ->
        convert_numbered_placeholders(sql, values_by_index, ~r/(@p\d+)/, ~r/^@p(\d+)$/i)

      String.contains?(sql, "?") ->
        convert_qmark_placeholders(sql, params)

      true ->
        sql
    end
  end

  defp convert_numbered_placeholders(sql, values_by_index, split_regex, capture_regex) do
    Regex.split(split_regex, sql, include_captures: true, trim: false)
    |> Enum.map(fn part ->
      case Regex.run(capture_regex, part, capture: :all_but_first) do
        [idx] ->
          case Map.fetch(values_by_index, String.to_integer(idx)) do
            {:ok, value} -> {:param, value}
            :error -> part
          end

        _ ->
          part
      end
    end)
  end

  defp convert_qmark_placeholders(sql, params) do
    segments = String.split(sql, "?", trim: false)

    if length(segments) == length(params) + 1 do
      [first | rest] = segments

      rest
      |> Enum.zip(params)
      |> Enum.reduce([first], fn {segment, value}, acc ->
        acc ++ [{:param, value}, segment]
      end)
    else
      sql
    end
  end

  defp build_subquery_root_alias(query_selecto) do
    table_segment =
      query_selecto
      |> Selecto.source_table()
      |> normalize_alias_segment("source")

    "subq_root_#{table_segment}"
  end

  defp normalize_alias_segment(value, fallback) do
    normalized =
      value
      |> to_string()
      |> String.downcase()
      |> String.replace(~r/[^a-z0-9]+/u, "_")
      |> String.trim("_")

    if normalized == "", do: fallback, else: normalized
  end

  defp rewrite_subquery_root_alias(subquery_sql, alias_name) when is_binary(subquery_sql) do
    Regex.replace(~r/\bselecto_root\b/u, subquery_sql, alias_name)
  end

  defp restore_outer_ref_alias(subquery_sql) when is_binary(subquery_sql) do
    String.replace(subquery_sql, "__selecto_outer__.", "selecto_root.")
  end
end
