defmodule Selecto.TextSearch do
  @moduledoc false

  alias Selecto.Builder.Sql.Helpers

  def text_search_rank(selecto, fields, opts \\ [])

  def text_search_rank(selecto, fields, opts) when is_map(opts) do
    text_search_rank(selecto, fields, Enum.into(opts, []))
  end

  def text_search_rank(selecto, fields, opts) when is_list(opts) do
    case Selecto.AdapterSupport.adapter_name(Map.get(selecto, :adapter)) do
      :mysql ->
        mysql_text_search_rank(selecto, fields, opts)

      :postgresql ->
        postgresql_text_search_rank(selecto, fields, opts)

      :sqlite ->
        sqlite_fts_rank(selecto, fields, opts)

      adapter_name ->
        raise ArgumentError,
              "text_search_rank/3 is not yet implemented for adapter #{inspect(adapter_name)}"
    end
  end

  @doc false
  def mysql_text_search_rank(selecto, fields, opts) when is_list(opts) do
    normalized_fields = Enum.map(List.wrap(fields), &to_string/1)

    if normalized_fields == [] do
      raise ArgumentError, "mysql text_search_rank/3 requires at least one field"
    end

    alias_name = Keyword.get(opts, :as, "fts_rank")
    query = Keyword.get(opts, :query)
    mode = Keyword.get(opts, :mode, :natural)

    if is_nil(query) do
      raise ArgumentError, "mysql text_search_rank/3 requires a :query option"
    end

    if Keyword.has_key?(opts, :weights) do
      raise ArgumentError, "mysql text_search_rank/3 does not support :weights yet"
    end

    match_args =
      normalized_fields
      |> Enum.map(&mysql_text_search_field_reference!(selecto, &1))
      |> Enum.intersperse(", ")

    selector =
      {:custom_sql,
       [
         "MATCH(",
         match_args,
         ") AGAINST (",
         {:param, query},
         mysql_rank_mode_sql(selecto, mode),
         ") AS ",
         quoted_alias!(selecto, alias_name)
       ], %{}}

    put_in(selecto.set[:selected], Enum.uniq(selecto.set.selected ++ [selector]))
  end

  @doc false
  def postgresql_text_search_rank(selecto, fields, opts) when is_list(opts) do
    normalized_fields = List.wrap(fields)

    if length(normalized_fields) != 1 do
      raise ArgumentError,
            "postgresql text_search_rank/3 currently requires exactly one tsvector field"
    end

    [field] = normalized_fields
    conf = postgresql_text_search_rank_field_conf!(selecto, field)
    alias_name = Keyword.get(opts, :as, "fts_rank")
    query = Keyword.get(opts, :query)
    mode = Keyword.get(opts, :mode, :websearch)

    if is_nil(query) do
      raise ArgumentError, "postgresql text_search_rank/3 requires a :query option"
    end

    if Keyword.has_key?(opts, :weights) do
      raise ArgumentError, "postgresql text_search_rank/3 does not support :weights yet"
    end

    query_function = postgresql_text_search_query_function!(mode)
    field_ref = Map.get(conf, :field, field)

    selector =
      {:field,
       {:func, :ts_rank, [to_string(field_ref), {:func, query_function, [{:literal, query}]}]},
       to_string(alias_name)}

    put_in(selecto.set[:selected], Enum.uniq(selecto.set.selected ++ [selector]))
  end

  @doc """
  Add a SQLite FTS5 ranking selector using `bm25(...)`.

  This helper is intentionally narrow: all referenced fields must be configured
  as SQLite FTS5 fields on the same source alias.
  """
  @spec sqlite_fts_rank(
          Selecto.t(),
          atom() | String.t() | [atom() | String.t()],
          keyword() | map()
        ) :: Selecto.t()
  def sqlite_fts_rank(selecto, fields, opts \\ [])

  def sqlite_fts_rank(selecto, fields, opts) when is_map(opts) do
    sqlite_fts_rank(selecto, fields, Enum.into(opts, []))
  end

  def sqlite_fts_rank(selecto, fields, opts) when is_list(opts) do
    adapter = Map.get(selecto, :adapter)

    if Selecto.AdapterSupport.adapter_name(adapter) != :sqlite do
      raise ArgumentError, "sqlite_fts_rank/3 requires the SQLite adapter"
    end

    normalized_fields = List.wrap(fields)

    if normalized_fields == [] do
      raise ArgumentError, "sqlite_fts_rank/3 requires at least one FTS field"
    end

    alias_name = Keyword.get(opts, :as, "fts_rank")
    weights = Keyword.get(opts, :weights, [])
    source_table = sqlite_fts_rank_source_table!(selecto, normalized_fields)
    weight_args = sqlite_fts_rank_weight_args!(weights)

    selector =
      {:custom_sql,
       [
         "bm25(",
         Helpers.quote_identifier(selecto, source_table),
         weight_args,
         ") AS ",
         quoted_alias!(selecto, alias_name)
       ], %{}}

    put_in(selecto.set[:selected], Enum.uniq(selecto.set.selected ++ [selector]))
  end

  defp sqlite_fts_rank_source_table!(selecto, fields) do
    fields
    |> Enum.map(&sqlite_fts_rank_field_conf!(selecto, &1))
    |> Enum.map(fn conf -> Map.get(conf, :requires_join, :selecto_root) end)
    |> Enum.map(fn
      :selecto_root -> "selecto_root"
      value -> to_string(value)
    end)
    |> Enum.uniq()
    |> case do
      ["selecto_root"] ->
        selecto.domain.source.source_table

      [alias_name] ->
        raise ArgumentError,
              "sqlite_fts_rank/3 currently supports only root-source FTS tables, got: #{inspect(alias_name)}"

      aliases ->
        raise ArgumentError,
              "sqlite_fts_rank/3 requires FTS fields from one source alias, got: #{inspect(aliases)}"
    end
  end

  defp sqlite_fts_rank_field_conf!(selecto, field) do
    field_key = to_string(field)
    conf = root_column_conf(selecto, field_key)

    cond do
      is_nil(conf) ->
        raise ArgumentError, "sqlite_fts_rank/3 field not found: #{inspect(field)}"

      Map.get(conf, :type) == :fts5 or Map.get(conf, :sqlite_fts5) == true or
          Map.get(conf, :text_search_backend) == :fts5 ->
        conf

      true ->
        raise ArgumentError,
              "sqlite_fts_rank/3 field is not configured for SQLite FTS5: #{inspect(field)}"
    end
  end

  defp postgresql_text_search_rank_field_conf!(selecto, field) do
    field_key = to_string(field)
    conf = root_column_conf(selecto, field_key)

    cond do
      is_nil(conf) ->
        raise ArgumentError, "postgresql text_search_rank/3 field not found: #{inspect(field)}"

      Map.get(conf, :type) == :tsvector or Map.get(conf, :text_search_backend) == :postgresql ->
        conf

      true ->
        raise ArgumentError,
              "postgresql text_search_rank/3 field is not configured for PostgreSQL text search: #{inspect(field)}"
    end
  end

  defp postgresql_text_search_query_function!(:web), do: :websearch_to_tsquery
  defp postgresql_text_search_query_function!(:websearch), do: :websearch_to_tsquery
  defp postgresql_text_search_query_function!(:plain), do: :plainto_tsquery
  defp postgresql_text_search_query_function!(:natural), do: :plainto_tsquery
  defp postgresql_text_search_query_function!(:phrase), do: :phraseto_tsquery
  defp postgresql_text_search_query_function!(:boolean), do: :to_tsquery

  defp postgresql_text_search_query_function!(mode) do
    raise ArgumentError, "postgresql text_search_rank/3 does not support mode #{inspect(mode)}"
  end

  defp mysql_rank_mode_sql(selecto, mode) do
    adapter = Map.get(selecto, :adapter)

    case mode do
      nil ->
        " IN NATURAL LANGUAGE MODE"

      :web ->
        " IN NATURAL LANGUAGE MODE"

      :websearch ->
        " IN NATURAL LANGUAGE MODE"

      :plain ->
        " IN NATURAL LANGUAGE MODE"

      :natural ->
        " IN NATURAL LANGUAGE MODE"

      :boolean ->
        if Selecto.AdapterSupport.supports_feature?(adapter, :text_search_boolean) do
          " IN BOOLEAN MODE"
        else
          raise ArgumentError, "mysql text_search_rank/3 requires boolean text search support"
        end

      :query_expansion ->
        if Selecto.AdapterSupport.supports_feature?(adapter, :text_search_query_expansion) do
          " IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION"
        else
          raise ArgumentError,
                "mysql text_search_rank/3 requires query expansion text search support"
        end

      :phrase ->
        raise ArgumentError, "mysql text_search_rank/3 does not support :phrase"

      other ->
        raise ArgumentError, "mysql text_search_rank/3 does not support mode #{inspect(other)}"
    end
  end

  defp mysql_text_search_field_reference!(selecto, field) do
    field_key = to_string(field)

    if is_nil(root_column_conf(selecto, field_key)) do
      raise ArgumentError, "mysql text_search_rank/3 field not found: #{inspect(field)}"
    end

    [
      Helpers.quote_identifier(selecto, "selecto_root"),
      ".",
      Helpers.quote_identifier(selecto, field_key)
    ]
  end

  defp sqlite_fts_rank_weight_args!([]), do: []

  defp sqlite_fts_rank_weight_args!(weights) when is_list(weights) do
    weights
    |> Enum.map(&sqlite_fts_rank_weight_sql!/1)
    |> case do
      [] -> []
      weight_sql -> [", ", Enum.intersperse(weight_sql, ", ")]
    end
  end

  defp sqlite_fts_rank_weight_args!(weights) do
    raise ArgumentError,
          "sqlite_fts_rank/3 :weights must be a list of numbers, got: #{inspect(weights)}"
  end

  defp sqlite_fts_rank_weight_sql!(weight) when is_integer(weight), do: Integer.to_string(weight)

  defp sqlite_fts_rank_weight_sql!(weight) when is_float(weight), do: Float.to_string(weight)

  defp sqlite_fts_rank_weight_sql!(weight) do
    raise ArgumentError,
          "sqlite_fts_rank/3 :weights must contain only numbers, got: #{inspect(weight)}"
  end

  defp quoted_alias!(selecto, alias_name) do
    alias_string = to_string(alias_name)
    _validated = Helpers.quote_identifier(selecto, alias_string)
    Helpers.force_quote_identifier(selecto, alias_string)
  end

  defp root_column_conf(selecto, field_key) do
    columns =
      selecto
      |> Map.get(:config, %{})
      |> Map.get(:columns, %{})

    Map.get(columns, field_key) || Map.get(columns, safe_existing_atom(field_key))
  end

  defp safe_existing_atom(value) when is_binary(value) do
    try do
      String.to_existing_atom(value)
    rescue
      ArgumentError -> nil
    end
  end

  defp safe_existing_atom(_value), do: nil
end
