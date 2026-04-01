defmodule Selecto.ViewPublisher do
  @moduledoc """
  Validates Selecto-authored published view specs before any DDL generation.

  This keeps publication constraints separate from ordinary runtime query
  validation so domains can register stable SQL view contracts explicitly.
  """

  @type validation_result :: :ok | {:error, [String.t()]}
  @type publish_result ::
          {:ok, %{sql: String.t(), ddl: String.t(), kind: atom(), database_name: String.t()}}
          | {:error, [String.t()]}

  @spec validate(Selecto.Types.domain(), map()) :: validation_result()
  def validate(domain, spec) when is_map(domain) and is_map(spec) do
    with {:ok, query} <- build_query(domain, spec) do
      errors =
        []
        |> validate_selected_columns(query, spec)
        |> validate_runtime_params(query)

      case errors do
        [] -> :ok
        _ -> {:error, errors}
      end
    end
  end

  def validate(_domain, _spec),
    do: {:error, ["published view validation requires a domain and map spec"]}

  @spec build_sql(Selecto.Types.domain(), map()) :: publish_result()
  def build_sql(domain, spec) when is_map(domain) and is_map(spec) do
    with :ok <- validate(domain, spec),
         {:ok, query} <- build_query(domain, spec) do
      {sql, _params} = Selecto.to_sql(query)
      database_name = spec[:database_name] || spec["database_name"]
      kind = spec[:kind] || spec["kind"]

      {:ok,
       %{
         sql: sql,
         ddl: ddl_for(kind, database_name, sql),
         kind: kind,
         database_name: database_name
       }}
    end
  end

  def build_sql(_domain, _spec),
    do: {:error, ["published view SQL generation requires a domain and map spec"]}

  @spec ddl_for(atom(), String.t(), String.t()) :: String.t()
  def ddl_for(:view, database_name, sql) when is_binary(database_name) and is_binary(sql) do
    "CREATE VIEW #{database_name} AS\n#{sql};"
  end

  def ddl_for(:materialized_view, database_name, sql)
      when is_binary(database_name) and is_binary(sql) do
    "CREATE MATERIALIZED VIEW #{database_name} AS\n#{sql};"
  end

  defp build_query(domain, spec) do
    query_builder = spec[:query] || spec["query"]
    selecto = Selecto.configure(domain, :view_publisher, validate: false)

    case query_builder.(selecto) do
      %Selecto{} = query -> {:ok, query}
      other -> {:error, [":query must return a Selecto struct, got: #{inspect(other)}"]}
    end
  end

  defp validate_selected_columns(errors, query, spec) do
    declared_columns = declared_column_names(spec)
    aliases = query_aliases(query)

    cond do
      aliases == [] ->
        errors ++ ["published view queries must select stable aliased columns"]

      Enum.sort(aliases) != Enum.sort(declared_columns) ->
        errors ++
          [
            "declared :columns #{inspect(declared_columns)} must exactly match query aliases #{inspect(aliases)}"
          ]

      true ->
        errors
    end
  end

  defp validate_runtime_params(errors, query) do
    {_sql, params} = Selecto.to_sql(query)

    if params == [] do
      errors
    else
      errors ++ ["published view queries cannot depend on runtime bind params"]
    end
  end

  defp declared_column_names(spec) do
    spec
    |> published_columns()
    |> Map.keys()
    |> Enum.map(&to_string/1)
  end

  defp published_columns(spec) do
    spec[:columns] || spec["columns"] || %{}
  end

  defp query_aliases(query) do
    {_sql, aliases, _params} = Selecto.gen_sql(query, [])

    aliases
    |> List.wrap()
    |> Enum.map(&to_string/1)
  end
end
