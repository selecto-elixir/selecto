defmodule Selecto.JsonQuery do
  @moduledoc false

  @spec select(Selecto.t(), list() | tuple(), keyword()) :: Selecto.t()
  def select(selecto, json_operations, opts \\ [])

  def select(selecto, json_operations, _opts) when is_list(json_operations) do
    json_specs = Enum.map(json_operations, &build_select_spec/1)

    selecto
    |> validate_specs!(json_specs)
    |> append_specs(:json_selects, json_specs)
  end

  def select(selecto, json_operation, opts) do
    select(selecto, [json_operation], opts)
  end

  @spec filter(Selecto.t(), list() | tuple(), keyword()) :: Selecto.t()
  def filter(selecto, json_filters, opts \\ [])

  def filter(selecto, json_filters, _opts) when is_list(json_filters) do
    json_specs = Enum.map(json_filters, &build_filter_spec/1)

    selecto
    |> validate_specs!(json_specs)
    |> append_specs(:json_filters, json_specs)
  end

  def filter(selecto, json_filter, opts) do
    filter(selecto, [json_filter], opts)
  end

  @spec order_by(Selecto.t(), list() | tuple(), keyword()) :: Selecto.t()
  def order_by(selecto, json_sorts, opts \\ [])

  def order_by(selecto, json_sorts, _opts) when is_list(json_sorts) do
    json_specs =
      Enum.map(json_sorts, fn
        {operation, column, path, direction} when is_binary(path) ->
          spec =
            Selecto.Advanced.JsonOperations.create_json_operation(operation, column, path: path)

          {spec, direction || :asc}

        {operation, column, direction} when direction in [:asc, :desc] ->
          spec = Selecto.Advanced.JsonOperations.create_json_operation(operation, column)
          {spec, direction}

        {operation, column, path} when is_binary(path) ->
          spec =
            Selecto.Advanced.JsonOperations.create_json_operation(operation, column, path: path)

          {spec, :asc}

        {operation, column} ->
          spec = Selecto.Advanced.JsonOperations.create_json_operation(operation, column)
          {spec, :asc}
      end)

    selecto
    |> validate_specs!(Enum.map(json_specs, fn {spec, _direction} -> spec end))
    |> append_specs(:json_order_by, json_specs)
  end

  def order_by(selecto, json_sort, opts) do
    order_by(selecto, [json_sort], opts)
  end

  defp build_select_spec({operation, column, path_or_opts}) when is_binary(path_or_opts) do
    Selecto.Advanced.JsonOperations.create_json_operation(operation, column, path: path_or_opts)
  end

  defp build_select_spec({operation, column, path, opts}) when is_binary(path) do
    Selecto.Advanced.JsonOperations.create_json_operation(operation, column, [path: path] ++ opts)
  end

  defp build_select_spec({operation, column, opts}) when is_list(opts) do
    Selecto.Advanced.JsonOperations.create_json_operation(operation, column, opts)
  end

  defp build_select_spec({operation, column}) do
    Selecto.Advanced.JsonOperations.create_json_operation(operation, column)
  end

  defp build_filter_spec({operation, column, path_or_value, comparison})
       when is_binary(path_or_value) do
    Selecto.Advanced.JsonOperations.create_json_operation(operation, column,
      path: path_or_value,
      comparison: comparison
    )
  end

  defp build_filter_spec({operation, column, path})
       when operation in [:json_extract, :json_extract_text, :json_exists, :json_path_exists] and
              is_binary(path) do
    Selecto.Advanced.JsonOperations.create_json_operation(operation, column, path: path)
  end

  defp build_filter_spec({operation, column, value}) do
    Selecto.Advanced.JsonOperations.create_json_operation(operation, column, value: value)
  end

  defp build_filter_spec({operation, column}) do
    Selecto.Advanced.JsonOperations.create_json_operation(operation, column)
  end

  defp validate_specs!(selecto, specs) do
    Selecto.QueryValidator.validate_json_specs!(selecto, specs)
    selecto
  end

  defp append_specs(selecto, key, specs) do
    current = Map.get(selecto.set, key, [])
    put_in(selecto.set[key], current ++ specs)
  end
end
