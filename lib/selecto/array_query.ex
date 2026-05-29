defmodule Selecto.ArrayQuery do
  @moduledoc false

  @spec select(Selecto.t(), list() | tuple(), keyword()) :: Selecto.t()
  def select(selecto, array_operations, opts \\ [])

  def select(selecto, array_operations, _opts) when is_list(array_operations) do
    array_specs = Enum.map(array_operations, &build_select_spec/1)
    append_specs(selecto, :array_operations, array_specs)
  end

  def select(selecto, array_operation, opts) do
    select(selecto, [array_operation], opts)
  end

  @spec filter(Selecto.t(), list() | tuple(), keyword()) :: Selecto.t()
  def filter(selecto, array_filters, opts \\ [])

  def filter(selecto, array_filters, _opts) when is_list(array_filters) do
    array_specs =
      Enum.map(array_filters, fn
        {operation, column, value} ->
          Selecto.Advanced.ArrayOperations.create_array_filter(operation, column, value)

        spec ->
          spec
      end)

    selecto
    |> validate_specs!(array_specs)
    |> append_specs(:array_filters, array_specs)
  end

  def filter(selecto, array_filter, opts) do
    filter(selecto, [array_filter], opts)
  end

  @spec manipulate(Selecto.t(), list() | tuple(), keyword()) :: Selecto.t()
  def manipulate(selecto, array_operations, opts \\ [])

  def manipulate(selecto, array_operations, _opts) when is_list(array_operations) do
    array_specs = Enum.map(array_operations, &build_manipulation_spec/1)

    selecto
    |> validate_specs!(array_specs)
    |> append_specs(:array_operations, array_specs)
  end

  def manipulate(selecto, array_operation, opts) do
    manipulate(selecto, [array_operation], opts)
  end

  defp build_select_spec({:array_agg, column, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_operation(:array_agg, column, opts)
  end

  defp build_select_spec({:array_agg_distinct, column, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_operation(:array_agg_distinct, column, opts)
  end

  defp build_select_spec({:string_agg, column, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_operation(:string_agg, column, opts)
  end

  defp build_select_spec({:array_length, column, dimension, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_size(:array_length, column, dimension, opts)
  end

  defp build_select_spec({:cardinality, column, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_size(:cardinality, column, nil, opts)
  end

  defp build_select_spec({:array_ndims, column, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_size(:array_ndims, column, nil, opts)
  end

  defp build_select_spec({:array_dims, column, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_size(:array_dims, column, nil, opts)
  end

  defp build_select_spec({:array_append, column, value, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_operation(
      :array_append,
      column,
      Keyword.put(opts, :value, value)
    )
  end

  defp build_select_spec({:array_prepend, column, value, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_operation(
      :array_prepend,
      column,
      Keyword.put(opts, :value, value)
    )
  end

  defp build_select_spec({:array_remove, column, value, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_operation(
      :array_remove,
      column,
      Keyword.put(opts, :value, value)
    )
  end

  defp build_select_spec({:array_replace, column, old_value, new_value, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_operation(
      :array_replace,
      column,
      opts |> Keyword.put(:value, old_value) |> Keyword.put(:new_value, new_value)
    )
  end

  defp build_select_spec({:array_cat, column, value, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_operation(
      :array_cat,
      column,
      Keyword.put(opts, :value, value)
    )
  end

  defp build_select_spec({:array_position, column, value, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_operation(
      :array_position,
      column,
      Keyword.put(opts, :value, value)
    )
  end

  defp build_select_spec({:array_positions, column, value, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_operation(
      :array_positions,
      column,
      Keyword.put(opts, :value, value)
    )
  end

  defp build_select_spec({:array_to_string, column, delimiter, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_operation(
      :array_to_string,
      column,
      Keyword.put(opts, :value, delimiter)
    )
  end

  defp build_select_spec({:string_to_array, column, delimiter, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_operation(
      :string_to_array,
      column,
      Keyword.put(opts, :value, delimiter)
    )
  end

  defp build_select_spec({:array, elements, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_operation(
      :array,
      nil,
      Keyword.put(opts, :value, elements)
    )
  end

  defp build_select_spec({operation, column, opts}) when is_atom(operation) and is_list(opts) do
    Selecto.Advanced.ArrayOperations.create_array_operation(operation, column, opts)
  end

  defp build_select_spec(spec), do: spec

  defp build_manipulation_spec({:array_append, column, value, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_operation(
      :array_append,
      column,
      Keyword.put(opts, :value, value)
    )
  end

  defp build_manipulation_spec({:array_prepend, column, value, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_operation(
      :array_prepend,
      column,
      Keyword.put(opts, :value, value)
    )
  end

  defp build_manipulation_spec({:array_remove, column, value, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_operation(
      :array_remove,
      column,
      Keyword.put(opts, :value, value)
    )
  end

  defp build_manipulation_spec({:array_replace, column, old_value, new_value, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_operation(
      :array_replace,
      column,
      opts |> Keyword.put(:value, old_value) |> Keyword.put(:new_value, new_value)
    )
  end

  defp build_manipulation_spec({:array_to_string, column, delimiter, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_operation(
      :array_to_string,
      column,
      Keyword.put(opts, :value, delimiter)
    )
  end

  defp build_manipulation_spec({:string_to_array, column, delimiter, opts}) do
    Selecto.Advanced.ArrayOperations.create_array_operation(
      :string_to_array,
      column,
      Keyword.put(opts, :value, delimiter)
    )
  end

  defp build_manipulation_spec({operation, column, opts}) when is_atom(operation) do
    Selecto.Advanced.ArrayOperations.create_array_operation(operation, column, opts)
  end

  defp build_manipulation_spec(spec), do: spec

  defp validate_specs!(selecto, specs) do
    Selecto.QueryValidator.validate_array_specs!(selecto, specs)
    selecto
  end

  defp append_specs(selecto, key, specs) do
    current = Map.get(selecto.set, key, [])
    put_in(selecto.set[key], current ++ specs)
  end
end
