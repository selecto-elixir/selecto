defmodule Selecto.Domain.Contract.PublishedViews do
  @moduledoc false

  alias Selecto.Domain.Contract.Shared.Core

  def validate(errors, query) do
    validate_published_views(errors, query)
  end

  def validate_published_views(errors, query) do
    case Core.map_value(query, :published_views) do
      nil ->
        errors

      published_views when is_map(published_views) ->
        Enum.reduce(published_views, errors, fn {view_id, view_spec}, acc ->
          acc
          |> validate_published_view_id(view_id)
          |> validate_published_view_spec(view_id, view_spec)
        end)

      published_views ->
        [
          Core.error(
            :invalid_section_shape,
            [:published_views],
            "domain section :published_views must be a map",
            expected: :map,
            actual: Core.value_type(published_views)
          )
          | errors
        ]
    end
  end

  def validate_published_view_id(errors, view_id) do
    if Core.non_empty_atom_or_string?(view_id) do
      errors
    else
      [
        Core.error(
          :invalid_published_view_id,
          [:published_views, view_id],
          "published view id #{inspect(view_id)} must be a non-empty atom or string",
          expected: "non-empty atom or string",
          actual: Core.value_type(view_id),
          view: view_id
        )
        | errors
      ]
    end
  end

  def validate_published_view_spec(errors, view_id, view_spec) when is_map(view_spec) do
    errors
    |> validate_published_view_database_name(view_id, view_spec)
    |> validate_published_view_kind(view_id, view_spec)
    |> validate_published_view_query(view_id, view_spec)
    |> validate_published_view_columns(view_id, view_spec)
    |> validate_published_view_indexes(view_id, view_spec)
    |> validate_published_view_refresh(view_id, view_spec)
  end

  def validate_published_view_spec(errors, view_id, view_spec) do
    [
      Core.error(
        :invalid_published_view_spec,
        [:published_views, view_id],
        "published view #{inspect(view_id)} spec must be a map",
        expected: :map,
        actual: Core.value_type(view_spec),
        view: view_id
      )
      | errors
    ]
  end

  def validate_published_view_database_name(errors, view_id, view_spec) do
    database_name = Core.map_value(view_spec, :database_name)

    if is_binary(database_name) and String.trim(database_name) != "" do
      errors
    else
      [
        Core.error(
          :invalid_published_view_database_name,
          [:published_views, view_id, :database_name],
          "published view #{inspect(view_id)} database_name must be a non-empty string",
          expected: "non-empty string",
          actual: Core.value_type(database_name),
          view: view_id,
          database_name: database_name
        )
        | errors
      ]
    end
  end

  def validate_published_view_kind(errors, view_id, view_spec) do
    case Core.map_value(view_spec, :kind) do
      kind when kind in [:view, :materialized_view] ->
        errors

      kind ->
        [
          Core.error(
            :invalid_published_view_kind,
            [:published_views, view_id, :kind],
            "published view #{inspect(view_id)} kind must be :view or :materialized_view",
            expected: [:view, :materialized_view],
            actual: Core.value_type(kind),
            view: view_id,
            kind: kind
          )
          | errors
        ]
    end
  end

  def validate_published_view_query(errors, view_id, view_spec) do
    query = Core.map_value(view_spec, :query)

    if Core.valid_arity?(query, [1]) do
      errors
    else
      [
        Core.error(
          :invalid_published_view_query,
          [:published_views, view_id, :query],
          "published view #{inspect(view_id)} query must be a function with arity 1",
          expected: "function with arity 1",
          actual: Core.value_type(query),
          view: view_id
        )
        | errors
      ]
    end
  end

  def validate_published_view_columns(errors, view_id, view_spec) do
    case Core.map_value(view_spec, :columns) do
      columns when is_map(columns) and map_size(columns) > 0 ->
        Enum.reduce(columns, errors, fn {column_id, column_spec}, acc ->
          validate_published_view_column(acc, view_id, column_id, column_spec)
        end)

      columns ->
        [
          Core.error(
            :invalid_published_view_columns,
            [:published_views, view_id, :columns],
            "published view #{inspect(view_id)} columns must be a non-empty map",
            expected: "non-empty map",
            actual: Core.value_type(columns),
            view: view_id
          )
          | errors
        ]
    end
  end

  def validate_published_view_column(errors, view_id, column_id, column_spec) do
    errors
    |> validate_published_view_column_id(view_id, column_id)
    |> validate_published_view_column_spec(view_id, column_id, column_spec)
  end

  def validate_published_view_column_id(errors, view_id, column_id) do
    if Core.non_empty_atom_or_string?(column_id) do
      errors
    else
      [
        Core.error(
          :invalid_published_view_column,
          [:published_views, view_id, :columns, column_id],
          "published view #{inspect(view_id)} column ids must be non-empty atoms or strings",
          expected: "non-empty atom or string",
          actual: Core.value_type(column_id),
          view: view_id,
          column: column_id
        )
        | errors
      ]
    end
  end

  def validate_published_view_column_spec(errors, view_id, column_id, column_spec) do
    if is_map(column_spec) do
      errors
    else
      [
        Core.error(
          :invalid_published_view_column,
          [:published_views, view_id, :columns, column_id],
          "published view #{inspect(view_id)} column #{inspect(column_id)} spec must be a map",
          expected: :map,
          actual: Core.value_type(column_spec),
          view: view_id,
          column: column_id
        )
        | errors
      ]
    end
  end

  def validate_published_view_indexes(errors, view_id, view_spec) do
    case Core.fetch_map_value(view_spec, :indexes) do
      :__missing__ ->
        errors

      nil ->
        errors

      indexes when is_list(indexes) ->
        indexes
        |> Enum.with_index()
        |> Enum.reduce(errors, fn {index_spec, index}, acc ->
          validate_published_view_index(acc, view_id, index_spec, index)
        end)

      indexes ->
        [
          Core.error(
            :invalid_published_view_indexes,
            [:published_views, view_id, :indexes],
            "published view #{inspect(view_id)} indexes must be a list when provided",
            expected: :list,
            actual: Core.value_type(indexes),
            view: view_id
          )
          | errors
        ]
    end
  end

  def validate_published_view_index(errors, view_id, index_spec, index)
      when is_map(index_spec) do
    errors
    |> validate_published_view_index_columns(view_id, index_spec, index)
    |> validate_published_view_index_boolean(view_id, index_spec, index, :unique)
    |> validate_published_view_index_boolean(view_id, index_spec, index, :concurrently)
  end

  def validate_published_view_index(errors, view_id, index_spec, index) do
    [
      Core.error(
        :invalid_published_view_index,
        [:published_views, view_id, :indexes, index],
        "published view #{inspect(view_id)} index specs must be maps",
        expected: :map,
        actual: Core.value_type(index_spec),
        view: view_id
      )
      | errors
    ]
  end

  def validate_published_view_index_columns(errors, view_id, index_spec, index) do
    columns = Core.map_value(index_spec, :columns)

    if is_list(columns) and columns != [] and
         Enum.all?(columns, &Core.non_empty_atom_or_string?/1) do
      errors
    else
      [
        Core.error(
          :invalid_published_view_index_columns,
          [:published_views, view_id, :indexes, index, :columns],
          "published view #{inspect(view_id)} index columns must be a non-empty list of atoms or strings",
          expected: "non-empty list of atoms or strings",
          actual: Core.value_type(columns),
          view: view_id,
          columns: columns
        )
        | errors
      ]
    end
  end

  def validate_published_view_index_boolean(errors, view_id, index_spec, index, key) do
    case Core.fetch_map_value(index_spec, key) do
      :__missing__ ->
        errors

      nil ->
        errors

      value when is_boolean(value) ->
        errors

      value ->
        [
          Core.error(
            :invalid_published_view_index_option,
            [:published_views, view_id, :indexes, index, key],
            "published view #{inspect(view_id)} index #{key} must be boolean when provided",
            expected: :boolean,
            actual: Core.value_type(value),
            view: view_id,
            option: key
          )
          | errors
        ]
    end
  end

  def validate_published_view_refresh(errors, view_id, view_spec) do
    case Core.fetch_map_value(view_spec, :refresh) do
      :__missing__ ->
        errors

      nil ->
        errors

      refresh when is_map(refresh) ->
        errors

      refresh ->
        [
          Core.error(
            :invalid_published_view_refresh,
            [:published_views, view_id, :refresh],
            "published view #{inspect(view_id)} refresh must be a map when provided",
            expected: :map,
            actual: Core.value_type(refresh),
            view: view_id
          )
          | errors
        ]
    end
  end
end
