defmodule Selecto.Domain.FieldBindings do
  @moduledoc false

  alias Selecto.Domain.Shared.Map, as: MapHelpers

  def field_choice_bindings(normalized) do
    []
    |> Kernel.++(
      relation_field_choice_bindings(:source, Map.get(normalized, :source), [:source, :columns])
    )
    |> Kernel.++(schema_field_choice_bindings(Map.get(normalized, :schemas)))
    |> Kernel.++(
      column_field_choice_bindings(
        MapHelpers.map_value(Map.get(normalized, :projection, %{}), :columns),
        [:columns],
        & &1
      )
    )
    |> Enum.sort_by(
      &{MapHelpers.field_id(&1.field), MapHelpers.field_id(&1.choice_source), inspect(&1.path)}
    )
  end

  def schema_field_choice_bindings(schemas) when is_map(schemas) do
    schemas
    |> MapHelpers.sorted_entries()
    |> Enum.flat_map(fn {schema_id, schema} ->
      relation_field_choice_bindings(schema_id, schema, [:schemas, schema_id, :columns])
    end)
  end

  def schema_field_choice_bindings(_schemas), do: []

  def relation_field_choice_bindings(relation_id, relation, path) when is_map(relation) do
    relation
    |> MapHelpers.map_value(:columns)
    |> column_field_choice_bindings(path, &MapHelpers.relation_field_ref(relation_id, &1))
  end

  def relation_field_choice_bindings(_relation_id, _relation, _path), do: []

  def column_field_choice_bindings(columns, path, field_ref_fun) when is_map(columns) do
    columns
    |> MapHelpers.sorted_entries()
    |> Enum.flat_map(fn {field, column} ->
      column_field_choice_binding(field_ref_fun.(field), column, path ++ [field])
    end)
  end

  def column_field_choice_bindings(_columns, _path, _field_ref_fun), do: []

  def column_field_choice_binding(field, column, path) when is_map(column) do
    compact_choice_source = MapHelpers.id_value(MapHelpers.map_value(column, :choice_source))

    reference_choice_source =
      case MapHelpers.map_value(column, :reference) do
        reference when is_map(reference) ->
          MapHelpers.id_value(MapHelpers.map_value(reference, :choice_source))

        _reference ->
          nil
      end

    cond do
      is_nil(compact_choice_source) and is_nil(reference_choice_source) ->
        []

      is_nil(reference_choice_source) or compact_choice_source == reference_choice_source ->
        [
          %{
            field: field,
            choice_source: compact_choice_source,
            compact?: not is_nil(compact_choice_source),
            reference?: not is_nil(reference_choice_source),
            path: path
          }
        ]

      is_nil(compact_choice_source) ->
        [
          %{
            field: field,
            choice_source: reference_choice_source,
            compact?: false,
            reference?: true,
            path: path ++ [:reference]
          }
        ]

      true ->
        [
          %{
            field: field,
            choice_source: compact_choice_source,
            compact?: true,
            reference?: false,
            path: path
          },
          %{
            field: field,
            choice_source: reference_choice_source,
            compact?: false,
            reference?: true,
            path: path ++ [:reference]
          }
        ]
    end
  end

  def column_field_choice_binding(_field, _column, _path), do: []
end
