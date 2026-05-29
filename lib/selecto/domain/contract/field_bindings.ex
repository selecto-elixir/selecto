defmodule Selecto.Domain.Contract.FieldBindings do
  @moduledoc false

  alias Selecto.Domain.Contract.Shared.Core
  alias Selecto.Domain.Contract.Shared.IdValue, as: IdValue

  def validate(errors, source, schemas, projection, choice_sources, field_index) do
    validate_field_choice_source_bindings(
      errors,
      source,
      schemas,
      projection,
      choice_sources,
      field_index
    )
  end

  def validate_field_choice_source_bindings(
         errors,
         _source,
         _schemas,
         _projection,
         choice_sources,
         _field_index
       )
       when not is_map(choice_sources),
       do: errors

  def validate_field_choice_source_bindings(
         errors,
         source,
         schemas,
         projection,
         choice_sources,
         field_index
       ) do
    errors
    |> validate_relation_choice_source_bindings(
      :source,
      source,
      [:source, :columns],
      choice_sources,
      field_index
    )
    |> validate_schema_choice_source_bindings(schemas, choice_sources, field_index)
    |> validate_projection_choice_source_bindings(projection, choice_sources, field_index)
  end

  def validate_schema_choice_source_bindings(errors, schemas, choice_sources, field_index)
       when is_map(schemas) do
    Enum.reduce(schemas, errors, fn {schema_id, schema}, acc ->
      validate_relation_choice_source_bindings(
        acc,
        schema_id,
        schema,
        [:schemas, schema_id, :columns],
        choice_sources,
        field_index
      )
    end)
  end

  def validate_schema_choice_source_bindings(errors, _schemas, _choice_sources, _field_index) do
    errors
  end

  def validate_projection_choice_source_bindings(errors, projection, choice_sources, field_index) do
    case Core.map_value(projection, :columns) do
      columns when is_map(columns) ->
        validate_field_choice_source_bindings_in_columns(
          errors,
          columns,
          [:columns],
          choice_sources,
          field_index,
          & &1
        )

      _ ->
        errors
    end
  end

  def validate_relation_choice_source_bindings(
         errors,
         relation_id,
         relation,
         path,
         choice_sources,
         field_index
       )
       when is_map(relation) do
    case Core.map_value(relation, :columns) do
      columns when is_map(columns) ->
        validate_field_choice_source_bindings_in_columns(
          errors,
          columns,
          path,
          choice_sources,
          field_index,
          &Core.relation_field_ref(relation_id, &1)
        )

      _ ->
        errors
    end
  end

  def validate_relation_choice_source_bindings(
         errors,
         _relation_id,
         _relation,
         _path,
         _choice_sources,
         _field_index
       ) do
    errors
  end

  def validate_field_choice_source_bindings_in_columns(
         errors,
         columns,
         path,
         choice_sources,
         field_index,
         field_ref_fun
       ) do
    Enum.reduce(columns, errors, fn {field, column}, acc ->
      field_ref = field_ref_fun.(field)
      column_path = path ++ [field]

      acc
      |> validate_column_choice_source(field_ref, column, column_path, choice_sources)
      |> validate_column_reference_binding(
        field_ref,
        column,
        column_path,
        choice_sources,
        field_index
      )
    end)
  end

  def validate_column_choice_source(errors, _field, column, _path, _choice_sources)
       when not is_map(column),
       do: errors

  def validate_column_choice_source(errors, field, column, path, choice_sources) do
    case Core.map_value(column, :choice_source) do
      nil ->
        errors

      choice_source when is_atom(choice_source) or is_binary(choice_source) ->
        if Core.fetch_key(choice_sources, choice_source) != :error do
          errors
        else
          [
            Core.error(
              :field_choice_source_not_found,
              path ++ [:choice_source],
              "field #{inspect(field)} references missing choice source #{inspect(choice_source)}",
              field: field,
              choice_source: choice_source
            )
            | errors
          ]
        end

      choice_source ->
        [
          Core.error(
            :invalid_field_choice_source,
            path ++ [:choice_source],
            "field #{inspect(field)} choice_source must be an atom or string",
            expected: "atom or string",
            actual: Core.value_type(choice_source),
            field: field,
            choice_source: choice_source
          )
          | errors
        ]
    end
  end

  def validate_column_reference_binding(
         errors,
         _field,
         column,
         _path,
         _choice_sources,
         _field_index
       )
       when not is_map(column),
       do: errors

  def validate_column_reference_binding(errors, field, column, path, choice_sources, field_index) do
    case Core.map_value(column, :reference) do
      nil ->
        errors

      reference when is_map(reference) ->
        reference_path = path ++ [:reference]

        errors
        |> validate_field_reference_choice_source(
          field,
          reference,
          reference_path,
          choice_sources
        )
        |> validate_field_reference_source_hint(
          field,
          reference,
          reference_path,
          :value_source,
          :invalid_field_reference_value_source
        )
        |> validate_field_reference_source_hint(
          field,
          reference,
          reference_path,
          :caption_source,
          :invalid_field_reference_caption_source
        )
        |> validate_field_reference_caption_field(field, reference, reference_path, field_index)

      reference ->
        [
          Core.error(
            :invalid_field_reference,
            path ++ [:reference],
            "field #{inspect(field)} reference must be a map",
            expected: :map,
            actual: Core.value_type(reference),
            field: field
          )
          | errors
        ]
    end
  end

  def validate_field_reference_choice_source(
         field_errors,
         field,
         reference,
         path,
         choice_sources
       ) do
    case Core.map_value(reference, :choice_source) do
      nil ->
        field_errors

      choice_source when is_atom(choice_source) or is_binary(choice_source) ->
        if Core.fetch_key(choice_sources, choice_source) != :error do
          field_errors
        else
          [
            Core.error(
              :field_reference_choice_source_not_found,
              path ++ [:choice_source],
              "field #{inspect(field)} reference points at missing choice source #{inspect(choice_source)}",
              field: field,
              choice_source: choice_source
            )
            | field_errors
          ]
        end

      choice_source ->
        [
          Core.error(
            :invalid_field_reference_choice_source,
            path ++ [:choice_source],
            "field #{inspect(field)} reference choice_source must be an atom or string",
            expected: "atom or string",
            actual: Core.value_type(choice_source),
            field: field,
            choice_source: choice_source
          )
          | field_errors
        ]
    end
  end

  def validate_field_reference_source_hint(errors, field, reference, path, key, code) do
    value = Core.map_value(reference, key)

    IdValue.validate_id_value(
      errors,
      value,
      path ++ [key],
      code,
      "field #{inspect(field)} reference #{key} must be an atom or string",
      [{:field, field}, {key, value}]
    )
  end

  def validate_field_reference_caption_field(errors, field, reference, path, field_index) do
    case Core.map_value(reference, :caption_field) do
      nil ->
        errors

      caption_field when is_atom(caption_field) or is_binary(caption_field) ->
        if Core.known_field?(field_index, caption_field) do
          errors
        else
          [
            Core.error(
              :field_reference_caption_field_not_found,
              path ++ [:caption_field],
              "field #{inspect(field)} reference caption_field #{inspect(caption_field)} is not defined in source, schemas, or custom columns",
              field: field,
              caption_field: caption_field
            )
            | errors
          ]
        end

      caption_field ->
        [
          Core.error(
            :invalid_field_reference_caption_field,
            path ++ [:caption_field],
            "field #{inspect(field)} reference caption_field must be an atom or string",
            expected: "atom or string",
            actual: Core.value_type(caption_field),
            field: field,
            caption_field: caption_field
          )
          | errors
        ]
    end
  end

end
