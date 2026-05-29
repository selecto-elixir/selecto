defmodule Selecto.Domain.Contract.SourceRelationships do
  @moduledoc false

  alias Selecto.Domain.Contract.Shared.Core
  alias Selecto.Domain.Contract.Shared.IdValue, as: IdValue
  alias Selecto.Domain.Contract.Shared.StaticFilters, as: StaticFilters

  def validate(errors, source_relationships, field_index) do
    validate_source_relationships(errors, source_relationships, field_index)
  end

  def validate_source_relationships(errors, source_relationships, field_index)
      when is_map(source_relationships) do
    Enum.reduce(source_relationships, errors, fn {relationship_id, relationship}, acc ->
      path = [:source_relationships, relationship_id]

      acc
      |> validate_source_relationship_id(relationship_id, path)
      |> validate_source_relationship(relationship_id, relationship, path, field_index)
    end)
  end

  def validate_source_relationships(errors, source_relationships, _field_index) do
    [
      Core.error(
        :invalid_section_shape,
        [:source_relationships],
        "domain section :source_relationships must be a map",
        expected: :map,
        actual: Core.value_type(source_relationships)
      )
      | errors
    ]
  end

  def validate_source_relationship_id(errors, relationship_id, _path)
      when is_atom(relationship_id) or is_binary(relationship_id) do
    errors
  end

  def validate_source_relationship_id(errors, relationship_id, path) do
    [
      Core.error(
        :invalid_source_relationship_id,
        path,
        "source relationship ids must be atoms or strings",
        expected: "atom or string",
        actual: Core.value_type(relationship_id),
        source_relationship: relationship_id
      )
      | errors
    ]
  end

  def validate_source_relationship(errors, relationship_id, relationship, path, field_index)
      when is_map(relationship) do
    errors
    |> validate_source_relationship_required_keys(relationship_id, relationship, path)
    |> IdValue.validate_id_value(
      Core.map_value(relationship, :target_domain),
      path ++ [:target_domain],
      :invalid_source_relationship_target_domain,
      "source relationship #{inspect(relationship_id)} target_domain must be an atom or string",
      source_relationship: relationship_id,
      target_domain: Core.map_value(relationship, :target_domain)
    )
    |> validate_source_relationship_source_field(relationship_id, relationship, path, field_index)
    |> IdValue.validate_id_value(
      Core.map_value(relationship, :target_field),
      path ++ [:target_field],
      :invalid_source_relationship_target_field,
      "source relationship #{inspect(relationship_id)} target_field must be an atom or string",
      source_relationship: relationship_id,
      target_field: Core.map_value(relationship, :target_field)
    )
    |> validate_source_relationship_source_path(relationship_id, relationship, path)
    |> validate_source_relationship_virtual_join(relationship_id, relationship, path, field_index)
    |> validate_source_relationship_filters(relationship_id, relationship, path)
  end

  def validate_source_relationship(errors, relationship_id, relationship, path, _field_index) do
    [
      Core.error(
        :invalid_section_shape,
        path,
        "source relationship #{inspect(relationship_id)} must be a map",
        expected: :map,
        actual: Core.value_type(relationship),
        source_relationship: relationship_id
      )
      | errors
    ]
  end

  def validate_source_relationship_required_keys(errors, relationship_id, relationship, path) do
    missing_keys =
      Enum.reject(
        [:target_domain, :source_field, :target_field],
        &Core.has_key?(relationship, &1)
      )

    case missing_keys do
      [] ->
        errors

      _ ->
        [
          Core.error(
            :source_relationship_missing_required_keys,
            path,
            "source relationship #{inspect(relationship_id)} is missing required keys #{inspect(missing_keys)}",
            source_relationship: relationship_id,
            keys: missing_keys
          )
          | errors
        ]
    end
  end

  def validate_source_relationship_source_field(
        errors,
        relationship_id,
        relationship,
        path,
        field_index
      ) do
    case Core.map_value(relationship, :source_field) do
      nil ->
        errors

      source_field when is_atom(source_field) or is_binary(source_field) ->
        if Core.known_field?(field_index, source_field) do
          errors
        else
          [
            Core.error(
              :source_relationship_source_field_not_found,
              path ++ [:source_field],
              "source relationship #{inspect(relationship_id)} source_field #{inspect(source_field)} is not defined in source, schemas, or custom columns",
              source_relationship: relationship_id,
              source_field: source_field
            )
            | errors
          ]
        end

      source_field ->
        [
          Core.error(
            :invalid_source_relationship_source_field,
            path ++ [:source_field],
            "source relationship #{inspect(relationship_id)} source_field must be an atom or string",
            expected: "atom or string",
            actual: Core.value_type(source_field),
            source_relationship: relationship_id,
            source_field: source_field
          )
          | errors
        ]
    end
  end

  def validate_source_relationship_source_path(errors, relationship_id, relationship, path) do
    case Core.map_value(relationship, :source_path) do
      nil ->
        errors

      source_path ->
        if Core.valid_static_source_path?(source_path) do
          errors
        else
          [
            Core.error(
              :invalid_source_relationship_source_path,
              path ++ [:source_path],
              "source relationship #{inspect(relationship_id)} source_path must be a non-empty atom or dotted string path",
              expected: "non-empty atom or dotted string path",
              actual: Core.value_type(source_path),
              source_relationship: relationship_id,
              source_path: source_path
            )
            | errors
          ]
        end
    end
  end

  def validate_source_relationship_virtual_join(
        errors,
        relationship_id,
        relationship,
        path,
        field_index
      ) do
    case Core.map_value(relationship, :virtual_join) do
      nil ->
        errors

      virtual_join when is_list(virtual_join) ->
        virtual_join
        |> Enum.with_index()
        |> Enum.reduce(errors, fn {entry, index}, acc ->
          validate_source_relationship_virtual_join_entry(
            acc,
            relationship_id,
            entry,
            path ++ [:virtual_join, index],
            field_index
          )
        end)

      virtual_join ->
        [
          Core.error(
            :invalid_source_relationship_virtual_join,
            path ++ [:virtual_join],
            "source relationship #{inspect(relationship_id)} virtual_join must be a list",
            expected: :list,
            actual: Core.value_type(virtual_join),
            source_relationship: relationship_id
          )
          | errors
        ]
    end
  end

  def validate_source_relationship_virtual_join_entry(
        errors,
        relationship_id,
        entry,
        path,
        field_index
      )
      when is_map(entry) do
    errors
    |> validate_source_relationship_virtual_join_required_keys(relationship_id, entry, path)
    |> validate_source_relationship_virtual_join_working_field(
      relationship_id,
      entry,
      path,
      field_index
    )
    |> validate_source_relationship_virtual_join_source_field(relationship_id, entry, path)
    |> validate_source_relationship_virtual_join_required(relationship_id, entry, path)
  end

  def validate_source_relationship_virtual_join_entry(
        errors,
        relationship_id,
        entry,
        path,
        _field_index
      ) do
    [
      Core.error(
        :invalid_source_relationship_virtual_join_entry,
        path,
        "source relationship #{inspect(relationship_id)} virtual_join entries must be maps",
        expected: :map,
        actual: Core.value_type(entry),
        source_relationship: relationship_id
      )
      | errors
    ]
  end

  def validate_source_relationship_virtual_join_required_keys(
        errors,
        relationship_id,
        entry,
        path
      ) do
    missing_keys = Enum.reject([:working_field, :source_field], &Core.has_key?(entry, &1))

    case missing_keys do
      [] ->
        errors

      _ ->
        [
          Core.error(
            :source_relationship_virtual_join_missing_required_keys,
            path,
            "source relationship #{inspect(relationship_id)} virtual_join entry is missing required keys #{inspect(missing_keys)}",
            source_relationship: relationship_id,
            keys: missing_keys
          )
          | errors
        ]
    end
  end

  def validate_source_relationship_virtual_join_working_field(
        errors,
        relationship_id,
        entry,
        path,
        field_index
      ) do
    case Core.map_value(entry, :working_field) do
      nil ->
        errors

      working_field when is_atom(working_field) or is_binary(working_field) ->
        if Core.known_field?(field_index, working_field) do
          errors
        else
          [
            Core.error(
              :source_relationship_virtual_join_working_field_not_found,
              path ++ [:working_field],
              "source relationship #{inspect(relationship_id)} virtual_join working_field #{inspect(working_field)} is not defined in source, schemas, or custom columns",
              source_relationship: relationship_id,
              working_field: working_field
            )
            | errors
          ]
        end

      working_field ->
        [
          Core.error(
            :invalid_source_relationship_virtual_join_working_field,
            path ++ [:working_field],
            "source relationship #{inspect(relationship_id)} virtual_join working_field must be an atom or string",
            expected: "atom or string",
            actual: Core.value_type(working_field),
            source_relationship: relationship_id,
            working_field: working_field
          )
          | errors
        ]
    end
  end

  def validate_source_relationship_virtual_join_source_field(
        errors,
        relationship_id,
        entry,
        path
      ) do
    case Core.map_value(entry, :source_field) do
      nil ->
        errors

      source_field ->
        if Core.valid_static_source_path?(source_field) do
          errors
        else
          [
            Core.error(
              :invalid_source_relationship_virtual_join_source_field,
              path ++ [:source_field],
              "source relationship #{inspect(relationship_id)} virtual_join source_field must be a non-empty atom or dotted string path",
              expected: "non-empty atom or dotted string path",
              actual: Core.value_type(source_field),
              source_relationship: relationship_id,
              source_field: source_field
            )
            | errors
          ]
        end
    end
  end

  def validate_source_relationship_virtual_join_required(
        errors,
        relationship_id,
        entry,
        path
      ) do
    case Core.map_value(entry, :required) do
      nil ->
        errors

      required when is_boolean(required) ->
        errors

      required ->
        [
          Core.error(
            :invalid_source_relationship_virtual_join_required,
            path ++ [:required],
            "source relationship #{inspect(relationship_id)} virtual_join required must be a boolean",
            expected: :boolean,
            actual: Core.value_type(required),
            source_relationship: relationship_id,
            required: required
          )
          | errors
        ]
    end
  end

  def validate_source_relationship_filters(errors, relationship_id, relationship, path) do
    case Core.map_value(relationship, :filters) do
      nil ->
        errors

      filters when is_list(filters) ->
        filters
        |> Enum.with_index()
        |> Enum.reduce(errors, fn {filter, index}, acc ->
          StaticFilters.validate_static_filter_expression(
            acc,
            StaticFilters.static_filter_owner(:source_relationship, relationship_id),
            filter,
            path ++ [:filters, index]
          )
        end)

      filters ->
        [
          Core.error(
            :invalid_source_relationship_filters,
            path ++ [:filters],
            "source relationship #{inspect(relationship_id)} filters must be a list",
            expected: :list,
            actual: Core.value_type(filters),
            source_relationship: relationship_id
          )
          | errors
        ]
    end
  end
end
