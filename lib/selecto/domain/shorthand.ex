defmodule Selecto.Domain.Shorthand do
  @moduledoc false

  alias Selecto.Domain.Shared.Map, as: MapHelpers

  def normalize_authoring_shorthand(domain) do
    choice_sources = MapHelpers.section(domain, :choice_sources, %{})
    source_relationships = MapHelpers.section(domain, :source_relationships, %{})

    if is_map(choice_sources) and is_map(source_relationships) do
      acc = %{
        choice_sources: choice_sources,
        source_relationships: source_relationships,
        changed?: false
      }

      {domain, acc} =
        domain
        |> normalize_source_choice_shorthand(acc)
        |> normalize_schema_choice_shorthand()
        |> normalize_projection_choice_shorthand()

      if acc.changed? do
        domain
        |> MapHelpers.put_section(:choice_sources, acc.choice_sources)
        |> MapHelpers.put_section(:source_relationships, acc.source_relationships)
      else
        domain
      end
    else
      domain
    end
  end

  def normalize_source_choice_shorthand(domain, acc) do
    case MapHelpers.section(domain, :source) do
      source when is_map(source) ->
        {source, acc} = normalize_relation_choice_shorthand(source, :source, acc)
        {MapHelpers.put_section(domain, :source, source), acc}

      _source ->
        {domain, acc}
    end
  end

  def normalize_schema_choice_shorthand({domain, acc}) do
    case MapHelpers.section(domain, :schemas, %{}) do
      schemas when is_map(schemas) ->
        {schemas, acc} =
          Enum.reduce(schemas, {%{}, acc}, fn {schema_id, schema}, {schemas_acc, acc} ->
            {schema, acc} = normalize_relation_choice_shorthand(schema, {:schema, schema_id}, acc)
            {Map.put(schemas_acc, schema_id, schema), acc}
          end)

        {MapHelpers.put_section(domain, :schemas, schemas), acc}

      _schemas ->
        {domain, acc}
    end
  end

  def normalize_projection_choice_shorthand({domain, acc}) do
    case MapHelpers.fetch_section(domain, :columns) do
      {:ok, columns} when is_map(columns) ->
        {columns, acc} = normalize_columns_choice_shorthand(columns, :projection, acc)
        {MapHelpers.put_section(domain, :columns, columns), acc}

      {:ok, _columns} ->
        {domain, acc}

      :error ->
        {domain, acc}
    end
  end

  def normalize_relation_choice_shorthand(relation, scope, acc) when is_map(relation) do
    case MapHelpers.map_value(relation, :columns) do
      columns when is_map(columns) ->
        {columns, acc} = normalize_columns_choice_shorthand(columns, scope, acc)
        {MapHelpers.put_map_value(relation, :columns, columns), acc}

      _columns ->
        {relation, acc}
    end
  end

  def normalize_relation_choice_shorthand(relation, _scope, acc), do: {relation, acc}

  def normalize_columns_choice_shorthand(columns, scope, acc) do
    Enum.reduce(columns, {%{}, acc}, fn {field, column}, {columns_acc, acc} ->
      {column, acc} = normalize_column_choice_shorthand(column, scope, field, acc)
      {Map.put(columns_acc, field, column), acc}
    end)
  end

  def normalize_column_choice_shorthand(column, scope, field, acc) when is_map(column) do
    case MapHelpers.map_value(column, :choice_source) do
      shorthand when is_map(shorthand) ->
        choice_source_id = shorthand_choice_source_id(shorthand, scope, field)

        {choice_source, acc} =
          shorthand_choice_source(shorthand, choice_source_id, scope, field, acc)

        acc =
          acc
          |> put_registry_entry(:choice_sources, choice_source_id, choice_source)
          |> Map.put(:changed?, true)

        column =
          column
          |> MapHelpers.delete_key_variants(:choice_source)
          |> MapHelpers.put_map_value(:choice_source, choice_source_id)
          |> MapHelpers.put_map_value(
            :reference,
            shorthand_field_reference(column, shorthand, choice_source_id)
          )

        {column, acc}

      _choice_source ->
        {column, acc}
    end
  end

  def normalize_column_choice_shorthand(column, _scope, _field, acc), do: {column, acc}

  def shorthand_choice_source(shorthand, _choice_source_id, scope, field, acc) do
    source_relationship = MapHelpers.map_value(shorthand, :source_relationship)

    source_relationship_id =
      shorthand_source_relationship_id(shorthand, source_relationship, scope, field)

    {relationship_ref, acc} =
      case source_relationship do
        relationship when is_map(relationship) ->
          relationship =
            shorthand_source_relationship(
              relationship,
              shorthand,
              source_relationship_id,
              scope,
              field
            )

          acc =
            acc
            |> put_registry_entry(:source_relationships, source_relationship_id, relationship)
            |> Map.put(:changed?, true)

          {source_relationship_id, acc}

        relationship
        when (is_atom(relationship) and not is_nil(relationship)) or is_binary(relationship) ->
          {relationship, acc}

        _relationship ->
          {nil, acc}
      end

    choice_source =
      shorthand
      |> MapHelpers.delete_key_variants(:id)
      |> MapHelpers.delete_key_variants(:source_relationship_id)
      |> MapHelpers.maybe_put_default(:domain, MapHelpers.map_value(shorthand, :domain))
      |> MapHelpers.maybe_put_default(
        :value_field,
        path_leaf(MapHelpers.map_value(shorthand, :value_source))
      )
      |> MapHelpers.maybe_put_default(
        :label_field,
        path_leaf(MapHelpers.map_value(shorthand, :caption_source))
      )
      |> MapHelpers.maybe_put_default(
        :source_path,
        path_parent(MapHelpers.map_value(shorthand, :value_source))
      )
      |> normalize_choice_source_presentation()

    choice_source =
      if is_nil(relationship_ref) do
        MapHelpers.delete_key_variants(choice_source, :source_relationship)
      else
        MapHelpers.put_map_value(choice_source, :source_relationship, relationship_ref)
      end

    {choice_source, acc}
  end

  def shorthand_source_relationship(relationship, choice_source, _relationship_id, scope, field) do
    virtual_join = MapHelpers.map_value(relationship, :virtual_join)
    first_virtual_join = first_virtual_join_entry(virtual_join)
    virtual_join_source_field = MapHelpers.map_value(first_virtual_join, :source_field)

    relationship
    |> MapHelpers.delete_key_variants(:id)
    |> MapHelpers.delete_key_variants(:domain)
    |> MapHelpers.maybe_put_default(
      :target_domain,
      MapHelpers.map_value(relationship, :target_domain) ||
        MapHelpers.map_value(relationship, :domain) ||
        MapHelpers.map_value(choice_source, :domain)
    )
    |> MapHelpers.maybe_put_default(
      :source_field,
      MapHelpers.map_value(relationship, :source_field) ||
        MapHelpers.map_value(first_virtual_join, :working_field) ||
        scoped_field_ref(scope, field)
    )
    |> MapHelpers.maybe_put_default(
      :target_field,
      MapHelpers.map_value(relationship, :target_field) || path_leaf(virtual_join_source_field) ||
        MapHelpers.map_value(choice_source, :value_field) ||
        path_leaf(MapHelpers.map_value(choice_source, :value_source))
    )
    |> MapHelpers.maybe_put_default(
      :source_path,
      MapHelpers.map_value(relationship, :source_path) || path_parent(virtual_join_source_field) ||
        MapHelpers.map_value(choice_source, :source_path)
    )
  end

  def shorthand_field_reference(column, shorthand, choice_source_id) do
    base_reference =
      case MapHelpers.map_value(column, :reference) do
        reference when is_map(reference) -> reference
        _reference -> %{}
      end

    base_reference
    |> MapHelpers.put_map_value(:choice_source, choice_source_id)
    |> MapHelpers.maybe_put_default(:value_source, MapHelpers.map_value(shorthand, :value_source))
    |> MapHelpers.maybe_put_default(
      :caption_source,
      MapHelpers.map_value(shorthand, :caption_source)
    )
  end

  def shorthand_choice_source_id(shorthand, scope, field) do
    case MapHelpers.map_value(shorthand, :id) do
      id when (is_atom(id) and not is_nil(id)) or is_binary(id) -> id
      _id -> "#{shorthand_field_prefix(scope, field)}_choice_source"
    end
  end

  def shorthand_source_relationship_id(shorthand, source_relationship, scope, field) do
    id =
      cond do
        is_map(source_relationship) -> MapHelpers.map_value(source_relationship, :id)
        true -> nil
      end

    cond do
      (is_atom(id) and not is_nil(id)) or is_binary(id) ->
        id

      source_relationship_id = MapHelpers.map_value(shorthand, :source_relationship_id) ->
        if (is_atom(source_relationship_id) and not is_nil(source_relationship_id)) or
             is_binary(source_relationship_id) do
          source_relationship_id
        else
          "#{shorthand_field_prefix(scope, field)}_source_relationship"
        end

      true ->
        "#{shorthand_field_prefix(scope, field)}_source_relationship"
    end
  end

  def shorthand_field_prefix(:source, field), do: MapHelpers.field_id(field)
  def shorthand_field_prefix(:projection, field), do: "projection_#{MapHelpers.field_id(field)}"

  def shorthand_field_prefix({:schema, schema_id}, field),
    do: "#{MapHelpers.field_id(schema_id)}_#{MapHelpers.field_id(field)}"

  def scoped_field_ref(:source, field), do: field
  def scoped_field_ref(:projection, field), do: field

  def scoped_field_ref({:schema, schema_id}, field),
    do: "#{MapHelpers.field_id(schema_id)}.#{MapHelpers.field_id(field)}"

  def normalize_choice_source_presentation(choice_source) do
    case MapHelpers.map_value(choice_source, :presentation) do
      presentation
      when (is_atom(presentation) and not is_nil(presentation)) or is_binary(presentation) ->
        MapHelpers.put_map_value(choice_source, :presentation, %{control: presentation})

      _presentation ->
        choice_source
    end
  end

  def first_virtual_join_entry([entry | _entries]) when is_map(entry), do: entry
  def first_virtual_join_entry(_virtual_join), do: %{}
  def path_leaf(path) when is_atom(path), do: path

  def path_leaf(path) when is_binary(path) do
    path
    |> String.split(".", trim: true)
    |> List.last()
  end

  def path_leaf(_path), do: nil

  def path_parent(path) when is_binary(path) do
    parts = String.split(path, ".", trim: true)

    case parts do
      [_only] -> nil
      [_ | _] -> parts |> Enum.drop(-1) |> Enum.join(".")
      [] -> nil
    end
  end

  def path_parent(_path), do: nil

  def put_registry_entry(acc, registry_key, id, entry) do
    registry = Map.fetch!(acc, registry_key)

    if registry_has_key?(registry, id) do
      acc
    else
      Map.put(acc, registry_key, Map.put(registry, id, entry))
    end
  end

  def registry_has_key?(registry, id) when is_map(registry) do
    Map.has_key?(registry, id) or
      (is_atom(id) and Map.has_key?(registry, Atom.to_string(id))) or
      (is_binary(id) and registry_has_atom_key?(registry, id))
  end

  def registry_has_key?(_registry, _id), do: false

  def registry_has_atom_key?(registry, id) do
    Enum.any?(Map.keys(registry), &(is_atom(&1) and Atom.to_string(&1) == id))
  end
end
