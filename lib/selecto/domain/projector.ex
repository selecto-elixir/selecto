defmodule Selecto.Domain.Projector do
  @moduledoc false

  use Selecto.Domain.Constants

  alias Selecto.Domain.Shared.Map, as: MapHelpers
  alias Selecto.Domain.FieldBindings

  @projections [:query, :write, :ui, :api, :query_contract]
  @query_contract_numeric_types ~w(integer float decimal)
  @query_contract_temporal_types ~w(date time datetime naive_datetime utc_datetime)
  @query_contract_text_types ~w(string text)
  @query_contract_exact_types ~w(boolean uuid enum)
  @query_contract_sortable_types @query_contract_numeric_types ++
                                   @query_contract_temporal_types ++
                                   @query_contract_text_types ++
                                   @query_contract_exact_types
  @query_contract_groupable_types @query_contract_numeric_types ++
                                    @query_contract_temporal_types ++
                                    ["string"] ++
                                    @query_contract_exact_types
  @query_projection_sections [
    :custom_columns,
    :jsonb_schemas,
    :subfilters,
    :window_functions,
    :pagination,
    :retarget
  ]
  @ui_query_sections [
    :default_selected,
    :required_selected,
    :required_filters,
    :required_order_by,
    :required_group_by,
    :filters
  ]
  @ui_projection_sections [
    :columns,
    :custom_columns,
    :jsonb_schemas,
    :pagination,
    :redact_fields
  ]
  @api_projection_sections [
    :columns,
    :custom_columns,
    :jsonb_schemas,
    :subfilters,
    :window_functions,
    :pagination,
    :retarget,
    :redact_fields
  ]

  @spec project(map(), :query | :write | :ui | :api | :query_contract) :: map()
  def project(%{schema_version: _schema_version, domain: _domain} = normalized, :query) do
    normalized
    |> base_projection()
    |> Map.merge(Map.fetch!(normalized, :query))
    |> Map.merge(take_projection_sections(normalized, @query_projection_sections))
  end

  def project(
        %{schema_version: _schema_version, domain: _domain, query: %{} = query} = normalized,
        :query_contract
      ) do
    field_choice_bindings = FieldBindings.field_choice_bindings(normalized)

    %{
      schema_version: Map.fetch!(normalized, :schema_version),
      name: MapHelpers.map_section(Map.fetch!(normalized, :domain), :name),
      projection: :query_contract,
      source: query_contract_source(normalized),
      fields: query_contract_fields(normalized, field_choice_bindings),
      joins: query_contract_joins(normalized),
      defaults: query_contract_defaults(query),
      filters: query_contract_filters(MapHelpers.map_value(query, :filters)),
      functions: query_contract_functions(MapHelpers.map_value(query, :functions)),
      query_members: query_contract_query_members(MapHelpers.map_value(query, :query_members)),
      published_views: query_contract_published_views(MapHelpers.map_value(query, :published_views)),
      source_relationships:
        query_contract_source_relationships(Map.get(normalized, :source_relationships, %{})),
      choice_sources: query_contract_choice_sources(Map.get(normalized, :choice_sources, %{})),
      field_choice_bindings: query_contract_choice_bindings(field_choice_bindings),
      capability_ids: MapHelpers.sorted_keys(Map.get(normalized, :capabilities, %{}))
    }
    |> MapHelpers.maybe_put(:domain_version, Map.get(normalized, :domain_version))
    |> MapHelpers.maybe_put(:domain_fingerprint, Map.get(normalized, :domain_fingerprint))
  end

  def project(%{schema_version: _schema_version, domain: _domain} = normalized, :write) do
    normalized
    |> base_projection()
    |> Map.merge(%{
      columns: projection_section(normalized, :columns, %{}),
      writes: Map.get(normalized, :writes, %{}),
      actions: Map.get(normalized, :actions, %{}),
      capabilities: Map.get(normalized, :capabilities, %{}),
      source_relationships: Map.get(normalized, :source_relationships, %{}),
      choice_sources: Map.get(normalized, :choice_sources, %{})
    })
  end

  def project(%{schema_version: _schema_version, domain: _domain} = normalized, :ui) do
    normalized
    |> base_projection()
    |> Map.merge(take_query_sections(normalized, @ui_query_sections))
    |> Map.merge(take_projection_sections(normalized, @ui_projection_sections))
    |> Map.merge(%{
      detail_actions: Map.get(normalized, :detail_actions, %{}),
      actions: Map.get(normalized, :actions, %{}),
      capabilities: Map.get(normalized, :capabilities, %{}),
      choice_sources: Map.get(normalized, :choice_sources, %{})
    })
  end

  def project(%{schema_version: _schema_version, domain: _domain} = normalized, :api) do
    normalized
    |> base_projection()
    |> Map.merge(Map.fetch!(normalized, :query))
    |> Map.merge(take_projection_sections(normalized, @api_projection_sections))
    |> Map.merge(%{
      writes: Map.get(normalized, :writes, %{}),
      actions: Map.get(normalized, :actions, %{}),
      capabilities: Map.get(normalized, :capabilities, %{}),
      source_relationships: Map.get(normalized, :source_relationships, %{}),
      choice_sources: Map.get(normalized, :choice_sources, %{}),
      detail_actions: Map.get(normalized, :detail_actions, %{})
    })
  end

  def project(%{schema_version: _schema_version, domain: _domain}, projection)
      when projection not in @projections do
    raise ArgumentError,
          "unknown Selecto domain projection #{inspect(projection)}; expected one of #{inspect(@projections)}"
  end

  def project(_normalized, projection) do
    raise ArgumentError,
          "expected a normalized Selecto domain from Selecto.Domain.normalize/1 before projecting #{inspect(projection)}"
  end

  def query_contract_source(normalized) do
    source = Map.get(normalized, :source, %{})

    %{
      source_table: MapHelpers.map_value(source, :source_table),
      primary_key: MapHelpers.map_value(source, :primary_key)
    }
  end
  def query_contract_defaults(query) do
    %{
      default_selected: MapHelpers.map_value(query, :default_selected) || [],
      required_selected: MapHelpers.map_value(query, :required_selected) || [],
      required_filters: MapHelpers.map_value(query, :required_filters) || [],
      required_order_by: MapHelpers.map_value(query, :required_order_by) || [],
      required_group_by: MapHelpers.map_value(query, :required_group_by) || []
    }
  end
  def query_contract_fields(normalized, field_choice_bindings) do
    choice_index = query_contract_choice_index(field_choice_bindings)

    filterable_fields =
      normalized
      |> Map.get(:query, %{})
      |> MapHelpers.map_value(:filters)
      |> query_contract_filterable_fields()

    []
    |> Kernel.++(
      query_contract_relation_fields(
        :source,
        Map.get(normalized, :source),
        :source,
        choice_index,
        filterable_fields
      )
    )
    |> Kernel.++(
      query_contract_schema_fields(Map.get(normalized, :schemas), choice_index, filterable_fields)
    )
    |> Kernel.++(
      query_contract_custom_fields(
        MapHelpers.map_value(Map.get(normalized, :projection, %{}), :custom_columns),
        choice_index,
        filterable_fields
      )
    )
    |> Enum.sort_by(& &1.id)
  end
  def query_contract_schema_fields(schemas, choice_index, filterable_fields)
       when is_map(schemas) do
    schemas
    |> MapHelpers.sorted_entries()
    |> Enum.flat_map(fn {schema_id, schema} ->
      query_contract_relation_fields(schema_id, schema, :schema, choice_index, filterable_fields)
    end)
  end

  def query_contract_schema_fields(_schemas, _choice_index, _filterable_fields), do: []
  def query_contract_relation_fields(
         relation_id,
         relation,
         source_kind,
         choice_index,
         filterable_fields
       )
       when is_map(relation) do
    relation
    |> MapHelpers.relation_field_entries()
    |> Enum.map(fn {field, column} ->
      field_ref = MapHelpers.relation_field_ref(relation_id, field)
      id = MapHelpers.field_id(field_ref)

      type = MapHelpers.map_value(column, :type)

      %{
        id: id,
        source: source_kind,
        relation: relation_id,
        field: MapHelpers.field_id(field),
        type: type,
        label: MapHelpers.field_label(column),
        capability: MapHelpers.map_value(column, :capability),
        capability_target: MapHelpers.map_value(column, :capability_target),
        choice_source: query_contract_choice_source(choice_index, id)
      }
      |> Map.merge(query_contract_field_surface(column, id, source_kind, type, filterable_fields))
    end)
  end

  def query_contract_relation_fields(
         _relation_id,
         _relation,
         _source_kind,
         _choice_index,
         _filterable_fields
       ),
       do: []
  def query_contract_custom_fields(custom_columns, choice_index, filterable_fields)
       when is_map(custom_columns) do
    custom_columns
    |> MapHelpers.sorted_entries()
    |> Enum.map(fn {field, column} ->
      id = MapHelpers.field_id(field)
      type = MapHelpers.map_value(column, :type)

      %{
        id: id,
        source: :custom_column,
        relation: nil,
        field: id,
        type: type,
        label: MapHelpers.field_label(column),
        capability: MapHelpers.map_value(column, :capability),
        capability_target: MapHelpers.map_value(column, :capability_target),
        choice_source: query_contract_choice_source(choice_index, id)
      }
      |> Map.merge(
        query_contract_field_surface(column, id, :custom_column, type, filterable_fields)
      )
    end)
  end

  def query_contract_custom_fields(_custom_columns, _choice_index, _filterable_fields), do: []
  def query_contract_joins(normalized) do
    query_contract_join_tree(
      Map.get(normalized, :joins, %{}),
      Map.get(normalized, :source),
      Map.get(normalized, :schemas, %{}),
      [],
      :source
    )
  end
  def query_contract_join_tree(joins, parent_relation, schemas, path, parent_id)
       when is_map(joins) do
    joins
    |> MapHelpers.sorted_entries()
    |> Enum.flat_map(fn {join_id, join_config} ->
      join_path = path ++ [join_id]
      association = MapHelpers.relation_association(parent_relation, join_id)
      target_schema = if is_map(association), do: MapHelpers.map_value(association, :queryable)

      target_relation =
        query_contract_join_target_relation(target_schema, parent_relation, schemas)

      nested_joins = MapHelpers.map_value(join_config, :joins)

      entry = %{
        id: MapHelpers.field_id(join_id),
        path: Enum.map(join_path, &MapHelpers.field_id/1),
        parent: parent_id,
        target_schema: target_schema,
        type: MapHelpers.map_value(join_config, :type),
        fields: MapHelpers.relation_field_ids(target_relation),
        nested_count: MapHelpers.map_count(nested_joins)
      }

      [
        entry
        | query_contract_join_tree(
            nested_joins,
            target_relation,
            schemas,
            join_path,
            target_schema
          )
      ]
    end)
  end

  def query_contract_join_tree(_joins, _parent_relation, _schemas, _path, _parent_id), do: []
  def query_contract_join_target_relation(target_schema, source, _schemas)
       when target_schema in [:source, "source"] do
    source
  end

  def query_contract_join_target_relation(target_schema, _source, schemas) do
    case MapHelpers.fetch_key(schemas, target_schema) do
      {:ok, schema} -> schema
      :error -> nil
    end
  end
  def query_contract_filters(filters) when is_map(filters) do
    filters
    |> MapHelpers.sorted_entries()
    |> Enum.map(fn {id, filter} ->
      type = MapHelpers.map_value(filter, :type)
      virtual? = is_nil(MapHelpers.map_value(filter, :field))

      %{
        id: id,
        field: MapHelpers.field_ref_or_nil(MapHelpers.map_value(filter, :field)),
        type: type,
        label: MapHelpers.field_label(filter),
        capability: MapHelpers.map_value(filter, :capability),
        virtual?: virtual?,
        comparators: query_contract_filter_comparators(filter, type, virtual?)
      }
    end)
  end

  def query_contract_filters(_filters), do: []
  def query_contract_filterable_fields(filters) when is_map(filters) do
    filters
    |> Map.values()
    |> Enum.reduce(MapSet.new(), fn
      filter, acc when is_map(filter) ->
        case MapHelpers.field_ref_or_nil(MapHelpers.map_value(filter, :field)) do
          nil -> acc
          field -> MapSet.put(acc, field)
        end

      _filter, acc ->
        acc
    end)
  end

  def query_contract_filterable_fields(_filters), do: MapSet.new()
  def query_contract_field_surface(column, id, source_kind, type, filterable_fields) do
    type_id = query_contract_type_id(type)
    detail_selectable? = query_contract_detail_selectable?(column)
    filterable? = query_contract_filterable?(column, id, source_kind, filterable_fields)
    aggregatable? = query_contract_aggregatable?(column, type_id, detail_selectable?)

    %{
      detail_selectable: detail_selectable?,
      filterable: filterable?,
      sortable: query_contract_sortable?(column, type_id, detail_selectable?),
      groupable: query_contract_groupable?(column, type_id, detail_selectable?),
      aggregatable: aggregatable?,
      comparators: query_contract_comparators(column, type_id, filterable?),
      aggregate_functions: query_contract_aggregate_functions(column, type_id, aggregatable?)
    }
  end
  def query_contract_detail_selectable?(column) do
    query_contract_bool(
      column,
      [:detail_selectable, :detail_selectable?, :selectable, :selectable?],
      true
    )
  end
  def query_contract_filterable?(column, id, source_kind, filterable_fields) do
    default = source_kind in [:source, :schema] or MapSet.member?(filterable_fields, id)

    query_contract_bool(
      column,
      [:filterable, :filterable?, :query_filterable, :query_filterable?],
      default
    )
  end
  def query_contract_sortable?(column, type_id, detail_selectable?) do
    query_contract_bool(
      column,
      [:sortable, :sortable?],
      detail_selectable? and type_id in @query_contract_sortable_types
    )
  end
  def query_contract_groupable?(column, type_id, detail_selectable?) do
    query_contract_bool(
      column,
      [:groupable, :groupable?],
      detail_selectable? and type_id in @query_contract_groupable_types
    )
  end
  def query_contract_aggregatable?(column, type_id, detail_selectable?) do
    query_contract_bool(
      column,
      [:aggregatable, :aggregatable?],
      detail_selectable? and type_id in @query_contract_numeric_types
    )
  end
  def query_contract_comparators(column, type_id, filterable?) do
    case query_contract_list_override(column, [:comparators, :operators]) do
      {:ok, comparators} -> comparators
      :error -> if filterable?, do: query_contract_type_comparators(type_id), else: []
    end
  end
  def query_contract_filter_comparators(filter, type, virtual?) do
    type_id = query_contract_type_id(type)

    case query_contract_list_override(filter, [:comparators, :operators]) do
      {:ok, comparators} -> comparators
      :error -> if virtual?, do: [], else: query_contract_type_comparators(type_id)
    end
  end
  def query_contract_aggregate_functions(column, _type_id, aggregatable?) do
    case query_contract_list_override(column, [:aggregate_functions, :aggregates]) do
      {:ok, aggregate_functions} -> aggregate_functions
      :error -> if aggregatable?, do: [:count, :count_distinct, :sum, :avg, :min, :max], else: []
    end
  end
  def query_contract_type_comparators(type_id) when type_id in @query_contract_numeric_types,
    do: [:eq, :neq, :gt, :gte, :lt, :lte, :between, :in, :not_in, :is_null, :not_null]

  def query_contract_type_comparators(type_id) when type_id in @query_contract_temporal_types,
    do: [:eq, :neq, :gt, :gte, :lt, :lte, :between, :in, :not_in, :is_null, :not_null]

  def query_contract_type_comparators(type_id) when type_id in @query_contract_text_types,
    do: [:eq, :neq, :contains, :starts_with, :ends_with, :in, :not_in, :is_null, :not_null]

  def query_contract_type_comparators("boolean"), do: [:eq, :neq, :is_null, :not_null]

  def query_contract_type_comparators(type_id) when type_id in @query_contract_exact_types,
    do: [:eq, :neq, :in, :not_in, :is_null, :not_null]

  def query_contract_type_comparators(_type_id),
    do: [:eq, :neq, :in, :not_in, :is_null, :not_null]
  def query_contract_bool(map, keys, default) do
    case query_contract_bool_override(map, keys) do
      {:ok, value} -> value
      :error -> default
    end
  end
  def query_contract_bool_override(map, keys) when is_map(map) do
    Enum.reduce_while(keys, :error, fn key, _acc ->
      case MapHelpers.map_value(map, key) do
        value when is_boolean(value) -> {:halt, {:ok, value}}
        _value -> {:cont, :error}
      end
    end)
  end

  def query_contract_bool_override(_map, _keys), do: :error
  def query_contract_list_override(map, keys) when is_map(map) do
    Enum.reduce_while(keys, :error, fn key, _acc ->
      case MapHelpers.map_value(map, key) do
        values when is_list(values) -> {:halt, {:ok, values}}
        _value -> {:cont, :error}
      end
    end)
  end

  def query_contract_list_override(_map, _keys), do: :error
  def query_contract_type_id(type) when is_atom(type), do: Atom.to_string(type)

  def query_contract_type_id(type) when is_binary(type) do
    type
    |> String.downcase()
    |> String.replace("-", "_")
  end

  def query_contract_type_id(_type), do: ""
  def query_contract_functions(functions) when is_map(functions) do
    functions
    |> MapHelpers.sorted_entries()
    |> Enum.map(fn {id, function} ->
      %{
        id: id,
        kind: MapHelpers.map_value(function, :kind),
        sql_name: MapHelpers.map_value(function, :sql_name),
        allowed_in: List.wrap(MapHelpers.map_value(function, :allowed_in)),
        args: query_contract_function_args(MapHelpers.map_value(function, :args)),
        returns: MapHelpers.map_value(function, :returns),
        capability: MapHelpers.map_value(function, :capability)
      }
    end)
  end

  def query_contract_functions(_functions), do: []
  def query_contract_function_args(args) when is_list(args) do
    Enum.map(args, fn
      arg when is_map(arg) ->
        %{
          name: MapHelpers.map_value(arg, :name),
          type: MapHelpers.map_value(arg, :type),
          source: MapHelpers.map_value(arg, :source)
        }

      _arg ->
        %{}
    end)
  end

  def query_contract_function_args(_args), do: []
  def query_contract_query_members(query_members) when is_map(query_members) do
    Enum.into(@query_member_groups, %{}, fn group ->
      {group, query_contract_query_member_group(group, MapHelpers.map_value(query_members, group))}
    end)
  end

  def query_contract_query_members(_query_members) do
    Enum.into(@query_member_groups, %{}, &{&1, []})
  end
  def query_contract_query_member_group(group, members) when is_map(members) do
    members
    |> MapHelpers.sorted_entries()
    |> Enum.map(fn {id, member} -> query_contract_query_member(group, id, member) end)
  end

  def query_contract_query_member_group(_group, _members), do: []
  def query_contract_query_member(group, id, member) when is_map(member) do
    base = %{
      id: id,
      capability: MapHelpers.map_value(member, :capability)
    }

    case group do
      :ctes ->
        Map.merge(base, %{
          columns: List.wrap(MapHelpers.map_value(member, :columns)),
          recursive?:
            MapHelpers.has_key_variant?(member, :base_query) or MapHelpers.has_key_variant?(member, :recursive_query),
          join?: not is_nil(MapHelpers.map_value(member, :join))
        })

      :values ->
        Map.merge(base, %{
          columns: List.wrap(MapHelpers.map_value(member, :columns)),
          alias: query_contract_alias(member),
          rows_count: MapHelpers.list_count(MapHelpers.map_value(member, :rows) || MapHelpers.map_value(member, :data))
        })

      :subqueries ->
        Map.merge(base, %{
          kind: MapHelpers.map_value(member, :kind),
          join_type: MapHelpers.map_value(member, :type),
          join_id: MapHelpers.map_value(member, :join_id),
          on_count: MapHelpers.list_count(MapHelpers.map_value(member, :on))
        })

      :laterals ->
        Map.merge(base, %{
          join_type: MapHelpers.map_value(member, :join_type) || MapHelpers.map_value(member, :type),
          alias: query_contract_alias(member),
          source: query_contract_lateral_source(member)
        })

      :unnests ->
        Map.merge(base, %{
          field: MapHelpers.field_ref_or_nil(MapHelpers.map_value(member, :array_field) || MapHelpers.map_value(member, :field)),
          alias: query_contract_alias(member),
          ordinality: MapHelpers.map_value(member, :ordinality)
        })
    end
  end

  def query_contract_query_member(_group, id, _member), do: %{id: id}
  def query_contract_published_views(published_views) when is_map(published_views) do
    published_views
    |> MapHelpers.sorted_entries()
    |> Enum.map(fn {id, view} ->
      %{
        id: id,
        database_name: MapHelpers.map_value(view, :database_name),
        kind: MapHelpers.map_value(view, :kind),
        columns: query_contract_columns(MapHelpers.map_value(view, :columns)),
        indexes_count: MapHelpers.list_count(MapHelpers.map_value(view, :indexes)),
        refresh: MapHelpers.map_value(view, :refresh),
        capability: MapHelpers.map_value(view, :capability)
      }
    end)
  end

  def query_contract_published_views(_published_views), do: []
  def query_contract_source_relationships(source_relationships)
       when is_map(source_relationships) do
    source_relationships
    |> MapHelpers.sorted_entries()
    |> Enum.map(fn {id, relationship} ->
      %{
        id: id,
        target_domain: MapHelpers.map_value(relationship, :target_domain),
        source_field: MapHelpers.field_ref_or_nil(MapHelpers.map_value(relationship, :source_field)),
        target_field: MapHelpers.field_ref_or_nil(MapHelpers.map_value(relationship, :target_field)),
        source_path: MapHelpers.map_value(relationship, :source_path),
        virtual_join_count: MapHelpers.list_count(MapHelpers.map_value(relationship, :virtual_join)),
        filters_count: MapHelpers.list_count(MapHelpers.map_value(relationship, :filters))
      }
    end)
  end

  def query_contract_source_relationships(_source_relationships), do: []
  def query_contract_choice_sources(choice_sources) when is_map(choice_sources) do
    choice_sources
    |> MapHelpers.sorted_entries()
    |> Enum.map(fn {id, choice_source} ->
      %{
        id: id,
        domain: MapHelpers.map_value(choice_source, :domain),
        source_relationship: MapHelpers.map_value(choice_source, :source_relationship),
        value_field: MapHelpers.field_ref_or_nil(MapHelpers.map_value(choice_source, :value_field)),
        label_field: MapHelpers.field_ref_or_nil(MapHelpers.map_value(choice_source, :label_field)),
        source_path: MapHelpers.map_value(choice_source, :source_path),
        value_source: MapHelpers.field_ref_or_nil(MapHelpers.map_value(choice_source, :value_source)),
        caption_source: MapHelpers.field_ref_or_nil(MapHelpers.map_value(choice_source, :caption_source)),
        filters_count: MapHelpers.list_count(MapHelpers.map_value(choice_source, :filters)),
        order_by_count: MapHelpers.list_count(MapHelpers.map_value(choice_source, :order_by)),
        presentation: MapHelpers.map_value(choice_source, :presentation),
        constraint_policy: MapHelpers.map_value(choice_source, :constraint_policy),
        capability: MapHelpers.map_value(choice_source, :capability)
      }
    end)
  end

  def query_contract_choice_sources(_choice_sources), do: []
  def query_contract_choice_bindings(field_choice_bindings) do
    Enum.map(field_choice_bindings, fn binding ->
      %{
        field: MapHelpers.field_id(binding.field),
        choice_source: binding.choice_source,
        compact?: binding.compact?,
        reference?: binding.reference?,
        path: binding.path
      }
    end)
  end
  def query_contract_columns(columns) when is_map(columns) do
    columns
    |> MapHelpers.sorted_entries()
    |> Enum.map(fn {id, column} ->
      %{
        id: MapHelpers.field_id(id),
        type: MapHelpers.map_value(column, :type),
        label: MapHelpers.field_label(column),
        capability: MapHelpers.map_value(column, :capability)
      }
    end)
  end

  def query_contract_columns(_columns), do: []
  def query_contract_alias(member) do
    MapHelpers.first_map_value(member, [:as, :alias, :alias_name])
  end
  def query_contract_lateral_source(member) do
    source = MapHelpers.first_map_value(member, [:query, :source, :lateral_source])

    cond do
      is_function(source) -> :function
      is_tuple(source) -> :tuple
      is_nil(source) -> nil
      true -> MapHelpers.value_type(source)
    end
  end
  def query_contract_choice_index(field_choice_bindings) do
    field_choice_bindings
    |> Enum.group_by(&MapHelpers.field_id(&1.field), & &1.choice_source)
    |> Map.new(fn {field, choice_sources} ->
      {field, choice_sources |> Enum.reject(&is_nil/1) |> Enum.uniq_by(&MapHelpers.field_id/1)}
    end)
  end
  def query_contract_choice_source(choice_index, field) do
    case Map.get(choice_index, field, []) do
      [] -> nil
      [choice_source] -> choice_source
      choice_sources -> choice_sources
    end
  end
  def base_projection(normalized) do
    authored_domain = Map.fetch!(normalized, :domain)

    %{
      schema_version: Map.fetch!(normalized, :schema_version),
      name: MapHelpers.map_section(authored_domain, :name),
      source: Map.get(normalized, :source),
      schemas: Map.get(normalized, :schemas, %{}),
      joins: Map.get(normalized, :joins, %{}),
      domain_data: Map.get(normalized, :domain_data, %{}),
      extensions: Map.get(normalized, :extensions, [])
    }
    |> MapHelpers.maybe_put(:domain_version, Map.get(normalized, :domain_version))
    |> MapHelpers.maybe_put(:domain_fingerprint, Map.get(normalized, :domain_fingerprint))
  end
  def take_query_sections(normalized, keys) do
    normalized
    |> Map.fetch!(:query)
    |> Map.take(keys)
  end
  def take_projection_sections(normalized, keys) do
    normalized
    |> Map.fetch!(:projection)
    |> Map.take(keys)
  end
  def projection_section(normalized, key, default) do
    normalized
    |> Map.fetch!(:projection)
    |> Map.get(key, default)
  end
  def query_sections(domain) do
    %{
      default_selected: MapHelpers.section(domain, :default_selected, []),
      required_selected: MapHelpers.section(domain, :required_selected, []),
      required_filters: MapHelpers.section(domain, :required_filters, []),
      required_order_by: MapHelpers.section(domain, :required_order_by, []),
      required_group_by: MapHelpers.section(domain, :required_group_by, []),
      filters: MapHelpers.section(domain, :filters, %{}),
      functions: MapHelpers.section(domain, :functions, %{}),
      query_members: MapHelpers.section(domain, :query_members, %{}),
      published_views: MapHelpers.section(domain, :published_views, %{})
    }
  end
  def projection_sections(domain) do
    %{
      columns: MapHelpers.section(domain, :columns, %{}),
      custom_columns: MapHelpers.section(domain, :custom_columns, %{}),
      jsonb_schemas: MapHelpers.section(domain, :jsonb_schemas, %{}),
      subfilters: MapHelpers.section(domain, :subfilters, %{}),
      window_functions: MapHelpers.section(domain, :window_functions, %{}),
      pagination: MapHelpers.section(domain, :pagination, %{}),
      retarget: MapHelpers.section(domain, :retarget, %{}),
      redact_fields: MapHelpers.section(domain, :redact_fields, [])
    }
  end
end
