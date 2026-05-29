defmodule Selecto.Domain.Contract.QueryMembers do
  @moduledoc false

  use Selecto.Domain.Constants
  alias Selecto.Domain.Contract.Shared.Core

  def validate(errors, query) do
    validate_query_members(errors, query)
  end

  def validate_query_members(errors, query) do
    case Core.map_value(query, :query_members) do
      nil ->
        errors

      query_members when is_map(query_members) ->
        Enum.reduce(@query_member_groups, errors, fn group_key, acc ->
          validate_query_member_group(acc, query_members, group_key)
        end)

      query_members ->
        [
          Core.error(
            :invalid_section_shape,
            [:query_members],
            "domain section :query_members must be a map",
            expected: :map,
            actual: Core.value_type(query_members)
          )
          | errors
        ]
    end
  end

  def validate_query_member_group(errors, query_members, group_key) do
    case Core.fetch_map_value(query_members, group_key) do
      :__missing__ ->
        errors

      nil ->
        errors

      members when is_map(members) ->
        Enum.reduce(members, errors, fn {member_id, member_spec}, acc ->
          acc
          |> validate_query_member_id(group_key, member_id)
          |> validate_query_member_spec(group_key, member_id, member_spec)
        end)

      members ->
        [
          Core.error(
            :invalid_query_member_group,
            [:query_members, group_key],
            "query_members.#{group_key} must be a map of named members",
            expected: :map,
            actual: Core.value_type(members),
            group: group_key
          )
          | errors
        ]
    end
  end

  def validate_query_member_id(errors, group_key, member_id) do
    if Core.non_empty_atom_or_string?(member_id) do
      errors
    else
      [
        Core.error(
          :invalid_query_member_id,
          [:query_members, group_key, member_id],
          "query member id #{inspect(member_id)} in #{group_key} must be a non-empty atom or string",
          expected: "non-empty atom or string",
          actual: Core.value_type(member_id),
          group: group_key,
          member: member_id
        )
        | errors
      ]
    end
  end

  def validate_query_member_spec(errors, group_key, member_id, member_spec)
      when is_map(member_spec) do
    case group_key do
      :ctes -> validate_cte_member(errors, member_id, member_spec)
      :values -> validate_values_member(errors, member_id, member_spec)
      :subqueries -> validate_subquery_member(errors, member_id, member_spec)
      :laterals -> validate_lateral_member(errors, member_id, member_spec)
      :unnests -> validate_unnest_member(errors, member_id, member_spec)
    end
  end

  def validate_query_member_spec(errors, group_key, member_id, member_spec) do
    [
      Core.error(
        :invalid_query_member_spec,
        [:query_members, group_key, member_id],
        "query member #{inspect(member_id)} in #{group_key} must be a map",
        expected: :map,
        actual: Core.value_type(member_spec),
        group: group_key,
        member: member_id
      )
      | errors
    ]
  end

  def validate_cte_member(errors, member_id, member_spec) do
    errors
    |> validate_cte_member_query(member_id, member_spec)
    |> validate_query_member_join(:ctes, member_id, member_spec)
  end

  def validate_cte_member_query(errors, member_id, member_spec) do
    recursive? =
      Core.map_value(member_spec, :type) == :recursive or
        not is_nil(Core.map_value(member_spec, :base_query)) or
        not is_nil(Core.map_value(member_spec, :recursive_query))

    if recursive? do
      errors
      |> validate_query_member_function(
        :ctes,
        member_id,
        Core.map_value(member_spec, :base_query),
        [:query_members, :ctes, member_id, :base_query],
        [0, 1],
        "recursive CTE #{inspect(member_id)} requires base_query function with arity 0 or 1"
      )
      |> validate_query_member_function(
        :ctes,
        member_id,
        Core.map_value(member_spec, :recursive_query),
        [:query_members, :ctes, member_id, :recursive_query],
        [1, 2],
        "recursive CTE #{inspect(member_id)} requires recursive_query function with arity 1 or 2"
      )
    else
      {query_builder, query_key} = query_member_query_builder(member_spec)

      validate_query_member_function(
        errors,
        :ctes,
        member_id,
        query_builder,
        [:query_members, :ctes, member_id, query_key],
        [0, 1],
        "CTE #{inspect(member_id)} requires query or query_builder function with arity 0 or 1"
      )
    end
  end

  def validate_values_member(errors, member_id, member_spec) do
    errors
    |> validate_values_member_rows(member_id, member_spec)
    |> validate_values_member_columns(member_id, member_spec)
    |> validate_query_member_join(:values, member_id, member_spec)
    |> validate_query_member_alias(:values, member_id, member_spec)
  end

  def validate_values_member_rows(errors, member_id, member_spec) do
    {rows, rows_key} =
      case Core.fetch_map_value(member_spec, :rows) do
        :__missing__ ->
          case Core.fetch_map_value(member_spec, :data) do
            :__missing__ -> {:__missing__, :rows}
            data -> {data, :data}
          end

        rows ->
          {rows, :rows}
      end

    if is_list(rows) do
      errors
    else
      [
        Core.error(
          :invalid_query_member_rows,
          [:query_members, :values, member_id, rows_key],
          "VALUES member #{inspect(member_id)} requires rows or data as a list",
          expected: :list,
          actual: Core.value_type(rows),
          group: :values,
          member: member_id
        )
        | errors
      ]
    end
  end

  def validate_values_member_columns(errors, member_id, member_spec) do
    case Core.fetch_map_value(member_spec, :columns) do
      :__missing__ ->
        errors

      columns when is_list(columns) ->
        errors

      columns ->
        [
          Core.error(
            :invalid_query_member_columns,
            [:query_members, :values, member_id, :columns],
            "VALUES member #{inspect(member_id)} columns must be a list when provided",
            expected: :list,
            actual: Core.value_type(columns),
            group: :values,
            member: member_id
          )
          | errors
        ]
    end
  end

  def validate_subquery_member(errors, member_id, member_spec) do
    errors
    |> validate_subquery_member_kind(member_id, member_spec)
    |> validate_subquery_member_query(member_id, member_spec)
    |> validate_subquery_member_on(member_id, member_spec)
    |> validate_query_member_join_type(:subqueries, member_id, member_spec, :type)
    |> validate_subquery_member_join_id(member_id, member_spec)
  end

  def validate_subquery_member_kind(errors, member_id, member_spec) do
    case Core.map_value(member_spec, :kind) do
      nil ->
        errors

      :join ->
        errors

      kind ->
        [
          Core.error(
            :invalid_query_member_kind,
            [:query_members, :subqueries, member_id, :kind],
            "subquery member #{inspect(member_id)} kind must be :join when provided",
            expected: :join,
            actual: Core.value_type(kind),
            group: :subqueries,
            member: member_id,
            kind: kind
          )
          | errors
        ]
    end
  end

  def validate_subquery_member_query(errors, member_id, member_spec) do
    {query_builder, query_key} = query_member_query_builder(member_spec)

    validate_query_member_function(
      errors,
      :subqueries,
      member_id,
      query_builder,
      [:query_members, :subqueries, member_id, query_key],
      [0, 1],
      "subquery member #{inspect(member_id)} requires query or query_builder function with arity 0 or 1"
    )
  end

  def validate_subquery_member_on(errors, member_id, member_spec) do
    case Core.fetch_map_value(member_spec, :on) do
      :__missing__ ->
        errors

      on when is_list(on) ->
        errors

      on ->
        [
          Core.error(
            :invalid_query_member_on,
            [:query_members, :subqueries, member_id, :on],
            "subquery member #{inspect(member_id)} on must be a list when provided",
            expected: :list,
            actual: Core.value_type(on),
            group: :subqueries,
            member: member_id
          )
          | errors
        ]
    end
  end

  def validate_subquery_member_join_id(errors, member_id, member_spec) do
    case Core.fetch_map_value(member_spec, :join_id) do
      :__missing__ ->
        errors

      join_id when is_atom(join_id) and not is_nil(join_id) ->
        errors

      join_id when is_binary(join_id) ->
        if String.trim(join_id) == "" do
          invalid_query_member_join_id(errors, member_id, join_id)
        else
          errors
        end

      join_id ->
        invalid_query_member_join_id(errors, member_id, join_id)
    end
  end

  def invalid_query_member_join_id(errors, member_id, join_id) do
    [
      Core.error(
        :invalid_query_member_join_id,
        [:query_members, :subqueries, member_id, :join_id],
        "subquery member #{inspect(member_id)} join_id must be a non-empty atom or string when provided",
        expected: "non-empty atom or string",
        actual: Core.value_type(join_id),
        group: :subqueries,
        member: member_id,
        join_id: join_id
      )
      | errors
    ]
  end

  def validate_lateral_member(errors, member_id, member_spec) do
    errors
    |> validate_lateral_member_source(member_id, member_spec)
    |> validate_query_member_join_type(:laterals, member_id, member_spec, :join_type)
    |> validate_query_member_alias(:laterals, member_id, member_spec)
    |> validate_query_member_options(:laterals, member_id, member_spec)
  end

  def validate_lateral_member_source(errors, member_id, member_spec) do
    {source, source_key} = lateral_member_source(member_spec)

    if valid_lateral_source?(source) do
      errors
    else
      [
        Core.error(
          :invalid_query_member_source,
          [:query_members, :laterals, member_id, source_key],
          "lateral member #{inspect(member_id)} requires query, source, or lateral_source as a tuple or function",
          expected: "tuple or function with arity 0, 1, or 2",
          actual: Core.value_type(source),
          group: :laterals,
          member: member_id
        )
        | errors
      ]
    end
  end

  def validate_unnest_member(errors, member_id, member_spec) do
    errors
    |> validate_unnest_member_field(member_id, member_spec)
    |> validate_unnest_member_ordinality(member_id, member_spec)
    |> validate_query_member_alias(:unnests, member_id, member_spec)
    |> validate_query_member_options(:unnests, member_id, member_spec)
  end

  def validate_unnest_member_field(errors, member_id, member_spec) do
    {field, field_key} = unnest_member_field(member_spec)

    if valid_unnest_field?(field) do
      errors
    else
      [
        Core.error(
          :invalid_query_member_field,
          [:query_members, :unnests, member_id, field_key],
          "UNNEST member #{inspect(member_id)} requires array_field or field as a non-empty atom, string, or tuple expression",
          expected: "non-empty atom, non-empty string, or tuple expression",
          actual: Core.value_type(field),
          group: :unnests,
          member: member_id,
          field: field
        )
        | errors
      ]
    end
  end

  def validate_unnest_member_ordinality(errors, member_id, member_spec) do
    case Core.map_value(member_spec, :ordinality) do
      nil ->
        errors

      ordinality when is_atom(ordinality) and not is_nil(ordinality) ->
        errors

      ordinality when is_binary(ordinality) ->
        if String.trim(ordinality) == "" do
          invalid_query_member_ordinality(errors, member_id, ordinality)
        else
          errors
        end

      ordinality ->
        invalid_query_member_ordinality(errors, member_id, ordinality)
    end
  end

  def invalid_query_member_ordinality(errors, member_id, ordinality) do
    [
      Core.error(
        :invalid_query_member_ordinality,
        [:query_members, :unnests, member_id, :ordinality],
        "UNNEST member #{inspect(member_id)} ordinality must be a non-empty atom or string when provided",
        expected: "non-empty atom or string",
        actual: Core.value_type(ordinality),
        group: :unnests,
        member: member_id,
        ordinality: ordinality
      )
      | errors
    ]
  end

  def validate_query_member_function(errors, group_key, member_id, fun, path, arities, message) do
    if Core.valid_arity?(fun, arities) do
      errors
    else
      [
        Core.error(
          :invalid_query_member_query,
          path,
          message,
          expected: "function with arity #{Enum.join(arities, " or ")}",
          actual: Core.value_type(fun),
          group: group_key,
          member: member_id
        )
        | errors
      ]
    end
  end

  def validate_query_member_join(errors, group_key, member_id, member_spec) do
    case Core.fetch_map_value(member_spec, :join) do
      :__missing__ ->
        errors

      join when join in [nil, false, true] ->
        errors

      join when is_list(join) or is_map(join) ->
        errors

      join ->
        [
          Core.error(
            :invalid_query_member_join,
            [:query_members, group_key, member_id, :join],
            "query member #{inspect(member_id)} join must be true, false, nil, a list, or a map",
            expected: "true, false, nil, list, or map",
            actual: Core.value_type(join),
            group: group_key,
            member: member_id
          )
          | errors
        ]
    end
  end

  def validate_query_member_join_type(errors, group_key, member_id, member_spec, preferred_key) do
    {join_type, join_type_key} = query_member_join_type(member_spec, preferred_key)

    cond do
      join_type == :__missing__ ->
        errors

      join_type in @query_member_join_types ->
        errors

      true ->
        [
          Core.error(
            :invalid_query_member_join_type,
            [:query_members, group_key, member_id, join_type_key],
            "query member #{inspect(member_id)} join type must be one of #{inspect(@query_member_join_types)}",
            expected: @query_member_join_types,
            actual: Core.value_type(join_type),
            group: group_key,
            member: member_id,
            join_type: join_type
          )
          | errors
        ]
    end
  end

  def validate_query_member_alias(errors, group_key, member_id, member_spec) do
    case query_member_alias(member_spec) do
      :__missing__ ->
        errors

      nil ->
        errors

      alias_name when is_atom(alias_name) and not is_nil(alias_name) ->
        errors

      alias_name when is_binary(alias_name) ->
        if String.trim(alias_name) == "" do
          invalid_query_member_alias(errors, group_key, member_id, alias_name)
        else
          errors
        end

      alias_name ->
        invalid_query_member_alias(errors, group_key, member_id, alias_name)
    end
  end

  def invalid_query_member_alias(errors, group_key, member_id, alias_name) do
    [
      Core.error(
        :invalid_query_member_alias,
        [:query_members, group_key, member_id, :as],
        "query member #{inspect(member_id)} alias must be a non-empty atom or string when provided",
        expected: "non-empty atom or string",
        actual: Core.value_type(alias_name),
        group: group_key,
        member: member_id,
        alias_name: alias_name
      )
      | errors
    ]
  end

  def validate_query_member_options(errors, group_key, member_id, member_spec) do
    case Core.fetch_map_value(member_spec, :options) do
      :__missing__ ->
        errors

      options when is_list(options) or is_map(options) ->
        errors

      options ->
        [
          Core.error(
            :invalid_query_member_options,
            [:query_members, group_key, member_id, :options],
            "query member #{inspect(member_id)} options must be a list or map when provided",
            expected: "list or map",
            actual: Core.value_type(options),
            group: group_key,
            member: member_id
          )
          | errors
        ]
    end
  end

  def query_member_query_builder(member_spec) do
    case Core.fetch_map_value(member_spec, :query_builder) do
      :__missing__ ->
        {Core.map_value(member_spec, :query), :query}

      query_builder ->
        {query_builder, :query_builder}
    end
  end

  def query_member_join_type(member_spec, preferred_key) do
    case Core.fetch_map_value(member_spec, preferred_key) do
      :__missing__ ->
        fallback_key = if preferred_key == :join_type, do: :type, else: :join_type

        case Core.fetch_map_value(member_spec, fallback_key) do
          :__missing__ -> {:__missing__, preferred_key}
          value -> {value, fallback_key}
        end

      value ->
        {value, preferred_key}
    end
  end

  def query_member_alias(member_spec) do
    case Core.fetch_map_value(member_spec, :as) do
      :__missing__ ->
        case Core.fetch_map_value(member_spec, :alias) do
          :__missing__ -> Core.fetch_map_value(member_spec, :alias_name)
          value -> value
        end

      value ->
        value
    end
  end

  def lateral_member_source(member_spec) do
    case Core.fetch_map_value(member_spec, :query) do
      :__missing__ ->
        case Core.fetch_map_value(member_spec, :source) do
          :__missing__ ->
            {Core.map_value(member_spec, :lateral_source), :lateral_source}

          source ->
            {source, :source}
        end

      query ->
        {query, :query}
    end
  end

  def unnest_member_field(member_spec) do
    case Core.fetch_map_value(member_spec, :array_field) do
      :__missing__ -> {Core.map_value(member_spec, :field), :field}
      array_field -> {array_field, :array_field}
    end
  end

  def valid_lateral_source?(source) when is_tuple(source), do: true
  def valid_lateral_source?(source) when is_function(source, 0), do: true
  def valid_lateral_source?(source) when is_function(source, 1), do: true
  def valid_lateral_source?(source) when is_function(source, 2), do: true
  def valid_lateral_source?(_source), do: false

  def valid_unnest_field?(field) when is_tuple(field), do: true

  def valid_unnest_field?(field), do: Core.non_empty_atom_or_string?(field)
end
