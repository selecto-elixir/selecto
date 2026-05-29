defmodule Selecto.Domain.Contract.Query do
  @moduledoc false

  use Selecto.Domain.Constants
  alias Selecto.Domain.Contract.Shared.Core
  alias Selecto.Domain.Contract.Shared.FieldReference, as: FieldReference

  @logical_filter_ops [:and, :or]
  @unary_filter_ops [:not]
  @query_order_directions [:asc, :desc, :asc_nulls_first, :asc_nulls_last, :desc_nulls_first, :desc_nulls_last]
  @query_group_wrappers [:rollup, :grouping_set]

  def validate(errors, query, field_index) do
    errors
    |> validate_query_field_lists(query, field_index)
    |> validate_filters(query, field_index)
    |> validate_functions(query)
  end

  def validate_query_field_lists(errors, query, field_index) do
    functions = Core.map_value(query, :functions)

    errors
    |> validate_query_selection_list(query, :default_selected, field_index, functions)
    |> validate_query_selection_list(query, :required_selected, field_index, functions)
    |> validate_query_order_list(query, :required_order_by, field_index, functions)
    |> validate_query_group_list(query, :required_group_by, field_index, functions)
  end

  def validate_query_selection_list(errors, query, section, field_index, functions) do
    case Core.map_value(query, section) do
      nil ->
        errors

      selections when is_list(selections) ->
        selections
        |> Enum.with_index()
        |> Enum.reduce(errors, fn {selection, index}, acc ->
          validate_query_selection_entry(
            acc,
            section,
            selection,
            [section, index],
            field_index,
            functions
          )
        end)

      selections ->
        invalid_query_list(errors, section, selections)
    end
  end

  def validate_query_order_list(errors, query, section, field_index, functions) do
    case Core.map_value(query, section) do
      nil ->
        errors

      order_by when is_list(order_by) ->
        order_by
        |> Enum.with_index()
        |> Enum.reduce(errors, fn {order_entry, index}, acc ->
          validate_query_order_entry(
            acc,
            section,
            order_entry,
            [section, index],
            field_index,
            functions
          )
        end)

      order_by ->
        invalid_query_list(errors, section, order_by)
    end
  end

  def validate_query_group_list(errors, query, section, field_index, functions) do
    case Core.map_value(query, section) do
      nil ->
        errors

      group_by when is_list(group_by) ->
        group_by
        |> Enum.with_index()
        |> Enum.reduce(errors, fn {group_entry, index}, acc ->
          validate_query_group_entry(
            acc,
            section,
            group_entry,
            [section, index],
            field_index,
            functions
          )
        end)

      group_by ->
        invalid_query_list(errors, section, group_by)
    end
  end

  def invalid_query_list(errors, section, value) do
    [
      Core.error(
        :invalid_section_shape,
        [section],
        "domain section #{inspect(section)} must be a list",
        expected: :list,
        actual: Core.value_type(value)
      )
      | errors
    ]
  end

  def validate_query_selection_entry(errors, section, field, path, field_index, _functions)
       when is_atom(field) or is_binary(field) do
    validate_query_field_reference(errors, section, field, path, field_index)
  end

  def validate_query_selection_entry(
         errors,
         section,
         {:field, field},
         path,
         field_index,
         functions
       ) do
    validate_query_selector_reference(
      errors,
      section,
      :select,
      field,
      path ++ [:field],
      field_index,
      functions
    )
  end

  def validate_query_selection_entry(
         errors,
         section,
         {:field, field, _alias},
         path,
         field_index,
         functions
       ) do
    validate_query_selector_reference(
      errors,
      section,
      :select,
      field,
      path ++ [:field],
      field_index,
      functions
    )
  end

  def validate_query_selection_entry(
         errors,
         section,
         {:udf, function_id, args},
         path,
         field_index,
         functions
       )
       when is_list(args) do
    validate_query_function_reference(
      errors,
      section,
      :select,
      function_id,
      args,
      path,
      field_index,
      functions
    )
  end

  def validate_query_selection_entry(errors, _section, entry, _path, _field_index, _functions)
       when is_tuple(entry) or is_map(entry) do
    errors
  end

  def validate_query_selection_entry(errors, section, entry, path, _field_index, _functions) do
    invalid_query_field_reference(errors, section, entry, path)
  end

  def validate_query_order_entry(
         errors,
         _section,
         {:raw_sql, _sql},
         _path,
         _field_index,
         _functions
       ) do
    errors
  end

  def validate_query_order_entry(
         errors,
         section,
         {:udf, function_id, args},
         path,
         field_index,
         functions
       )
       when is_list(args) do
    validate_query_function_reference(
      errors,
      section,
      :order_by,
      function_id,
      args,
      path,
      field_index,
      functions
    )
  end

  def validate_query_order_entry(
         errors,
         section,
         {direction, field},
         path,
         field_index,
         functions
       )
       when direction in @query_order_directions do
    validate_query_selector_reference(
      errors,
      section,
      :order_by,
      field,
      path ++ [:field],
      field_index,
      functions
    )
  end

  def validate_query_order_entry(
         errors,
         section,
         {field, direction},
         path,
         field_index,
         functions
       )
       when direction in @query_order_directions do
    validate_query_selector_reference(
      errors,
      section,
      :order_by,
      field,
      path ++ [:field],
      field_index,
      functions
    )
  end

  def validate_query_order_entry(
         errors,
         section,
         {field, direction},
         path,
         field_index,
         _functions
       )
       when (is_atom(field) or is_binary(field)) and (is_atom(direction) or is_binary(direction)) do
    if query_order_direction?(direction) do
      validate_query_field_reference(errors, section, field, path ++ [:field], field_index)
    else
      [
        Core.error(
          :invalid_query_order_direction,
          path ++ [:direction],
          "query order direction #{inspect(direction)} is not supported",
          expected: @query_order_directions,
          actual: Core.value_type(direction),
          section: section,
          direction: direction
        )
        | errors
      ]
    end
  end

  def validate_query_order_entry(errors, section, field, path, field_index, _functions)
       when is_atom(field) or is_binary(field) do
    validate_query_field_reference(errors, section, field, path, field_index)
  end

  def validate_query_order_entry(errors, _section, entry, _path, _field_index, _functions)
       when is_tuple(entry) or is_map(entry) do
    errors
  end

  def validate_query_order_entry(errors, section, entry, path, _field_index, _functions) do
    invalid_query_field_reference(errors, section, entry, path)
  end

  def validate_query_group_entry(
         errors,
         _section,
         {:raw_sql, _sql},
         _path,
         _field_index,
         _functions
       ) do
    errors
  end

  def validate_query_group_entry(
         errors,
         section,
         {:udf, function_id, args},
         path,
         field_index,
         functions
       )
       when is_list(args) do
    validate_query_function_reference(
      errors,
      section,
      :group_by,
      function_id,
      args,
      path,
      field_index,
      functions
    )
  end

  def validate_query_group_entry(
         errors,
         section,
         {wrapper, groups},
         path,
         field_index,
         functions
       )
       when wrapper in @query_group_wrappers do
    if is_list(groups) do
      groups
      |> Enum.with_index()
      |> Enum.reduce(errors, fn {group, index}, acc ->
        validate_query_group_entry(
          acc,
          section,
          group,
          path ++ [wrapper, index],
          field_index,
          functions
        )
      end)
    else
      [
        Core.error(
          :invalid_query_group_wrapper,
          path,
          "query group wrapper #{inspect(wrapper)} must contain a list of fields",
          expected: :list,
          actual: Core.value_type(groups),
          section: section,
          wrapper: wrapper
        )
        | errors
      ]
    end
  end

  def validate_query_group_entry(errors, section, field, path, field_index, _functions)
       when is_atom(field) or is_binary(field) do
    validate_query_field_reference(errors, section, field, path, field_index)
  end

  def validate_query_group_entry(errors, _section, entry, _path, _field_index, _functions)
       when is_tuple(entry) or is_map(entry) do
    errors
  end

  def validate_query_group_entry(errors, section, entry, path, _field_index, _functions) do
    invalid_query_field_reference(errors, section, entry, path)
  end

  def validate_query_selector_reference(
         errors,
         section,
         call_site,
         {:udf, function_id, args},
         path,
         field_index,
         functions
       )
       when is_list(args) do
    validate_query_function_reference(
      errors,
      section,
      call_site,
      function_id,
      args,
      path,
      field_index,
      functions
    )
  end

  def validate_query_selector_reference(
         errors,
         section,
         _call_site,
         field,
         path,
         field_index,
         _functions
       )
       when is_atom(field) or is_binary(field) do
    validate_query_field_reference(errors, section, field, path, field_index)
  end

  def validate_query_selector_reference(
         errors,
         _section,
         _call_site,
         expression,
         _path,
         _field_index,
         _functions
       )
       when is_tuple(expression) or is_map(expression) do
    errors
  end

  def validate_query_selector_reference(
         errors,
         section,
         _call_site,
         field,
         path,
         _field_index,
         _functions
       ) do
    invalid_query_field_reference(errors, section, field, path)
  end

  def validate_query_function_reference(
         errors,
         section,
         call_site,
         function_id,
         args,
         path,
         field_index,
         functions
       ) do
    function_path = path ++ [:function]

    cond do
      not Core.non_empty_atom_or_string?(function_id) ->
        [
          Core.error(
            :invalid_query_function_id,
            function_path,
            "query function references must use a non-empty atom or string id",
            expected: "non-empty atom or string",
            actual: Core.value_type(function_id),
            section: section,
            function: function_id,
            call_site: call_site
          )
          | errors
        ]

      not is_map(functions) ->
        query_function_not_found_error(errors, section, call_site, function_id, function_path)

      true ->
        case Core.fetch_key(functions, function_id) do
          {:ok, function_spec} ->
            errors
            |> validate_query_function_call_site(
              section,
              call_site,
              function_id,
              function_spec,
              function_path
            )
            |> validate_query_function_args(
              section,
              call_site,
              function_id,
              args,
              function_spec,
              path,
              field_index,
              functions
            )

          :error ->
            query_function_not_found_error(errors, section, call_site, function_id, function_path)
        end
    end
  end

  def query_function_not_found_error(errors, section, call_site, function_id, path) do
    [
      Core.error(
        :query_function_not_found,
        path,
        "query references missing function #{inspect(function_id)}",
        section: section,
        function: function_id,
        call_site: call_site
      )
      | errors
    ]
  end

  def validate_query_function_call_site(
         errors,
         _section,
         _call_site,
         _function_id,
         function_spec,
         _path
       )
       when not is_map(function_spec) do
    errors
  end

  def validate_query_function_call_site(
         errors,
         section,
         call_site,
         function_id,
         function_spec,
         path
       ) do
    case Core.map_value(function_spec, :allowed_in) do
      nil ->
        errors

      allowed_in when is_list(allowed_in) ->
        if call_site in allowed_in do
          errors
        else
          [
            Core.error(
              :query_function_call_site_not_allowed,
              path,
              "function #{inspect(function_id)} is not allowed in query #{call_site}",
              section: section,
              function: function_id,
              call_site: call_site,
              allowed_in: allowed_in
            )
            | errors
          ]
        end

      _allowed_in ->
        errors
    end
  end

  def validate_query_function_args(
         errors,
         _section,
         _call_site,
         _function_id,
         _args,
         function_spec,
         _path,
         _field_index,
         _functions
       )
       when not is_map(function_spec) do
    errors
  end

  def validate_query_function_args(
         errors,
         section,
         call_site,
         function_id,
         args,
         function_spec,
         path,
         field_index,
         functions
       ) do
    case query_function_spec_args(function_spec) do
      :invalid ->
        errors

      spec_args ->
        errors
        |> validate_query_function_arg_count(
          section,
          call_site,
          function_id,
          args,
          spec_args,
          path
        )
        |> validate_query_function_selector_args(
          section,
          function_id,
          args,
          spec_args,
          path,
          field_index,
          functions
        )
    end
  end

  def query_function_spec_args(function_spec) do
    case Core.map_value(function_spec, :args) do
      nil -> []
      args when is_list(args) -> args
      _args -> :invalid
    end
  end

  def validate_query_function_arg_count(
         errors,
         section,
         call_site,
         function_id,
         args,
         spec_args,
         path
       ) do
    expected = length(spec_args)
    actual = length(args)

    if expected == actual do
      errors
    else
      [
        Core.error(
          :query_function_arg_count_mismatch,
          path ++ [:args],
          "function #{inspect(function_id)} expects #{expected} query argument(s), got #{actual}",
          expected: expected,
          actual: actual,
          section: section,
          function: function_id,
          call_site: call_site
        )
        | errors
      ]
    end
  end

  def validate_query_function_selector_args(
         errors,
         section,
         function_id,
         args,
         spec_args,
         path,
         field_index,
         functions
       ) do
    args
    |> Enum.zip(spec_args)
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {{arg, arg_spec}, index}, acc ->
      validate_query_function_arg(
        acc,
        section,
        function_id,
        arg,
        arg_spec,
        path ++ [:args, index],
        field_index,
        functions
      )
    end)
  end

  def validate_query_function_arg(
         errors,
         section,
         _function_id,
         arg,
         arg_spec,
         path,
         field_index,
         functions
       )
       when is_map(arg_spec) do
    case Core.map_value(arg_spec, :source) do
      :selector ->
        validate_query_function_selector_arg(errors, section, arg, path, field_index, functions)

      _source ->
        errors
    end
  end

  def validate_query_function_arg(
         errors,
         _section,
         _function_id,
         _arg,
         _arg_spec,
         _path,
         _field_index,
         _functions
       ) do
    errors
  end

  def validate_query_function_selector_arg(
         errors,
         section,
         {:field, field},
         path,
         field_index,
         functions
       ) do
    validate_query_selector_reference(
      errors,
      section,
      :select,
      field,
      path ++ [:field],
      field_index,
      functions
    )
  end

  def validate_query_function_selector_arg(
         errors,
         section,
         {:field, field, _alias},
         path,
         field_index,
         functions
       ) do
    validate_query_selector_reference(
      errors,
      section,
      :select,
      field,
      path ++ [:field],
      field_index,
      functions
    )
  end

  def validate_query_function_selector_arg(
         errors,
         section,
         {:udf, function_id, args},
         path,
         field_index,
         functions
       )
       when is_list(args) do
    validate_query_function_reference(
      errors,
      section,
      :select,
      function_id,
      args,
      path,
      field_index,
      functions
    )
  end

  def validate_query_function_selector_arg(
         errors,
         section,
         field,
         path,
         field_index,
         _functions
       )
       when is_atom(field) or is_binary(field) do
    validate_query_field_reference(errors, section, field, path, field_index)
  end

  def validate_query_function_selector_arg(
         errors,
         _section,
         _expression,
         _path,
         _field_index,
         _functions
       ) do
    errors
  end

  def validate_query_field_reference(errors, section, field, path, field_index) do
    cond do
      not Core.valid_static_source_path?(field) ->
        invalid_query_field_reference(errors, section, field, path)

      Core.known_field?(field_index, field) ->
        errors

      true ->
        [
          Core.error(
            :query_field_not_found,
            path,
            "query field #{inspect(field)} is not defined in source, schemas, or custom columns",
            section: section,
            field: field
          )
          | errors
        ]
    end
  end

  def invalid_query_field_reference(errors, section, field, path) do
    [
      Core.error(
        :invalid_query_field_reference,
        path,
        "query field references must be non-empty atoms or dotted string paths",
        expected: "non-empty atom or dotted string path",
        actual: Core.value_type(field),
        section: section,
        field: field
      )
      | errors
    ]
  end

  def query_order_direction?(direction), do: Core.enum_value?(direction, @query_order_directions)

  def validate_filters(errors, query, field_index) do
    filters = Core.map_value(query, :filters) || %{}
    required_filters = Core.map_value(query, :required_filters) || []

    errors
    |> validate_filter_registry(filters, field_index)
    |> validate_required_filters(required_filters, field_index)
  end

  def validate_filter_registry(errors, filters, field_index) when is_map(filters) do
    Enum.reduce(filters, errors, fn {filter_id, filter_config}, acc ->
      acc
      |> validate_filter_id(filter_id)
      |> validate_filter_config(filter_id, filter_config, field_index)
    end)
  end

  def validate_filter_registry(errors, filters, _field_index) do
    [
      Core.error(
        :invalid_section_shape,
        [:filters],
        "domain section :filters must be a map",
        expected: :map,
        actual: Core.value_type(filters)
      )
      | errors
    ]
  end

  def validate_filter_id(errors, filter_id) do
    if Core.non_empty_atom_or_string?(filter_id) do
      errors
    else
      [
        Core.error(
          :invalid_filter_id,
          [:filters, filter_id],
          "filter id #{inspect(filter_id)} must be a non-empty atom or string",
          expected: "non-empty atom or string",
          actual: Core.value_type(filter_id),
          filter: filter_id
        )
        | errors
      ]
    end
  end

  def validate_filter_config(errors, filter_id, filter_config, field_index)
       when is_map(filter_config) do
    errors
    |> validate_filter_config_field(filter_id, filter_config, field_index)
    |> validate_filter_config_type(filter_id, filter_config)
  end

  def validate_filter_config(errors, filter_id, filter_config, _field_index) do
    [
      Core.error(
        :invalid_filter_config,
        [:filters, filter_id],
        "filter #{inspect(filter_id)} config must be a map",
        expected: :map,
        actual: Core.value_type(filter_config),
        filter: filter_id
      )
      | errors
    ]
  end

  def validate_filter_config_field(errors, filter_id, filter_config, field_index) do
    if Core.has_key?(filter_config, :field) do
      field = Core.map_value(filter_config, :field)

      if Core.valid_static_source_path?(field) do
        FieldReference.validate_field_reference(errors, field, [:filters, filter_id, :field], field_index)
      else
        [
          Core.error(
            :invalid_filter_field,
            [:filters, filter_id, :field],
            "filter #{inspect(filter_id)} field must be a non-empty atom or dotted string path",
            expected: "non-empty atom or dotted string path",
            actual: Core.value_type(field),
            filter: filter_id,
            field: field
          )
          | errors
        ]
      end
    else
      errors
    end
  end

  def validate_filter_config_type(errors, filter_id, filter_config) do
    if Core.has_key?(filter_config, :type) do
      type = Core.map_value(filter_config, :type)

      if Core.non_empty_atom_or_string?(type) do
        errors
      else
        [
          Core.error(
            :invalid_filter_type,
            [:filters, filter_id, :type],
            "filter #{inspect(filter_id)} type must be a non-empty atom or string",
            expected: "non-empty atom or string",
            actual: Core.value_type(type),
            filter: filter_id,
            type: type
          )
          | errors
        ]
      end
    else
      errors
    end
  end

  def validate_required_filters(errors, required_filters, field_index)
       when is_list(required_filters) do
    required_filters
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {filter, index}, acc ->
      validate_filter_expression(acc, filter, [:required_filters, index], field_index)
    end)
  end

  def validate_required_filters(errors, required_filters, _field_index) do
    [
      Core.error(
        :invalid_section_shape,
        [:required_filters],
        "domain section :required_filters must be a list",
        expected: :list,
        actual: Core.value_type(required_filters)
      )
      | errors
    ]
  end

  def validate_filter_expression(errors, {op, filters}, path, field_index)
       when op in @logical_filter_ops and is_list(filters) do
    filters
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {filter, index}, acc ->
      validate_filter_expression(acc, filter, path ++ [index], field_index)
    end)
  end

  def validate_filter_expression(errors, {op, filter}, path, field_index)
       when op in @unary_filter_ops do
    validate_filter_expression(errors, filter, path ++ [op], field_index)
  end

  def validate_filter_expression(errors, {op, field, _value}, path, field_index)
       when op in @field_filter_ops do
    FieldReference.validate_field_reference(errors, field, path ++ [:field], field_index)
  end

  def validate_filter_expression(errors, {op, field, _left, _right}, path, field_index)
       when op in @field_filter_ops do
    FieldReference.validate_field_reference(errors, field, path ++ [:field], field_index)
  end

  def validate_filter_expression(errors, {field, _value}, path, field_index) do
    FieldReference.validate_field_reference(errors, field, path ++ [:field], field_index)
  end

  def validate_filter_expression(errors, {field, _op, _value}, path, field_index) do
    FieldReference.validate_field_reference(errors, field, path ++ [:field], field_index)
  end

  def validate_filter_expression(errors, filter, path, field_index) when is_map(filter) do
    case Core.map_value(filter, :field) do
      nil -> errors
      field -> FieldReference.validate_field_reference(errors, field, path ++ [:field], field_index)
    end
  end

  def validate_filter_expression(errors, _filter, _path, _field_index), do: errors

  def validate_functions(errors, query) do
    case Core.map_value(query, :functions) do
      nil ->
        errors

      functions when is_map(functions) ->
        Enum.reduce(functions, errors, fn {function_id, function_spec}, acc ->
          acc
          |> validate_function_id(function_id)
          |> validate_function_spec(function_id, function_spec)
        end)

      functions ->
        [
          Core.error(
            :invalid_section_shape,
            [:functions],
            "domain section :functions must be a map",
            expected: :map,
            actual: Core.value_type(functions)
          )
          | errors
        ]
    end
  end

  def validate_function_id(errors, function_id) do
    if Core.non_empty_atom_or_string?(function_id) do
      errors
    else
      [
        Core.error(
          :invalid_function_id,
          [:functions, function_id],
          "function id #{inspect(function_id)} must be a non-empty atom or string",
          expected: "non-empty atom or string",
          actual: Core.value_type(function_id),
          function: function_id
        )
        | errors
      ]
    end
  end

  def validate_function_spec(errors, function_id, function_spec) when is_map(function_spec) do
    errors
    |> validate_function_kind(function_id, function_spec)
    |> validate_function_sql_name(function_id, function_spec)
    |> validate_function_allowed_in(function_id, function_spec)
    |> validate_function_args(function_id, function_spec)
    |> validate_function_returns(function_id, function_spec)
  end

  def validate_function_spec(errors, function_id, function_spec) do
    [
      Core.error(
        :invalid_function_spec,
        [:functions, function_id],
        "function #{inspect(function_id)} spec must be a map",
        expected: :map,
        actual: Core.value_type(function_spec),
        function: function_id
      )
      | errors
    ]
  end

  def validate_function_kind(errors, function_id, function_spec) do
    case Core.map_value(function_spec, :kind) do
      kind when kind in [:scalar, :predicate, :table] ->
        errors

      kind ->
        [
          Core.error(
            :invalid_function_kind,
            [:functions, function_id, :kind],
            "function #{inspect(function_id)} kind must be :scalar, :predicate, or :table",
            expected: [:scalar, :predicate, :table],
            actual: Core.value_type(kind),
            function: function_id,
            kind: kind
          )
          | errors
        ]
    end
  end

  def validate_function_sql_name(errors, function_id, function_spec) do
    sql_name = Core.map_value(function_spec, :sql_name)

    if Selecto.UDF.valid_sql_name?(sql_name) do
      errors
    else
      [
        Core.error(
          :invalid_function_sql_name,
          [:functions, function_id, :sql_name],
          "function #{inspect(function_id)} sql_name must be a safe qualified SQL function name",
          expected: "safe qualified SQL function name",
          actual: Core.value_type(sql_name),
          function: function_id,
          sql_name: sql_name
        )
        | errors
      ]
    end
  end

  def validate_function_allowed_in(errors, function_id, function_spec) do
    case Core.map_value(function_spec, :allowed_in) do
      nil ->
        errors

      allowed_in when is_list(allowed_in) ->
        allowed_in
        |> Enum.with_index()
        |> Enum.reduce(errors, fn {call_site, index}, acc ->
          validate_function_call_site(acc, function_id, call_site, [
            :functions,
            function_id,
            :allowed_in,
            index
          ])
        end)

      allowed_in ->
        [
          Core.error(
            :invalid_function_allowed_in,
            [:functions, function_id, :allowed_in],
            "function #{inspect(function_id)} allowed_in must be a list",
            expected: :list,
            actual: Core.value_type(allowed_in),
            function: function_id
          )
          | errors
        ]
    end
  end

  def validate_function_call_site(errors, function_id, call_site, path) do
    if Selecto.UDF.valid_call_site?(call_site) do
      errors
    else
      [
        Core.error(
          :invalid_function_call_site,
          path,
          "function #{inspect(function_id)} call site #{inspect(call_site)} is not supported",
          expected: Selecto.UDF.allowed_call_sites(),
          actual: Core.value_type(call_site),
          function: function_id,
          call_site: call_site
        )
        | errors
      ]
    end
  end

  def validate_function_args(errors, function_id, function_spec) do
    case Core.map_value(function_spec, :args) do
      nil ->
        errors

      args when is_list(args) ->
        args
        |> Enum.with_index()
        |> Enum.reduce(errors, fn {arg_spec, index}, acc ->
          validate_function_arg_spec(acc, function_id, arg_spec, [
            :functions,
            function_id,
            :args,
            index
          ])
        end)

      args ->
        [
          Core.error(
            :invalid_function_args,
            [:functions, function_id, :args],
            "function #{inspect(function_id)} args must be a list",
            expected: :list,
            actual: Core.value_type(args),
            function: function_id
          )
          | errors
        ]
    end
  end

  def validate_function_arg_spec(errors, function_id, arg_spec, path) when is_map(arg_spec) do
    errors
    |> validate_function_arg_name(function_id, arg_spec, path)
    |> validate_function_arg_type(function_id, arg_spec, path)
    |> validate_function_arg_source(function_id, arg_spec, path)
  end

  def validate_function_arg_spec(errors, function_id, arg_spec, path) do
    [
      Core.error(
        :invalid_function_arg_spec,
        path,
        "function #{inspect(function_id)} arg spec must be a map",
        expected: :map,
        actual: Core.value_type(arg_spec),
        function: function_id
      )
      | errors
    ]
  end

  def validate_function_arg_name(errors, function_id, arg_spec, path) do
    name = Core.map_value(arg_spec, :name)

    if Core.non_empty_atom_or_string?(name) do
      errors
    else
      [
        Core.error(
          :invalid_function_arg_name,
          path ++ [:name],
          "function #{inspect(function_id)} arg name must be a non-empty atom or string",
          expected: "non-empty atom or string",
          actual: Core.value_type(name),
          function: function_id,
          name: name
        )
        | errors
      ]
    end
  end

  def validate_function_arg_type(errors, function_id, arg_spec, path) do
    if Core.has_key?(arg_spec, :type) do
      errors
    else
      [
        Core.error(
          :missing_function_arg_type,
          path ++ [:type],
          "function #{inspect(function_id)} arg must declare a type",
          function: function_id
        )
        | errors
      ]
    end
  end

  def validate_function_arg_source(errors, function_id, arg_spec, path) do
    source = Core.map_value(arg_spec, :source)

    if Selecto.UDF.valid_arg_source?(source) do
      errors
    else
      [
        Core.error(
          :invalid_function_arg_source,
          path ++ [:source],
          "function #{inspect(function_id)} arg source must be :selector, :value, or :literal",
          expected: Selecto.UDF.allowed_arg_sources(),
          actual: Core.value_type(source),
          function: function_id,
          source: source
        )
        | errors
      ]
    end
  end

  def validate_function_returns(errors, function_id, function_spec) do
    case Core.map_value(function_spec, :kind) do
      :predicate ->
        validate_predicate_function_returns(errors, function_id, function_spec)

      :table ->
        validate_table_function_returns(errors, function_id, function_spec)

      :scalar ->
        validate_scalar_function_returns(errors, function_id, function_spec)

      _kind ->
        errors
    end
  end

  def validate_predicate_function_returns(errors, function_id, function_spec) do
    case Core.map_value(function_spec, :returns) do
      :boolean ->
        errors

      returns ->
        [
          Core.error(
            :invalid_function_returns,
            [:functions, function_id, :returns],
            "predicate function #{inspect(function_id)} must declare returns: :boolean",
            expected: :boolean,
            actual: Core.value_type(returns),
            function: function_id,
            returns: returns
          )
          | errors
        ]
    end
  end

  def validate_table_function_returns(errors, function_id, function_spec) do
    columns =
      case Core.map_value(function_spec, :returns) do
        %{} = returns -> Core.map_value(returns, :columns)
        _returns -> nil
      end

    if is_map(columns) and map_size(columns) > 0 do
      errors
    else
      [
        Core.error(
          :invalid_function_returns,
          [:functions, function_id, :returns],
          "table function #{inspect(function_id)} must declare returns: %{columns: %{...}}",
          expected: "%{columns: %{...}}",
          actual: Core.value_type(Core.map_value(function_spec, :returns)),
          function: function_id,
          returns: Core.map_value(function_spec, :returns)
        )
        | errors
      ]
    end
  end

  def validate_scalar_function_returns(errors, function_id, function_spec) do
    case Core.map_value(function_spec, :returns) do
      nil ->
        errors

      returns when is_atom(returns) ->
        errors

      {:array, _type} ->
        errors

      returns ->
        [
          Core.error(
            :invalid_function_returns,
            [:functions, function_id, :returns],
            "scalar function #{inspect(function_id)} returns must be an atom or array tuple when provided",
            expected: "atom or {:array, type}",
            actual: Core.value_type(returns),
            function: function_id,
            returns: returns
          )
          | errors
        ]
    end
  end

end
