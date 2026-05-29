defmodule Selecto.Domain.Contract.Query.FieldLists do
  @moduledoc false

  use Selecto.Domain.Constants

  alias Selecto.Domain.Contract.Shared.Core

  @query_order_directions [:asc, :desc, :asc_nulls_first, :asc_nulls_last, :desc_nulls_first, :desc_nulls_last]
  @query_group_wrappers [:rollup, :grouping_set]

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
end
