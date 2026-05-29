defmodule Selecto.Domain.Contract.Shared.StaticFilters do
  @moduledoc false

  use Selecto.Domain.Constants
  alias Selecto.Domain.Contract.Shared.Core

  @logical_filter_ops [:and, :or]
  @unary_filter_ops [:not]
  def validate_static_filter_expression(errors, owner, filter, path) when is_tuple(filter) do
    validate_static_filter_parts(errors, owner, Tuple.to_list(filter), filter, path)
  end

  def validate_static_filter_expression(errors, owner, filter, path) when is_list(filter) do
    validate_static_filter_parts(errors, owner, filter, filter, path)
  end

  def validate_static_filter_expression(errors, owner, filter, path) do
    invalid_static_filter_expression(errors, owner, filter, path)
  end

  def validate_static_filter_parts(
        errors,
        owner,
        [op, operands],
        _filter,
        path
      ) do
    cond do
      static_logical_filter_op?(op) ->
        validate_static_logical_filter(errors, owner, op, operands, path)

      static_unary_filter_op?(op) ->
        validate_static_filter_expression(
          errors,
          owner,
          operands,
          path ++ [:operand]
        )

      static_known_filter_op?(op) ->
        invalid_static_filter_operands(errors, owner, op, path, operands)

      static_filter_operator_value?(op) ->
        invalid_static_filter_operator(errors, owner, op, path)

      true ->
        invalid_static_filter_expression(errors, owner, [op, operands], path)
    end
  end

  def validate_static_filter_parts(
        errors,
        owner,
        [op, field, _value],
        _filter,
        path
      ) do
    validate_static_field_filter(errors, owner, op, field, path)
  end

  def validate_static_filter_parts(
        errors,
        owner,
        [op, field, _left, _right],
        _filter,
        path
      ) do
    validate_static_field_filter(errors, owner, op, field, path)
  end

  def validate_static_filter_parts(
        errors,
        owner,
        [op | _] = filter,
        _raw,
        path
      ) do
    cond do
      static_known_filter_op?(op) ->
        invalid_static_filter_operands(errors, owner, op, path, filter)

      static_filter_operator_value?(op) ->
        invalid_static_filter_operator(errors, owner, op, path)

      true ->
        invalid_static_filter_expression(errors, owner, filter, path)
    end
  end

  def validate_static_filter_parts(errors, owner, filter, _raw, path) do
    invalid_static_filter_expression(errors, owner, filter, path)
  end

  def validate_static_logical_filter(errors, owner, _op, filters, path)
      when is_list(filters) do
    filters
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {filter, index}, acc ->
      validate_static_filter_expression(acc, owner, filter, path ++ [index])
    end)
  end

  def validate_static_logical_filter(errors, owner, op, filters, path) do
    invalid_static_filter_operands(errors, owner, op, path, filters)
  end

  def validate_static_field_filter(errors, owner, op, field, path) do
    cond do
      static_field_filter_op?(op) and Core.valid_static_source_path?(field) ->
        errors

      static_field_filter_op?(op) ->
        [
          Core.error(
            static_filter_error_code(owner, :path),
            path ++ [:field],
            "#{static_filter_subject(owner)} filter field must be a non-empty atom or dotted string path",
            static_filter_attrs(owner,
              expected: "non-empty atom or dotted string path",
              actual: Core.value_type(field),
              field: field
            )
          )
          | errors
        ]

      static_known_filter_op?(op) ->
        invalid_static_filter_operands(errors, owner, op, path, field)

      static_filter_operator_value?(op) ->
        invalid_static_filter_operator(errors, owner, op, path)

      true ->
        invalid_static_filter_expression(errors, owner, [op, field], path)
    end
  end

  def invalid_static_filter_operator(errors, owner, op, path) do
    [
      Core.error(
        static_filter_error_code(owner, :operator),
        path,
        "#{static_filter_subject(owner)} filter operator #{inspect(op)} is not supported",
        static_filter_attrs(owner,
          expected: "known filter operator",
          actual: Core.value_type(op),
          operator: op
        )
      )
      | errors
    ]
  end

  def invalid_static_filter_operands(errors, owner, op, path, operands) do
    [
      Core.error(
        static_filter_error_code(owner, :operands),
        path,
        "#{static_filter_subject(owner)} filter operator #{inspect(op)} has invalid operands",
        static_filter_attrs(owner,
          expected: "operator operands",
          actual: Core.value_type(operands),
          operator: op
        )
      )
      | errors
    ]
  end

  def invalid_static_filter_expression(errors, owner, filter, path) do
    [
      Core.error(
        static_filter_error_code(owner, :expression),
        path,
        "#{static_filter_subject(owner)} filter must be an operator tuple or list",
        static_filter_attrs(owner,
          expected: "operator tuple or list",
          actual: Core.value_type(filter),
          filter: filter
        )
      )
      | errors
    ]
  end

  def static_known_filter_op?(op) do
    static_logical_filter_op?(op) or static_unary_filter_op?(op) or static_field_filter_op?(op)
  end

  def static_logical_filter_op?(op), do: Core.enum_value?(op, @logical_filter_ops)

  def static_unary_filter_op?(op), do: Core.enum_value?(op, @unary_filter_ops)

  def static_field_filter_op?(op), do: Core.enum_value?(op, @field_filter_ops)

  def static_filter_operator_value?(op) when is_atom(op), do: not is_nil(op)

  def static_filter_operator_value?(op) when is_binary(op), do: String.trim(op) != ""

  def static_filter_operator_value?(_op), do: false

  def static_filter_owner(:choice_source, id) do
    %{kind: :choice_source, id: id, attr: :choice_source, label: "choice source"}
  end

  def static_filter_owner(:source_relationship, id) do
    %{
      kind: :source_relationship,
      id: id,
      attr: :source_relationship,
      label: "source relationship"
    }
  end

  def static_filter_subject(owner), do: "#{owner.label} #{inspect(owner.id)}"

  def static_filter_attrs(owner, attrs), do: Keyword.put(attrs, owner.attr, owner.id)

  def static_filter_error_code(%{kind: :choice_source}, :operator),
    do: :invalid_choice_source_filter_operator

  def static_filter_error_code(%{kind: :choice_source}, :path),
    do: :invalid_choice_source_filter_path

  def static_filter_error_code(%{kind: :choice_source}, :expression),
    do: :invalid_choice_source_filter_expression

  def static_filter_error_code(%{kind: :choice_source}, :operands),
    do: :invalid_choice_source_filter_operands

  def static_filter_error_code(%{kind: :source_relationship}, :operator),
    do: :invalid_source_relationship_filter_operator

  def static_filter_error_code(%{kind: :source_relationship}, :path),
    do: :invalid_source_relationship_filter_path

  def static_filter_error_code(%{kind: :source_relationship}, :expression),
    do: :invalid_source_relationship_filter_expression

  def static_filter_error_code(%{kind: :source_relationship}, :operands),
    do: :invalid_source_relationship_filter_operands
end
