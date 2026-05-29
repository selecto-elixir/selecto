defmodule Selecto.Domain.Contract.Query.Functions do
  @moduledoc false

  use Selecto.Domain.Constants

  alias Selecto.Domain.Contract.Shared.Core

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
