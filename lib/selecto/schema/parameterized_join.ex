defmodule Selecto.Schema.ParameterizedJoin do
  @moduledoc """
  Processing and configuration for parameterized joins.
  
  This module handles the creation and management of parameterized joins,
  including parameter validation, SQL generation context, and join condition resolution.
  """

  alias Selecto.FieldResolver.ParameterizedParser
  require Logger

  @type parameter_definition :: %{
    name: atom(),
    type: atom(),
    required: boolean(),
    default: any(),
    description: String.t() | nil
  }

  @type validated_parameter :: %{
    name: atom(),
    value: any(),
    type: atom()
  }

  @type parameterized_join_config :: %{
    base_config: map(),
    parameters: [validated_parameter()],
    parameter_context: map(),
    join_condition: String.t() | nil,
    parameter_signature: String.t()
  }

  @doc """
  Process a parameterized join configuration by validating parameters and building context.
  """
  @spec process_parameterized_join(
    join_id :: atom(),
    join_config :: map(),
    parameters :: [ParameterizedParser.parameter()],
    parent :: atom(),
    from_source :: module(),
    queryable :: map()
  ) :: parameterized_join_config()
  def process_parameterized_join(join_id, join_config, parameters, parent, from_source, queryable) do
    # Validate parameters against join definition
    validated_params = validate_parameters(join_config.parameters || [], parameters)
    
    # Build join with parameter context
    base_join = configure_base_join(join_id, join_config, parent, from_source, queryable)
    
    parameter_context = build_parameter_context(validated_params)
    parameter_signature = build_parameter_signature(parameters)
    join_condition = resolve_parameterized_condition(join_config, validated_params)

    %{
      base_config: base_join,
      parameters: validated_params,
      parameter_context: parameter_context,
      join_condition: join_condition,
      parameter_signature: parameter_signature
    }
  end

  @doc """
  Validate provided parameters against parameter definitions from join configuration.
  """
  @spec validate_parameters([parameter_definition()], [ParameterizedParser.parameter()]) :: [validated_parameter()]
  def validate_parameters(param_definitions, provided_params) do
    # Match provided parameters with definitions, apply defaults, validate types
    param_definitions
    |> Enum.with_index()
    |> Enum.map(fn {definition, index} ->
      case Enum.at(provided_params, index) do
        nil -> 
          # Use default value if provided
          default_value = Map.get(definition, :default)
          required = Map.get(definition, :required, false)
          case {default_value, required} do
            {nil, true} ->
              raise "Required parameter '#{definition.name}' missing at position #{index + 1}"
            {default_value, _} ->
              %{name: definition.name, value: default_value, type: definition.type}
          end
        {provided_type, provided_value} ->
          expected_type = definition.type
          case validate_parameter_type(provided_type, provided_value, expected_type) do
            {:ok, validated_value} ->
              %{name: definition.name, value: validated_value, type: expected_type}
            {:error, reason} ->
              raise "Parameter '#{definition.name}' at position #{index + 1}: #{reason}"
          end
      end
    end)
  end

  @doc """
  Build parameter context map for SQL template resolution.
  """
  @spec build_parameter_context([validated_parameter()]) :: map()
  def build_parameter_context(validated_params) do
    validated_params
    |> Enum.into(%{}, fn param ->
      {param.name, param.value}
    end)
  end

  @doc """
  Build parameter signature string for caching and identification.
  """
  @spec build_parameter_signature([ParameterizedParser.parameter()]) :: String.t()
  def build_parameter_signature(parameters) when is_list(parameters) and parameters != [] do
    parameters
    |> Enum.map(fn 
      {_type, value} when is_binary(value) -> value
      {_type, value} -> to_string(value)
    end)
    |> Enum.join(":")
  end

  def build_parameter_signature(_), do: ""

  @doc """
  Resolve parameterized join condition by replacing parameter placeholders.
  """
  @spec resolve_parameterized_condition(map(), [validated_parameter()]) :: String.t() | nil
  def resolve_parameterized_condition(join_config, validated_params) do
    case Map.get(join_config, :join_condition) do
      nil -> nil
      condition_template when is_binary(condition_template) ->
        # Replace parameter placeholders with actual values
        Enum.reduce(validated_params, condition_template, fn param, acc ->
          placeholder = "$param_#{param.name}"
          replacement = format_parameter_for_sql(param.value, param.type)
          String.replace(acc, placeholder, replacement)
        end)
      _ -> nil
    end
  end

  @doc """
  Update join configuration to include parameterized context.
  """
  @spec enhance_join_with_parameters(map(), parameterized_join_config()) :: map()
  def enhance_join_with_parameters(base_join, parameterized_config) do
    Map.merge(base_join, %{
      parameters: parameterized_config.parameters,
      parameter_context: parameterized_config.parameter_context,
      join_condition: parameterized_config.join_condition,
      parameter_signature: parameterized_config.parameter_signature,
      is_parameterized: true
    })
  end

  # Private implementation

  defp configure_base_join(join_id, join_config, parent, from_source, queryable) do
    # Delegate to existing join configuration logic
    # This will be integrated with the main Selecto.Schema.Join module
    %{
      id: join_id,
      config: join_config,
      from_source: from_source,
      queryable: queryable,
      parent: parent,
      name: Map.get(join_config, :name, join_id)
    }
  end

  defp validate_parameter_type(provided_type, provided_value, expected_type) do
    case {provided_type, expected_type} do
      # Exact type match
      {type, type} -> {:ok, provided_value}
      
      # String can be converted to atom
      {:string, :atom} -> {:ok, String.to_atom(provided_value)}
      
      # Integer can be converted to float
      {:integer, :float} -> {:ok, provided_value * 1.0}
      
      # String can be parsed as integer
      {:string, :integer} ->
        case Integer.parse(provided_value) do
          {int_val, ""} -> {:ok, int_val}
          _ -> {:error, "Cannot parse '#{provided_value}' as integer"}
        end
      
      # String can be parsed as float
      {:string, :float} ->
        case Float.parse(provided_value) do
          {float_val, ""} -> {:ok, float_val}
          _ -> {:error, "Cannot parse '#{provided_value}' as float"}
        end
      
      # String can be parsed as boolean
      {:string, :boolean} ->
        case String.downcase(provided_value) do
          "true" -> {:ok, true}
          "false" -> {:ok, false}
          "1" -> {:ok, true}
          "0" -> {:ok, false}
          _ -> {:error, "Cannot parse '#{provided_value}' as boolean"}
        end
      
      # Type mismatch
      _ -> {:error, "Expected #{expected_type}, got #{provided_type} '#{provided_value}'"}
    end
  end

  defp format_parameter_for_sql(value, type) do
    case type do
      :string -> "'#{String.replace(to_string(value), "'", "''")}'"
      :integer -> to_string(value)
      :float -> to_string(value)
      :boolean -> if value, do: "true", else: "false"
      :atom -> "'#{String.replace(to_string(value), "'", "''")}'"
      _ -> "'#{String.replace(to_string(value), "'", "''")}'"
    end
  end
end