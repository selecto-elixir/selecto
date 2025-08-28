defmodule Selecto.FieldResolver.ParameterizedParser do
  @moduledoc """
  Parser for parameterized joins with dot notation support.

  This module handles parsing field references that include parameterized joins
  using the new dot notation syntax: `table.field` and `table:param1:param2.field`
  
  ## Examples
  
      # Simple dot notation
      "posts.title" → %{type: :qualified, join: "posts", field: "title"}
      
      # Parameterized join with single parameter
      "posts:published.title" → %{type: :parameterized, join: "posts", field: "title", parameters: []}
      
      # Multiple parameters with type inference
      "products:electronics:25.0:true.name" → %{
        type: :parameterized, 
        join: "products", 
        field: "name",
        parameters: [
          {:string, "electronics"}, 
          {:float, 25.0}, 
          {:boolean, true}
        ]
      }
  """

  @type parameter :: {:string, String.t()} | {:integer, integer()} | {:float, float()} | {:boolean, boolean()}
  @type parsed_field :: %{
    type: :simple | :qualified | :parameterized | :bracket_legacy,
    field: String.t(),
    join: String.t() | nil,
    parameters: [parameter()] | nil
  }

  @doc """
  Parse a field reference string into its components.
  
  Supports both legacy bracket notation and new dot notation with parameters.
  """
  @spec parse_field_reference(String.t()) :: {:ok, parsed_field()} | {:error, String.t()}
  def parse_field_reference(field_ref) when is_binary(field_ref) do
    cond do
      # Legacy bracket notation: "table[field]"
      String.contains?(field_ref, "[") && String.contains?(field_ref, "]") ->
        parse_bracket_notation(field_ref)
      
      # Dot notation: "table.field" or "table:params.field"
      String.contains?(field_ref, ".") ->
        parse_dot_notation(field_ref)
      
      # Simple field name
      true ->
        {:ok, %{type: :simple, field: field_ref, join: nil, parameters: nil}}
    end
  end

  def parse_field_reference(field_ref) when is_atom(field_ref) do
    parse_field_reference(Atom.to_string(field_ref))
  end

  def parse_field_reference(_field_ref) do
    {:error, "Field reference must be a string or atom"}
  end

  @doc """
  Parse parameters from a join string like "table:param1:param2"
  """
  @spec parse_join_with_parameters(String.t()) :: {String.t(), [parameter()]} | {:error, String.t()}
  def parse_join_with_parameters(join_string) when is_binary(join_string) do
    case String.split(join_string, ":") do
      [join_name] -> 
        {join_name, []}
      [join_name | params] -> 
        case parse_parameters(params) do
          {:ok, parsed_params} -> {join_name, parsed_params}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @doc """
  Validate parsed parameters against join parameter definitions.
  """
  @spec validate_parameters([parameter()], [map()]) :: {:ok, [map()]} | {:error, String.t()}
  def validate_parameters(provided_params, param_definitions) do
    try do
      validated_params = 
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
                  throw({:error, "Required parameter '#{definition.name}' missing at position #{index + 1}"})
                {default_value, _} ->
                  %{name: definition.name, value: default_value, type: definition.type}
              end
            {provided_type, provided_value} ->
              expected_type = definition.type
              case validate_parameter_type(provided_type, provided_value, expected_type) do
                {:ok, validated_value} ->
                  %{name: definition.name, value: validated_value, type: expected_type}
                {:error, reason} ->
                  throw({:error, "Parameter '#{definition.name}' at position #{index + 1}: #{reason}"})
              end
          end
        end)
      
      {:ok, validated_params}
    catch
      {:error, reason} -> {:error, reason}
    end
  end

  # Private Implementation

  defp parse_bracket_notation(field_ref) do
    # Extract "table[field]" format
    case Regex.run(~r/^(.+?)\[([^\]]+)\]$/, field_ref) do
      [_, join, field] ->
        {:ok, %{type: :bracket_legacy, join: join, field: field, parameters: nil}}
      nil ->
        {:error, "Invalid bracket notation format: #{field_ref}"}
    end
  end

  defp parse_dot_notation(field_ref) do
    case String.split(field_ref, ".", parts: 2) do
      [join_with_params, field] ->
        case parse_join_with_parameters(join_with_params) do
          {join_name, []} ->
            {:ok, %{type: :qualified, join: join_name, field: field, parameters: nil}}
          {join_name, parameters} ->
            {:ok, %{type: :parameterized, join: join_name, field: field, parameters: parameters}}
          {:error, reason} ->
            {:error, reason}
        end
      [field] ->
        {:ok, %{type: :simple, field: field, join: nil, parameters: nil}}
      _ ->
        {:error, "Invalid dot notation format: #{field_ref}"}
    end
  end

  defp parse_parameters(params) do
    try do
      parsed_params = Enum.map(params, &parse_single_parameter/1)
      
      # Check for any error results
      case Enum.find(parsed_params, fn 
        {:error, _} -> true
        _ -> false
      end) do
        {:error, reason} -> {:error, reason}
        nil -> {:ok, parsed_params}
      end
    rescue
      e -> {:error, "Error parsing parameters: #{Exception.message(e)}"}
    end
  end

  @doc """
  Parse a single parameter string into a typed value.
  Used for testing and debugging individual parameters.
  """
  def parse_single_parameter(param) when is_binary(param) do
    cond do
      # Boolean literals
      param == "true" -> {:boolean, true}
      param == "false" -> {:boolean, false}
      
      # Float literals (must come before integer check)
      String.match?(param, ~r/^-?\d+\.\d+$/) -> 
        case Float.parse(param) do
          {float_val, ""} -> {:float, float_val}
          _ -> {:error, "Invalid float format: #{param}"}
        end
      
      String.match?(param, ~r/^-?\d+$/) -> 
        case Integer.parse(param) do
          {int_val, ""} -> {:integer, int_val}
          _ -> {:error, "Invalid integer format: #{param}"}
        end
        
      # Single-quoted strings
      String.starts_with?(param, "'") && String.ends_with?(param, "'") && String.length(param) >= 2 ->
        unquoted = String.slice(param, 1..-2//1)
        # Handle escaped single quotes
        unescaped = String.replace(unquoted, "\\'", "'")
        {:string, unescaped}
      
      # Double-quoted strings  
      String.starts_with?(param, "\"") && String.ends_with?(param, "\"") && String.length(param) >= 2 ->
        unquoted = String.slice(param, 1..-2//1)
        # Handle escaped double quotes
        unescaped = String.replace(unquoted, "\\\"", "\"")
        {:string, unescaped}
        
      # Unquoted identifiers (must be valid identifier characters)
      String.match?(param, ~r/^[a-zA-Z_][a-zA-Z0-9_]*$/) ->
        {:string, param}
        
      # Error case
      true -> {:error, "Invalid parameter format: '#{param}'. Parameters must be boolean literals, numbers, quoted strings, or valid identifiers."}
    end
  end

  def parse_single_parameter(param) do
    {:error, "Parameter must be a string, got: #{inspect(param)}"}
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
end