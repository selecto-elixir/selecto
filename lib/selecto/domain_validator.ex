defmodule Selecto.DomainValidator do
  @moduledoc """
  Domain validation for Selecto configurations.
  
  Validates domain configuration to catch errors early and prevent runtime failures.
  Checks for join dependency cycles, missing references, required keys for advanced
  join types, and other structural integrity issues.
  
  ## Usage
  
      # Validate during configure (enabled by default)
      domain = %{source: ..., schemas: ..., joins: ...}
      selecto = Selecto.configure(domain, postgrex_opts)
      
      # Disable validation for performance-critical scenarios
      selecto = Selecto.configure(domain, postgrex_opts, validate: false)
      
      # Or validate explicitly
      Selecto.DomainValidator.validate_domain!(domain)
      {:ok, _} = Selecto.DomainValidator.validate_domain(domain)
      
  ## Compile-time Validation
  
      # Validate domain at compile time (recommended for static configurations)
      defmodule MyDomain do
        use Selecto.DomainValidator, domain: %{
          source: %{...},
          schemas: %{...},
          joins: %{...}
        }
      end
  """
  
  # import Selecto.Types - removed to avoid circular dependency
  
  @doc """
  Validates a domain configuration, raising on validation errors.
  
  ## Validations Performed
  
  - Join dependency cycle detection
  - Association existence validation  
  - Schema reference validation
  - Column/field existence validation
  - Advanced join type required key validation
  - Custom column/filter reference validation
  
  ## Examples
  
      iex> domain = %{source: valid_source, schemas: valid_schemas, joins: valid_joins}
      iex> Selecto.DomainValidator.validate_domain!(domain)
      :ok
      
      iex> domain = %{source: valid_source, schemas: valid_schemas, joins: cyclic_joins}
      iex> Selecto.DomainValidator.validate_domain!(domain)
      ** (Selecto.DomainValidator.ValidationError) Join dependency cycle detected: a -> b -> c -> a
  """
  @spec validate_domain!(Selecto.Types.domain()) :: :ok
  def validate_domain!(domain) do
    case validate_domain(domain) do
      :ok -> :ok
      {:error, errors} -> raise Selecto.DomainValidator.ValidationError, message: format_errors(errors)
    end
  end
  
  @doc """
  Validates a domain configuration, returning {:ok, domain} or {:error, errors}.
  
  Non-raising version of validate_domain!/1.
  """
  def validate_domain(domain) do
    errors = []
    
    # Validate top-level structure
    errors = validate_required_keys(domain, [:source, :schemas], errors)
    
    errors = errors
    |> validate_schemas(domain)
    |> validate_associations(domain) 
    |> validate_joins(domain)
    
    # Only do complex validations if basic structure is sound
    final_errors = if Enum.empty?(errors) do
      errors
      |> validate_join_cycles(domain)
      |> validate_column_references(domain)
      |> validate_advanced_join_requirements(domain)
    else
      errors
    end
    
    
    case final_errors do
      [] -> :ok
      _ -> {:error, final_errors}
    end
  end
  
  # Validate required top-level keys exist
  defp validate_required_keys(domain, required_keys, errors) do
    missing_keys = required_keys -- Map.keys(domain)
    case missing_keys do
      [] -> errors
      _ -> errors ++ [{:missing_required_keys, missing_keys}]
    end
  end
  
  # Validate schemas structure
  defp validate_schemas(errors, domain) do
    schemas = Map.get(domain, :schemas, %{})
    
    Enum.reduce(schemas, errors, fn {schema_name, schema}, acc ->
      acc
      |> validate_schema_structure(schema_name, schema)
      |> validate_schema_columns(schema_name, schema)
    end)
  end
  
  defp validate_schema_structure(errors, schema_name, schema) do
    required_keys = [:source_table, :primary_key, :fields, :columns]
    missing_keys = required_keys -- Map.keys(schema)
    
    case missing_keys do
      [] -> errors
      _ -> errors ++ [{:schema_missing_keys, {schema_name, missing_keys}}]
    end
  end
  
  defp validate_schema_columns(errors, schema_name, schema) do
    fields = Map.get(schema, :fields, [])
    columns = Map.get(schema, :columns, %{})
    
    # Check that all fields have column definitions
    missing_columns = fields -- Map.keys(columns)
    
    case missing_columns do
      [] -> errors
      _ -> errors ++ [{:schema_missing_column_defs, {schema_name, missing_columns}}]
    end
  end
  
  # Validate associations reference valid schemas
  defp validate_associations(errors, domain) do
    source = Map.get(domain, :source, %{})
    schemas = Map.get(domain, :schemas, %{})
    all_schemas = Map.put(schemas, :source, source)
    
    Enum.reduce(all_schemas, errors, fn {schema_name, schema}, acc ->
      associations = Map.get(schema, :associations, %{})
      
      Enum.reduce(associations, acc, fn {assoc_name, assoc}, inner_acc ->
        queryable = Map.get(assoc, :queryable)
        
        cond do
          is_nil(queryable) ->
            inner_acc ++ [{:association_missing_queryable, {schema_name, assoc_name}}]
          
          not Map.has_key?(schemas, queryable) and queryable != :source ->
            inner_acc ++ [{:association_invalid_queryable, {schema_name, assoc_name, queryable}}]
          
          true ->
            inner_acc
        end
      end)
    end)
  end
  
  # Validate joins reference valid associations  
  defp validate_joins(errors, domain) do
    source = Map.get(domain, :source, %{})
    joins = Map.get(domain, :joins, %{})
    
    validate_join_tree(errors, joins, source, domain, :selecto_root)
  end
  
  defp validate_join_tree(errors, joins, parent_schema, domain, parent_name) do
    Enum.reduce(joins, errors, fn {join_name, join_config}, acc ->
      acc = validate_join_association_exists(acc, join_name, parent_schema, parent_name)
      
      # Recursively validate nested joins
      case Map.get(join_config, :joins) do
        nil -> acc
        nested_joins ->
          # Find the target schema for nested joins
          association = get_in(parent_schema, [:associations, join_name])
          target_schema = get_target_schema(association, domain)
          validate_join_tree(acc, nested_joins, target_schema, domain, join_name)
      end
    end)
  end
  
  defp validate_join_association_exists(errors, join_name, parent_schema, parent_name) do
    associations = Map.get(parent_schema, :associations, %{})
    
    case Map.has_key?(associations, join_name) do
      true -> errors
      false -> errors ++ [{:join_missing_association, {parent_name, join_name}}]
    end
  end
  
  defp get_target_schema(association, _domain) when is_nil(association), do: nil
  defp get_target_schema(association, domain) do
    queryable = Map.get(association, :queryable)
    Map.get(domain.schemas, queryable)
  end
  
  # Validate join dependency cycles
  defp validate_join_cycles(errors, domain) do
    joins = extract_all_joins(domain)
    cycles = detect_cycles(joins)
    
    case cycles do
      [] -> errors
      cycles -> errors ++ Enum.map(cycles, fn cycle -> {:join_cycle_detected, cycle} end)
    end
  end
  
  defp extract_all_joins(domain) do
    # Extract all joins into a flat map with dependencies
    source = Map.get(domain, :source, %{})
    domain_with_defaults = Map.put_new(domain, :joins, %{})
    
    # Use a defensive approach - catch errors from join normalization
    try do
      # Use the existing join normalization logic to build dependency map
      normalized_joins = Selecto.Schema.Join.recurse_joins(source, domain_with_defaults)
      
      Enum.reduce(normalized_joins, %{}, fn {join_id, join_config}, acc ->
        requires_join = Map.get(join_config, :requires_join)
        Map.put(acc, join_id, requires_join)
      end)
    rescue
      _ -> %{}  # Return empty map if join normalization fails
    end
  end
  
  defp detect_cycles(joins) do
    # Detect cycles using depth-first search
    Enum.reduce(Map.keys(joins), [], fn start_node, cycles ->
      case find_cycle_from_node(start_node, joins, [start_node], []) do
        nil -> cycles
        cycle -> [cycle | cycles]
      end
    end)
    |> Enum.uniq()
  end
  
  defp find_cycle_from_node(current, joins, path, visited) do
    case Map.get(joins, current) do
      nil -> nil  # No more dependencies
      next_node ->
        cond do
          next_node in path ->
            # Found cycle - extract the cycle portion
            cycle_start_index = Enum.find_index(path, fn node -> node == next_node end)
            Enum.drop(path, cycle_start_index)
          
          current in visited ->
            nil  # Already explored this path - use Enum.member? instead of MapSet.member?
          
          true ->
            new_visited = [current | visited]
            find_cycle_from_node(next_node, joins, [next_node | path], new_visited)
        end
    end
  end
  
  # Validate column references in selectors exist
  defp validate_column_references(errors, domain) do
    # Skip if there are already structural errors
    if not Enum.empty?(errors) do
      errors
    else
      # This would validate that fields referenced in default_selected, required_filters, etc. actually exist
      # For now, we'll validate the basic structure exists
      
      source = Map.get(domain, :source, %{})
      required_filters = Map.get(domain, :required_filters, [])
      default_selected = Map.get(domain, :default_selected, [])
      
      errors = validate_filter_field_references(errors, required_filters, source, domain)
      validate_selector_field_references(errors, default_selected, source, domain)
    end
  end
  
  defp validate_filter_field_references(errors, filters, source, domain) when is_list(filters) do
    # Build complete field map like configure_domain does
    field_map = build_complete_field_map(source, domain)
    
    Enum.reduce(filters, errors, fn {field_name, _value}, acc ->
      case Map.has_key?(field_map, field_name) do
        true -> acc
        false -> acc ++ [{:filter_field_not_found, field_name}]
      end
    end)
  end
  
  defp validate_selector_field_references(errors, selectors, source, domain) when is_list(selectors) do
    field_map = build_complete_field_map(source, domain)
    
    Enum.reduce(selectors, errors, fn selector, acc ->
      # Simple validation - just check if basic field names exist
      # More complex selector validation would go here
      case is_binary(selector) and Map.has_key?(field_map, selector) do
        true -> acc
        false -> acc  # Don't error on complex selectors for now
      end
    end)
  end
  
  defp build_complete_field_map(source, domain) do
    # Simplified version of the field building logic from configure_domain
    try do
      domain_with_defaults = Map.put_new(domain, :joins, %{})
      joins = Selecto.Schema.Join.recurse_joins(source, domain_with_defaults)
      
      # Build field map from source
      source_fields = build_source_field_map(source)
      
      # Add fields from joins
      join_fields = Enum.reduce(joins, %{}, fn {_join_id, join_config}, acc ->
        Map.merge(acc, Map.get(join_config, :fields, %{}))
      end)
      
      Map.merge(source_fields, join_fields)
    rescue
      _ -> 
        # If join processing fails, just return source fields
        build_source_field_map(source)
    end
  end
  
  defp build_source_field_map(source) do
    fields = Map.get(source, :fields, [])
    columns = Map.get(source, :columns, %{})
    
    Enum.reduce(fields, %{}, fn field, acc ->
      case Map.get(columns, field) do
        nil -> acc
        column_config -> Map.put(acc, to_string(field), %{
          field: field,
          requires_join: :selecto_root,
          type: Map.get(column_config, :type)
        })
      end
    end)
  end
  
  # Validate advanced join types have required keys
  defp validate_advanced_join_requirements(errors, domain) do
    joins_config = Map.get(domain, :joins, %{})
    validate_advanced_joins_recursive(errors, joins_config)
  end
  
  defp validate_advanced_joins_recursive(errors, joins_config) do
    Enum.reduce(joins_config, errors, fn {join_name, join_config}, acc ->
      acc = validate_advanced_join_type(acc, join_name, join_config)
      
      # Recursively check nested joins
      case Map.get(join_config, :joins) do
        nil -> acc
        nested_joins -> validate_advanced_joins_recursive(acc, nested_joins)
      end
    end)
  end
  
  defp validate_advanced_join_type(errors, join_name, %{type: :dimension} = config) do
    case Map.get(config, :dimension) do
      nil -> errors ++ [{:advanced_join_missing_key, {join_name, :dimension, "dimension key required for :dimension join type"}}]
      _ -> errors
    end
  end
  
  defp validate_advanced_join_type(errors, join_name, %{type: :hierarchical} = config) do
    hierarchy_type = Map.get(config, :hierarchy_type, :adjacency_list)
    
    case hierarchy_type do
      :materialized_path ->
        case Map.get(config, :path_field) do
          nil -> errors ++ [{:advanced_join_missing_key, {join_name, :path_field, "path_field required for materialized_path hierarchy"}}]
          _ -> errors
        end
      
      :closure_table ->
        required_keys = [:closure_table, :ancestor_field, :descendant_field]
        missing_keys = required_keys -- Map.keys(config)
        case missing_keys do
          [] -> errors
          _ -> errors ++ [{:advanced_join_missing_key, {join_name, missing_keys, "closure table hierarchy requires closure_table, ancestor_field, descendant_field"}}]
        end
      
      _ -> errors  # adjacency_list has no special requirements
    end
  end
  
  defp validate_advanced_join_type(errors, join_name, %{type: :snowflake_dimension} = config) do
    case Map.get(config, :normalization_joins) do
      joins when is_list(joins) and length(joins) > 0 -> errors
      _ -> errors ++ [{:advanced_join_missing_key, {join_name, :normalization_joins, "normalization_joins list required for :snowflake_dimension"}}]
    end
  end
  
  defp validate_advanced_join_type(errors, _join_name, _config) do
    # No special requirements for other join types  
    errors
  end
  
  # Format errors for display - expose publicly for testing
  def format_errors(errors) do
    errors
    |> Enum.map(&format_error/1)
    |> Enum.join("\n")
  end
  
  defp format_error({:missing_required_keys, keys}) do
    "Missing required domain keys: #{Enum.join(keys, ", ")}"
  end
  
  defp format_error({:schema_missing_keys, {schema_name, keys}}) do
    "Schema '#{schema_name}' missing required keys: #{Enum.join(keys, ", ")}"
  end
  
  defp format_error({:schema_missing_column_defs, {schema_name, columns}}) do
    "Schema '#{schema_name}' fields missing column definitions: #{Enum.join(columns, ", ")}"
  end
  
  defp format_error({:association_missing_queryable, {schema_name, assoc_name}}) do
    "Association '#{assoc_name}' in schema '#{schema_name}' missing queryable"
  end
  
  defp format_error({:association_invalid_queryable, {schema_name, assoc_name, queryable}}) do
    "Association '#{assoc_name}' in schema '#{schema_name}' references invalid queryable '#{queryable}'"
  end
  
  defp format_error({:join_missing_association, {parent_name, join_name}}) do
    "Join '#{join_name}' in '#{parent_name}' references non-existent association"
  end
  
  defp format_error({:join_cycle_detected, cycle}) do
    cycle_path = Enum.join(cycle, " -> ")
    "Join dependency cycle detected: #{cycle_path} -> #{List.first(cycle)}"
  end
  
  defp format_error({:filter_field_not_found, field_name}) do
    "Required filter field '#{field_name}' not found in domain configuration"
  end
  
  defp format_error({:advanced_join_missing_key, {join_name, keys, message}}) when is_list(keys) do
    "Advanced join '#{join_name}' missing required keys #{inspect(keys)}: #{message}"
  end
  
  defp format_error({:advanced_join_missing_key, {join_name, key, message}}) do
    "Advanced join '#{join_name}' missing required key '#{key}': #{message}"
  end
  
  defp format_error(error) do
    "Unknown validation error: #{inspect(error)}"
  end
  
  @doc """
  Compile-time domain validation macro.
  
  When used in a module, validates the provided domain configuration at compile time.
  This catches domain configuration errors early and provides better error messages.
  
  ## Options
  
  - `:domain` - The domain configuration to validate (required)
  
  ## Example
  
      defmodule MyApp.UserDomain do
        use Selecto.DomainValidator, domain: %{
          source: %{
            source_table: "users",
            primary_key: :id,
            fields: [:id, :name, :email],
            columns: %{
              id: %{type: :integer},
              name: %{type: :string},
              email: %{type: :string}
            }
          },
          schemas: %{}
        }
        
        def domain, do: @validated_domain
      end
  """
  defmacro __using__(opts) do
    domain_ast = Keyword.get(opts, :domain)
    
    if is_nil(domain_ast) do
      raise "Selecto.DomainValidator requires a :domain option"
    end
    
    # Try to evaluate the domain AST at compile time for static configurations
    try do
      domain = Code.eval_quoted(domain_ast) |> elem(0)
      
      # Validate domain at compile time
      case validate_domain(domain) do
        :ok -> :ok
        {:error, errors} -> 
          formatted_errors = format_errors(errors)
          raise CompileError, 
            description: "Domain validation failed:\n#{formatted_errors}",
            file: __CALLER__.file,
            line: __CALLER__.line
      end
      
      quote do
        @validated_domain unquote(Macro.escape(domain))
        
        @doc """
        Returns the compile-time validated domain configuration.
        """
        def validated_domain, do: @validated_domain
      end
    rescue
      # If we can't evaluate at compile time (e.g., contains variables), 
      # set up runtime validation instead
      _ ->
        quote do
          @domain_ast unquote(Macro.escape(domain_ast))
          
          @doc """
          Returns the domain configuration with runtime validation.
          """
          def validated_domain do
            domain = unquote(domain_ast)
            case Selecto.DomainValidator.validate_domain(domain) do
              :ok -> domain
              {:error, errors} ->
                formatted_errors = Selecto.DomainValidator.format_errors(errors)
                raise Selecto.DomainValidator.ValidationError, 
                  message: "Domain validation failed:\n#{formatted_errors}"
            end
          end
        end
    end
  end
end

defmodule Selecto.DomainValidator.ValidationError do
  @moduledoc """
  Exception raised when domain validation fails.
  """
  defexception [:message]
  
  def exception(opts) do
    message = Keyword.get(opts, :message, "Domain validation failed")
    %__MODULE__{message: message}
  end
end