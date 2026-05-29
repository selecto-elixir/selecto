defmodule Selecto.DomainValidator do
  @moduledoc """
  Domain validation for Selecto configurations.

  Validates domain configuration to catch errors early and prevent runtime failures.
  Checks for join dependency cycles, missing references, required keys for advanced
  join types, and other structural integrity issues.

  ## Usage

      # Validate during configure (enabled by default)
      domain = %{source: ..., schemas: ..., joins: ...}
      selecto = Selecto.configure(domain, connection_input)
      
      # Disable validation for performance-critical scenarios
      selecto = Selecto.configure(domain, connection_input, validate: false)
      
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

  use Selecto.Domain.Constants

  @domain_projections [:query, :write, :ui, :api]

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
  @spec validate_domain!(Selecto.Types.domain(), keyword()) :: :ok
  def validate_domain!(domain, opts \\ []) do
    case validate_domain(domain, opts) do
      :ok ->
        :ok

      {:error, errors} ->
        raise Selecto.DomainValidator.ValidationError, message: format_errors(errors)
    end
  end

  @doc """
  Validates a domain configuration, returning `:ok` or `{:error, errors}`.

  Non-raising version of validate_domain!/1.

  Options:

  - `:normalize` - when `true`, validate a normalized domain projection instead
    of the authored map. Defaults to `false` to preserve existing runtime
    behavior.
  - `:projection` - normalized projection to validate when `:normalize` is
    enabled. Defaults to `:api`, because it includes query metadata and current
    detail action metadata.
  """
  def validate_domain(domain, opts \\ [])

  def validate_domain(domain, opts) when is_list(opts) do
    if Keyword.get(opts, :normalize, false) do
      validate_normalized_domain(domain, opts)
    else
      validate_authored_domain(domain)
    end
  end

  defp validate_normalized_domain(domain, opts) do
    projection = Keyword.get(opts, :projection, :api)

    with :ok <- validate_projection_name(projection),
         {:ok, normalized, diagnostics} <- Selecto.Domain.normalize(domain),
         :ok <- validate_normalization_diagnostics(diagnostics),
         :ok <- Selecto.Domain.Contract.validate(normalized) do
      normalized
      |> Selecto.Domain.project(projection)
      |> validate_authored_domain()
    else
      {:error, %Selecto.Domain.Diagnostics{} = diagnostics} ->
        {:error, [{:domain_normalization_invalid, diagnostics.errors}]}

      {:error, errors} when is_list(errors) ->
        {:error, errors}
    end
  end

  defp validate_projection_name(projection) when projection in @domain_projections, do: :ok

  defp validate_projection_name(projection) do
    {:error, [{:domain_projection_invalid, projection}]}
  end

  defp validate_normalization_diagnostics(diagnostics) do
    errors =
      diagnostics.warnings
      |> Enum.flat_map(fn
        %{code: :invalid_section_shape, section: section, expected: expected, actual: actual} ->
          [{:domain_section_invalid_shape, {section, expected, actual}}]

        _warning ->
          []
      end)

    case errors do
      [] -> :ok
      _ -> {:error, errors}
    end
  end

  defp validate_authored_domain(domain) do
    errors = []

    # Validate top-level structure
    errors = validate_required_keys(domain, [:source, :schemas], errors)

    errors =
      errors
      |> validate_source(domain)
      |> validate_schemas(domain)
      |> validate_associations(domain)
      |> validate_joins(domain)
      |> validate_functions(domain)
      |> validate_query_members(domain)
      |> validate_published_views(domain)
      |> validate_detail_actions(domain)

    # Only do complex validations if basic structure is sound
    final_errors =
      if Enum.empty?(errors) do
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
  defp validate_source(errors, domain) do
    source = Map.get(domain, :source, %{})

    if source == %{} do
      errors
    else
      errors
      |> validate_source_structure(source)
      |> validate_source_columns(source)
      |> validate_relation_source_metadata(:source, source)
    end
  end

  defp validate_source_structure(errors, source) do
    required_keys = [:source_table, :primary_key, :fields, :columns]
    missing_keys = required_keys -- Map.keys(source)

    case missing_keys do
      [] -> errors
      _ -> errors ++ [{:source_missing_keys, missing_keys}]
    end
  end

  defp validate_source_columns(errors, source) do
    fields = Map.get(source, :fields, [])
    columns = Map.get(source, :columns, %{})
    missing_columns = fields -- Map.keys(columns)

    case missing_columns do
      [] -> errors
      _ -> errors ++ [{:source_missing_column_defs, missing_columns}]
    end
  end

  defp validate_schemas(errors, domain) do
    schemas = Map.get(domain, :schemas, %{})

    Enum.reduce(schemas, errors, fn {schema_name, schema}, acc ->
      acc
      |> validate_schema_structure(schema_name, schema)
      |> validate_schema_columns(schema_name, schema)
      |> validate_relation_source_metadata(schema_name, schema)
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

  defp validate_relation_source_metadata(errors, relation_name, relation) do
    errors
    |> validate_relation_source_kind(relation_name, relation)
    |> validate_relation_readonly(relation_name, relation)
  end

  defp validate_relation_source_kind(errors, relation_name, relation) do
    case normalize_relation_source_kind(map_value(relation, :source_kind)) do
      nil ->
        errors

      {:ok, _kind} ->
        errors

      {:error, invalid_kind} when relation_name == :source ->
        errors ++ [{:source_invalid_source_kind, invalid_kind}]

      {:error, invalid_kind} ->
        errors ++ [{:schema_invalid_source_kind, {relation_name, invalid_kind}}]
    end
  end

  defp validate_relation_readonly(errors, relation_name, relation) do
    case map_value(relation, :readonly) do
      nil ->
        errors

      readonly when is_boolean(readonly) ->
        errors

      invalid_value when relation_name == :source ->
        errors ++ [{:source_invalid_readonly, invalid_value}]

      invalid_value ->
        errors ++ [{:schema_invalid_readonly, {relation_name, invalid_value}}]
    end
  end

  defp normalize_relation_source_kind(nil), do: nil
  defp normalize_relation_source_kind(:table), do: {:ok, :table}
  defp normalize_relation_source_kind(:view), do: {:ok, :view}
  defp normalize_relation_source_kind(:materialized_view), do: {:ok, :materialized_view}
  defp normalize_relation_source_kind("table"), do: {:ok, :table}
  defp normalize_relation_source_kind("view"), do: {:ok, :view}
  defp normalize_relation_source_kind("materialized_view"), do: {:ok, :materialized_view}
  defp normalize_relation_source_kind(other), do: {:error, other}

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
        nil ->
          acc

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
      # Return empty map if join normalization fails
      _ -> %{}
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
      # No more dependencies
      nil ->
        nil

      next_node ->
        cond do
          next_node in path ->
            # Found cycle - extract the cycle portion
            cycle_start_index = Enum.find_index(path, fn node -> node == next_node end)
            Enum.drop(path, cycle_start_index)

          current in visited ->
            # Already explored this path - use Enum.member? instead of MapSet.member?
            nil

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

  defp validate_selector_field_references(errors, selectors, source, domain)
       when is_list(selectors) do
    field_map = build_complete_field_map(source, domain)

    Enum.reduce(selectors, errors, fn selector, acc ->
      # Simple validation - just check if basic field names exist
      # More complex selector validation would go here
      case is_binary(selector) and Map.has_key?(field_map, selector) do
        true -> acc
        # Don't error on complex selectors for now
        false -> acc
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
      join_fields =
        Enum.reduce(joins, %{}, fn {_join_id, join_config}, acc ->
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
        nil ->
          acc

        column_config ->
          Map.put(acc, to_string(field), %{
            field: field,
            requires_join: :selecto_root,
            type: Map.get(column_config, :type)
          })
      end
    end)
  end

  defp validate_detail_actions(errors, domain) do
    detail_actions = Map.get(domain, :detail_actions, %{})

    cond do
      is_map(detail_actions) and map_size(detail_actions) == 0 ->
        errors

      is_map(detail_actions) ->
        field_map = build_complete_field_map(Map.get(domain, :source, %{}), domain)

        Enum.reduce(detail_actions, errors, fn {action_id, action_config}, acc ->
          validate_detail_action(acc, action_id, action_config, field_map)
        end)

      true ->
        errors ++ [{:detail_actions_invalid, {:detail_actions, "must be a map"}}]
    end
  end

  defp validate_detail_action(errors, action_id, action_config, field_map)
       when is_map(action_config) do
    action_name = detail_action_value(action_config, :name)
    action_type = normalize_detail_action_type(detail_action_value(action_config, :type))
    raw_payload = detail_action_value(action_config, :payload, %{})
    payload = detail_action_payload(raw_payload)

    required_fields =
      normalize_required_fields(detail_action_value(action_config, :required_fields, []))

    errors
    |> maybe_add_detail_action_error(action_name in [nil, ""], action_id, "name is required")
    |> maybe_add_detail_action_error(
      is_nil(action_type),
      action_id,
      "type must be one of #{inspect(@detail_action_types)}"
    )
    |> maybe_add_detail_action_error(
      not is_map(raw_payload),
      action_id,
      "payload must be a map"
    )
    |> maybe_add_detail_action_error(
      action_type in [:external_link, :iframe_modal] and
        not is_binary(detail_action_value(payload, :url_template)),
      action_id,
      "#{action_type} actions require payload.url_template"
    )
    |> maybe_add_detail_action_error(
      action_type == :live_component and not is_atom(detail_action_value(payload, :module)),
      action_id,
      "live_component actions require payload.module"
    )
    |> validate_detail_action_required_fields(action_id, required_fields, field_map)
  end

  defp validate_detail_action(errors, action_id, _action_config, _field_map) do
    errors ++ [{:detail_actions_invalid, {action_id, "detail action must be a map"}}]
  end

  defp validate_detail_action_required_fields(errors, _action_id, [], _field_map), do: errors

  defp validate_detail_action_required_fields(errors, action_id, required_fields, field_map) do
    Enum.reduce(required_fields, errors, fn field_name, acc ->
      if Map.has_key?(field_map, field_name) do
        acc
      else
        acc ++
          [
            {:detail_actions_invalid,
             {action_id, "required field '#{field_name}' was not found in domain configuration"}}
          ]
      end
    end)
  end

  defp maybe_add_detail_action_error(errors, false, _action_id, _message), do: errors

  defp maybe_add_detail_action_error(errors, true, action_id, message) do
    errors ++ [{:detail_actions_invalid, {action_id, message}}]
  end

  defp detail_action_payload(payload) do
    case payload do
      payload when is_map(payload) -> payload
      _ -> %{}
    end
  end

  defp detail_action_value(config, key, default \\ nil)

  defp detail_action_value(config, key, default) when is_map(config) and is_atom(key) do
    Map.get(config, key, Map.get(config, Atom.to_string(key), default))
  end

  defp detail_action_value(_config, _key, default), do: default

  defp normalize_detail_action_type(value) when value in @detail_action_types, do: value

  defp normalize_detail_action_type(value) when is_binary(value) do
    case String.trim(value) do
      "modal" -> :modal
      "iframe_modal" -> :iframe_modal
      "external_link" -> :external_link
      "live_component" -> :live_component
      _ -> nil
    end
  end

  defp normalize_detail_action_type(_value), do: nil

  defp normalize_required_fields(required_fields) when is_list(required_fields) do
    Enum.map(required_fields, &normalize_required_field/1)
  end

  defp normalize_required_fields(_required_fields), do: []

  defp normalize_required_field(field) when is_atom(field), do: Atom.to_string(field)
  defp normalize_required_field(field) when is_binary(field), do: field
  defp normalize_required_field(field), do: to_string(field)

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
      nil ->
        errors ++
          [
            {:advanced_join_missing_key,
             {join_name, :dimension, "dimension key required for :dimension join type"}}
          ]

      _ ->
        errors
    end
  end

  defp validate_advanced_join_type(errors, join_name, %{type: :hierarchical} = config) do
    hierarchy_type = Map.get(config, :hierarchy_type, :adjacency_list)

    case hierarchy_type do
      :materialized_path ->
        case Map.get(config, :path_field) do
          nil ->
            errors ++
              [
                {:advanced_join_missing_key,
                 {join_name, :path_field, "path_field required for materialized_path hierarchy"}}
              ]

          _ ->
            errors
        end

      :closure_table ->
        required_keys = [:closure_table, :ancestor_field, :descendant_field]
        missing_keys = required_keys -- Map.keys(config)

        case missing_keys do
          [] ->
            errors

          _ ->
            errors ++
              [
                {:advanced_join_missing_key,
                 {join_name, missing_keys,
                  "closure table hierarchy requires closure_table, ancestor_field, descendant_field"}}
              ]
        end

      # adjacency_list has no special requirements
      _ ->
        errors
    end
  end

  defp validate_advanced_join_type(errors, join_name, %{type: :snowflake_dimension} = config) do
    case Map.get(config, :normalization_joins) do
      joins when is_list(joins) and length(joins) > 0 ->
        errors

      _ ->
        errors ++
          [
            {:advanced_join_missing_key,
             {join_name, :normalization_joins,
              "normalization_joins list required for :snowflake_dimension"}}
          ]
    end
  end

  defp validate_advanced_join_type(errors, _join_name, _config) do
    # No special requirements for other join types  
    errors
  end

  defp validate_functions(errors, domain) do
    case Map.get(domain, :functions) do
      nil ->
        errors

      functions when is_map(functions) ->
        Enum.reduce(functions, errors, fn {function_id, spec}, acc ->
          validate_function_spec(acc, function_id, spec)
        end)

      _invalid ->
        errors ++ [{:functions_invalid, {:functions, ":functions must be a map of named specs"}}]
    end
  end

  defp validate_function_spec(errors, function_id, spec) when is_map(spec) do
    errors
    |> validate_function_kind(spec, function_id)
    |> validate_function_sql_name(spec, function_id)
    |> validate_function_allowed_in(spec, function_id)
    |> validate_function_args(spec, function_id)
    |> validate_function_returns(spec, function_id)
  end

  defp validate_function_spec(errors, function_id, _invalid_spec) do
    errors ++ [{:functions_invalid, {function_id, "function spec must be a map"}}]
  end

  defp validate_function_kind(errors, spec, function_id) do
    kind = map_value(spec, :kind)

    if Selecto.UDF.valid_kind?(kind) do
      errors
    else
      errors ++
        [{:functions_invalid, {function_id, ":kind must be :scalar, :predicate, or :table"}}]
    end
  end

  defp validate_function_sql_name(errors, spec, function_id) do
    sql_name = map_value(spec, :sql_name)

    if Selecto.UDF.valid_sql_name?(sql_name) do
      errors
    else
      errors ++
        [
          {:functions_invalid,
           {function_id,
            ":sql_name must be a safe function identifier like my_fn or public.my_fn"}}
        ]
    end
  end

  defp validate_function_allowed_in(errors, spec, function_id) do
    allowed_in = map_value(spec, :allowed_in)

    cond do
      is_nil(allowed_in) ->
        errors

      is_list(allowed_in) and Enum.all?(allowed_in, &Selecto.UDF.valid_call_site?/1) ->
        errors

      true ->
        errors ++
          [
            {:functions_invalid,
             {function_id,
              ":allowed_in must be a list of valid call sites like :select, :filter, or :order_by"}}
          ]
    end
  end

  defp validate_function_args(errors, spec, function_id) do
    case map_value(spec, :args) do
      nil ->
        errors

      args when is_list(args) ->
        Enum.reduce(args, errors, fn arg_spec, acc ->
          validate_function_arg_spec(acc, function_id, arg_spec)
        end)

      _invalid ->
        errors ++ [{:functions_invalid, {function_id, ":args must be a list when provided"}}]
    end
  end

  defp validate_function_arg_spec(errors, function_id, arg_spec) when is_map(arg_spec) do
    name = map_value(arg_spec, :name)
    type = fetch_map_value(arg_spec, :type)
    source = map_value(arg_spec, :source)

    errors =
      if is_atom(name) or is_binary(name) do
        errors
      else
        errors ++
          [{:functions_invalid, {function_id, "each arg must declare :name as atom or string"}}]
      end

    errors =
      if type == :__missing__ do
        errors ++ [{:functions_invalid, {function_id, "each arg must declare :type"}}]
      else
        errors
      end

    if Selecto.UDF.valid_arg_source?(source) do
      errors
    else
      errors ++
        [
          {:functions_invalid,
           {function_id, "each arg :source must be :selector, :value, or :literal"}}
        ]
    end
  end

  defp validate_function_arg_spec(errors, function_id, _invalid_arg_spec) do
    errors ++ [{:functions_invalid, {function_id, "each arg spec must be a map"}}]
  end

  defp validate_function_returns(errors, spec, function_id) do
    kind = map_value(spec, :kind)
    returns = map_value(spec, :returns)

    case kind do
      :predicate ->
        if returns == :boolean do
          errors
        else
          errors ++
            [
              {:functions_invalid,
               {function_id, "predicate functions must declare returns: :boolean"}}
            ]
        end

      :table ->
        columns =
          case returns do
            %{} = returns_map ->
              Map.get(returns_map, :columns) || Map.get(returns_map, "columns")

            _ ->
              nil
          end

        if is_map(columns) and map_size(columns) > 0 do
          errors
        else
          errors ++
            [
              {:functions_invalid,
               {function_id, "table functions must declare returns: %{columns: %{...}}"}}
            ]
        end

      :scalar ->
        if is_nil(returns) or is_atom(returns) or match?({:array, _}, returns) do
          errors
        else
          errors ++
            [
              {:functions_invalid,
               {function_id, "scalar functions must declare an atom or array return type"}}
            ]
        end

      _ ->
        errors
    end
  end

  defp validate_query_members(errors, domain) do
    case Map.get(domain, :query_members) do
      nil ->
        errors

      query_members when is_map(query_members) ->
        errors
        |> validate_query_member_group(query_members, :ctes)
        |> validate_query_member_group(query_members, :values)
        |> validate_query_member_group(query_members, :subqueries)
        |> validate_query_member_group(query_members, :laterals)
        |> validate_query_member_group(query_members, :unnests)

      _invalid ->
        errors ++
          [
            {:query_members_invalid,
             {:query_members, "query_members must be a map with :ctes/:values/:subqueries maps"}}
          ]
    end
  end

  defp validate_published_views(errors, domain) do
    case Map.get(domain, :published_views) do
      nil ->
        errors

      published_views when is_map(published_views) ->
        Enum.reduce(published_views, errors, fn {view_id, spec}, acc ->
          validate_published_view_spec(acc, domain, view_id, spec)
        end)

      _invalid ->
        errors ++
          [
            {:published_views_invalid,
             {:published_views, ":published_views must be a map of named published view specs"}}
          ]
    end
  end

  defp validate_published_view_spec(errors, _domain, view_id, spec) when not is_map(spec) do
    errors ++ [{:published_views_invalid, {view_id, "published view spec must be a map"}}]
  end

  defp validate_published_view_spec(errors, domain, view_id, spec) do
    errors
    |> validate_published_view_database_name(spec, view_id)
    |> validate_published_view_kind(spec, view_id)
    |> validate_published_view_query(spec, view_id)
    |> validate_published_view_columns(spec, view_id)
    |> validate_published_view_indexes(spec, view_id)
    |> validate_published_view_refresh(spec, view_id)
    |> validate_published_view_compilation(domain, spec, view_id)
  end

  defp validate_published_view_database_name(errors, spec, view_id) do
    database_name = map_value(spec, :database_name)

    if is_binary(database_name) and String.trim(database_name) != "" do
      errors
    else
      errors ++
        [{:published_views_invalid, {view_id, ":database_name must be a non-empty string"}}]
    end
  end

  defp validate_published_view_kind(errors, spec, view_id) do
    case map_value(spec, :kind) do
      kind when kind in [:view, :materialized_view] ->
        errors

      _ ->
        errors ++
          [{:published_views_invalid, {view_id, ":kind must be :view or :materialized_view"}}]
    end
  end

  defp validate_published_view_query(errors, spec, view_id) do
    query = map_value(spec, :query)

    if valid_arity?(query, [1]) do
      errors
    else
      errors ++
        [{:published_views_invalid, {view_id, ":query must be a function with arity 1"}}]
    end
  end

  defp validate_published_view_columns(errors, spec, view_id) do
    case map_value(spec, :columns) do
      columns when is_map(columns) and map_size(columns) > 0 ->
        Enum.reduce(columns, errors, fn {column_name, column_spec}, acc ->
          validate_published_view_column(acc, view_id, column_name, column_spec)
        end)

      _ ->
        errors ++
          [
            {:published_views_invalid,
             {view_id, ":columns must be a non-empty map of published columns"}}
          ]
    end
  end

  defp validate_published_view_column(errors, _view_id, column_name, column_spec)
       when (is_atom(column_name) or is_binary(column_name)) and is_map(column_spec) do
    errors
  end

  defp validate_published_view_column(errors, view_id, _column_name, _column_spec) do
    errors ++
      [
        {:published_views_invalid,
         {view_id, "each published view column must use an atom/string key and map value"}}
      ]
  end

  defp validate_published_view_refresh(errors, spec, view_id) do
    case map_value(spec, :refresh) do
      nil ->
        errors

      refresh when is_map(refresh) ->
        errors

      _ ->
        errors ++ [{:published_views_invalid, {view_id, ":refresh must be a map when provided"}}]
    end
  end

  defp validate_published_view_indexes(errors, spec, view_id) do
    case map_value(spec, :indexes) do
      nil ->
        errors

      indexes when is_list(indexes) ->
        Enum.reduce(indexes, errors, fn index_spec, acc ->
          validate_published_view_index(acc, view_id, index_spec)
        end)

      _ ->
        errors ++ [{:published_views_invalid, {view_id, ":indexes must be a list when provided"}}]
    end
  end

  defp validate_published_view_index(errors, view_id, index_spec) when is_map(index_spec) do
    columns = map_value(index_spec, :columns)
    unique = map_value(index_spec, :unique)
    concurrently = map_value(index_spec, :concurrently)

    errors =
      if is_list(columns) and columns != [] and
           Enum.all?(columns, &(is_atom(&1) or is_binary(&1))) do
        errors
      else
        errors ++
          [
            {:published_views_invalid,
             {view_id, "each published view index must declare a non-empty :columns list"}}
          ]
      end

    errors =
      if is_nil(unique) or is_boolean(unique) do
        errors
      else
        errors ++
          [
            {:published_views_invalid,
             {view_id, "each published view index :unique must be boolean when provided"}}
          ]
      end

    if is_nil(concurrently) or is_boolean(concurrently) do
      errors
    else
      errors ++
        [
          {:published_views_invalid,
           {view_id, "each published view index :concurrently must be boolean when provided"}}
        ]
    end
  end

  defp validate_published_view_index(errors, view_id, _index_spec) do
    errors ++
      [{:published_views_invalid, {view_id, "each published view index spec must be a map"}}]
  end

  defp validate_published_view_compilation(errors, domain, spec, view_id) do
    case Selecto.ViewPublisher.validate(domain, spec) do
      :ok ->
        errors

      {:error, validation_errors} ->
        errors ++ Enum.map(validation_errors, &{:published_views_invalid, {view_id, &1}})
    end
  rescue
    error ->
      errors ++ [{:published_views_invalid, {view_id, Exception.message(error)}}]
  end

  defp validate_query_member_group(errors, query_members, group_key) do
    case fetch_map_value(query_members, group_key) do
      :__missing__ ->
        errors

      nil ->
        errors

      members when members == %{} ->
        errors

      members when is_map(members) ->
        Enum.reduce(members, errors, fn {member_id, spec}, acc ->
          validate_query_member_spec(acc, group_key, member_id, spec)
        end)

      _invalid ->
        errors ++
          [
            {:query_members_invalid,
             {group_key, "query_members.#{group_key} must be a map of named presets"}}
          ]
    end
  end

  defp validate_query_member_spec(errors, group_key, member_id, spec) when is_map(spec) do
    case group_key do
      :ctes ->
        validate_cte_member(errors, member_id, spec)

      :values ->
        validate_values_member(errors, member_id, spec)

      :subqueries ->
        validate_subquery_member(errors, member_id, spec)

      :laterals ->
        validate_lateral_member(errors, member_id, spec)

      :unnests ->
        validate_unnest_member(errors, member_id, spec)
    end
  end

  defp validate_query_member_spec(errors, group_key, member_id, _invalid_spec) do
    errors ++
      [
        {:query_members_invalid,
         {group_key, member_id, "query member '#{member_id}' must be a map"}}
      ]
  end

  defp validate_cte_member(errors, member_id, spec) do
    errors
    |> maybe_validate_cte_query(spec, member_id)
    |> maybe_validate_cte_join(spec, member_id)
  end

  defp maybe_validate_cte_query(errors, spec, member_id) do
    recursive? =
      map_value(spec, :type) == :recursive or not is_nil(map_value(spec, :base_query)) or
        not is_nil(map_value(spec, :recursive_query))

    if recursive? do
      base_query = map_value(spec, :base_query)
      recursive_query = map_value(spec, :recursive_query)

      errors =
        if valid_arity?(base_query, [0, 1]) do
          errors
        else
          errors ++
            [
              {:query_members_invalid,
               {:ctes, member_id, "recursive CTE requires :base_query function with arity 0 or 1"}}
            ]
        end

      if valid_arity?(recursive_query, [1, 2]) do
        errors
      else
        errors ++
          [
            {:query_members_invalid,
             {:ctes, member_id,
              "recursive CTE requires :recursive_query function with arity 1 or 2"}}
          ]
      end
    else
      query_builder = map_value(spec, :query_builder) || map_value(spec, :query)

      if valid_arity?(query_builder, [0, 1]) do
        errors
      else
        errors ++
          [
            {:query_members_invalid,
             {:ctes, member_id,
              "CTE requires :query (or :query_builder) function with arity 0 or 1"}}
          ]
      end
    end
  end

  defp maybe_validate_cte_join(errors, spec, member_id) do
    case fetch_map_value(spec, :join) do
      :__missing__ ->
        errors

      join_opts when join_opts in [nil, false, true] ->
        errors

      join_opts when is_list(join_opts) or is_map(join_opts) ->
        errors

      _invalid ->
        errors ++
          [
            {:query_members_invalid,
             {:ctes, member_id, ":join must be true, false, nil, keyword list, or map"}}
          ]
    end
  end

  defp validate_values_member(errors, member_id, spec) do
    rows =
      fetch_map_value(spec, :rows)
      |> case do
        :__missing__ -> fetch_map_value(spec, :data)
        value -> value
      end

    errors =
      if is_list(rows) do
        errors
      else
        errors ++
          [
            {:query_members_invalid,
             {:values, member_id, "VALUES member requires :rows (or :data) list"}}
          ]
      end

    errors =
      case fetch_map_value(spec, :columns) do
        :__missing__ ->
          errors

        columns when is_list(columns) ->
          errors

        _invalid ->
          errors ++
            [
              {:query_members_invalid,
               {:values, member_id, ":columns must be a list when provided"}}
            ]
      end

    errors =
      case fetch_map_value(spec, :join) do
        :__missing__ ->
          errors

        join_opts when join_opts in [nil, false, true] ->
          errors

        join_opts when is_list(join_opts) or is_map(join_opts) ->
          errors

        _invalid ->
          errors ++
            [
              {:query_members_invalid,
               {:values, member_id, ":join must be true, false, nil, keyword list, or map"}}
            ]
      end

    case values_alias(spec) do
      nil ->
        errors

      alias_name when is_binary(alias_name) and alias_name != "" ->
        errors

      alias_name when is_atom(alias_name) ->
        errors

      _invalid ->
        errors ++
          [
            {:query_members_invalid,
             {:values, member_id, ":as (or :alias/:alias_name) must be a string or atom"}}
          ]
    end
  end

  defp validate_subquery_member(errors, member_id, spec) do
    kind = map_value(spec, :kind)

    errors =
      case kind do
        nil ->
          errors

        :join ->
          errors

        _ ->
          errors ++
            [
              {:query_members_invalid,
               {:subqueries, member_id, "Only kind: :join is currently supported"}}
            ]
      end

    query_builder = map_value(spec, :query_builder) || map_value(spec, :query)

    errors =
      if valid_arity?(query_builder, [0, 1]) do
        errors
      else
        errors ++
          [
            {:query_members_invalid,
             {:subqueries, member_id,
              "Subquery requires :query (or :query_builder) function with arity 0 or 1"}}
          ]
      end

    errors =
      case fetch_map_value(spec, :on) do
        :__missing__ ->
          errors

        on when is_list(on) ->
          errors

        _invalid ->
          errors ++
            [
              {:query_members_invalid,
               {:subqueries, member_id, ":on must be a list when provided"}}
            ]
      end

    errors =
      case fetch_map_value(spec, :type) do
        :__missing__ ->
          errors

        type when type in [:left, :inner, :right, :full] ->
          errors

        _invalid ->
          errors ++
            [
              {:query_members_invalid,
               {:subqueries, member_id, ":type must be one of :left, :inner, :right, :full"}}
            ]
      end

    case fetch_map_value(spec, :join_id) do
      :__missing__ ->
        errors

      join_id when is_atom(join_id) or is_binary(join_id) ->
        errors

      _invalid ->
        errors ++
          [
            {:query_members_invalid,
             {:subqueries, member_id, ":join_id must be a string or atom when provided"}}
          ]
    end
  end

  defp validate_lateral_member(errors, member_id, spec) do
    lateral_source =
      map_value(spec, :query) || map_value(spec, :source) || map_value(spec, :lateral_source)

    errors =
      if valid_lateral_source?(lateral_source) do
        errors
      else
        errors ++
          [
            {:query_members_invalid,
             {:laterals, member_id,
              "Lateral member requires :query/:source/:lateral_source as tuple or function (arity 0/1/2)"}}
          ]
      end

    join_type = map_value(spec, :join_type) || map_value(spec, :type)

    errors =
      case join_type do
        nil ->
          errors

        type when type in [:left, :inner, :right, :full] ->
          errors

        _invalid ->
          errors ++
            [
              {:query_members_invalid,
               {:laterals, member_id,
                ":join_type (or :type) must be one of :left, :inner, :right, :full"}}
            ]
      end

    errors
    |> validate_member_alias(:laterals, member_id, spec)
    |> validate_member_options(:laterals, member_id, spec)
  end

  defp validate_unnest_member(errors, member_id, spec) do
    array_field = map_value(spec, :array_field) || map_value(spec, :field)

    errors =
      if valid_unnest_field?(array_field) do
        errors
      else
        errors ++
          [
            {:query_members_invalid,
             {:unnests, member_id,
              "UNNEST member requires :array_field (or :field) as string, atom, or tuple expression"}}
          ]
      end

    errors =
      case map_value(spec, :ordinality) do
        nil ->
          errors

        ordinality when is_binary(ordinality) or is_atom(ordinality) ->
          errors

        _invalid ->
          errors ++
            [
              {:query_members_invalid,
               {:unnests, member_id, ":ordinality must be a string or atom when provided"}}
            ]
      end

    errors
    |> validate_member_alias(:unnests, member_id, spec)
    |> validate_member_options(:unnests, member_id, spec)
  end

  defp validate_member_alias(errors, section, member_id, spec) do
    case values_alias(spec) do
      nil ->
        errors

      alias_name when is_binary(alias_name) and alias_name != "" ->
        errors

      alias_name when is_atom(alias_name) ->
        errors

      _invalid ->
        errors ++
          [
            {:query_members_invalid,
             {section, member_id, ":as (or :alias/:alias_name) must be a string or atom"}}
          ]
    end
  end

  defp validate_member_options(errors, section, member_id, spec) do
    case fetch_map_value(spec, :options) do
      :__missing__ ->
        errors

      options when is_list(options) or is_map(options) ->
        errors

      _invalid ->
        errors ++
          [
            {:query_members_invalid,
             {section, member_id, ":options must be a keyword list or map when provided"}}
          ]
    end
  end

  defp valid_lateral_source?(source) when is_tuple(source), do: true
  defp valid_lateral_source?(source) when is_function(source, 0), do: true
  defp valid_lateral_source?(source) when is_function(source, 1), do: true
  defp valid_lateral_source?(source) when is_function(source, 2), do: true
  defp valid_lateral_source?(_source), do: false

  defp valid_unnest_field?(field) when is_binary(field) or is_atom(field), do: true
  defp valid_unnest_field?(field) when is_tuple(field), do: true
  defp valid_unnest_field?(_field), do: false

  defp fetch_map_value(map, key) when is_map(map) do
    cond do
      Map.has_key?(map, key) ->
        Map.get(map, key)

      is_atom(key) and Map.has_key?(map, Atom.to_string(key)) ->
        Map.get(map, Atom.to_string(key))

      true ->
        :__missing__
    end
  end

  defp map_value(map, key) do
    case fetch_map_value(map, key) do
      :__missing__ -> nil
      value -> value
    end
  end

  defp valid_arity?(fun, arities) when is_function(fun),
    do: Enum.any?(arities, &is_function(fun, &1))

  defp valid_arity?(_value, _arities), do: false

  defp values_alias(spec) do
    alias_name =
      fetch_map_value(spec, :as)
      |> case do
        :__missing__ ->
          fetch_map_value(spec, :alias)
          |> case do
            :__missing__ ->
              fetch_map_value(spec, :alias_name)

            value ->
              value
          end

        value ->
          value
      end

    if alias_name == :__missing__, do: nil, else: alias_name
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

  defp format_error({:source_missing_keys, keys}) do
    "Source missing required keys: #{Enum.join(keys, ", ")}"
  end

  defp format_error({:source_missing_column_defs, columns}) do
    "Source fields missing column definitions: #{Enum.join(columns, ", ")}"
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

  defp format_error({:advanced_join_missing_key, {join_name, keys, message}})
       when is_list(keys) do
    "Advanced join '#{join_name}' missing required keys #{inspect(keys)}: #{message}"
  end

  defp format_error({:advanced_join_missing_key, {join_name, key, message}}) do
    "Advanced join '#{join_name}' missing required key '#{key}': #{message}"
  end

  defp format_error({:query_members_invalid, {section, member_id, message}}) do
    "Invalid query member '#{member_id}' in '#{section}': #{message}"
  end

  defp format_error({:query_members_invalid, {section, message}}) do
    "Invalid query_members section '#{section}': #{message}"
  end

  defp format_error({:functions_invalid, {function_id, message}}) do
    "Invalid function '#{function_id}': #{message}"
  end

  defp format_error({:published_views_invalid, {view_id, message}}) do
    "Invalid published view '#{view_id}': #{message}"
  end

  defp format_error({:source_invalid_source_kind, source_kind}) do
    "Source has invalid source_kind '#{inspect(source_kind)}'; expected :table, :view, or :materialized_view"
  end

  defp format_error({:source_invalid_readonly, value}) do
    "Source has invalid readonly value '#{inspect(value)}'; expected a boolean"
  end

  defp format_error({:schema_invalid_source_kind, {schema_name, source_kind}}) do
    "Schema '#{schema_name}' has invalid source_kind '#{inspect(source_kind)}'; expected :table, :view, or :materialized_view"
  end

  defp format_error({:schema_invalid_readonly, {schema_name, value}}) do
    "Schema '#{schema_name}' has invalid readonly value '#{inspect(value)}'; expected a boolean"
  end

  defp format_error({:detail_actions_invalid, {action_id, message}}) do
    "Invalid detail action '#{action_id}': #{message}"
  end

  defp format_error({:domain_projection_invalid, projection}) do
    "Invalid normalized domain projection '#{inspect(projection)}'"
  end

  defp format_error({:domain_normalization_invalid, errors}) do
    "Domain normalization failed: #{inspect(errors)}"
  end

  defp format_error({:domain_section_invalid_shape, {section, expected, actual}}) do
    "Domain section '#{section}' has invalid shape '#{actual}'; expected #{expected}"
  end

  defp format_error(%{message: message}) when is_binary(message) do
    message
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
        :ok ->
          :ok

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
              :ok ->
                domain

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
