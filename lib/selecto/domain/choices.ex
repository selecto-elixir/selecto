defmodule Selecto.Domain.Choices do
  @moduledoc """
  Choice-source membership helpers.

  This module is intentionally metadata-only in its first slice. It can resolve
  a working-domain field to its declared choice source and build a stable
  membership request, but it only proves membership when a caller supplies an
  explicit resolver.
  """

  alias Selecto.Domain.Choices.{OptionsRequest, OptionsResult, Request, Result}

  @type field :: atom() | String.t()
  @type choice_error :: %{
          required(:code) => atom(),
          required(:message) => String.t(),
          required(:path) => [term()]
        }

  @doc """
  Builds a choice-source membership request for a field/value pair.

  The domain may be either an authored domain map or a normalized domain from
  `Selecto.Domain.normalize/1`. Invalid domain contracts and unbound fields
  return structured errors.
  """
  @spec request(map(), field(), term(), map() | keyword()) ::
          {:ok, Request.t()} | {:error, choice_error()}
  def request(domain_or_normalized, field, value, attrs \\ [])
      when is_map(domain_or_normalized) do
    attrs = attrs_map(attrs)

    with {:ok, normalized} <- normalized_domain(domain_or_normalized),
         {:ok, binding} <- binding(normalized, field),
         {:ok, choice_source_config} <-
           fetch_choice_source(normalized, Map.fetch!(binding, :choice_source)),
         {:ok, source_relationship_config} <-
           source_relationship_config(normalized, choice_source_config) do
      constraint_filters =
        constraint_filters(choice_source_config, source_relationship_config, attrs)

      {:ok,
       Request.new(
         Map.merge(attrs, %{
           domain: domain_name(normalized),
           field: Map.fetch!(binding, :field),
           value: value,
           choice_source: Map.fetch!(binding, :choice_source),
           choice_source_config: choice_source_config,
           source_relationship: map_value(choice_source_config, :source_relationship),
           source_relationship_config: source_relationship_config,
           field_binding: binding,
           reference: Map.get(binding, :reference, %{}),
           constraint_filters: constraint_filters
         })
       )}
    end
  end

  @doc """
  Resolves the choice-source binding for a working-domain field.

  Rich references use `reference.choice_source`; compact bindings use
  `choice_source` directly on the column metadata.
  """
  @spec binding(map(), field()) :: {:ok, map()} | {:error, choice_error()}
  def binding(domain_or_normalized, field) when is_map(domain_or_normalized) do
    with {:ok, normalized} <- normalized_domain(domain_or_normalized),
         {:ok, column_info} <- field_column(normalized, field),
         {:ok, choice_source, binding_kind} <- column_choice_source(column_info.column, field) do
      {:ok,
       %{
         field: column_info.field,
         path: column_info.path,
         column: column_info.column,
         choice_source: choice_source,
         binding_kind: binding_kind,
         reference: map_value(column_info.column, :reference) || %{}
       }}
    end
  end

  @doc """
  Builds a choice-source option-list request.

  By default the target is treated as a working-domain field and resolved
  through its field binding. Pass `by: :choice_source` to build the request
  directly from a declared choice source.
  """
  @spec options_request(map(), field(), map() | keyword()) ::
          {:ok, OptionsRequest.t()} | {:error, choice_error()}
  def options_request(domain_or_normalized, target, attrs \\ [])
      when is_map(domain_or_normalized) do
    attrs = attrs_map(attrs)
    {by, request_attrs} = pop_attr(attrs, :by, :field)

    case options_target_kind(by) do
      :field ->
        field_options_request(domain_or_normalized, target, request_attrs)

      :choice_source ->
        choice_source_options_request(domain_or_normalized, target, request_attrs)

      :error ->
        {:error,
         error(
           :invalid_options_request_target,
           [:options_request, :by],
           "options request :by must be :field or :choice_source",
           by: by
         )}
    end
  end

  @doc """
  Builds an option-list request directly from a declared choice source.
  """
  @spec choice_source_options_request(map(), atom() | String.t(), map() | keyword()) ::
          {:ok, OptionsRequest.t()} | {:error, choice_error()}
  def choice_source_options_request(domain_or_normalized, choice_source_id, attrs \\ [])
      when is_map(domain_or_normalized) do
    attrs = attrs_map(attrs)

    with {:ok, normalized} <- normalized_domain(domain_or_normalized),
         {:ok, choice_source_config} <- fetch_choice_source(normalized, choice_source_id),
         {:ok, source_relationship_config} <-
           source_relationship_config(normalized, choice_source_config) do
      constraint_filters =
        constraint_filters(choice_source_config, source_relationship_config, attrs)

      {:ok,
       OptionsRequest.new(
         Map.merge(attrs, %{
           domain: domain_name(normalized),
           choice_source: choice_source_id,
           choice_source_config: choice_source_config,
           source_relationship: map_value(choice_source_config, :source_relationship),
           source_relationship_config: source_relationship_config,
           constraint_filters: constraint_filters
         })
       )}
    end
  end

  @doc """
  Validates a choice-source membership question.

  Without a resolver this returns an `:unknown` result instead of guessing. A
  resolver may be a one-arity function that receives a `%Request{}` and returns
  a `%Result{}`, `{:ok, %Result{}}`, or `{:error, %Result{}}`.
  """
  @spec validate_choice(map(), field(), term(), map() | keyword()) ::
          {:ok, Result.t()} | {:error, Result.t() | choice_error()}
  def validate_choice(domain_or_normalized, field, value, attrs \\ []) do
    attrs = attrs_map(attrs)
    {resolver, request_attrs} = pop_attr(attrs, :resolver)

    with {:ok, request} <- request(domain_or_normalized, field, value, request_attrs) do
      case resolver do
        nil ->
          {:error, Result.unknown(:resolver_required, request: request)}

        resolver when is_function(resolver, 1) ->
          resolver
          |> resolver_result(request)
          |> normalize_resolver_result(request)

        resolver ->
          {:error,
           Result.unknown(:invalid_resolver,
             request: request,
             metadata: %{resolver: inspect(resolver)}
           )}
      end
    end
  end

  @doc """
  Boolean convenience for callers that already supplied a resolver.

  Unknown and invalid results both return `false`; use `validate_choice/4` when
  callers need to distinguish those states.
  """
  @spec valid_choice?(map(), field(), term(), map() | keyword()) :: boolean()
  def valid_choice?(domain_or_normalized, field, value, attrs \\ []) do
    case validate_choice(domain_or_normalized, field, value, attrs) do
      {:ok, %Result{status: :valid}} -> true
      _ -> false
    end
  end

  @doc """
  Resolves an option-list request through an explicit resolver.

  Without a resolver this returns an `:unknown` result. A resolver may be a
  one-arity function that receives an `%OptionsRequest{}` and returns an
  `%OptionsResult{}`, `{:ok, %OptionsResult{}}`, or
  `{:error, %OptionsResult{}}`.
  """
  @spec list_options(map(), field(), map() | keyword()) ::
          {:ok, OptionsResult.t()} | {:error, OptionsResult.t() | choice_error()}
  def list_options(domain_or_normalized, target, attrs \\ []) do
    attrs = attrs_map(attrs)
    {resolver, request_attrs} = pop_attr(attrs, :resolver)

    with {:ok, request} <- options_request(domain_or_normalized, target, request_attrs) do
      case resolver do
        nil ->
          {:error, OptionsResult.unknown(:resolver_required, request: request)}

        resolver when is_function(resolver, 1) ->
          resolver
          |> options_resolver_result(request)
          |> normalize_options_resolver_result(request)

        resolver ->
          {:error,
           OptionsResult.unknown(:invalid_resolver,
             request: request,
             metadata: %{resolver: inspect(resolver)}
           )}
      end
    end
  end

  @doc """
  Builds a valid membership result.
  """
  @spec valid(atom() | String.t(), map() | keyword()) :: Result.t()
  def valid(reason_code \\ :choice_valid, attrs \\ []), do: Result.valid(reason_code, attrs)

  @doc """
  Builds an invalid membership result.
  """
  @spec invalid(atom() | String.t(), map() | keyword()) :: Result.t()
  def invalid(reason_code \\ :choice_invalid, attrs \\ []), do: Result.invalid(reason_code, attrs)

  @doc """
  Builds an unknown membership result.
  """
  @spec unknown(atom() | String.t(), map() | keyword()) :: Result.t()
  def unknown(reason_code \\ :resolver_required, attrs \\ []),
    do: Result.unknown(reason_code, attrs)

  @doc """
  Builds a resolved option-list result.
  """
  @spec options_resolved([map()], map() | keyword()) :: OptionsResult.t()
  def options_resolved(options, attrs \\ []), do: OptionsResult.resolved(options, attrs)

  @doc """
  Builds an unknown option-list result.
  """
  @spec options_unknown(atom() | String.t(), map() | keyword()) :: OptionsResult.t()
  def options_unknown(reason_code \\ :resolver_required, attrs \\ []),
    do: OptionsResult.unknown(reason_code, attrs)

  @doc """
  Builds an error option-list result.
  """
  @spec options_error(atom() | String.t(), map() | keyword()) :: OptionsResult.t()
  def options_error(reason_code \\ :options_error, attrs \\ []),
    do: OptionsResult.error(reason_code, attrs)

  defp normalized_domain(
         %{schema_version: _schema_version, authored_domain: _authored} = normalized
       ) do
    case Selecto.Domain.Contract.errors(normalized) do
      [] ->
        {:ok, normalized}

      errors ->
        {:error,
         error(:invalid_domain_contract, [], "domain contract is invalid", errors: errors)}
    end
  end

  defp normalized_domain(domain) when is_map(domain) do
    case Selecto.Domain.validate(domain) do
      {:ok, normalized, _diagnostics} ->
        {:ok, normalized}

      {:error, diagnostics} ->
        {:error,
         error(:invalid_domain_contract, [], "domain contract is invalid",
           errors: diagnostics.errors
         )}
    end
  end

  defp field_options_request(domain_or_normalized, field, attrs) do
    with {:ok, normalized} <- normalized_domain(domain_or_normalized),
         {:ok, binding} <- binding(normalized, field),
         {:ok, choice_source_config} <-
           fetch_choice_source(normalized, Map.fetch!(binding, :choice_source)),
         {:ok, source_relationship_config} <-
           source_relationship_config(normalized, choice_source_config) do
      constraint_filters =
        constraint_filters(choice_source_config, source_relationship_config, attrs)

      {:ok,
       OptionsRequest.new(
         Map.merge(attrs, %{
           domain: domain_name(normalized),
           field: Map.fetch!(binding, :field),
           choice_source: Map.fetch!(binding, :choice_source),
           choice_source_config: choice_source_config,
           source_relationship: map_value(choice_source_config, :source_relationship),
           source_relationship_config: source_relationship_config,
           field_binding: binding,
           reference: Map.get(binding, :reference, %{}),
           constraint_filters: constraint_filters
         })
       )}
    end
  end

  defp field_column(normalized, field) do
    source = Map.get(normalized, :source)
    schemas = Map.get(normalized, :schemas, %{})
    projection = Map.get(normalized, :projection, %{})

    with :error <- source_field_column(source, projection, field),
         :error <- schema_field_column(schemas, projection, field),
         :error <- projection_field_column(projection, field) do
      {:error,
       error(
         :field_not_found,
         [field],
         "field #{inspect(field)} is not defined in source, schemas, or projection columns",
         field: field
       )}
    end
  end

  defp source_field_column(source, projection, field) when is_map(source) do
    columns = map_value(source, :columns)

    case fetch_key(columns, field) do
      {:ok, column} when is_map(column) ->
        projection_column = projection_column(projection, field)

        {:ok,
         %{
           field: field,
           path: [:source, :columns, field],
           column: Map.merge(column, projection_column)
         }}

      _ ->
        :error
    end
  end

  defp source_field_column(_source, _projection, _field), do: :error

  defp schema_field_column(schemas, projection, field) when is_map(schemas) do
    with {:ok, schema_id, schema_field} <- split_schema_field(field),
         {:ok, schema} <- fetch_key(schemas, schema_id),
         columns when is_map(columns) <- map_value(schema, :columns),
         {:ok, column} when is_map(column) <- fetch_key(columns, schema_field) do
      projected_field = "#{field_id(schema_id)}.#{field_id(schema_field)}"
      projection_column = projection_column(projection, projected_field)

      {:ok,
       %{
         field: projected_field,
         path: [:schemas, schema_id, :columns, schema_field],
         column: Map.merge(column, projection_column)
       }}
    else
      _ -> :error
    end
  end

  defp schema_field_column(_schemas, _projection, _field), do: :error

  defp projection_field_column(projection, field) do
    case projection_column(projection, field) do
      column when map_size(column) > 0 ->
        {:ok, %{field: field, path: [:columns, field], column: column}}

      _ ->
        :error
    end
  end

  defp projection_column(projection, field) do
    case map_value(projection, :columns) do
      columns when is_map(columns) ->
        case fetch_key(columns, field) do
          {:ok, column} when is_map(column) -> column
          _ -> %{}
        end

      _ ->
        %{}
    end
  end

  defp column_choice_source(column, field) do
    reference = map_value(column, :reference)

    cond do
      is_map(reference) and id_ref?(map_value(reference, :choice_source)) ->
        {:ok, map_value(reference, :choice_source), :reference}

      id_ref?(map_value(column, :choice_source)) ->
        {:ok, map_value(column, :choice_source), :choice_source}

      is_map(reference) ->
        {:error,
         error(
           :field_choice_source_not_bound,
           [field, :reference, :choice_source],
           "field #{inspect(field)} reference does not declare a choice_source",
           field: field
         )}

      true ->
        {:error,
         error(
           :field_choice_source_not_bound,
           [field, :choice_source],
           "field #{inspect(field)} is not bound to a choice_source",
           field: field
         )}
    end
  end

  defp fetch_choice_source(normalized, choice_source_id) do
    choice_sources = Map.get(normalized, :choice_sources, %{})

    case fetch_key(choice_sources, choice_source_id) do
      {:ok, choice_source} when is_map(choice_source) ->
        {:ok, choice_source}

      _ ->
        {:error,
         error(
           :choice_source_not_found,
           [:choice_sources, choice_source_id],
           "choice source #{inspect(choice_source_id)} is not declared",
           choice_source: choice_source_id
         )}
    end
  end

  defp source_relationship_config(normalized, choice_source_config) do
    source_relationship = map_value(choice_source_config, :source_relationship)

    cond do
      is_nil(source_relationship) ->
        {:ok, nil}

      id_ref?(source_relationship) ->
        case fetch_key(Map.get(normalized, :source_relationships, %{}), source_relationship) do
          {:ok, source_relationship_config} when is_map(source_relationship_config) ->
            {:ok, source_relationship_config}

          _ ->
            {:error,
             error(
               :source_relationship_not_found,
               [:source_relationships, source_relationship],
               "source relationship #{inspect(source_relationship)} is not declared",
               source_relationship: source_relationship
             )}
        end

      true ->
        {:error,
         error(
           :invalid_source_relationship,
           [:choice_sources, :source_relationship],
           "choice source source_relationship must be an atom or string",
           source_relationship: source_relationship
         )}
    end
  end

  defp constraint_filters(choice_source_config, source_relationship_config, attrs) do
    %{
      source_relationship: config_filters(source_relationship_config),
      choice_source: config_filters(choice_source_config),
      domain_of_interest: request_filters(attrs)
    }
  end

  defp config_filters(config) when is_map(config) do
    case map_value(config, :filters) do
      filters when is_list(filters) -> filters
      _filters -> []
    end
  end

  defp config_filters(_config), do: []

  defp request_filters(attrs) when is_map(attrs) do
    case map_value(attrs, :filters) do
      filters when is_list(filters) -> filters
      _filters -> []
    end
  end

  defp request_filters(_attrs), do: []

  defp resolver_result(resolver, request), do: resolver.(request)

  defp normalize_resolver_result({:ok, %Result{} = result}, request) do
    {:ok, ensure_result_request(result, request)}
  end

  defp normalize_resolver_result({:error, %Result{} = result}, request) do
    {:error, ensure_result_request(result, request)}
  end

  defp normalize_resolver_result(%Result{status: :valid} = result, request) do
    {:ok, ensure_result_request(result, request)}
  end

  defp normalize_resolver_result(%Result{} = result, request) do
    {:error, ensure_result_request(result, request)}
  end

  defp normalize_resolver_result(other, request) do
    {:error,
     Result.unknown(:invalid_resolver_result,
       request: request,
       metadata: %{result: inspect(other)}
     )}
  end

  defp options_resolver_result(resolver, request), do: resolver.(request)

  defp normalize_options_resolver_result({:ok, %OptionsResult{} = result}, request) do
    {:ok, ensure_options_result_request(result, request)}
  end

  defp normalize_options_resolver_result({:error, %OptionsResult{} = result}, request) do
    {:error, ensure_options_result_request(result, request)}
  end

  defp normalize_options_resolver_result(%OptionsResult{status: :resolved} = result, request) do
    {:ok, ensure_options_result_request(result, request)}
  end

  defp normalize_options_resolver_result(%OptionsResult{} = result, request) do
    {:error, ensure_options_result_request(result, request)}
  end

  defp normalize_options_resolver_result(other, request) do
    {:error,
     OptionsResult.unknown(:invalid_resolver_result,
       request: request,
       metadata: %{result: inspect(other)}
     )}
  end

  defp ensure_options_result_request(%OptionsResult{request: nil} = result, request),
    do: %{result | request: request}

  defp ensure_options_result_request(%OptionsResult{} = result, _request), do: result

  defp ensure_result_request(%Result{request: nil} = result, request),
    do: %{result | request: request}

  defp ensure_result_request(%Result{} = result, _request), do: result

  defp split_schema_field(field) when is_atom(field),
    do: split_schema_field(Atom.to_string(field))

  defp split_schema_field(field) when is_binary(field) do
    case String.split(field, ".", parts: 2) do
      [schema_id, schema_field] when schema_id != "" and schema_field != "" ->
        {:ok, schema_id, schema_field}

      _ ->
        :error
    end
  end

  defp split_schema_field(_field), do: :error

  defp domain_name(normalized) do
    normalized
    |> Map.get(:domain, %{})
    |> map_value(:name)
  end

  defp attrs_map(attrs) when is_map(attrs), do: attrs
  defp attrs_map(attrs) when is_list(attrs), do: Enum.into(attrs, %{})

  defp id_ref?(value), do: (is_atom(value) and not is_nil(value)) or is_binary(value)

  defp options_target_kind(:field), do: :field
  defp options_target_kind("field"), do: :field
  defp options_target_kind(:choice_source), do: :choice_source
  defp options_target_kind("choice_source"), do: :choice_source
  defp options_target_kind(_by), do: :error

  defp pop_attr(attrs, key, default \\ nil) do
    cond do
      Map.has_key?(attrs, key) ->
        Map.pop(attrs, key)

      Map.has_key?(attrs, Atom.to_string(key)) ->
        Map.pop(attrs, Atom.to_string(key))

      true ->
        {default, attrs}
    end
  end

  defp map_value(map, key) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp map_value(_map, _key), do: nil

  defp fetch_key(map, key) when is_map(map) do
    cond do
      Map.has_key?(map, key) ->
        {:ok, Map.fetch!(map, key)}

      is_atom(key) and Map.has_key?(map, Atom.to_string(key)) ->
        {:ok, Map.fetch!(map, Atom.to_string(key))}

      is_binary(key) ->
        atom_key = safe_existing_atom(key)

        if not is_nil(atom_key) and Map.has_key?(map, atom_key) do
          {:ok, Map.fetch!(map, atom_key)}
        else
          :error
        end

      true ->
        :error
    end
  end

  defp fetch_key(_map, _key), do: :error

  defp safe_existing_atom(value) when is_binary(value) do
    try do
      String.to_existing_atom(value)
    rescue
      ArgumentError -> nil
    end
  end

  defp field_id(field) when is_atom(field), do: Atom.to_string(field)
  defp field_id(field) when is_binary(field), do: field
  defp field_id(field), do: inspect(field)

  defp error(code, path, message, attrs) do
    attrs
    |> Enum.into(%{})
    |> Map.merge(%{
      code: code,
      path: path,
      message: message
    })
  end
end
