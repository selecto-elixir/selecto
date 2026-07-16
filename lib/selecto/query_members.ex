defmodule Selecto.QueryMembers do
  @moduledoc false

  use Selecto.Domain.Constants

  @key_map %{
    "name" => :name,
    "type" => :type,
    "query" => :query,
    "query_builder" => :query_builder,
    "base_query" => :base_query,
    "recursive_query" => :recursive_query,
    "columns" => :columns,
    "dependencies" => :dependencies,
    "join" => :join,
    "rows" => :rows,
    "data" => :data,
    "as" => :as,
    "alias" => :alias,
    "alias_name" => :alias_name,
    "join_id" => :join_id,
    "on" => :on,
    "kind" => :kind,
    "source" => :source,
    "lateral_source" => :lateral_source,
    "join_type" => :join_type,
    "field" => :field,
    "array_field" => :array_field,
    "ordinality" => :ordinality,
    "options" => :options,
    "max_depth" => :max_depth,
    "cycle_detection" => :cycle_detection
  }

  @spec normalize_opts(keyword() | map()) :: keyword()
  def normalize_opts(opts) when is_map(opts), do: opts |> Map.to_list() |> normalize_opts()

  def normalize_opts(opts) when is_list(opts) do
    Enum.map(opts, fn
      {key, value} ->
        {normalize_key(key), value}

      other ->
        raise ArgumentError,
              "Named query member options must be a keyword list or map. Invalid option: #{inspect(other)}"
    end)
  end

  def normalize_opts(invalid_opts) do
    raise ArgumentError,
          "Named query member options must be a keyword list or map. Got: #{inspect(invalid_opts)}"
  end

  @spec normalize_spec(map()) :: map()
  def normalize_spec(spec) when is_map(spec) do
    spec
    |> Enum.map(fn {key, value} -> {normalize_key(key), value} end)
    |> Map.new()
  end

  def normalize_spec(spec) do
    raise ArgumentError,
          "Named query member specifications must be maps. Got: #{inspect(spec)}"
  end

  @spec fetch!(Selecto.t(), atom(), atom() | String.t()) :: {String.t(), map()}
  def fetch!(selecto, kind, member_id) when kind in @query_member_groups do
    members = members_for_kind!(selecto, kind)
    member_name = to_string(member_id)

    Enum.find_value(members, fn {key, spec} ->
      if to_string(key) == member_name do
        {member_name, spec}
      end
    end) ||
      raise ArgumentError,
            "Named #{kind_label(kind)} '#{member_name}' was not found in domain query_members. Available: #{available_members(members)}"
  end

  @spec resolve_alias_name(keyword(), term()) :: term()
  def resolve_alias_name(overrides, default \\ nil) do
    Keyword.get(
      overrides,
      :as,
      Keyword.get(overrides, :alias, Keyword.get(overrides, :alias_name))
    ) ||
      default
  end

  @spec resolve_override_or_spec_value(keyword(), map(), [atom()], [atom()]) :: term()
  def resolve_override_or_spec_value(overrides, spec, override_keys, spec_keys) do
    Enum.find_value(override_keys, fn key -> Keyword.get(overrides, key) end) ||
      Enum.find_value(spec_keys, fn key -> Map.get(spec, key) end)
  end

  @spec merge_member_options(keyword(), atom(), String.t(), [atom()]) :: keyword()
  def merge_member_options(overrides, kind, member_name, drop_keys) do
    override_options = Keyword.drop(overrides, drop_keys ++ [:options])

    nested_options =
      ensure_keyword_opts(
        Keyword.get(overrides, :options, :__missing__),
        kind,
        member_name
      )

    Keyword.merge(override_options, nested_options)
  end

  @spec maybe_put_keyword(keyword(), atom(), term()) :: keyword()
  def maybe_put_keyword(keyword, _key, value) when value in [nil, :__missing__], do: keyword
  def maybe_put_keyword(keyword, key, value), do: Keyword.put(keyword, key, value)

  @spec map_get(map(), atom(), atom()) :: term()
  def map_get(map, key, alt_key) when is_map(map) do
    Map.get(map, key, Map.get(map, alt_key))
  end

  @spec map_get(map(), atom(), atom(), atom()) :: term()
  def map_get(map, key, alt_key1, alt_key2) when is_map(map) do
    Map.get(map, key, Map.get(map, alt_key1, Map.get(map, alt_key2)))
  end

  @spec normalize_lateral_source!(term(), Selecto.t(), String.t()) :: term()
  def normalize_lateral_source!(source, selecto, member_name) do
    case source do
      source when is_tuple(source) ->
        source

      %Selecto{} = query ->
        fn _base_query -> query end

      query_builder when is_function(query_builder, 0) ->
        fn _base_query ->
          result = query_builder.()

          if match?(%Selecto{}, result) do
            result
          else
            raise ArgumentError,
                  "Named lateral '#{member_name}' query function must return a Selecto struct. Got: #{inspect(result)}"
          end
        end

      query_builder when is_function(query_builder, 1) ->
        fn base_query ->
          result = query_builder.(base_query)

          if match?(%Selecto{}, result) do
            result
          else
            raise ArgumentError,
                  "Named lateral '#{member_name}' query function must return a Selecto struct. Got: #{inspect(result)}"
          end
        end

      query_builder when is_function(query_builder, 2) ->
        fn base_query ->
          result = query_builder.(selecto, base_query)

          if match?(%Selecto{}, result) do
            result
          else
            raise ArgumentError,
                  "Named lateral '#{member_name}' query function must return a Selecto struct. Got: #{inspect(result)}"
          end
        end

      nil ->
        raise ArgumentError,
              "Named lateral '#{member_name}' requires :query, :source, or :lateral_source."

      invalid ->
        raise ArgumentError,
              "Named lateral '#{member_name}' source must be a tuple, Selecto struct, or function with arity 0/1/2. Got: #{inspect(invalid)}"
    end
  end

  @spec normalize_lateral_join_type!(atom(), term()) :: atom()
  def normalize_lateral_join_type!(join_type, _member_name)
      when join_type in @query_member_join_types,
      do: join_type

  def normalize_lateral_join_type!(join_type, member_name) do
    raise ArgumentError,
          "Named lateral '#{member_name}' join type must be one of :left, :inner, :right, :full. Got: #{inspect(join_type)}"
  end

  @spec ensure_keyword_opts(term(), atom(), String.t()) :: keyword()
  def ensure_keyword_opts(nil, _kind, _member_name), do: []
  def ensure_keyword_opts(:__missing__, _kind, _member_name), do: []
  def ensure_keyword_opts(opts, _kind, _member_name) when is_list(opts), do: opts
  def ensure_keyword_opts(opts, _kind, _member_name) when is_map(opts), do: Map.to_list(opts)

  def ensure_keyword_opts(opts, kind, member_name) do
    raise ArgumentError,
          "Named #{kind_label(kind)} '#{member_name}' expects :options to be a keyword list or map. Got: #{inspect(opts)}"
  end

  @spec evaluate_query!(term(), Selecto.t(), atom(), String.t()) :: Selecto.t()
  def evaluate_query!(query_source, selecto, kind, member_name) do
    query =
      case query_source do
        %Selecto{} = query ->
          query

        query_builder when is_function(query_builder, 0) ->
          query_builder.()

        query_builder when is_function(query_builder, 1) ->
          query_builder.(selecto)

        nil ->
          raise ArgumentError,
                "Named #{kind_label(kind)} '#{member_name}' requires a query source (:query or :query_builder)."

        invalid ->
          raise ArgumentError,
                "Named #{kind_label(kind)} '#{member_name}' query source must be a Selecto struct or function with arity 0/1. Got: #{inspect(invalid)}"
      end

    if match?(%Selecto{}, query) do
      query
    else
      raise ArgumentError,
            "Named #{kind_label(kind)} '#{member_name}' query builder must return a Selecto struct. Got: #{inspect(query)}"
    end
  end

  @spec wrap_query_builder!(term(), Selecto.t(), atom(), String.t()) :: (-> Selecto.t())
  def wrap_query_builder!(query_source, selecto, kind, member_name) do
    case query_source do
      %Selecto{} = query ->
        fn -> query end

      query_builder when is_function(query_builder, 0) ->
        fn ->
          result = query_builder.()

          if match?(%Selecto{}, result) do
            result
          else
            raise ArgumentError,
                  "Named #{kind_label(kind)} '#{member_name}' query builder must return a Selecto struct. Got: #{inspect(result)}"
          end
        end

      query_builder when is_function(query_builder, 1) ->
        fn ->
          result = query_builder.(selecto)

          if match?(%Selecto{}, result) do
            result
          else
            raise ArgumentError,
                  "Named #{kind_label(kind)} '#{member_name}' query builder must return a Selecto struct. Got: #{inspect(result)}"
          end
        end

      nil ->
        raise ArgumentError,
              "Named #{kind_label(kind)} '#{member_name}' requires :query or :query_builder."

      invalid ->
        raise ArgumentError,
              "Named #{kind_label(kind)} '#{member_name}' query source must be a Selecto struct or function with arity 0/1. Got: #{inspect(invalid)}"
    end
  end

  @spec wrap_base_query!(term(), Selecto.t(), String.t()) :: (-> Selecto.t())
  def wrap_base_query!(base_query, selecto, member_name) do
    case base_query do
      query_builder when is_function(query_builder, 0) ->
        fn ->
          result = query_builder.()

          if match?(%Selecto{}, result) do
            result
          else
            raise ArgumentError,
                  "Named CTE '#{member_name}' base_query must return a Selecto struct. Got: #{inspect(result)}"
          end
        end

      query_builder when is_function(query_builder, 1) ->
        fn ->
          result = query_builder.(selecto)

          if match?(%Selecto{}, result) do
            result
          else
            raise ArgumentError,
                  "Named CTE '#{member_name}' base_query must return a Selecto struct. Got: #{inspect(result)}"
          end
        end

      invalid ->
        raise ArgumentError,
              "Named CTE '#{member_name}' requires :base_query function with arity 0 or 1. Got: #{inspect(invalid)}"
    end
  end

  @spec wrap_recursive_query!(term(), Selecto.t(), String.t()) :: (term() -> Selecto.t())
  def wrap_recursive_query!(recursive_query, selecto, member_name) do
    case recursive_query do
      query_builder when is_function(query_builder, 1) ->
        fn cte_ref ->
          result = query_builder.(cte_ref)

          if match?(%Selecto{}, result) do
            result
          else
            raise ArgumentError,
                  "Named CTE '#{member_name}' recursive_query must return a Selecto struct. Got: #{inspect(result)}"
          end
        end

      query_builder when is_function(query_builder, 2) ->
        fn cte_ref ->
          result = query_builder.(selecto, cte_ref)

          if match?(%Selecto{}, result) do
            result
          else
            raise ArgumentError,
                  "Named CTE '#{member_name}' recursive_query must return a Selecto struct. Got: #{inspect(result)}"
          end
        end

      invalid ->
        raise ArgumentError,
              "Named CTE '#{member_name}' requires :recursive_query function with arity 1 or 2. Got: #{inspect(invalid)}"
    end
  end

  @spec merge_join_opts(term(), term(), (term() -> term())) :: term()
  def merge_join_opts(default_join_opts, :__missing__, normalizer_fun) do
    normalize_join_opts(default_join_opts, normalizer_fun)
  end

  def merge_join_opts(_default_join_opts, override_join_opts, _normalizer_fun)
      when override_join_opts in [nil, false, true],
      do: override_join_opts

  def merge_join_opts(default_join_opts, override_join_opts, normalizer_fun)
      when is_list(override_join_opts) or is_map(override_join_opts) do
    override_join_opts = normalize_join_opts(override_join_opts, normalizer_fun)
    default_join_opts = normalize_join_opts(default_join_opts, normalizer_fun)

    if is_list(default_join_opts) do
      Keyword.merge(default_join_opts, override_join_opts)
    else
      override_join_opts
    end
  end

  def merge_join_opts(_default_join_opts, invalid_join_opts, _normalizer_fun) do
    raise ArgumentError,
          "Named query member :join must be true, false, nil, a keyword list, or a map. Got: #{inspect(invalid_join_opts)}"
  end

  @spec values_member_alias(map()) :: term()
  def values_member_alias(spec) when is_map(spec) do
    Map.get(spec, :as) || Map.get(spec, :alias) || Map.get(spec, :alias_name)
  end

  @spec normalize_subquery_on(term()) :: term()
  def normalize_subquery_on(nil), do: nil
  def normalize_subquery_on(:__missing__), do: nil

  def normalize_subquery_on(on_conditions) when is_list(on_conditions) do
    Enum.map(on_conditions, fn
      condition when is_map(condition) ->
        normalize_subquery_on_condition(condition)

      condition when is_list(condition) ->
        condition |> Map.new() |> normalize_subquery_on_condition()

      invalid ->
        raise ArgumentError, "Invalid subquery :on condition: #{inspect(invalid)}"
    end)
  end

  def normalize_subquery_on(invalid_on) do
    raise ArgumentError,
          "Subquery :on option must be a list of conditions. Got: #{inspect(invalid_on)}"
  end

  @spec normalize_values_join_opts(keyword() | map()) :: keyword()
  def normalize_values_join_opts(join_opts) when is_map(join_opts),
    do: join_opts |> Map.to_list() |> normalize_values_join_opts()

  def normalize_values_join_opts(join_opts) when is_list(join_opts) do
    Enum.map(join_opts, fn
      {key, value} -> {normalize_values_join_key(key), value}
      other -> other
    end)
  end

  @spec normalize_values_join_id(term()) :: atom() | String.t()
  def normalize_values_join_id(join_id) when is_atom(join_id), do: join_id
  def normalize_values_join_id(join_id) when is_binary(join_id), do: join_id

  def normalize_values_join_id(join_id) do
    raise ArgumentError,
          "VALUES auto-join id must be an atom or string. Got: #{inspect(join_id)}"
  end

  @spec normalize_cte_option_list(keyword() | map()) :: keyword()
  def normalize_cte_option_list(opts) when is_map(opts),
    do: opts |> Map.to_list() |> normalize_cte_option_list()

  def normalize_cte_option_list(opts) when is_list(opts) do
    Enum.map(opts, fn
      {key, value} -> {normalize_cte_option_key(key), value}
      other -> other
    end)
  end

  @spec normalize_cte_join_opts(keyword() | map()) :: keyword()
  def normalize_cte_join_opts(join_opts) when is_map(join_opts),
    do: join_opts |> Map.to_list() |> normalize_cte_join_opts()

  def normalize_cte_join_opts(join_opts) when is_list(join_opts) do
    Enum.map(join_opts, fn
      {key, value} -> {normalize_cte_join_key(key), value}
      other -> other
    end)
  end

  defp members_for_kind!(selecto, kind) when kind in @query_member_groups do
    query_members =
      selecto
      |> Map.get(:domain, %{})
      |> Map.get(:query_members, %{})

    if not is_map(query_members) do
      raise ArgumentError,
            "domain.query_members must be a map to resolve named #{kind_label(kind)} members."
    end

    members = Map.get(query_members, kind, Map.get(query_members, Atom.to_string(kind), %{}))

    if is_map(members) do
      members
    else
      raise ArgumentError,
            "domain.query_members.#{kind} must be a map of named members. Got: #{inspect(members)}"
    end
  end

  defp available_members(members) do
    case members |> Map.keys() |> Enum.map(&to_string/1) |> Enum.sort() do
      [] -> "none"
      ids -> Enum.join(ids, ", ")
    end
  end

  defp kind_label(:ctes), do: "CTE"
  defp kind_label(:values), do: "VALUES"
  defp kind_label(:subqueries), do: "subquery"
  defp kind_label(:laterals), do: "lateral"
  defp kind_label(:unnests), do: "unnest"

  defp normalize_key(key) when is_atom(key), do: key
  defp normalize_key(key) when is_binary(key), do: Map.get(@key_map, key, key)
  defp normalize_key(key), do: key

  defp normalize_join_opts(join_opts, _normalizer_fun)
       when join_opts in [nil, false, true, :__missing__],
       do: join_opts

  defp normalize_join_opts(join_opts, normalizer_fun)
       when is_list(join_opts) or is_map(join_opts),
       do: normalizer_fun.(join_opts)

  defp normalize_join_opts(invalid_join_opts, _normalizer_fun) do
    raise ArgumentError,
          "Named query member :join must be true, false, nil, a keyword list, or a map. Got: #{inspect(invalid_join_opts)}"
  end

  defp normalize_subquery_on_condition(condition) do
    left = Map.get(condition, :left, Map.get(condition, "left"))
    right = Map.get(condition, :right, Map.get(condition, "right"))
    operator = Map.get(condition, :operator, Map.get(condition, "operator"))

    if is_nil(left) or is_nil(right) do
      raise ArgumentError,
            "Subquery :on conditions must include :left and :right. Got: #{inspect(condition)}"
    end

    result = %{left: left, right: right}

    if is_nil(operator) do
      result
    else
      Map.put(result, :operator, operator)
    end
  end

  defp normalize_values_join_key(key) when is_atom(key), do: key

  defp normalize_values_join_key(key) when is_binary(key) do
    case key do
      "id" -> :id
      "source" -> :source
      "type" -> :type
      "owner_key" -> :owner_key
      "related_key" -> :related_key
      "on" -> :on
      "fields" -> :fields
      other -> other
    end
  end

  defp normalize_values_join_key(key), do: key

  defp normalize_cte_option_key(key) when is_atom(key), do: key

  defp normalize_cte_option_key(key) when is_binary(key) do
    case key do
      "base_query" -> :base_query
      "recursive_query" -> :recursive_query
      "columns" -> :columns
      "dependencies" -> :dependencies
      "join" -> :join
      "joins" -> :joins
      "max_depth" -> :max_depth
      "cycle_detection" -> :cycle_detection
      other -> other
    end
  end

  defp normalize_cte_option_key(key), do: key

  defp normalize_cte_join_key(key) when is_atom(key), do: key

  defp normalize_cte_join_key(key) when is_binary(key) do
    case key do
      "name" -> :name
      "id" -> :id
      "source" -> :source
      "type" -> :type
      "owner_key" -> :owner_key
      "related_key" -> :related_key
      "on" -> :on
      "fields" -> :fields
      other -> other
    end
  end

  defp normalize_cte_join_key(key), do: key
end
