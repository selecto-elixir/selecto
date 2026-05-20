defmodule Selecto.Capabilities do
  @moduledoc """
  Shared capability request and decision helpers.

  Selecto owns the shape of capability questions and answers, but host
  applications remain responsible for policy truth. These helpers are small
  value constructors for adapters, components, write paths, and API layers that
  need to ask the same kind of question.
  """

  alias Selecto.Capabilities.{Decision, Request}

  @type capability_id :: atom() | String.t()
  @type operation :: atom() | String.t()
  @type target :: map()
  @type context :: map()
  @type resolver ::
          module()
          | (Request.t() -> term())
          | (Request.t(), map() -> term())
          | {module(), atom()}

  @doc """
  Builds a capability request.
  """
  @spec request(map() | keyword()) :: Request.t()
  def request(attrs), do: Request.new(attrs)

  @doc """
  Resolves one capability request through a host resolver.

  Supported resolver shapes are:

  - a one-arity function receiving the request
  - a two-arity function receiving the request and resolver context
  - a module implementing `Selecto.Capabilities.Resolver.decide/2`
  - `{module, function}` where the function has arity 2

  Resolver return values are normalized to `Selecto.Capabilities.Decision`.
  """
  @spec decide(resolver() | nil, Request.t(), keyword() | map()) :: Decision.t()
  def decide(resolver, %Request{} = request, opts \\ []) do
    context = resolver_context(opts)

    resolver
    |> invoke_decide(request, context)
    |> normalize_decision()
  end

  @doc """
  Resolves many capability requests.

  Module resolvers can implement `decide_many/2` for a true batch path. Other
  resolver shapes fall back to `decide/3` for each request while preserving
  request order.
  """
  @spec decide_many(resolver() | nil, [Request.t()], keyword() | map()) :: [Decision.t()]
  def decide_many(resolver, requests, opts \\ []) when is_list(requests) do
    context = resolver_context(opts)

    case invoke_decide_many(resolver, requests, context) do
      :fallback ->
        Enum.map(requests, &decide(resolver, &1, opts))

      result ->
        normalize_decisions(result, length(requests))
    end
  end

  @doc """
  Builds an allow decision.
  """
  @spec allow(atom() | String.t(), map() | keyword()) :: Decision.t()
  def allow(reason_code \\ :allowed, attrs \\ []) do
    Decision.allow(reason_code, attrs)
  end

  @doc """
  Builds a deny decision.
  """
  @spec deny(atom() | String.t(), map() | keyword()) :: Decision.t()
  def deny(reason_code \\ :denied, attrs \\ []) do
    Decision.deny(reason_code, attrs)
  end

  @doc """
  Builds a hidden deny decision.
  """
  @spec hidden(atom() | String.t(), map() | keyword()) :: Decision.t()
  def hidden(reason_code \\ :hidden, attrs \\ []) do
    Decision.hidden(reason_code, attrs)
  end

  @doc """
  Builds a conditional preview-only decision.
  """
  @spec preview_only(atom() | String.t(), map() | keyword()) :: Decision.t()
  def preview_only(reason_code \\ :preview_only, attrs \\ []) do
    Decision.preview_only(reason_code, attrs)
  end

  @doc """
  Builds a not-applicable decision.
  """
  @spec not_applicable(atom() | String.t(), map() | keyword()) :: Decision.t()
  def not_applicable(reason_code \\ :not_applicable, attrs \\ []) do
    Decision.not_applicable(reason_code, attrs)
  end

  defp invoke_decide(nil, _request, _context), do: allow(:no_resolver)

  defp invoke_decide(resolver, request, _context) when is_function(resolver, 1),
    do: resolver.(request)

  defp invoke_decide(resolver, request, context) when is_function(resolver, 2),
    do: resolver.(request, context)

  defp invoke_decide({module, function}, request, context)
       when is_atom(module) and is_atom(function) do
    apply(module, function, [request, context])
  end

  defp invoke_decide(module, request, context) when is_atom(module) do
    if function_exported?(module, :decide, 2) do
      module.decide(request, context)
    else
      deny(:resolver_missing_decide,
        user_message: "Capability resolver does not implement decide/2.",
        metadata: %{resolver: inspect(module)}
      )
    end
  end

  defp invoke_decide(_resolver, _request, _context) do
    deny(:invalid_resolver, user_message: "Capability resolver is invalid.")
  end

  defp invoke_decide_many(module, requests, context) when is_atom(module) do
    cond do
      function_exported?(module, :decide_many, 2) ->
        module.decide_many(requests, context)

      function_exported?(module, :decide, 2) ->
        :fallback

      true ->
        {:error, :resolver_missing_decide}
    end
  end

  defp invoke_decide_many(_resolver, _requests, _context), do: :fallback

  defp normalize_decisions({:ok, decisions}, expected_count),
    do: normalize_decisions(decisions, expected_count)

  defp normalize_decisions({:error, reason}, expected_count) do
    List.duplicate(
      deny(:resolver_error,
        user_message: inspect(reason),
        metadata: %{reason: reason}
      ),
      expected_count
    )
  end

  defp normalize_decisions(decisions, expected_count) when is_list(decisions) do
    decisions
    |> Enum.map(&normalize_decision/1)
    |> case do
      normalized when length(normalized) == expected_count ->
        normalized

      normalized ->
        normalized ++
          List.duplicate(
            deny(:resolver_result_count_mismatch,
              user_message: "Capability resolver returned the wrong number of decisions."
            ),
            max(expected_count - length(normalized), 0)
          )
    end
    |> Enum.take(expected_count)
  end

  defp normalize_decisions(decision, expected_count) do
    List.duplicate(normalize_decision(decision), expected_count)
  end

  defp normalize_decision({:ok, decision}), do: normalize_decision(decision)

  defp normalize_decision({:error, reason}) do
    deny(:resolver_error,
      user_message: inspect(reason),
      metadata: %{reason: reason}
    )
  end

  defp normalize_decision(%Decision{} = decision), do: decision
  defp normalize_decision(%{} = attrs), do: Decision.new(normalize_decision_attrs(attrs))
  defp normalize_decision(true), do: allow()
  defp normalize_decision(false), do: deny()
  defp normalize_decision(:allow), do: allow()
  defp normalize_decision(:deny), do: deny()
  defp normalize_decision(:hidden), do: hidden()
  defp normalize_decision(nil), do: allow(:no_decision)
  defp normalize_decision(_decision), do: deny(:invalid_decision)

  defp normalize_decision_attrs(attrs) do
    status = get_attr(attrs, :status, get_attr(attrs, :visibility))

    attrs
    |> maybe_put_status_visibility(status)
    |> maybe_copy_attr(:code, :reason_code)
    |> maybe_copy_attr(:reason, :user_message)
  end

  defp maybe_put_status_visibility(attrs, status)
       when status in [:enabled, :allow, "enabled", "allow"] do
    attrs
    |> Map.put(:status, :allow)
    |> Map.put_new(:visibility, :enabled)
  end

  defp maybe_put_status_visibility(attrs, status)
       when status in [:disabled, :deny, "disabled", "deny"] do
    attrs
    |> Map.put(:status, :deny)
    |> Map.put_new(:visibility, :disabled)
  end

  defp maybe_put_status_visibility(attrs, status)
       when status in [:hidden, :not_applicable, "hidden", "not_applicable"] do
    attrs
    |> Map.put(:status, :deny)
    |> Map.put(:visibility, :hidden)
  end

  defp maybe_put_status_visibility(attrs, status)
       when status in [:preview_only, :conditional, "preview_only", "conditional"] do
    attrs
    |> Map.put(:status, :conditional)
    |> Map.put(:visibility, :preview_only)
  end

  defp maybe_put_status_visibility(attrs, _status), do: attrs

  defp maybe_copy_attr(attrs, from, to) do
    case {get_attr(attrs, from), get_attr(attrs, to)} do
      {nil, _existing} -> attrs
      {_value, existing} when not is_nil(existing) -> attrs
      {value, nil} -> Map.put(attrs, to, value)
    end
  end

  defp resolver_context(opts) when is_list(opts) do
    opts
    |> Keyword.get(:resolver_context, Keyword.get(opts, :context, %{}))
    |> map_or_empty()
  end

  defp resolver_context(%{} = opts) do
    opts
    |> Map.get(:resolver_context, Map.get(opts, "resolver_context", Map.get(opts, :context, %{})))
    |> map_or_empty()
  end

  defp resolver_context(_opts), do: %{}

  defp map_or_empty(value) when is_map(value), do: value
  defp map_or_empty(_value), do: %{}

  defp get_attr(attrs, key, default \\ nil) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> value
      :error -> Map.get(attrs, Atom.to_string(key), default)
    end
  end
end
