defmodule Selecto.Capabilities.Request do
  @moduledoc """
  Stable capability question shape.

  A request describes who is asking, which domain capability is being checked,
  what operation is being attempted, and any target/context data needed by a
  host policy resolver.
  """

  @enforce_keys [:capability, :operation]
  defstruct actor: nil,
            tenant: nil,
            domain: nil,
            capability: nil,
            operation: nil,
            target: %{},
            context: %{},
            metadata: %{}

  @type t :: %__MODULE__{
          actor: term(),
          tenant: term(),
          domain: term(),
          capability: atom() | String.t(),
          operation: atom() | String.t(),
          target: map(),
          context: map(),
          metadata: map()
        }

  @doc """
  Builds a request from atom-keyed or string-keyed map/keyword attributes.
  """
  @spec new(map() | keyword()) :: t()
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Enum.into(attrs, %{})

    %__MODULE__{
      actor: get_attr(attrs, :actor),
      tenant: get_attr(attrs, :tenant),
      domain: get_attr(attrs, :domain),
      capability: required_id_attr!(attrs, :capability),
      operation: required_id_attr!(attrs, :operation),
      target: map_attr(attrs, :target),
      context: map_attr(attrs, :context),
      metadata: map_attr(attrs, :metadata)
    }
  end

  defp required_attr!(attrs, key) do
    case get_attr(attrs, key, :__missing__) do
      :__missing__ ->
        raise ArgumentError, "missing required capability request attribute #{inspect(key)}"

      value ->
        value
    end
  end

  defp required_id_attr!(attrs, key) do
    value = required_attr!(attrs, key)

    if is_atom(value) or is_binary(value) do
      value
    else
      raise ArgumentError,
            "capability request #{inspect(key)} must be an atom or string, got #{inspect(value)}"
    end
  end

  defp map_attr(attrs, key) do
    case get_attr(attrs, key, %{}) do
      value when is_map(value) ->
        value

      nil ->
        %{}

      value ->
        raise ArgumentError,
              "capability request #{inspect(key)} must be a map, got #{inspect(value)}"
    end
  end

  defp get_attr(attrs, key, default \\ nil) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> value
      :error -> Map.get(attrs, Atom.to_string(key), default)
    end
  end
end
