defmodule Selecto.Domain.Choices.Request do
  @moduledoc """
  Stable choice-source membership question shape.

  A request describes the working-domain field/value being checked, the choice
  source it is bound to, any actor, tenant, record, or context data a future
  resolver needs to prove membership, and the server-owned Domain-of-Interest
  filter bundle that constrains the choice.
  """

  defstruct domain: nil,
            field: nil,
            value: nil,
            choice_source: nil,
            choice_source_config: %{},
            source_relationship: nil,
            source_relationship_config: nil,
            field_binding: %{},
            reference: %{},
            actor: nil,
            tenant: nil,
            record: %{},
            filters: [],
            constraint_filters: %{},
            context: %{},
            metadata: %{}

  @type t :: %__MODULE__{
          domain: atom() | String.t() | nil,
          field: atom() | String.t(),
          value: term(),
          choice_source: atom() | String.t(),
          choice_source_config: map(),
          source_relationship: atom() | String.t() | nil,
          source_relationship_config: map() | nil,
          field_binding: map(),
          reference: map(),
          actor: term(),
          tenant: term(),
          record: map(),
          filters: [term()],
          constraint_filters: map(),
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
      domain: get_attr(attrs, :domain),
      field: required_id_attr!(attrs, :field),
      value: get_attr(attrs, :value),
      choice_source: required_id_attr!(attrs, :choice_source),
      choice_source_config: map_attr(attrs, :choice_source_config),
      source_relationship: id_attr(attrs, :source_relationship),
      source_relationship_config: nullable_map_attr(attrs, :source_relationship_config),
      field_binding: map_attr(attrs, :field_binding),
      reference: map_attr(attrs, :reference),
      actor: get_attr(attrs, :actor),
      tenant: get_attr(attrs, :tenant),
      record: map_attr(attrs, :record),
      filters: list_attr(attrs, :filters),
      constraint_filters: map_attr(attrs, :constraint_filters),
      context: map_attr(attrs, :context),
      metadata: map_attr(attrs, :metadata)
    }
  end

  defp required_id_attr!(attrs, key) do
    case id_attr(attrs, key, :__missing__) do
      :__missing__ ->
        raise ArgumentError, "missing required choice request attribute #{inspect(key)}"

      value ->
        value
    end
  end

  defp id_attr(attrs, key, default \\ nil) do
    value = get_attr(attrs, key, default)

    cond do
      value == default ->
        value

      (is_atom(value) and not is_nil(value)) or is_binary(value) ->
        value

      true ->
        raise ArgumentError,
              "choice request #{inspect(key)} must be an atom or string, got #{inspect(value)}"
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
              "choice request #{inspect(key)} must be a map, got #{inspect(value)}"
    end
  end

  defp nullable_map_attr(attrs, key) do
    case get_attr(attrs, key) do
      value when is_map(value) ->
        value

      nil ->
        nil

      value ->
        raise ArgumentError, "choice request #{inspect(key)} must be a map, got #{inspect(value)}"
    end
  end

  defp list_attr(attrs, key) do
    case get_attr(attrs, key, []) do
      value when is_list(value) ->
        value

      nil ->
        []

      value ->
        raise ArgumentError,
              "choice request #{inspect(key)} must be a list, got #{inspect(value)}"
    end
  end

  defp get_attr(attrs, key, default \\ nil) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> value
      :error -> Map.get(attrs, Atom.to_string(key), default)
    end
  end
end
