defmodule Selecto.Domain.Choices.OptionsRequest do
  @moduledoc """
  Stable choice-source option-list question shape.

  An options request describes which choice source should provide selectable
  options plus the actor, tenant, working record, search term, paging, context,
  and server-owned Domain-of-Interest filter data a future resolver may need.
  """

  defstruct domain: nil,
            field: nil,
            choice_source: nil,
            choice_source_config: %{},
            source_relationship: nil,
            source_relationship_config: nil,
            field_binding: %{},
            reference: %{},
            actor: nil,
            tenant: nil,
            record: %{},
            context: %{},
            search: nil,
            limit: nil,
            offset: 0,
            cursor: nil,
            filters: [],
            constraint_filters: %{},
            order_by: [],
            metadata: %{}

  @type t :: %__MODULE__{
          domain: atom() | String.t() | nil,
          field: atom() | String.t() | nil,
          choice_source: atom() | String.t(),
          choice_source_config: map(),
          source_relationship: atom() | String.t() | nil,
          source_relationship_config: map() | nil,
          field_binding: map(),
          reference: map(),
          actor: term(),
          tenant: term(),
          record: map(),
          context: map(),
          search: String.t() | nil,
          limit: pos_integer() | nil,
          offset: non_neg_integer(),
          cursor: term(),
          filters: [term()],
          constraint_filters: map(),
          order_by: [term()],
          metadata: map()
        }

  @doc """
  Builds an options request from atom-keyed or string-keyed map/keyword attrs.
  """
  @spec new(map() | keyword()) :: t()
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Enum.into(attrs, %{})

    %__MODULE__{
      domain: get_attr(attrs, :domain),
      field: optional_id_attr!(attrs, :field),
      choice_source: required_id_attr!(attrs, :choice_source),
      choice_source_config: map_attr(attrs, :choice_source_config),
      source_relationship: optional_id_attr!(attrs, :source_relationship),
      source_relationship_config: nullable_map_attr(attrs, :source_relationship_config),
      field_binding: map_attr(attrs, :field_binding),
      reference: map_attr(attrs, :reference),
      actor: get_attr(attrs, :actor),
      tenant: get_attr(attrs, :tenant),
      record: map_attr(attrs, :record),
      context: map_attr(attrs, :context),
      search: search_attr(attrs),
      limit: limit_attr(attrs),
      offset: offset_attr(attrs),
      cursor: get_attr(attrs, :cursor),
      filters: list_attr(attrs, :filters),
      constraint_filters: map_attr(attrs, :constraint_filters),
      order_by: list_attr(attrs, :order_by),
      metadata: map_attr(attrs, :metadata)
    }
  end

  defp required_id_attr!(attrs, key) do
    case optional_id_attr!(attrs, key, :__missing__) do
      :__missing__ ->
        raise ArgumentError, "missing required options request attribute #{inspect(key)}"

      value ->
        value
    end
  end

  defp optional_id_attr!(attrs, key, default \\ nil) do
    value = get_attr(attrs, key, default)

    cond do
      value == default ->
        value

      (is_atom(value) and not is_nil(value)) or is_binary(value) ->
        value

      true ->
        raise ArgumentError,
              "options request #{inspect(key)} must be an atom or string, got #{inspect(value)}"
    end
  end

  defp search_attr(attrs) do
    case get_attr(attrs, :search) do
      value when is_binary(value) ->
        value

      nil ->
        nil

      value ->
        raise ArgumentError, "options request :search must be a string, got #{inspect(value)}"
    end
  end

  defp limit_attr(attrs) do
    case get_attr(attrs, :limit) do
      value when is_integer(value) and value > 0 ->
        value

      nil ->
        nil

      value ->
        raise ArgumentError,
              "options request :limit must be a positive integer, got #{inspect(value)}"
    end
  end

  defp offset_attr(attrs) do
    case get_attr(attrs, :offset, 0) do
      value when is_integer(value) and value >= 0 ->
        value

      nil ->
        0

      value ->
        raise ArgumentError,
              "options request :offset must be a non-negative integer, got #{inspect(value)}"
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
              "options request #{inspect(key)} must be a list, got #{inspect(value)}"
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
              "options request #{inspect(key)} must be a map, got #{inspect(value)}"
    end
  end

  defp nullable_map_attr(attrs, key) do
    case get_attr(attrs, key) do
      value when is_map(value) ->
        value

      nil ->
        nil

      value ->
        raise ArgumentError,
              "options request #{inspect(key)} must be a map, got #{inspect(value)}"
    end
  end

  defp get_attr(attrs, key, default \\ nil) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> value
      :error -> Map.get(attrs, Atom.to_string(key), default)
    end
  end
end
