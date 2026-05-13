defmodule Selecto.Domain.Choices.OptionsResult do
  @moduledoc """
  Choice-source option-list answer shape.

  Core Selecto can resolve the option-list metadata, but it cannot fetch options
  until a caller supplies a resolver. The `:unknown` status is the safe default.
  """

  @statuses [:resolved, :unknown, :error]

  defstruct status: :unknown,
            reason_code: :resolver_required,
            request: nil,
            options: [],
            total_count: nil,
            next_cursor: nil,
            user_message: nil,
            metadata: %{}

  @type status :: :resolved | :unknown | :error

  @type option :: map()

  @type t :: %__MODULE__{
          status: status(),
          reason_code: atom() | String.t() | nil,
          request: Selecto.Domain.Choices.OptionsRequest.t() | nil,
          options: [option()],
          total_count: non_neg_integer() | nil,
          next_cursor: term(),
          user_message: String.t() | nil,
          metadata: map()
        }

  @doc """
  Builds an options result from atom-keyed or string-keyed map/keyword attrs.
  """
  @spec new(map() | keyword()) :: t()
  def new(attrs \\ []) when is_map(attrs) or is_list(attrs) do
    attrs = Enum.into(attrs, %{})

    %__MODULE__{
      status: validate_status!(get_attr(attrs, :status, :unknown)),
      reason_code: get_attr(attrs, :reason_code, :resolver_required),
      request: get_attr(attrs, :request),
      options: list_attr(attrs, :options),
      total_count: non_negative_integer_attr(attrs, :total_count),
      next_cursor: get_attr(attrs, :next_cursor),
      user_message: get_attr(attrs, :user_message),
      metadata: map_attr(attrs, :metadata)
    }
  end

  @doc """
  Builds a resolved option-list result.
  """
  @spec resolved([option()], map() | keyword()) :: t()
  def resolved(options, attrs \\ []) when is_list(options) do
    attrs
    |> attrs_map()
    |> Map.put(:status, :resolved)
    |> Map.put_new(:reason_code, :options_resolved)
    |> Map.put(:options, options)
    |> new()
  end

  @doc """
  Builds an unknown option-list result.
  """
  @spec unknown(atom() | String.t(), map() | keyword()) :: t()
  def unknown(reason_code \\ :resolver_required, attrs \\ []) do
    attrs
    |> attrs_map()
    |> Map.put(:status, :unknown)
    |> Map.put(:reason_code, reason_code)
    |> new()
  end

  @doc """
  Builds an error option-list result.
  """
  @spec error(atom() | String.t(), map() | keyword()) :: t()
  def error(reason_code \\ :options_error, attrs \\ []) do
    attrs
    |> attrs_map()
    |> Map.put(:status, :error)
    |> Map.put(:reason_code, reason_code)
    |> new()
  end

  defp attrs_map(attrs) when is_map(attrs), do: attrs
  defp attrs_map(attrs) when is_list(attrs), do: Enum.into(attrs, %{})

  defp validate_status!(status) when status in @statuses, do: status

  defp validate_status!(status) do
    raise ArgumentError,
          "unknown choice options status #{inspect(status)}; expected one of #{inspect(@statuses)}"
  end

  defp list_attr(attrs, key) do
    case get_attr(attrs, key, []) do
      value when is_list(value) ->
        value

      nil ->
        []

      value ->
        raise ArgumentError,
              "options result #{inspect(key)} must be a list, got #{inspect(value)}"
    end
  end

  defp map_attr(attrs, key) do
    case get_attr(attrs, key, %{}) do
      value when is_map(value) ->
        value

      nil ->
        %{}

      value ->
        raise ArgumentError, "options result #{inspect(key)} must be a map, got #{inspect(value)}"
    end
  end

  defp non_negative_integer_attr(attrs, key) do
    case get_attr(attrs, key) do
      value when is_integer(value) and value >= 0 ->
        value

      nil ->
        nil

      value ->
        raise ArgumentError,
              "options result #{inspect(key)} must be a non-negative integer, got #{inspect(value)}"
    end
  end

  defp get_attr(attrs, key, default \\ nil) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> value
      :error -> Map.get(attrs, Atom.to_string(key), default)
    end
  end
end
