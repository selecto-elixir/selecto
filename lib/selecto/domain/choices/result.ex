defmodule Selecto.Domain.Choices.Result do
  @moduledoc """
  Choice-source membership answer shape.

  Core Selecto can resolve the domain metadata for a choice-source membership
  question, but it cannot prove external membership until a caller supplies a
  resolver. The `:unknown` status represents that deliberate safe default.
  """

  @statuses [:valid, :invalid, :unknown]

  defstruct status: :unknown,
            reason_code: :resolver_required,
            request: nil,
            user_message: nil,
            metadata: %{}

  @type status :: :valid | :invalid | :unknown

  @type t :: %__MODULE__{
          status: status(),
          reason_code: atom() | String.t() | nil,
          request: Selecto.Domain.Choices.Request.t() | nil,
          user_message: String.t() | nil,
          metadata: map()
        }

  @doc """
  Builds a result from atom-keyed or string-keyed map/keyword attributes.
  """
  @spec new(map() | keyword()) :: t()
  def new(attrs \\ []) when is_map(attrs) or is_list(attrs) do
    attrs = Enum.into(attrs, %{})

    %__MODULE__{
      status: validate_status!(get_attr(attrs, :status, :unknown)),
      reason_code: get_attr(attrs, :reason_code, :resolver_required),
      request: get_attr(attrs, :request),
      user_message: get_attr(attrs, :user_message),
      metadata: map_attr(attrs, :metadata)
    }
  end

  @doc """
  Builds a valid membership result.
  """
  @spec valid(atom() | String.t(), map() | keyword()) :: t()
  def valid(reason_code \\ :choice_valid, attrs \\ []) do
    attrs
    |> attrs_map()
    |> Map.put(:status, :valid)
    |> Map.put(:reason_code, reason_code)
    |> new()
  end

  @doc """
  Builds an invalid membership result.
  """
  @spec invalid(atom() | String.t(), map() | keyword()) :: t()
  def invalid(reason_code \\ :choice_invalid, attrs \\ []) do
    attrs
    |> attrs_map()
    |> Map.put(:status, :invalid)
    |> Map.put(:reason_code, reason_code)
    |> new()
  end

  @doc """
  Builds an unknown membership result.
  """
  @spec unknown(atom() | String.t(), map() | keyword()) :: t()
  def unknown(reason_code \\ :resolver_required, attrs \\ []) do
    attrs
    |> attrs_map()
    |> Map.put(:status, :unknown)
    |> Map.put(:reason_code, reason_code)
    |> new()
  end

  defp attrs_map(attrs) when is_map(attrs), do: attrs
  defp attrs_map(attrs) when is_list(attrs), do: Enum.into(attrs, %{})

  defp validate_status!(status) when status in @statuses, do: status

  defp validate_status!(status) do
    raise ArgumentError,
          "unknown choice membership status #{inspect(status)}; expected one of #{inspect(@statuses)}"
  end

  defp map_attr(attrs, key) do
    case get_attr(attrs, key, %{}) do
      value when is_map(value) ->
        value

      nil ->
        %{}

      value ->
        raise ArgumentError,
              "choice result #{inspect(key)} must be a map, got #{inspect(value)}"
    end
  end

  defp get_attr(attrs, key, default \\ nil) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> value
      :error -> Map.get(attrs, Atom.to_string(key), default)
    end
  end
end
