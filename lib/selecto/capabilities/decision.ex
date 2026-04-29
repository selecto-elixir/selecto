defmodule Selecto.Capabilities.Decision do
  @moduledoc """
  Stable capability answer shape.

  Decisions are richer than booleans so consumers can represent hidden,
  disabled, preview-only, and query-shaping outcomes consistently.
  """

  @statuses [:allow, :deny, :conditional, :not_applicable]
  @visibilities [:enabled, :disabled, :hidden, :preview_only]

  defstruct status: :deny,
            visibility: :disabled,
            reason_code: nil,
            user_message: nil,
            audit_reason: nil,
            effects: [],
            obligations: [],
            metadata: %{}

  @type status :: :allow | :deny | :conditional | :not_applicable
  @type visibility :: :enabled | :disabled | :hidden | :preview_only

  @type t :: %__MODULE__{
          status: status(),
          visibility: visibility(),
          reason_code: atom() | String.t() | nil,
          user_message: String.t() | nil,
          audit_reason: String.t() | nil,
          effects: [term()],
          obligations: [term()],
          metadata: map()
        }

  @doc """
  Builds a decision from atom-keyed or string-keyed map/keyword attributes.
  """
  @spec new(map() | keyword()) :: t()
  def new(attrs \\ []) when is_map(attrs) or is_list(attrs) do
    attrs = Enum.into(attrs, %{})
    status = validate_status!(get_attr(attrs, :status, :deny))

    visibility =
      attrs
      |> get_attr(:visibility, default_visibility(status))
      |> validate_visibility!()

    %__MODULE__{
      status: status,
      visibility: visibility,
      reason_code: get_attr(attrs, :reason_code),
      user_message: get_attr(attrs, :user_message),
      audit_reason: get_attr(attrs, :audit_reason),
      effects: list_attr(attrs, :effects),
      obligations: list_attr(attrs, :obligations),
      metadata: map_attr(attrs, :metadata)
    }
  end

  @doc """
  Builds an allow decision.
  """
  @spec allow(atom() | String.t(), map() | keyword()) :: t()
  def allow(reason_code \\ :allowed, attrs \\ []) do
    attrs
    |> attrs_map()
    |> Map.put(:status, :allow)
    |> Map.put_new(:visibility, :enabled)
    |> Map.put(:reason_code, reason_code)
    |> new()
  end

  @doc """
  Builds a deny decision.
  """
  @spec deny(atom() | String.t(), map() | keyword()) :: t()
  def deny(reason_code \\ :denied, attrs \\ []) do
    attrs
    |> attrs_map()
    |> Map.put(:status, :deny)
    |> Map.put_new(:visibility, :disabled)
    |> Map.put(:reason_code, reason_code)
    |> new()
  end

  @doc """
  Builds a hidden deny decision.
  """
  @spec hidden(atom() | String.t(), map() | keyword()) :: t()
  def hidden(reason_code \\ :hidden, attrs \\ []) do
    attrs
    |> attrs_map()
    |> Map.put(:status, :deny)
    |> Map.put(:visibility, :hidden)
    |> Map.put(:reason_code, reason_code)
    |> new()
  end

  @doc """
  Builds a conditional preview-only decision.
  """
  @spec preview_only(atom() | String.t(), map() | keyword()) :: t()
  def preview_only(reason_code \\ :preview_only, attrs \\ []) do
    attrs
    |> attrs_map()
    |> Map.put(:status, :conditional)
    |> Map.put(:visibility, :preview_only)
    |> Map.put(:reason_code, reason_code)
    |> new()
  end

  @doc """
  Builds a not-applicable decision.
  """
  @spec not_applicable(atom() | String.t(), map() | keyword()) :: t()
  def not_applicable(reason_code \\ :not_applicable, attrs \\ []) do
    attrs
    |> attrs_map()
    |> Map.put(:status, :not_applicable)
    |> Map.put_new(:visibility, :hidden)
    |> Map.put(:reason_code, reason_code)
    |> new()
  end

  defp attrs_map(attrs) when is_map(attrs), do: attrs
  defp attrs_map(attrs) when is_list(attrs), do: Enum.into(attrs, %{})

  defp default_visibility(:allow), do: :enabled
  defp default_visibility(:deny), do: :disabled
  defp default_visibility(:conditional), do: :preview_only
  defp default_visibility(:not_applicable), do: :hidden

  defp validate_status!(status) when status in @statuses, do: status

  defp validate_status!(status) do
    raise ArgumentError,
          "unknown capability decision status #{inspect(status)}; expected one of #{inspect(@statuses)}"
  end

  defp validate_visibility!(visibility) when visibility in @visibilities, do: visibility

  defp validate_visibility!(visibility) do
    raise ArgumentError,
          "unknown capability decision visibility #{inspect(visibility)}; expected one of #{inspect(@visibilities)}"
  end

  defp list_attr(attrs, key) do
    case get_attr(attrs, key, []) do
      value when is_list(value) ->
        value

      nil ->
        []

      value ->
        raise ArgumentError,
              "capability decision #{inspect(key)} must be a list, got #{inspect(value)}"
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
              "capability decision #{inspect(key)} must be a map, got #{inspect(value)}"
    end
  end

  defp get_attr(attrs, key, default \\ nil) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> value
      :error -> Map.get(attrs, Atom.to_string(key), default)
    end
  end
end
