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

  @doc """
  Builds a capability request.
  """
  @spec request(map() | keyword()) :: Request.t()
  def request(attrs), do: Request.new(attrs)

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
end
