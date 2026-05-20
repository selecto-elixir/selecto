defmodule Selecto.Capabilities.Resolver do
  @moduledoc """
  Behaviour for host-owned capability policy resolvers.

  Selecto owns the request and decision data shapes, but host applications own
  authorization truth. A resolver may implement only `c:decide/2`; callers can
  still use the shared batch helper, which falls back to calling `c:decide/2`
  for each request. Resolvers that can answer more efficiently may also
  implement `c:decide_many/2`.
  """

  alias Selecto.Capabilities.{Decision, Request}

  @type context :: map()
  @type decision_result :: Decision.t() | {:ok, Decision.t()} | {:error, term()}

  @callback decide(Request.t(), context()) :: decision_result()
  @callback decide_many([Request.t()], context()) ::
              [Decision.t()] | {:ok, [Decision.t()]} | {:error, term()}

  @optional_callbacks decide_many: 2
end
