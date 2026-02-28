defmodule Selecto.Performance.HookBehaviour do
  @moduledoc """
  Behaviour for implementing custom performance monitoring hooks.

  Modules implementing this behaviour can be used as hook providers
  for the Selecto performance monitoring system.
  """

  @type context :: map()
  @type hook_point :: atom()
  @type hook_fn :: (context -> context)

  @doc """
  Initialize the hook provider.

  Called once when the hook provider is registered.
  Should return {:ok, state} or {:error, reason}.
  """
  @callback init(opts :: keyword()) :: {:ok, term()} | {:error, term()}

  @doc """
  Handle a hook point execution.

  Receives the hook point name, current context, and provider state.
  Should return an updated context.
  """
  @callback handle_hook(hook_point, context, state :: term()) :: context

  @doc """
  Clean up resources when the hook provider is unregistered.
  """
  @callback terminate(reason :: term(), state :: term()) :: :ok

  @optional_callbacks terminate: 2
end
