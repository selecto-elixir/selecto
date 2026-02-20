defmodule Selecto.Schema do
  @moduledoc """
  Namespace module for schema processing helpers.

  Selecto uses schema helpers to normalize domain metadata into structures the
  SQL builders can consume. Concrete behavior lives in:

  - `Selecto.Schema.Join`
  - `Selecto.Schema.Column`
  - `Selecto.Schema.ParameterizedJoin`
  """
end
