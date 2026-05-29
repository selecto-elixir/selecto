defmodule Selecto.Domain.Contract.Shared.IdValue do
  @moduledoc false

  alias Selecto.Domain.Contract.Shared.Core
  def validate_id_value(errors, nil, _path, _code, _message, _attrs), do: errors

  def validate_id_value(errors, value, _path, _code, _message, _attrs)
      when is_atom(value) or is_binary(value),
      do: errors

  def validate_id_value(errors, value, path, code, message, attrs) do
    [
      Core.error(
        code,
        path,
        message,
        Keyword.put(attrs, :actual, Core.value_type(value))
      )
      | errors
    ]
  end
end
