defmodule Selecto.Domain.Contract.Shared.FieldReference do
  @moduledoc false

  alias Selecto.Domain.Contract.Shared.Core

  def validate_field_reference(errors, field, path, field_index) do
    if Core.known_field?(field_index, field) do
      errors
    else
      [
        Core.error(
          :filter_field_not_found,
          path,
          "filter field #{inspect(field)} is not defined in source, schemas, or custom columns",
          field: field
        )
        | errors
      ]
    end
  end
end
