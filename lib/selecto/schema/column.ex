defmodule Selecto.Schema.Column do
  # Configure columns - move to column
  def configure_columns(join, fields, source, domain) do
    fields
    |> Enum.map(&configure(&1, join, source, domain))
    |> Map.new()
  end

  def configure(field, join, source, domain) do
    config = Map.get(Map.get(domain, :columns, %{}), field, %{})

    colid =
      Map.get(
        config,
        :id,
        case join do
          :selecto_root -> Atom.to_string(field)
          _ -> "#{Atom.to_string(join)}[#{Atom.to_string(field)}]"
        end
      )

    name = Map.get(config, :name, field)

    col = {
      colid,
      %{
        colid: colid,
        field: field,
        name:
          if :selecto_root == join do
            "#{domain.name}: #{name}"
          else
            "#{domain.name}: #{name}"
          end,
        type: source.__schema__(:type, field),
        requires_join: join,
        format: Map.get(config, :format)
      }
    }

    if function_exported?(source, :selecto_meta, 1) do
      source.selecto_meta(col)
    else
      col
    end
  end
end
