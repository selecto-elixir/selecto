defmodule Listable.Schema.Column do

  # Configure columns - move to column
  def configure_columns(join, fields, source) do
    fields
    |> Enum.map(&configure(&1, join, source))
    |> Map.new()
  end

  def configure(field, join, source) do
    colid =
      case join do
        :listable_root -> Atom.to_string(field)
        _ -> "#{Atom.to_string(join)}[#{Atom.to_string(field)}]"
      end

    col = {
      colid,
      %{
        colid: colid,
        field: field,
        name:
          if :listable_root == join do
            field
          else
            "#{join}: #{field}"
          end,
        type: source.__schema__(:type, field),
        requires_join: join
      }
    }

    if function_exported?(source, :listable_meta, 1) do
      source.listable_meta(col)
    else
      col
    end
  end
end
