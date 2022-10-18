defmodule Listable.Schema.Column do
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
