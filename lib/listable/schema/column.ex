defmodule Listable.Schema.Column do

  def configure(field, join, source) do
    colid = case join do
      :listable_root -> Atom.to_string(field)
      _   -> "#{Atom.to_string(join)}[#{Atom.to_string(field)}]"
    end

    {
      colid,
      %{
        colid: colid,
        field: field,
        type: source.__schema__(:type, field),
        meta: if function_exported?(source, :listable_meta, 1) do source.listable_meta(field) else %{} end,
        requires_join: join
      }
    }
  end



end
