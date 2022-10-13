defmodule Listable.Schema.Column do

  def configure(field, join, source) do
    colid = case join do
      nil -> field
      _   -> "#{Atom.to_charlist(join)}[#{Atom.to_charlist(field)}]"
    end

    {
      colid,
      %{
        field: field,
        type: source.__schema__(:type, field),
        meta: if function_exported?(source, :listable_meta, 1) do source.listable_meta(field) else %{} end,
        requires_join: join
      }
    }
  end



end
