defmodule Listable.Schema.Column do

  def configure(field, join, source) do
    {
      "#{Atom.to_charlist(join)}.#{Atom.to_charlist(field)}",
      %{
        type: source.__schema__(:type, field)
      }
    }
  end



end
