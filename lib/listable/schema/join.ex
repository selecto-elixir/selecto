defmodule Listable.Schema.Join do

  #listable meta join can edit, add, alter this join!

  def configure(join) do
    if function_exported?(join.i_am, :listable_meta_join, 1) do
      join.i_am.listable_meta_join(join)
    else
      join
    end
  end

end
