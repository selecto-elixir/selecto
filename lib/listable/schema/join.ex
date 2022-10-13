defmodule Listable.Schema.Join do


  def configure(assoc, join, source) do
    meta = if function_exported?(source, :listable_meta_join, 1) do source.listable_meta_join(assoc) else %{} end

    %{


      join: join,
      meta: meta
    }
  end

end
