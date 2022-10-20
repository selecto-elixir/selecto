defmodule Listable.Schema.Column do
  # Configure columns - move to column
  def configure_columns(join, fields, source, domain) do
    IO.puts("HRE")
    fields
    |> IO.inspect()
    |> Enum.map(&configure(&1, join, source, domain))
    |> IO.inspect()
    |> Map.new()
  end

  def configure(field, join, source, domain) do
    IO.puts("HERE #{field} #{join}")
    colid =
      case join do
        :listable_root -> Atom.to_string(field)
        _ -> "#{Atom.to_string(join)}[#{Atom.to_string(field)}]"
      end

    config = get_in(domain, [:columns, field])

    name =
      if config do
        config.name
      else
        field
      end

    col = {
      colid,
      %{
        colid: colid,
        field: field,
        name:
          if :listable_root == join do
            "#{domain.name}: #{name}"
          else
            "#{domain.name}: #{name}"
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
