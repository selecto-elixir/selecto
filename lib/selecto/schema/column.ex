defmodule Selecto.Schema.Column do
  # Configure columns - move to column
  def configure_columns(join, fields, source, domain) do
    columns = fields
    |> Enum.map(&configure(&1, join, source, domain))

    custom_columns = get_custom_columns(join, source, domain)

    columns ++ custom_columns |> Map.new()
  end

  ### how to do custom columns?
  def get_custom_columns(join, source, domain) do
    ### TODO
    Map.get(domain, :custom_columns, %{})
    |> Enum.reduce([], fn {f, v}, acc ->
      [{
        f,
        Map.merge(
          v,
          %{
            colid: f,
            type: :custom_column,
            requires_join: join
          }
        )
      } | acc]

    end)
  end

  defp add_filter_type(col, %{filter_type: ft} = config) do
    Map.put(col, :filter_type, ft)
  end
  defp add_filter_type(col, _config) do
    col
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

      add_filter_type(
        %{
          colid: colid,
          field: field,
          name: "#{domain.name}: #{name}",
          type: source.__schema__(:type, field),
          requires_join: join,
          format: Map.get(config, :format)
        },
        config
      )

    }

  end
end
