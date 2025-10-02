defmodule Selecto.Schema.Column do
  # Configure columns - move to column
  def configure_columns(join, fields, source, domain) do
    columns =
      fields
      |> Enum.map(&configure(&1, join, source, domain))

    custom_columns = get_custom_columns(join, source, domain)

    (columns ++ custom_columns) |> Map.new()

  end

  ### how to do custom columns?
  def get_custom_columns(join, _source, domain) do
    Map.get(domain, :custom_columns, %{})
    |> Enum.reduce([], fn {f, v}, acc ->
      [
        {
          f,
          Map.merge(
            v,
            %{
              colid: f,
              requires_join: join
            }
          ) |> Map.put_new(:type, :custom_column)
        }
        | acc
      ]
    end)
  end

  defp add_filter_type(col, %{filter_type: ft}) do
    Map.put(col, :filter_type, ft)
  end

  defp add_filter_type(col, _config) do
    col
  end

  defp add_option_provider(col, %{option_provider: provider}) do
    Map.merge(col, %{
      type: :select_options,
      option_provider: provider,
      multiple: Map.get(col, :multiple, false),
      searchable: Map.get(col, :searchable, true)
    })
  end

  defp add_option_provider(col, _config) do
    col
  end

  def configure(field, join, source, domain) do
    # Convert field to string - handle both atoms and strings for Postgrex compatibility
    field_str = if is_atom(field), do: Atom.to_string(field), else: field
    # Try to get atom version if field is string (for map lookup)
    field_atom = if is_atom(field) do
      field
    else
      try do
        String.to_existing_atom(field)
      rescue
        ArgumentError -> nil
      end
    end

    # Try both string and atom keys for column config lookup
    columns_map = Map.get(domain, :columns, %{})
    config = if field_atom do
      Map.get(columns_map, field_atom, Map.get(columns_map, field_str, %{}))
    else
      Map.get(columns_map, field_str, %{})
    end

    colid =
      Map.get(
        config,
        :id,
        case join do
          :selecto_root -> field_str
          _ ->
            # Handle both atom and string join names for Postgrex compatibility
            join_str = if is_atom(join), do: Atom.to_string(join), else: join
            "#{join_str}.#{field_str}"
        end
      )

    name = Map.get(config, :name, humanize(field))

    # Add appropriate prefix based on join type
    display_name = case join do
      :selecto_root ->
        # For root table, use the domain name from the configuration
        "#{Map.get(domain, :name, "Domain")}: #{name}"
      _ ->
        # For joined tables, use the join name
        join_info = Map.get(Map.get(domain, :joins, %{}), join, %{})
        join_name = Map.get(join_info, :name, humanize(join))
        "#{join_name}: #{name}"
    end

    # Get column type from source, handling both atom and string field keys
    source_col = if field_atom do
      Map.get(source.columns, field_atom, Map.get(source.columns, field_str))
    else
      Map.get(source.columns, field_str)
    end

    base_col = %{
      colid: colid,
      field: field,
      name: display_name,
      type: source_col.type,
      requires_join: join,
      format: Map.get(config, :format)
    }

    {
      colid,
      base_col
      |> add_filter_type(config)
      |> add_option_provider(config)
    }
  end

  # stolen from phoenix :D
  def humanize(atom) when is_atom(atom),
    do: humanize(Atom.to_string(atom))

  def humanize(bin) when is_binary(bin) do
    bin |> String.replace("_", " ") |> String.capitalize()
  end
end
