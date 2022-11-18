defmodule Selecto.Builder.Ecto.Group do
  import Ecto.Query

  import Selecto.Helpers

  # From Ecto.OLAP Copyright (c) 2017 Łukasz Jan Niemier THANKS!
  defmacro rollup(columns), do: mkquery(columns, "ROLLUP")

  defp mkquery(data, name) do
    quote do: fragment(unquote(name <> " ?"), unquote(fragment_list(data)))
  end

  defp fragment_list(list) when is_list(list) do
    query = "?" |> List.duplicate(Enum.count(list)) |> Enum.join(",")
    quote do: fragment(unquote("(" <> query <> ")"), unquote_splicing(list))
  end

  ###

  defp recurse_group_by(config, group_by) do
    ## Todo make these use 1, 2, 3 etc when possible

    case group_by do
      {:extract, field, format} ->
        check_string(format)

        dynamic(
          [{^config.columns[field].requires_join, owner}],
          fragment(
            "extract( ? from ? )",
            literal(^format),
            field(owner, ^config.columns[field].field)
          )
        )

      ### how to dedupe?!?!
      {:rollup, [a]} ->
        dynamic([], rollup([^recurse_group_by(config, a)]))

      {:rollup, [a, b]} ->
        dynamic([], rollup([^recurse_group_by(config, a), ^recurse_group_by(config, b)]))

      {:rollup, [a, b, c]} ->
        dynamic(
          [],
          rollup([
            ^recurse_group_by(config, a),
            ^recurse_group_by(config, b),
            ^recurse_group_by(config, c)
          ])
        )

      {:rollup, [a, b, c, d]} ->
        dynamic(
          [],
          rollup([
            ^recurse_group_by(config, a),
            ^recurse_group_by(config, b),
            ^recurse_group_by(config, c),
            ^recurse_group_by(config, d)
          ])
        )

      field ->
        dynamic(
          [{^config.columns[field].requires_join, owner}],
          field(owner, ^config.columns[field].field)
        )
    end
  end

  def apply_group_by(query, _config, []) do
    query
  end

  def apply_group_by(query, config, group_bys) do
    group_bys = group_bys |> Enum.map(fn g -> recurse_group_by(config, g) end)

    from(query, group_by: ^group_bys)
  end
end
