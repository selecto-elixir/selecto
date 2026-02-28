defmodule Selecto.SQL.Formatter do
  @moduledoc """
  Lightweight SQL formatting and highlighting helpers.

  This module is intentionally conservative: it improves readability for generated
  Selecto SQL without attempting to be a full SQL parser.
  """

  @default_indent "  "

  @main_clauses [
    "WITH RECURSIVE",
    "WITH",
    "SELECT",
    "FROM",
    "WHERE",
    "GROUP BY",
    "HAVING",
    "ORDER BY",
    "LIMIT",
    "OFFSET",
    "UNION ALL",
    "UNION",
    "INTERSECT",
    "EXCEPT"
  ]

  @join_clauses [
    "LEFT JOIN",
    "RIGHT JOIN",
    "INNER JOIN",
    "FULL JOIN",
    "CROSS JOIN",
    "JOIN"
  ]

  @doc """
  Format SQL for readability.

  Options:
  - `:indent` string used for indentation (default: two spaces)
  - `:where_multiline` boolean to split `AND`/`OR` conditions across lines (default: true)
  """
  @spec format(String.t(), keyword()) :: String.t()
  def format(sql, opts \\ []) when is_binary(sql) do
    indent = Keyword.get(opts, :indent, @default_indent)
    where_multiline? = Keyword.get(opts, :where_multiline, true)

    sql
    |> String.replace(~r/\s+/, " ")
    |> String.trim()
    |> uppercase_leading_clause()
    |> break_main_clauses()
    |> break_joins(indent)
    |> format_select_list(indent)
    |> format_where(where_multiline?, indent)
    |> String.trim()
  end

  defp uppercase_leading_clause(sql) do
    cond do
      Regex.match?(~r/\Aselect\b/i, sql) ->
        Regex.replace(~r/\Aselect\b/i, sql, "SELECT")

      Regex.match?(~r/\Awith recursive\b/i, sql) ->
        Regex.replace(~r/\Awith recursive\b/i, sql, "WITH RECURSIVE")

      Regex.match?(~r/\Awith\b/i, sql) ->
        Regex.replace(~r/\Awith\b/i, sql, "WITH")

      true ->
        sql
    end
  end

  @doc """
  Apply lightweight SQL highlighting.

  Supported styles:
  - `:ansi` - terminal ANSI colors
  - `:markdown` - markdown emphasis for keywords
  """
  @spec highlight(String.t(), :ansi | :markdown | nil) :: String.t()
  def highlight(sql, nil), do: sql
  def highlight(sql, false), do: sql

  def highlight(sql, :ansi) when is_binary(sql) do
    keyword_wrapped =
      Enum.reduce(@main_clauses ++ @join_clauses, sql, fn kw, acc ->
        Regex.replace(
          ~r/\b#{Regex.escape(kw)}\b/i,
          acc,
          IO.ANSI.bright() <> String.upcase(kw) <> IO.ANSI.reset()
        )
      end)

    Regex.replace(~r/\$\d+/, keyword_wrapped, fn param ->
      IO.ANSI.cyan() <> param <> IO.ANSI.reset()
    end)
  end

  def highlight(sql, :markdown) when is_binary(sql) do
    keyword_wrapped =
      Enum.reduce(@main_clauses ++ @join_clauses, sql, fn kw, acc ->
        Regex.replace(~r/\b#{Regex.escape(kw)}\b/i, acc, "**#{String.upcase(kw)}**")
      end)

    Regex.replace(~r/\$\d+/, keyword_wrapped, "<code style=\"color:#0b6\">\\0</code>")
  end

  def highlight(sql, _unknown_style), do: sql

  defp break_main_clauses(sql) do
    Enum.reduce(@main_clauses, sql, fn clause, acc ->
      Regex.replace(
        ~r/\s+#{Regex.escape(clause)}\s+/i,
        acc,
        "\n#{String.upcase(clause)} "
      )
    end)
  end

  defp break_joins(sql, indent) do
    Enum.reduce(@join_clauses, sql, fn clause, acc ->
      Regex.replace(
        ~r/\s+#{Regex.escape(clause)}\s+/i,
        acc,
        "\n#{indent}#{String.upcase(clause)} "
      )
    end)
  end

  defp format_select_list(sql, indent) do
    case Regex.run(~r/\A(.*\bSELECT)\s+(.*)\s+\bFROM\b(.*)\z/si, sql, capture: :all_but_first) do
      [prefix, select_part, from_rest] ->
        selectors =
          select_part
          |> split_top_level(",")
          |> Enum.map(&String.trim/1)
          |> Enum.reject(&(&1 == ""))

        formatted_select =
          selectors
          |> Enum.with_index()
          |> Enum.map_join(",\n", fn {item, idx} ->
            head = if idx == 0, do: "#{indent}", else: "#{indent}"
            head <> item
          end)

        "#{prefix}\n#{formatted_select}\nFROM#{from_rest}"

      _ ->
        sql
    end
  end

  defp format_where(sql, false, _indent), do: sql

  defp format_where(sql, true, indent) do
    sql
    |> then(fn text -> Regex.replace(~r/\bWHERE\b\s+/i, text, "WHERE\n#{indent}") end)
    |> then(fn text -> Regex.replace(~r/\s+(AND|OR)\s+/i, text, "\n#{indent}\\1 ") end)
  end

  defp split_top_level(binary, delimiter) when is_binary(binary) and is_binary(delimiter) do
    chars = String.graphemes(binary)
    delimiter_chars = String.graphemes(delimiter)
    delimiter_len = length(delimiter_chars)

    do_split(chars, delimiter_chars, delimiter_len, 0, false, false, "", [])
    |> Enum.reverse()
  end

  defp do_split([], _delim, _delim_len, _depth, _in_single, _in_double, current, acc) do
    [current | acc]
  end

  defp do_split(chars, delim, delim_len, depth, in_single, in_double, current, acc) do
    [char | rest] = chars

    cond do
      char == "'" and not in_double ->
        do_split(rest, delim, delim_len, depth, not in_single, in_double, current <> char, acc)

      char == "\"" and not in_single ->
        do_split(rest, delim, delim_len, depth, in_single, not in_double, current <> char, acc)

      in_single or in_double ->
        do_split(rest, delim, delim_len, depth, in_single, in_double, current <> char, acc)

      char == "(" ->
        do_split(rest, delim, delim_len, depth + 1, in_single, in_double, current <> char, acc)

      char == ")" and depth > 0 ->
        do_split(rest, delim, delim_len, depth - 1, in_single, in_double, current <> char, acc)

      depth == 0 and starts_with_chars?(chars, delim, delim_len) ->
        remaining = Enum.drop(chars, delim_len)
        do_split(remaining, delim, delim_len, depth, in_single, in_double, "", [current | acc])

      true ->
        do_split(rest, delim, delim_len, depth, in_single, in_double, current <> char, acc)
    end
  end

  defp starts_with_chars?(chars, delim, delim_len) do
    chars
    |> Enum.take(delim_len)
    |> Kernel.==(delim)
  end
end
