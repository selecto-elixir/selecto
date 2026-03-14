defmodule Selecto.Builder.Sql.Order do
  @moduledoc """
  ORDER BY clause builder for Selecto queries.

  Supports field/function selectors, explicit sort directions with NULL handling,
  and CASE-based ordering expressions. Returns join dependencies, iodata SQL
  fragments, and bind parameters for the ordering clause.
  """

  alias Selecto.AdapterSupport

  @dirs %{
    asc: "asc",
    desc: "desc",
    asc_nulls_first: "asc nulls first",
    asc_nulls_last: "asc nulls last",
    desc_nulls_first: "desc nulls first",
    desc_nulls_last: "desc nulls last"
  }

  @dir_list [
    :asc,
    :desc,
    :asc_nulls_first,
    :asc_nulls_last,
    :desc_nulls_first,
    :desc_nulls_last
  ]

  # Handle CASE expressions in ORDER BY
  def order(selecto, {:case, when_clauses, else_clause}) when is_list(when_clauses) do
    {case_iodata, joins, params} = build_order_case_expression(selecto, when_clauses, else_clause)
    {joins, case_iodata, params}
  end

  def order(selecto, {:case, when_clauses}) when is_list(when_clauses) do
    order(selecto, {:case, when_clauses, nil})
  end

  # Handle CASE with direction: {{:case, ...}, :asc}
  def order(selecto, {{:case, when_clauses, else_clause}, dir})
      when dir in @dir_list and is_list(when_clauses) do
    {case_iodata, joins, params} = build_order_case_expression(selecto, when_clauses, else_clause)
    {joins, build_order_clause(selecto, case_iodata, dir), order_params(params, dir)}
  end

  def order(selecto, {{:case, when_clauses}, dir})
      when dir in @dir_list and is_list(when_clauses) do
    order(selecto, {{:case, when_clauses, nil}, dir})
  end

  def order(selecto, {dir, order}) when dir in @dir_list do
    {c, j, p, _a} = Selecto.Builder.Sql.Select.build(selecto, order)
    {j, build_order_clause(selecto, c, dir), order_params(p, dir)}
  end

  def order(selecto, {order, dir}) when dir in @dir_list do
    # Handle {field, direction} format which is the standard in this codebase
    {c, j, p, _a} = Selecto.Builder.Sql.Select.build(selecto, order)
    {j, build_order_clause(selecto, c, dir), order_params(p, dir)}
  end

  def order(selecto, order_by) do
    order(selecto, {:asc_nulls_first, order_by})
  end

  def build(selecto) do
    {joins, clauses_iodata, params} =
      selecto.set.order_by
      |> Enum.reduce(
        {[], [], []},
        fn g, {joins, clauses, params} ->
          {j, c, p} = order(selecto, g)
          {joins ++ [j], clauses ++ [c], params ++ p}
        end
      )

    # Join clauses with ", " separator as iodata
    clause_parts = Enum.intersperse(clauses_iodata, ", ")
    {joins, clause_parts, params}
  end

  # Build CASE expression for ORDER BY clause
  defp build_order_case_expression(selecto, when_clauses, else_clause) do
    alias Selecto.Builder.Sql.Where
    # alias Selecto.Builder.Sql.Select

    # Collect joins and params from all parts
    {when_parts, all_joins, all_params} =
      when_clauses
      |> Enum.reduce({[], [], []}, fn {condition, result}, {parts_acc, joins_acc, params_acc} ->
        # Build the condition - use standard WHERE builder  
        {cond_joins, cond_iodata, cond_params} = Where.build(selecto, condition)

        # Build the result (typically a number for ordering)
        result_iodata =
          case result do
            r when is_number(r) -> [{:param, r}]
            r when is_binary(r) -> [{:param, r}]
            nil -> ["NULL"]
          end

        result_params =
          case result do
            r when is_number(r) or is_binary(r) -> [r]
            _ -> []
          end

        when_part = ["WHEN ", cond_iodata, " THEN ", result_iodata]

        {parts_acc ++ [when_part], joins_acc ++ List.wrap(cond_joins),
         params_acc ++ cond_params ++ result_params}
      end)

    # Build ELSE clause
    {else_iodata, else_params} =
      case else_clause do
        nil -> {[], []}
        e when is_number(e) -> {[" ELSE ", {:param, e}], [e]}
        e when is_binary(e) -> {[" ELSE ", {:param, e}], [e]}
      end

    case_iodata = ["CASE ", Enum.intersperse(when_parts, " ")] ++ else_iodata ++ [" END"]

    {case_iodata, all_joins, all_params ++ else_params}
  end

  defp build_order_clause(selecto, order_iodata, dir) do
    if native_null_ordering?(selecto) do
      [order_iodata, " ", @dirs[dir]]
    else
      case dir do
        :asc ->
          [order_iodata, " asc"]

        :desc ->
          [order_iodata, " desc"]

        :asc_nulls_first ->
          [order_iodata, " asc"]

        :desc_nulls_last ->
          [order_iodata, " desc"]

        :asc_nulls_last ->
          ["CASE WHEN ", order_iodata, " IS NULL THEN 1 ELSE 0 END asc, ", order_iodata, " asc"]

        :desc_nulls_first ->
          ["CASE WHEN ", order_iodata, " IS NULL THEN 0 ELSE 1 END asc, ", order_iodata, " desc"]
      end
    end
  end

  defp order_params(params, dir) when dir in [:asc_nulls_last, :desc_nulls_first],
    do: params ++ params

  defp order_params(params, _dir), do: params

  defp native_null_ordering?(selecto) do
    AdapterSupport.adapter_name(Map.get(selecto, :adapter)) == :postgresql
  end
end
