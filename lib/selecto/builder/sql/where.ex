defmodule Selecto.Builder.Sql.Where do
  import Selecto.Builder.Sql.Helpers

  alias Selecto.Builder.Sql.Select

  @doc """
      {SELECTOR} # for boolean fields
      {SELECTOR, nil} #is null
      {SELECTOR, :not_nil} #is not null
      {SELECTOR, SELECTOR} #=
      {SELECTOR, [SELECTOR2, ...]}# in ()
      {SELECTOR, {comp, SELECTOR2}} #<= etc
      {SELECTOR, {:between, SELECTOR2, SELECTOR2}
      {:not, PREDICATE}
      {:and, [PREDICATES]}
      {:or, [PREDICATES]}
      {SELECTOR, :in, SUBQUERY}
      {SELECTOR, comp, {:subquery, :any, SUBQUERY}}  ## Or :all
      {:exists, SUBQUERY}
  """

  def build(selecto, {field, {:text_search, value}}) do
    conf = Selecto.field(selecto, field)
    ### Don't think we ever have to cook the field because it has to be the tsvector...
    {conf.requires_join, [" ", build_selector_string(selecto, conf.requires_join, conf.field), " @@ websearch_to_tsquery(", {:param, value}, ") "], []}
  end

  def build(selecto, {field, {:subquery, :in, query, params}}) do
    conf = Selecto.field(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(conf.requires_join) ++ List.wrap(join), [" ", sel, " in ", query, " "], param ++ params}
  end

  def build(selecto, {field, {:subquery, :in, query}}) do
    conf = Selecto.field(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(conf.requires_join) ++ List.wrap(join), [" ", sel, " in ", query, " "], param}
  end

  def build(selecto, {field, comp, {:subquery, agg, query, params}}) when agg in [:any, :all] do
    conf = Selecto.field(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(conf.requires_join) ++ List.wrap(join), [" ", sel, " ", comp, " ", to_string(agg), " (", query, ") "], param ++ params}
  end

  def build(selecto, {field, comp, {:subquery, agg, query}}) when agg in [:any, :all] do
    conf = Selecto.field(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(conf.requires_join) ++ List.wrap(join), [" ", sel, " ", comp, " ", to_string(agg), " (", query, ") "], param}
  end

  def build(_selecto, {:exists, query, params}) do
    {[], [" exists (", query, ") "], params}
  end

  def build(_selecto, {:exists, query}) do
    {[], [" exists (", query, ") "], []}
  end

  def build(selecto, {:not, filter}) do
    {j, c, p} = build(selecto, filter)
    {j, ["not ( ", c, " ) "], p}
  end

  def build(selecto, {conj, filters}) when conj in [:and, :or] do
    # Handle empty filter list - return empty result to avoid empty WHERE clauses
    if Enum.empty?(filters) do
      {[], [], []}
    else
      {joins, clauses, params} =
        Enum.reduce(filters, {[], [], []}, fn
          f, {joins, clauses, params} ->
            {j, c, p} = build(selecto, f)
            {joins ++ [j], clauses ++ [c], params ++ p}
        end)

      clause_parts = Enum.map(clauses, fn c -> ["(", c, ")"] end)
      conj_str = " #{conj} "
      final_clause = [
        "(",
        Enum.intersperse(clause_parts, conj_str),
        ")"
      ]

      {joins, final_clause, params}
    end
  end

  def build(selecto, {field, {:between, min, max}}) do
    conf = Selecto.field(selecto, field)

    {conf.requires_join,
     [" ", build_selector_string(selecto, conf.requires_join, conf.field), " between ", {:param, to_type(conf.type, min)}, " and ", {:param, to_type(conf.type, max)}, " "],
     []}
  end

  def build(selecto, {field, {comp, value}}) when comp in [:like, :ilike] do
    # ### Value must have a % in it to work!
    # ### TODO sanitize like value!
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(join), [" ", sel, " ", to_string(comp), " ", {:param, value}, " "], param}

  end

  def build(selecto, {field, {comp, value}}) when comp in ~w[= != < > <= >=] do
    conf = Selecto.field(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)

    {List.wrap(conf.requires_join) ++ List.wrap(join), [" ", sel, " ", comp, " ", {:param, to_type(conf.type, value)}, " "],
     param}
  end

  def build(selecto, {field, list}) when is_list(list) do
    conf = Selecto.field(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)

    {List.wrap(conf.requires_join) ++ List.wrap(join), [" ", sel, " = ANY(", {:param, Enum.map(list, fn i -> to_type(conf.type, i) end)}, ") "],
     param}
  end

  def build(selecto, {field, :not_null}) do
    conf = Selecto.field(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(conf.requires_join) ++ List.wrap(join), [" ", sel, " is not null "], param}
  end

  def build(selecto, {field, value}) when is_nil(value) do
    conf = Selecto.field(selecto, field)
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(conf.requires_join) ++ List.wrap(join), [" ", sel, " is null "], param}
  end

  def build(selecto, {field, value}) do
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(join), [" ", sel, " = ", {:param, value}, " "], param}
  end

  def build(_sel, other) do
    IO.inspect(other, label: "Where clause not handled")
    raise "Not Found"
  end

  defp to_type(:id, value) when is_integer(value) do
    value
  end

  defp to_type(:id, value) when is_bitstring(value) do
    String.to_integer(value)
  end

  defp to_type(:integer, value) when is_integer(value) do
    value
  end

  defp to_type(:integer, value) when is_bitstring(value) do
    String.to_integer(value)
  end

  defp to_type(_t, val) do
    val
  end
end
