defmodule Selecto.Builder.Sql.Where do
  import Selecto.Helpers

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
      {SELECTOR, comp, {:all, SUBQUERY}} ## any, all, ???
      {:exists, SUBQUERY}

      SUBQUERY:
      {:subquery, query, params}
      %Selecto{} ##


  """

  def build(selecto, {field, {:text_search, value}}) do
    conf = selecto.config.columns[field]
    ### Don't think we ever have to cook the field because it has to be the tsvector...
    {conf.requires_join, " #{double_wrap(conf.requires_join)}.#{double_wrap(conf.field)} @@ websearch_to_tsquery(^SelectoParam^) ", [value]}
  end

### Subqueries ?? how to do correlated?
  ### EG field > any (subq)
  def build(selecto, {field, comp, {reducer, {:subquery, query, params}}}) when reducer in [:any, :all] and comp in ~w(= < > <= >= <>) do
    conf = selecto.config.columns[field]
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(conf.requires_join) ++ List.wrap(join), " #{sel} #{comp} #{reducer} (#{query}) ", param ++ params}
  end

  def build(selecto, {field, {:in, {:subquery, query, params}}}) do
    conf = selecto.config.columns[field]
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(conf.requires_join) ++ List.wrap(join), " #{sel} in #{query} ", param ++ params}
  end

  def build(selecto, {:exists, {:subquery, query, params}}) do
    {[], "exists(#{query})", params}
  end

### Selecto Subqueries - need to be able to tell selecto to not use selecto_root etc.
# main = Selecto.configure(SelectoTest.Repo, domain)
# subq = Selecto.configure(SelectoTest.Repo, domain)
# |> Selecto.filter({"actor_id", {:parent_selecto, main, "actor_id"}})
# |> Selecto.select("actor_id")
# main
# |> Selecto.filter({:exists,  subq })
# |> Selecto.select({:concat, ["first_name", {:literal, " "}, "last_name"]})
# |> Selecto.execute()
# def build(selecto, {:exists, %Selecto{} = subselecto}) do
#   {query, aliases, params} = Selecto.gen_sql(subselecto, %{ subquery: true })
#   {[], "(#{query})", params}

# end


  def build(selecto, {:not, filter}) do
    {j, c, p} = build(selecto, filter)
    {j, "not ( #{c} ) ", p}
  end

  def build(selecto, {conj, filters}) when conj in [:and, :or] do
    {joins, clauses, params} =
      Enum.reduce(filters, {[], [], []}, fn
        f, {joins, clauses, params} ->
          {j, c, p} = build(selecto, f)
          {joins ++ [j], clauses ++ [c], params ++ p}
      end)

    {joins, "(#{Enum.join(Enum.map(clauses, fn c -> "(#{c})" end), " #{conj} ")})", params}
    # Joins, clause, params
  end

  def build(selecto, {field, {:between, min, max}}) do
    conf = selecto.config.columns[field]

    {conf.requires_join,
     " #{double_wrap(conf.requires_join)}.#{double_wrap(conf.field)} between ^SelectoParam^ and ^SelectoParam^ ",
     [to_type(conf.type, min), to_type(conf.type, max)]}
  end

  def build(selecto, {field, {comp, value}}) when comp in [:like, :ilike] do
    # ### Value must have a % in it to work!
    # ### TODO sanitize like value!
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(join), " #{sel} #{comp} ^SelectoParam^ ", param ++ [ value ]}

  end

  def build(selecto, {field, {comp, value}}) when comp in ~w[= != < > <= >=] do
    conf = selecto.config.columns[field]
    {sel, join, param} = Select.prep_selector(selecto, field)

    {List.wrap(conf.requires_join) ++ List.wrap(join), " #{sel} #{comp} ^SelectoParam^ ",
     param ++ [to_type(conf.type, value)]}
  end

  def build(selecto, {field, list}) when is_list(list) do
    conf = selecto.config.columns[field]
    {sel, join, param} = Select.prep_selector(selecto, field)

    {List.wrap(conf.requires_join) ++ List.wrap(join), " #{sel} = ANY(^SelectoParam^) ",
     param ++ [Enum.map(list, fn i -> to_type(conf.type, i) end)]}
  end

  def build(selecto, {field, :not_null}) do
    conf = selecto.config.columns[field]
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(conf.requires_join) ++ List.wrap(join), " #{sel} is not null ", param}
  end

  def build(selecto, {field, value}) when is_nil(value) do
    conf = selecto.config.columns[field]
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(conf.requires_join) ++ List.wrap(join), " #{sel} is null ", param}
  end

  def build(selecto, {field, value}) do
    {sel, join, param} = Select.prep_selector(selecto, field)
    {List.wrap(join), " #{sel} = ^SelectoParam^ ", param ++ [ value ]}
  end

  def build(_sel, other) do
    IO.inspect(other)
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
