defmodule Selecto.Builder.Ecto.Select do

  import Ecto.Query
  import Selecto.Helpers

  ## need more? upper, lower, ???, postgres specifics?
  defp apply_selection({query, aliases}, config, {:subquery, {:dyn, as, dynamic}}) do
    dyn = %{
      as => dynamic
    }
    query = from(a in query, select_merge: ^dyn)
    {query, [as | aliases]}
  end


  defp apply_selection({query, aliases}, config, {:subquery, func, field}) do
    conf = config.columns[field]

    join = config.joins[conf.requires_join]
    my_func = check_string( Atom.to_string(func) )
    my_key = Atom.to_string(join.my_key)
    my_field = Atom.to_string(conf.field)



    # from a in SelectoTest.Test.SolarSystem, select: {fragment("(select json_agg(planets) from planets where solar_system_id = ?)", a.id)}
    # from a in SelectoTest.Test.SolarSystem, select: {fragment("(select count(id) from planets where solar_system_id = ?)", a.id)}
    as = "#{func}(#{field})"

    dyn = %{
      as =>
        dynamic(
          [{^join.requires_join, par}],
          fragment(
            "(select ?(?) from ? where ? = ?)",
            literal(^my_func),
            literal(^my_field),
            literal(^join.source),
            literal(^my_key),
            par.id
          )
        )
    }

    query = from(a in query, select_merge: ^dyn)
    {query, [as | aliases]}
  end

  # ARRAY - auto gen array from otherwise denorm'ing selects using postgres 'array' func
  # ---- eg {"array", "item_orders", select: ["item[name]", "item_orders[quantity]"], filters: [{item[type], "Pin"}]}
  # ---- postgres has functions to put those into json!
  # to select the items into an array and apply the filter to the subq. Would ahve to be something that COULD join
  # to one of the main query joins
  #TODOs
  # defp apply_selection({query, aliases}, _config, {:array, _field, _selects}) do
  #   {query, aliases}
  # end

  # # COALESCE ... ??
  # defp apply_selection({query, aliases}, _config, {:coalesce, _field, _selects}) do
  #   {query, aliases}
  # end

  # # CASE ... {:case, %{{...filter...}}=>val, cond2=>val, :else=>val}}
  # defp apply_selection({query, aliases}, _config, {:case, _field, _case_map}) do
  #   {query, aliases}
  # end

  defp apply_selection({query, aliases}, config, {:extract, field, format}) do
    conf = config.columns[field]
    as = "#{format} from #{field}"

    check_string(format)

    query =
      from({^conf.requires_join, owner} in query,
        select_merge: %{
          ^"#{as}" => fragment("extract(? from ?)", literal(^format), field(owner, ^conf.field))
        }
      )

    {query, [as | aliases]}
  end

  defp apply_selection({query, aliases}, config, {:to_char, {field, format}, as}) do
    conf = config.columns[field]

    query =
      from({^conf.requires_join, owner} in query,
        select_merge: %{
          ^"#{as}" => fragment("to_char(?, ?)", field(owner, ^conf.field), ^format)
        }
      )

    {query, [as | aliases]}
  end

  ## Todo why this does not work with numbers?
  defp apply_selection({query, aliases}, _config, {:literal, name, value}) do
    query = from({:selecto_root, owner} in query, select_merge: %{^name => ^value})
    {query, [name | aliases]}
  end

  ### works with any func/agg of normal form
  defp apply_selection({query, aliases}, config, {func, field}) when is_atom(func) do
    use_as = "#{func}(#{field})"
    apply_selection({query, aliases}, config, {func, field, use_as})
  end

  ## Case of literal value arg
  defp apply_selection({query, aliases}, _config, {func, {:literal, field}, as})
       when is_atom(func) do
    func = Atom.to_string(func) |> check_string()

    query =
      from(query,
        select_merge: %{
          ^"#{as}" => fragment("?(?)", literal(^func), ^field)
        }
      )

    {query, [as | aliases]}
  end

  # Case for func call with field as arg
  ## Check for SQL INJ TODO
  ## TODO allow for func call args
  ## TODO variant for 2 arg aggs eg string_agg, jsonb_object_agg, Grouping
  ## ^^ and mixed lit/field args - field as list?

  defp apply_selection({query, aliases}, config, {func, field, as}) when is_atom(func) do
    conf = config.columns[field]
    func = Atom.to_string(func) |> check_string()

    query =
      from({^conf.requires_join, owner} in query,
        select_merge: %{
          ^"#{as}" => fragment("?(?)", literal(^func), field(owner, ^conf.field))
        }
      )

    {query, [as | aliases]}
  end

  # Case of 'count(*)' which we can just ref as count
  defp apply_selection({query, aliases}, _config, {:count}) do
    query = from(query, select_merge: %{"count" => fragment("count(*)")})
    {query, ["count" | aliases]}
  end

  # case of other non-arg funcs eg now()
  defp apply_selection({query, aliases}, _config, {func}) when is_atom(func) do
    func = Atom.to_string(func) |> check_string()
    from(query, select_merge: %{^func => fragment("?()", literal(^func))})
    {query, [func | aliases]}
  end

  ### regular old fields. Allow atoms?
  defp apply_selection({query, aliases}, config, field) when is_binary(field) do
    conf = config.columns[field]

    query =
      from({^conf.requires_join, owner} in query,
        select_merge: %{^field => field(owner, ^conf.field)}
      )

    {query, [field | aliases]}
  end

  ### applies the selections to the query
  def apply_selections(query, config, selected) do
    {query, aliases} =
      selected
      |> Enum.reduce({query, []}, fn s, acc ->
        apply_selection(acc, config, s)
      end)

    {query, Enum.reverse(aliases)}
  end



  defp apply_order_by(query, config, order_bys) do
    order_bys =
      order_bys
      |> Enum.map(fn
        {dir, field} -> {dir, field}
        field -> {:asc_nulls_first, field}
      end)
      |> Enum.map(fn
        {dir, field} ->
          {dir,
           dynamic(
             [{^config.columns[field].requires_join, owner}],
             field(owner, ^config.columns[field].field)
           )}
      end)

    from(query,
      order_by: ^order_bys
    )
  end




end
