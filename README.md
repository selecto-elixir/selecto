# Selecto

A Query and Report Writing system

This is very young software and might spill milk in your computer.

Better documentation is planned once the API is finalized.

Selecto is configured by passing in a 'domain' which tells it which 
table to start at, which tables it can join (assocs are supported now, 
but I will be adding support for non-assoc & parameterized joins), 
what columns are available (currently it's the columns from the schema, but guess what),
and what filters are available (currently its generated from the list of
columns, but that will also be expanded to custom filters).

For now, see [selecto_test](https://github.com/seeken/selecto_test) for some examples of domains.

```elixir
selecto = Selecto.configure( YourApp.Repo,  %{} = domain )
```

The domain is a map, and contains:

- source: (req) This is the starting point, the table that will always be included in the query, as the module name, Eg YourApp.Accounts.Users
- columns: A map of definitions and metadata for schema columns and ad hoc columns
- filters: A map of ad hoc filters. But it does not work yet. 
- joins: A map containing assoc names (the atom!) which can also recursively contain joins, columns, filters, and name
- required_filters: This is a list of filters that will always be applied to the query. This is where you'd put a filter telling Selecto to restrict results, such as if you have fk based multi-tenant or want to build queryies restricted to a certain context. A quirk of the way filters are converted means that a fitler is required, or the system will add 'false'
- required_*: these might go away

```elixir
domain = %{
  name: "Solar System",
  source: SelectoTest.Test.SolarSystem,
  joins: [
    planets: %{
      name: "Planet",
      joins: [
        satellites: %{
          name: "Satellites"
        }
      ],
    }
  ],
  ### These filters will always be applied to a query in the domain
  required_filters: [{"id", [1, 2, 3, 4, 5, 6]}]
}
```

Selecto will walk through the configuration and configure columns from the ecto schema. From the source table, 
they will be named as a string of the column name. Columns in associated tables will be given name as association atom 
and the column name in brackets, eg "assoctable[id]".

To select data, use Selecto.select: 

```elixir
Selecto.select(selecto, ["id", "name", "assoctable[id]" ])
```

To filter data, use Selecto.filter: 

```elixir
Selecto.filter(selecto, [ {"id", 1} ])
```

Selecto will add the joins needed to build the query for the requested selections. It currently uses left joins only, but I will add support to specify join type.

To get results, use Selecto.execute

```elixir
Selecto.execute(selecto)
```

Which will return a list of maps, one per row.

Selections in Detail

When func is referenced below, it is referring to a SQL function

- "field" Just select the column
- {:count} select count of returns (aggregate)
- {func, field, as} when is_atom(func) select func(field) as "as"
- {func, field} when is_atom(func) select func(field)
- {func, {:literal, value}, as} when is_atom(func) select func(value) as "as"
- {:literal, name, value} select this literal value as "as"
- planned: case, coalesce, array

Filters in Detail

A filter is given as a tuple with the following forms allowed:

- {field, value} (value is string, numbe, boolean) -> regular old =
- {field, nil} -> is null clause
- {field, list_of_valeus } -> in clause
- {field, {comp, value}} -> comp is !=, >, <, >=, <=
- {field, {between, min, max}}-> you get it
- {field, :not_true} -> gives not(field) (should be a bool...)
- {:or, [list of filters]} -> recurses, joining items in the list with OR puts the result in ()
- {:and, [list of filters]} -> recurses and puts the result in ()

The selects and filters are composed into an Ecto.Query and you can get that by

```elixir
Selecto.gen_query(selecto)
```

Planned Features:

- Many 'TODO' sprinkled around the code +
- custom filters, joins, columns, self join
- parameterized joins (eg joining against a flags or tags table )
- json/array selects and predicates
- subqueries in filters
- ability to tell selecto to put some selects into an array from a subquery
- ability to select full schema structs / arrays of schema structs
- ability to configure without requiring domain structure
- generate SQL directly
- API & Vue lib
- Components (in progress for [tailwind/liveview](https://github.com/seeken/selecto_components) )
- tests (when domain/filters/select is stabilized)
- Documentation
- CTEs
- Window functions
- UNion, etc - pass in list of predicates and query will union all the alts together
- index hints
- join controls - eg manually add a join and tell Selecto which join variant
- form integration for validation selections and providing options to form elements
- more flexable selector and predicate structures, allow joins to use any predicates:

```elixir
    #standardize predicate format FUTURE

    {SELECTOR, nil} #is null
    {SELECTOR, :not_nil} #is not null
    {SELECTOR, SELECTOR} #=
    {SELECTOR, [SELECTOR2, ...]}# in ()
    {SELECTOR, {comp, SELECTOR2}} #<= etc
    {SELECTOR, {:between, SELECTOR2, SELECTOR2}
    {:not, PREDICATE}
    {:and, [PREDICATES]}
    {:or, [PREDICATES]}
    {:in, SUBQUERY}
    {:exists, SUBQUERY}

    #Standardize selectors to make more complex queries possible

    "field" # - plain old field from one of the tables
    {:field, field } #- same as above disamg for predicate second+ position
    {:literal, "value"} #- for literal values
    {:literal, 1.0}
    {:literal, 1}
    {:literal, datetime} etc
    {:func, SELECTOR}
    {:count, *} (for count(*))
    {:func, SELECTOR, SELECTOR}
    {:func, SELECTOR, SELECTOR, SELECTOR} #...
    {:extract, part, SELECTOR}
    {:case, [PREDICATE, SELECTOR, ..., :else, SELECTOR]}

    {:coalese, [SELECTOR, SELECTOR, ...]}
    {:greatest, [SELECTOR, SELECTOR, ...]}
    {:least, [SELECTOR, SELECTOR, ...]}
    {:nullif, [SELECTOR, LITERAL_SELECTOR]} #LITERAL_SELECTOR means naked value treated as lit not field

    {:subquery, [SELECTOR, SELECTOR, ...], PREDICATE}
```

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `selecto` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:selecto, "~> 0.1.0"}
  ]
end
```





Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/selecto>.

