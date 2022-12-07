# Selecto

A Query Writing System

Selecto allows you to create queries within a configured domain. The domain has a main table that will
always be included in queries, required filters that will always be applied, and a tree of available 
tables for joins.

This is very young software and might spill milk in your computer.

Better documentation is planned once the API is finalized.

For now, see [selecto_test](https://github.com/seeken/selecto_test).

Selecto is configured by passing in a 'domain' which tells it which 
table to start at, which tables it can join (assocs are supported now, 
but I will be adding support for non-assoc & parameterized joins), 
what columns are available (currently it's the columns from the schema, but guess what),
and what filters are available (currently its generated from the list of
columns, but that will also be expanded to custom filters).

```elixir
selecto = Selecto.configure( YourApp.Repo,  domain )
```

The domain is a map, and contains:

- source: (req) This is the starting point, the table that will always be included in the query, as the module name, Eg YourApp.Accounts.Users
- columns: A map of definitions and metadata for schema columns and (planned) ad hoc columns
- filters: A map of ad hoc filters. But it does not work yet. 
- joins: A keyword list containing assoc names to maps which can also recursively contain joins, columns, filters, and name (name is required currently). The joins need to be set up as proper associatinos in your schema!
- required_filters: This is a list of filters that will always be applied to the query. This is where you'd put a filter telling Selecto to restrict results, such as if you have fk based multi-tenant or want to build queryies restricted to a certain context. A quirk of the way filters are converted means that a fitler is required, or the system will add 'false'

```elixir
domain = %{
  name: "Solar System",
  source: SelectoTest.Test.SolarSystem,
  joins: [ ### Joins require a Name currently-- may change and allow a format similar to the list from preload
    planets: %{
      name: "Planet",
      joins: [
        satellites: %{
          name: "Satellites"
        }
      ],
    }
  ],
  ### These filters will always be applied to a query in the domain. Note due to a bug/feature, if no filters are provided 
  ### you will not get any rows returned
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

Selections in Detail

When func is referenced below, it is referring to a SQL function

```elixir
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

    {:subquery, ...}
```

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

Planned Features:

- Many 'TODO' sprinkled around the code +
- custom joins, columns, self join
- parameterized joins (eg joining against a flags or tags table )
- json/array selects and predicates
- subqueries in filters
- ability to tell selecto to put some selects into an array from a subquery
- ability to select full schema structs / arrays of schema structs
- ability to configure without requiring domain structure
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

Planned new format :

```elixir
    #standardize predicate format FUTURE NOT AVAILABLE YET! 

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
    {SELECTOR, comp, :any, SUBQUERY}
    {SELECTOR, comp, :all, SUBQUERY}
    {:exists, SUBQUERY}

 
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

