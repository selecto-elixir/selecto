# Listable

A Query and Report Writing system (Name is subject to change)

This is very young software and might spill milk in your computer.

Documentation is planned once the API is finalized. For now please 
see the notes in the listable_test repo.

A note: I use the word filter to mean a couple different things: 
 - A configuration that tells Listable how to build a predicate in the where clause
 - A specific invocation of one of those configurations


Listable is configured by passing in a 'domain' which tells it which 
table to start at, which tables it can join (assocs are supported now, 
but I will be adding support for non-assoc & parameterized joins), 
what columns are available (currently it's the columns from the schema, but guess what),
and what filters are available (currently its generated from the list of
columns, but that will also be expanded to ad hoc filters).

```elixir
listable = Listable.configure( %{} = domain )
```

The domain is a map, and contains: 
 - source: (req) This is the starting point, the table that will always be included in the query, as the module name, Eg YourApp.Accounts.Users
 - columns: A map of definitions and metadata for schema columns and ad hoc columns
 - filters: A map of ad hoc filters. But it does not work yet. 
 - joins: A map containing assoc names (the atom!) which can also recursively contain joins, columns, filters, and name
 - required_filters: This is a list of filters that will always be applied to the query. This is where you'd put a filter telling 
 Listable to restrict results, such as if you have fk based multi-tenant or want to build queryies restricted to a certain context.
 - required_*: these might go away

Listable will walk through the configuration and configure columns from the ecto schema. From the source table, 
they will be named as a string of the column name. Columns in associated tables will be given name as association atom 
and the column name in brackets, eg "assoctable[id]".

To select data, use Listable.select: 

```elixir
Listable.select(listable, ["id", "name", "assoctable[id]" ])
```

To filter data, use Listable.filter: 
```elixir
Listable.filter(listable, [ {"id", 1} ])
```

To get results, use Listable.execute
```elixir
Listable.execute(listable)
```
Which will return a list of maps, one per row. 

Selections in Detail







disregard all this for now, you have to install from github if you're 
pulling this into another app:

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `listable` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:listable, "~> 0.1.0"}
  ]
end
```





Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/listable>.

