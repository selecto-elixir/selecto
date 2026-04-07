# Selecto Expression DSL

This guide covers the shipped expression-authoring layer in `selecto`.

The key design point is that all helper, macro, and sigil forms normalize into
the same Selecto AST used by the existing query APIs.

## Layers

### `Selecto.Expr`

Use `Selecto.Expr` for data-first query composition.

This is the best fit when filters or selectors are assembled dynamically from
runtime values, UI state, or reusable helper functions.

```elixir
alias Selecto.Expr, as: X

filters =
  X.compact_and([
    X.eq("status", "active"),
    X.when_present(search, &X.ilike("customer.name", "%#{&1}%")),
    X.gte("total", 100)
  ])

selects = [
  X.field("id"),
  X.field("customer.name"),
  X.as(X.count("*"), "total_rows")
]

query =
  selecto
  |> Selecto.filter(filters)
  |> Selecto.select(selects)
  |> Selecto.order_by([X.desc_nulls_last("total")])
```

### `Selecto.ExprMacros`

Use macros when you want lighter Elixir-native authoring inside source files.

```elixir
import Selecto.ExprMacros

where_ast =
  where(status == ^status and ilike(customer.name, ^pattern))

select_ast =
  select([
    customer.name,
    count_distinct(customer.id),
    case_when([{status == ^status, "Open"}], "Other"),
    window(row_number(), over: [partition_by: [status], order_by: [desc(created_at)]], as: "row_num")
  ])

order_ast = order_by([desc_nulls_last(total), asc(customer.name)])
```

### `Selecto.Sigil`

Use `~SELECTO` for compact filter expressions.

```elixir
import Selecto.Sigil

filter_ast = ~SELECTO"total >= ^min_total and starts_with(customer.name, ^prefix)"
```

`~SELECTO` is currently filter-only.

## Current Helper Coverage

### Filters

- `eq/2`, `neq/2`, `gt/2`, `gte/2`, `lt/2`, `lte/2`
- `like/2`, `ilike/2`, `contains/2`, `starts_with/2`, `ends_with/2`
- `between/3`, `in/2`, `is_null/1`, `not_null/1`
- `and/1`, `or/1`, `not/1`
- `exists/1,2`, `subquery_in/2,3`
- `when_present/2`, `maybe/2`, `compact_and/1`, `compact_or/1`

### Selectors

- `field/1`, `lit/1`, `as/2`
- `count/1`, `sum/1`, `avg/1`, `min/1`, `max/1`
- `coalesce/1`, `case_when/2`
- `window/3`, `frame/3`
- `json_extract/3`, `json_extract_text/3`, `json_agg/2`, `json_object_agg/3`
- wrapper selectors like `greatest`, `least`, `nullif`, and `concat`

### Ordering

- `asc/1`, `desc/1`
- `asc_nulls_first/1`, `asc_nulls_last/1`
- `desc_nulls_first/1`, `desc_nulls_last/1`

## Macro Coverage Snapshot

### `where/1`

Current support includes:

- comparisons: `==`, `!=`, `>`, `>=`, `<`, `<=`
- boolean composition: `and`, `or`, `not`
- helpers: `ilike`, `like`, `contains`, `starts_with`, `ends_with`, `between`, `in`, `is_nil`
- pinned runtime values with `^value`
- dotted field references like `customer.name`

### `select/1`

Current support includes:

- bare fields and dotted fields
- `field/1`, `lit/1`, `as/2`
- `count/0`, `count_distinct/1`, `sum/1`, `avg/1`, `min/1`, `max/1`
- `coalesce(...)`, `case_when(...)`
- window selectors
- JSON selectors
- wrapper selectors like `greatest(...)`, `least(...)`, `nullif(...)`, `concat(...)`

### `order_by/1`

Current support includes:

- bare field ordering
- `asc/1`, `desc/1`
- null-order helpers
- selector expressions usable in ordering

## Integration with `Selecto.Query`

The expression layer does not bypass the normal query pipeline.

- `Selecto.select/2`
- `Selecto.filter/2`
- `Selecto.pre_retarget_filter/2`
- `Selecto.post_retarget_filter/2`
- `Selecto.order_by/2`
- `Selecto.group_by/2`

These entry points normalize helper-shaped inputs before validation, so helper,
macro, and sigil output still flows through the same validator and SQL builder
path as the existing tuple forms.

## Recommendation for `~SELECTO`

Current recommendation: keep it Elixir-AST-based for now.

That keeps maintenance low and preserves `Selecto.Expr` as the real canonical
surface. A dedicated parser only becomes compelling if the filter language needs
to be shared outside Elixir source files.
