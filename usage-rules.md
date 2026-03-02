# Selecto Usage Rules

## Core Usage
- Build queries from a configured domain with `Selecto.configure/2` or `Selecto.configure/3`.
- Use dot notation for joined field paths (for example `customer.name`, `items.product.name`).
- Prefer composable query steps (`select`, `filter`, `group_by`, `order_by`, `limit`) over ad-hoc raw SQL.

## Selection and SQL
- Prefer current function tuple formats (for example `{:count, "*"}`, `{:sum, "amount"}`) over legacy `{:func, ...}` forms.
- Use `Selecto.to_sql/1` to validate generated SQL before debugging execution behavior.
- Use `Selecto.select_shape/2` + `Selecto.execute_shape/2` when returning nested list/tuple result structures.

## Domain and Joins
- Keep domain configuration explicit (`source`, `schemas`, `joins`, `filters`, `custom_columns`).
- Preserve custom behavior during generator updates by keeping custom logic in overlay modules where possible.
- Validate parameterized joins when join params or field refs change.

## Testing and Safety
- In SQL assertions, test SQL keywords case-insensitively.
- Add focused tests when changing query building behavior.
- Avoid introducing compile warnings in changed modules.
