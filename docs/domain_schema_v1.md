# Selecto Domain Schema v1

Selecto domain schema v1 is the first small, documented contract for authored
domain maps. It is intentionally compatibility-safe: `Selecto.Domain.normalize/1`
and `Selecto.Domain.validate/1` expose diagnostics and normalized projections,
but `Selecto.configure/3` does not consume the normalized contract yet.

## Version

Generated domains should declare the current schema version:

```elixir
%{
  schema_version: 1,
  name: "Orders",
  source: %{
    source_table: "orders",
    primary_key: :id,
    fields: [:id],
    columns: %{id: %{type: :integer}}
  },
  schemas: %{},
  joins: %{}
}
```

When `schema_version` is missing, `Selecto.Domain.normalize/1` infers version
`1` and returns a `:schema_version_inferred` warning. Invalid versions fall back
to the current version with an `:invalid_schema_version` warning. Newer positive
integer versions are preserved and receive an `:unsupported_schema_version`
warning.

## Top-Level Sections

The normalizer classifies authored top-level keys into four categories.

### Canonical

Canonical sections are part of the current domain contract:

- `schema_version`
- `name`
- `source`
- `schemas`
- `joins`
- `default_selected`
- `required_selected`
- `required_filters`
- `required_order_by`
- `required_group_by`
- `filters`
- `functions`
- `query_members`
- `published_views`
- `detail_actions`
- `domain_data`
- `extensions`

### Projection

Projection sections are recognized implementation or consumer-facing sections.
They are not unknown, but diagnostics call them out because future normalized
projections may reshape them:

- `columns`
- `custom_columns`
- `jsonb_schemas`
- `subfilters`
- `window_functions`
- `pagination`
- `retarget`
- `redact_fields`

### Proposed

Proposed sections are reserved for the write/action/reference contract that is
still being formalized:

- `writes`
- `actions`
- `capabilities`
- `source_relationships`
- `choice_sources`

### Unknown

Any other top-level key is unknown and appears in diagnostics. Unknown keys are
not preserved as a legacy contract. Selecto is still pre-0.5 and has no shipped
domain compatibility burden; old experimental write-like keys should be migrated
or removed rather than carried forward as `legacy` support.

## Core Relation Shape

The first strict contract validates `source` and every entry in `schemas` as
relation maps. A relation map uses this shape:

```elixir
%{
  source_table: "orders",
  primary_key: :id,
  fields: [:id, :status, :total],
  columns: %{
    id: %{type: :integer},
    status: %{type: :string},
    total: %{type: :decimal}
  },
  associations: %{
    customer: %{
      queryable: :customers,
      owner_key: :customer_id,
      related_key: :id
    }
  }
}
```

Validation checks:

- `source` and `schemas` must be present in the authored domain.
- `source_table` must be an atom or string.
- `primary_key` must be an atom or string and must appear in `fields`.
- `fields` must be a list.
- `columns` must be a map.
- Every listed field must have a matching column definition.

`joins` must be a map when present. Each join key must be declared as an
association on its parent relation, and each association must point at a schema
available in `schemas` unless it explicitly targets `:source`.

## Filter References

The first contract also validates filter references. Registered filters with a
`field` and expressions in `required_filters` must refer to known fields from:

- the root `source`
- entries in `schemas`, addressed as `"schema.field"`
- `custom_columns`

Unknown filter fields produce `:filter_field_not_found` diagnostics.

## Elixir Example

```elixir
domain = %{
  schema_version: 1,
  name: "Orders",
  source: %{
    source_table: "orders",
    primary_key: :id,
    fields: [:id, :status, :customer_id],
    columns: %{
      id: %{type: :integer},
      status: %{type: :string},
      customer_id: %{type: :integer}
    },
    associations: %{
      customer: %{queryable: :customers}
    }
  },
  schemas: %{
    customers: %{
      source_table: "customers",
      primary_key: :id,
      fields: [:id, :name],
      columns: %{
        id: %{type: :integer},
        name: %{type: :string}
      },
      associations: %{}
    }
  },
  joins: %{
    customer: %{}
  },
  filters: %{
    "customer_name" => %{field: "customers.name"}
  }
}

{:ok, normalized, diagnostics} = Selecto.Domain.validate(domain)
```

## JSON Equivalent

JSON domains use string keys and string field identifiers:

```json
{
  "schema_version": 1,
  "name": "Orders",
  "source": {
    "source_table": "orders",
    "primary_key": "id",
    "fields": ["id", "status", "customer_id"],
    "columns": {
      "id": {"type": "integer"},
      "status": {"type": "string"},
      "customer_id": {"type": "integer"}
    },
    "associations": {
      "customer": {"queryable": "customers"}
    }
  },
  "schemas": {
    "customers": {
      "source_table": "customers",
      "primary_key": "id",
      "fields": ["id", "name"],
      "columns": {
        "id": {"type": "integer"},
        "name": {"type": "string"}
      },
      "associations": {}
    }
  },
  "joins": {
    "customer": {}
  },
  "filters": {
    "customer_name": {"field": "customers.name"}
  }
}
```

## Diagnostics Example

```elixir
{:ok, _normalized, diagnostics} = Selecto.Domain.normalize(%{
  source: %{
    source_table: "orders",
    primary_key: :id,
    fields: [:id],
    columns: %{id: %{type: :integer}}
  },
  schemas: %{},
  joins: %{},
  custom_columns: %{},
  writes: %{},
  old_write_flag: true
})

diagnostics.schema_version_inferred
#=> true

Enum.map(diagnostics.warnings, & &1.code)
#=> [:schema_version_inferred, :projection_sections, :proposed_sections, :unknown_sections]

diagnostics.unknown_sections
#=> [:old_write_flag]
```

Use `Selecto.Domain.validate/1` when callers want contract errors in addition to
normalization diagnostics.
