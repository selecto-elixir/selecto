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

## Write Transitions

`writes.transitions` is the first proposed write contract section with strict
validation. It is a direct state graph keyed by a known domain field:

```elixir
%{
  writes: %{
    transitions: %{
      status: %{
        "pending" => ["ready", "cancelled"],
        "ready" => [:complete, "cancelled"],
        complete: []
      }
    }
  }
}
```

Validation checks:

- `writes` must be a map when present.
- `writes.transitions` must be a map when present.
- each transition field key must be an atom or string
- each transition field must exist in the source, schemas, or custom columns
- each transition graph must be a map
- each source state must be an atom or string
- each target list must be a list of atoms or strings

This validation does not execute writes and does not make `Selecto.configure/3`
depend on the write contract.

## Capability Catalog

`capabilities` declares the stable capability names a domain can reference. It
does not decide which actors have those capabilities; host applications and
future resolver adapters own that policy decision.

```elixir
%{
  capabilities: %{
    "order.view" => %{
      label: "View orders",
      operations: [:select, :detail],
      target: :order
    },
    "order.approve" => %{
      label: "Approve order",
      operations: [:action],
      action: :approve_order
    },
    "order.export" => %{
      label: "Export orders",
      operations: [:export],
      sensitivity: :high
    }
  }
}
```

Validation checks:

- `capabilities` must be a map when present.
- capability ids must be atoms or strings.
- each capability entry must be a map.
- each capability must declare a non-empty `operations` list.
- each operation must be an atom or string.

Runtime capability checks use a shared request/decision value shape:

```elixir
request =
  Selecto.Capabilities.request(
    actor: current_user,
    tenant: tenant_context,
    domain: :orders,
    capability: "order.approve",
    operation: :execute_action,
    target: %{type: :row, id: order_id},
    context: %{surface: :components}
  )

decision =
  Selecto.Capabilities.allow(:role_allowed,
    effects: [{:required_filter, "tenant_id", {:eq, tenant_id}}],
    obligations: [:audit_action]
  )
```

Decision statuses are `:allow`, `:deny`, `:conditional`, and
`:not_applicable`. Visibility recommendations are `:enabled`, `:disabled`,
`:hidden`, and `:preview_only`.

## Direct Transition Actions

`actions` declares named business commands. The first strict action shape is a
row action that directly references a `writes.transitions` edge:

```elixir
%{
  actions: %{
    complete_order: %{
      target: :order,
      scope: :row,
      capability: "order.complete",
      transition: %{
        field: :status,
        from: "ready",
        to: "complete"
      },
      execution: %{
        kind: :updato,
        operation: :update,
        set: %{status: "complete"}
      }
    }
  }
}
```

Validation checks:

- `actions` must be a map when present.
- action ids must be atoms or strings.
- each action entry must be a map.
- `capability`, when present, must be an atom or string and must exist in the
  domain capability catalog.
- actions with `type: :transition` must declare a direct transition map.
- `transition` must be a map with `field`, `from`, and `to`.
- the transition field must exist in the source, schemas, or custom columns.
- the transition edge must exist in `writes.transitions`.
- optional direct execution metadata currently supports only
  `%{kind: :updato, operation: :update}`.
- optional execution `set` must set the transition field to the target state.

This validates that preview and execution can ask the same domain question; it
does not execute actions.

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
  },
  capabilities: %{
    "order.view" => %{operations: [:select, :detail]},
    "order.approve" => %{operations: [:action], action: :approve_order}
  },
  writes: %{
    transitions: %{
      status: %{
        "pending" => ["ready", "cancelled"],
        "ready" => ["complete", "cancelled"],
        "complete" => []
      }
    }
  },
  actions: %{
    complete_order: %{
      target: :order,
      scope: :row,
      capability: "order.approve",
      transition: %{field: :status, from: "ready", to: "complete"},
      execution: %{kind: :updato, operation: :update, set: %{status: "complete"}}
    }
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
  },
  "capabilities": {
    "order.view": {"operations": ["select", "detail"]},
    "order.approve": {"operations": ["action"], "action": "approve_order"}
  },
  "writes": {
    "transitions": {
      "status": {
        "pending": ["ready", "cancelled"],
        "ready": ["complete", "cancelled"],
        "complete": []
      }
    }
  },
  "actions": {
    "complete_order": {
      "target": "order",
      "scope": "row",
      "capability": "order.approve",
      "transition": {"field": "status", "from": "ready", "to": "complete"},
      "execution": {
        "kind": "updato",
        "operation": "update",
        "set": {"status": "complete"}
      }
    }
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
