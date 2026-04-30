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

The first contract also validates filter registry metadata and filter
references. `filters` must be a map. Each filter id must be a non-empty atom or
string, and each filter config must be a map. Virtual filters may omit `field`.
When present, `field` must be a non-empty atom or dotted string path and `type`
must be a non-empty atom or string.

Registered filters with a `field` and expressions in `required_filters` must
refer to known fields from:

- the root `source`
- entries in `schemas`, addressed as `"schema.field"`
- `custom_columns`

Unknown filter fields produce `:filter_field_not_found` diagnostics.
Invalid filter registry metadata produces `:invalid_filter_id`,
`:invalid_filter_config`, `:invalid_filter_field`, or `:invalid_filter_type`
diagnostics.

## Function Registry

`functions` must be a map when present. Each function id must be a non-empty
atom or string, and each function spec must be a map. Function specs validate
the current UDF metadata contract:

- `kind` must be `:scalar`, `:predicate`, or `:table`.
- `sql_name` must be a safe SQL function identifier such as `"lower"` or
  `"public.similarity"`.
- optional `allowed_in` must be a list of supported call sites.
- optional `args` must be a list of arg maps with non-empty `name`, declared
  `type`, and `source` set to `:selector`, `:value`, or `:literal`.
- predicate functions must return `:boolean`.
- table functions must return `%{columns: %{...}}`.
- scalar function returns may be omitted or declared as an atom or
  `{:array, type}` tuple.

Invalid function metadata produces diagnostics such as `:invalid_function_id`,
`:invalid_function_spec`, `:invalid_function_kind`,
`:invalid_function_sql_name`, `:invalid_function_call_site`,
`:invalid_function_arg_source`, or `:invalid_function_returns`.

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

## Source Relationships And Choice Sources

`source_relationships` declares the first compact working-domain to source-domain
binding shape. It is used by `choice_sources` to describe context-safe option
providers.

```elixir
%{
  source: %{
    columns: %{
      customer_id: %{
        type: :integer,
        reference: %{
          choice_source: :customer_choices,
          value_source: "customers.id",
          caption_source: "customers.name"
        }
      }
    }
  },
  source_relationships: %{
    customer: %{
      target_domain: :customers,
      source_field: :customer_id,
      target_field: :id,
      source_path: "customers",
      virtual_join: [
        %{working_field: :customer_id, source_field: "customers.id", required: true}
      ],
      filters: [
        {:eq, "customers.active", true}
      ]
    }
  },
  choice_sources: %{
    customer_choices: %{
      domain: :customers,
      value_field: :id,
      label_field: :name,
      source_path: "customers",
      value_source: "customers.id",
      caption_source: "customers.name",
      description_source: "customers.description",
      filters: [{:eq, "customers.active", true}],
      order_by: ["customers.name", {"customers.id", :desc}],
      presentation: %{
        control: :autocomplete,
        mode: :searchable,
        cardinality: :one
      },
      source_relationship: :customer,
      capability: "customer.choose"
    }
  }
}
```

Source relationship validation checks:

- `source_relationships` must be a map when present.
- source relationship ids must be atoms or strings.
- each source relationship entry must be a map.
- each source relationship must declare `target_domain`, `source_field`, and
  `target_field`.
- `target_domain`, `source_field`, and `target_field` must be atoms or strings.
- `source_field` must exist in the working domain source, schemas, or custom
  columns.
- optional `source_path` must be a non-empty atom or dotted string path.
- optional `virtual_join` must be a list of maps with `working_field` and
  `source_field`; `working_field` must exist in the working domain,
  `source_field` must be a non-empty atom or dotted string path, and optional
  `required` must be a boolean.
- optional `filters` must be a list of static filter expressions using the same
  operator and path syntax as choice-source filters.

Choice source validation checks:

- `choice_sources` must be a map when present.
- choice source ids must be atoms or strings.
- each choice source entry must be a map.
- each choice source must declare `domain`, `value_field`, and `label_field`.
- `domain`, `value_field`, and `label_field` must be atoms or strings.
- optional `source_relationship` must reference a declared source relationship.
- optional `capability` must reference a declared capability.
- optional `source_path`, `value_source`, `caption_source`, and
  `description_source` must be non-empty atom or dotted string paths.
- optional `filters` must be a list of static filter expressions. Field
  operators such as `:eq`, `:gt`, `:between`, and `:in`, plus logical
  `:and`, `:or`, and `:not`, may be atoms or strings.
- choice-source filter field operands must be non-empty atom or dotted string
  paths. Literal, context, and runtime values are preserved without evaluation.
- optional `order_by` must be a list of paths or `{path, direction}` entries;
  direction must be `:asc` or `:desc`.
- optional `presentation` must be a map. Known presentation hints are:
  `control: :select | :autocomplete | :table_picker`,
  `mode: :static | :searchable | :async | :inline`, and
  `cardinality: :one | :many`.

Field binding validation checks:

- source, schema, and projection column metadata may use
  `choice_source: choice_source_id` as compact field binding.
- source, schema, and projection column metadata may use
  `reference: %{choice_source: choice_source_id}` for richer bindings.
- field-level `choice_source` references must be atoms or strings and must
  reference a declared choice source.
- `reference`, when present, must be a map.
- `reference.choice_source`, when present, must be an atom or string and must
  reference a declared choice source.
- optional `reference.value_source` and `reference.caption_source` must be atoms
  or strings.
- optional `reference.caption_field` must be an atom or string and must refer to
  a known working-domain field.

### Authoring Shorthand

For authoring ergonomics, a field may declare `choice_source: %{...}` directly.
`Selecto.Domain.normalize/1` expands that supported shorthand into the canonical
registries without changing `Selecto.configure/3` behavior:

```elixir
customer_id: %{
  type: :integer,
  choice_source: %{
    id: :customer_choices,
    domain: :customers,
    source_relationship: %{
      id: :customer,
      virtual_join: [
        %{working_field: :customer_id, source_field: "customers.id", required: true}
      ]
    },
    value_source: "customers.id",
    caption_source: "customers.name",
    filters: [{:eq, "customers.active", true}],
    presentation: :select
  }
}
```

The normalized form contains:

- `source_relationships.customer`
- `choice_sources.customer_choices`
- `reference: %{choice_source: :customer_choices, ...}` on the field
- compact `choice_source: :customer_choices` on the field

If `id` values are omitted, the normalizer generates deterministic string ids
from the field path. This is canonical shorthand only; pre-0.5 legacy sections
are still not preserved or expanded.

The current slice validates static choice-source metadata and filter expression
syntax plus static source-relationship metadata. It does not resolve external
source-domain schemas, apply filters, fetch options, or execute membership
checks.

## Domain Composition

`Selecto.Domain.compose/2` is the opt-in Stage 2 boundary for combining an
authored domain with overlays before projecting or validating it. It does not
change `Selecto.configure/3` behavior.

```elixir
{:ok, normalized, diagnostics} =
  Selecto.Domain.compose(base_domain, [
    %{
      source: %{
        columns: %{total: %{label: "Total", format: :currency}},
        redact_fields: [:tenant_secret]
      },
      filters: %{"status" => %{field: :status}}
    }
  ])
```

Composition semantics are deterministic:

- maps deep-merge by section.
- `redact_fields`, including `source.redact_fields`, are unioned.
- `extensions` are appended uniquely.
- other lists and scalar values are replaced by later overlays.
- governance/reference registry collisions, such as `choice_sources` or
  `source_relationships`, produce `:domain_composition_collision` warnings.

After overlays merge, declared extension `merge_domain/2` callbacks run in
declaration order and the result is normalized again.

## Domain Inspection

`Selecto.Domain.describe/1` returns a compact structured inspection map for an
authored or normalized domain. The output is intended for generators, docs,
Studio, tests, and other tools that need stable metadata without walking the
full normalized domain shape.

```elixir
{:ok, inspection, diagnostics} = Selecto.Domain.describe(domain)

inspection.counts.choice_sources
inspection.registries.source_fields
inspection.source_relationships
inspection.field_choice_bindings
```

The inspection output includes:

- section categories and normalization diagnostics summary
- counts for source fields, registries, writes, actions, capabilities,
  source relationships, choice sources, and field choice bindings
- sorted registry ids for filters, functions, query members, joins, schemas,
  actions, capabilities, source relationships, and choice sources
- compact summaries of `writes`, actions, capabilities, source relationships,
  choice sources, and field-to-choice-source bindings

## Choice Membership API

`Selecto.Domain.Choices` is the first shared API for asking whether a submitted
value belongs to a field's declared choice source. In this slice it resolves
domain metadata and builds a membership request, but it does not query source
domains or databases unless a caller supplies an explicit resolver.

```elixir
{:ok, request} =
  Selecto.Domain.Choices.request(domain, :customer_id, 42,
    actor: current_user,
    tenant: tenant_context,
    record: %{customer_id: 42},
    context: %{surface: :components}
  )

request.choice_source
#=> :customer_choices

{:error, result} =
  Selecto.Domain.Choices.validate_choice(domain, :customer_id, 42)

result.status
#=> :unknown

result.reason_code
#=> :resolver_required
```

Callers that have a membership implementation can pass a resolver function:

```elixir
resolver = fn request ->
  if source_member?(request) do
    Selecto.Domain.Choices.valid(:source_member)
  else
    Selecto.Domain.Choices.invalid(:not_in_choice_source)
  end
end

{:ok, result} =
  Selecto.Domain.Choices.validate_choice(domain, :customer_id, 42,
    resolver: resolver
  )
```

The request/result shape lets Components, API, GraphQL, AI, actions, and Updato
share one membership question later. Core Selecto remains conservative: without
a resolver, membership is `:unknown`, not assumed valid.

## Choice Option Lists

`Selecto.Domain.Choices` also exposes the sibling option-list request shape for
surfaces that need to ask "what options should this field show?" The request can
be built from a field binding or directly from a declared choice source.

```elixir
{:ok, request} =
  Selecto.Domain.Choices.options_request(domain, :customer_id,
    search: "acme",
    limit: 20,
    offset: 0,
    actor: current_user,
    tenant: tenant_context,
    record: %{customer_id: 42},
    context: %{surface: :components}
  )

request.choice_source
#=> :customer_choices

{:ok, direct_request} =
  Selecto.Domain.Choices.options_request(domain, :customer_choices,
    by: :choice_source,
    search: "acme"
  )
```

As with membership checks, core Selecto does not fetch option rows without an
explicit resolver:

```elixir
{:error, result} =
  Selecto.Domain.Choices.list_options(domain, :customer_id, search: "acme")

result.status
#=> :unknown

resolver = fn request ->
  options =
    fetch_options(request)
    |> Enum.map(&%{value: &1.id, label: &1.name})

  Selecto.Domain.Choices.options_resolved(options)
end

{:ok, result} =
  Selecto.Domain.Choices.list_options(domain, :customer_id,
    search: "acme",
    resolver: resolver
  )
```

The option-list API is a projection contract for future Components, API,
GraphQL, AI, operations, and Updato integrations. It does not change
`Selecto.configure/3` behavior.

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
      customer_id: %{
        type: :integer,
        reference: %{
          choice_source: :customer_choices,
          value_source: "customers.id",
          caption_source: "customers.name"
        }
      }
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
  source_relationships: %{
    customer: %{
      target_domain: :customers,
      source_field: :customer_id,
      target_field: :id,
      source_path: "customers",
      virtual_join: [
        %{working_field: :customer_id, source_field: "customers.id", required: true}
      ],
      filters: [{:eq, "customers.active", true}]
    }
  },
  choice_sources: %{
    customer_choices: %{
      domain: :customers,
      value_field: :id,
      label_field: :name,
      source_path: "customers",
      value_source: "customers.id",
      caption_source: "customers.name",
      filters: [{:eq, "customers.active", true}],
      order_by: ["customers.name"],
      presentation: %{
        control: :autocomplete,
        mode: :searchable,
        cardinality: :one
      },
      source_relationship: :customer,
      capability: "customer.choose"
    }
  },
  capabilities: %{
    "order.view" => %{operations: [:select, :detail]},
    "order.approve" => %{operations: [:action], action: :approve_order},
    "customer.choose" => %{operations: [:choice_source]}
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
      "customer_id": {
        "type": "integer",
        "reference": {
          "choice_source": "customer_choices",
          "value_source": "customers.id",
          "caption_source": "customers.name"
        }
      }
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
  "source_relationships": {
    "customer": {
      "target_domain": "customers",
      "source_field": "customer_id",
      "target_field": "id",
      "source_path": "customers",
      "virtual_join": [
        {"working_field": "customer_id", "source_field": "customers.id", "required": true}
      ],
      "filters": [["eq", "customers.active", true]]
    }
  },
  "choice_sources": {
    "customer_choices": {
      "domain": "customers",
      "value_field": "id",
      "label_field": "name",
      "source_path": "customers",
      "value_source": "customers.id",
      "caption_source": "customers.name",
      "filters": [["eq", "customers.active", true]],
      "order_by": ["customers.name"],
      "presentation": {
        "control": "autocomplete",
        "mode": "searchable",
        "cardinality": "one"
      },
      "source_relationship": "customer",
      "capability": "customer.choose"
    }
  },
  "capabilities": {
    "order.view": {"operations": ["select", "detail"]},
    "order.approve": {"operations": ["action"], "action": "approve_order"},
    "customer.choose": {"operations": ["choice_source"]}
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
