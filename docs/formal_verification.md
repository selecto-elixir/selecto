# Selecto Formal Verification

Selecto ships a deterministic bounded model checker and two built-in proof
suites. They provide stronger evidence than sampled tests: every invariant is
checked against every state in an explicitly defined finite model.

Run the suites with:

```sh
mise exec -- mix selecto.verify
mise exec -- mix selecto.verify --output tmp/selecto-verification.json
```

The command exits non-zero when it finds a counterexample. The optional JSON
artifact records the proof level, model identity, state count, invariant count,
check count, and any reproducible counterexamples.

## Current Proofs

### Query scope

`selecto.query_scope.v1` exhaustively crosses:

- tenant-required and tenant-optional domains;
- absent, row-tenant, and schema-prefix contexts;
- applied and unapplied row scope;
- absent and present ordinary user filters.

For all 24 states it proves:

- required scope fails closed;
- required filters survive query composition;
- row-tenant values reach SQL through parameters rather than interpolation;
- ordinary user filters cannot substitute for required tenant scope.

This model exposed and fixed a real fail-open condition: a tenant id attached as
context was previously accepted as sufficient scope before it had been applied
as a required query filter.

### Provider/consumer contracts

`selecto.domain_contract_compatibility.v1` exhaustively crosses compatible and
incompatible field, filter, required-scope, version, and fingerprint
dependencies.

For all 32 states it proves:

- only a fully compatible consumer is accepted;
- every incompatible dimension is rejected;
- each rejection includes its specific contract diagnostic.

This complements stable contract snapshots and breaking-change classification
in `Selecto.Domain.ContractVerification`.

## Proof Meaning

Reports use `proof_level: bounded_exhaustive`. A passing report means there is
no counterexample in the complete, versioned finite model. It does not claim an
unbounded theorem about every possible domain, custom SQL fragment, adapter, or
database.

The checker is intentionally in normal package code:

```elixir
Selecto.Verification.BoundedModel.check("my.model.v1", states, invariants)
```

Applications can therefore add finite models for their own tenant modes,
published contracts, domain compositions, and policy rules.

## Remaining Boundary

These suites do not yet prove full relational equivalence between arbitrary
Selecto intent and generated SQL. That requires an independent relational
interpreter with explicit SQL `NULL`, bag, join, grouping, and ordering
semantics, followed by bounded comparison against PostgreSQL. Until that layer
exists, the existing SQL tests and live database suites remain necessary.
