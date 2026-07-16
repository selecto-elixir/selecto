# Elixir Code Audit: Selecto

**Scope:** `lib/` (~154 modules, ~59k LOC, ~4.7k function clauses)
**Tools:** Credo (default + atom audit), structural review of largest modules
**Version context:** library (`:selecto` 0.4.6), Elixir `~> 1.18`

Overall: solid query-builder architecture (iodata SQL, params, adapters, telemetry), but growth has left **god modules**, **atom-leak risk**, **exception-heavy control flow**, and **stringly-typed domain maps**. Credo is configured so narrowly it reports “no issues” while real problems remain.

---

## Critical / high severity

### 1. Atom table growth (`String.to_atom/1`)

Unsafe atom creation appears on paths that can be driven by query config / result metadata:

| Location | Pattern |
|---|---|
| `lib/selecto/output/transformers/structs.ex` | `String.to_atom/1` for field names |
| `lib/selecto/output/transformers/maps.ex` | `:atoms` key type → `String.to_atom/1` |
| `lib/selecto/output/transformers/json.ex` | fallback `to_existing` → `to_atom` |
| `lib/selecto/subfilter/join_path_resolver.ex` | path segments → `String.to_atom/1` |
| `lib/selecto/dynamic_join.ex` | filter keys → `String.to_atom/1` |
| `lib/selecto/auto_retarget.ex`, `lib/selecto/ecto_adapter.ex`, `lib/selecto/query_members.ex` | join/table IDs |

Atoms are never GC’d. Converting unbounded column names / join paths is a classic BEAM DOS vector.

**Prefer:** keep strings (or require existing atoms only), or use a bounded internal atom registry for known domain keys.

---

### 2. Unsupervised process fallbacks

In `lib/selecto/task_supervisor.ex`:

```elixir
def async(fun) when is_function(fun, 0) do
  case ensure_started() do
    {:ok, _pid} -> Task.Supervisor.async_nolink(@name, fun)
    {:error, _reason} -> Task.async(fun)
  end
end

def start_child(fun) when is_function(fun, 0) do
  case ensure_started() do
    {:ok, _pid} -> Task.Supervisor.start_child(@name, fun)
    {:error, _reason} -> {:ok, spawn(fun)}
  end
end
```

- Application already starts `Task.Supervisor` under `Selecto.TaskSupervisor`.
- Fallback `Task.async/1` **links** to the caller (different failure semantics).
- Fallback `spawn/1` is **unsupervised** — no logging, no restart, easy to leak work.

**Prefer:** hard-fail if the supervisor is unavailable, or only start it from the app tree (no silent degrade).

---

### 3. SQL safety still incomplete

`raw_sql` in `lib/selecto/builder/sql/select.ex` is intentionally passthrough and **not validated**:

```elixir
def prep_selector(_selecto, {:raw_sql, sql}, _retarget_aliases) when is_binary(sql) do
  # For raw SQL, just return it as-is
  {[sql], :selecto_root, []}
end
```

Inline TODOs still say “Check for SQL INJ”. Identifier helpers are better, but:

- `maybe_quote_identifier/1` interpolates without escaping embedded quotes: `~s["#{str}"]`.
- Fallback quote path in `quote_identifier/2` does the same: `"#{quote}#{str}#{quote}"`.
- Allowed character class includes space / `:` / `&` / `-` — unusual for SQL identifiers and easy to misuse if adapters don’t escape.

**Prefer:** document `raw_sql` as trusted-only; escape quotes in all quote helpers; prefer adapter `quote_identifier/1` always.

---

### 4. Broad `rescue` / `catch` swallowing real failures

`Selecto.Executor` wraps execution in nested `try/rescue/catch` that converts almost anything into `{:error, %Selecto.Error{}}`, including exits. That:

- Hides bugs (bad pattern matches, `ArgumentError`, etc.) as “connection”/query errors.
- Duplicates the same error path (outer execute + task wrapper).
- Makes debugging harder for library users.

Similar “never break the query” swallowing appears in hooks and metrics tracking.

**Prefer:** rescue known DB/adapter exceptions; let programmer errors raise; use `with` for expected error tuples.

---

## Code smells & non-idiomatic Elixir

### 5. God modules / multi-clause explosion

| Module | Lines | Smell |
|---|---:|---|
| `lib/selecto.ex` | ~2445 | Public façade + many non-delegate implementations |
| `lib/selecto/domain_validator.ex` | ~1773 | Validation monolith |
| `lib/selecto/builder/sql/select.ex` | ~1680 | Hundreds of `prep_selector` clauses |
| `lib/selecto/builder/subselect.ex` | ~1523 | Large builder |
| `lib/selecto/config/overlay_dsl.ex` | ~1510 | DSL + runtime mixed |

`prep_selector` in particular is a **mega multi-head function**: many near-identical array/function wrappers that only `raise "… not properly implemented"` on `nil`. That is hard to maintain, dialyze, and document.

**Prefer:** protocol or dispatch table (`@impl` modules per function family); keep `Selecto` as thin `defdelegate` surface only.

---

### 6. Bare `raise "string"` / wrong exception types

Widespread pattern:

```elixir
raise "array_length function not properly implemented"
raise RuntimeError, message: "Invalid String #{string}"
```

Idiomatic Elixir uses typed exceptions (`ArgumentError`, custom `defexception`) so callers can pattern-match and tools can classify severity. `Helpers.check_safe_phrase/1` even has **dead code after raise** (`false`).

---

### 7. `throw`/`catch` for control flow

Used in parameterized parser and struct transformers (`throw {:transform_error, reason}` then `catch`). Works, but is non-idiomatic for library code; `reduce_while` / `{:error, _}` is clearer and cheaper to reason about.

---

### 8. Dual atom/string map keys everywhere

Domain / config / field resolution repeatedly does:

```elixir
Map.get(columns, field_key) || Map.get(columns, safe_existing_atom(field_key))
get_in(domain, [:custom_columns, field_str]) || get_in(..., [camel]) || ...
```

This is a long-term tax: every API must accept both; bugs appear only when one form is missing. `Selecto.Domain.Shared.Map` helps, but callers still reimplement local variants.

**Prefer:** normalize domain to one key type at the boundary (`configure/3`) and keep internals uniform.

---

### 9. List building with `++` in builders

Hot paths in SQL select/where/join build with:

```elixir
{select ++ [s_iodata], join ++ List.wrap(j), param ++ p}
```

For large select lists this is quadratic. Codebase already uses prepend+reverse in some places — inconsistent.

**Prefer:** prepend + `Enum.reverse/1` (or iodata-only accumulation) everywhere in builders.

---

### 10. Dead / no-op code

In `lib/selecto/domain/contract_verification.ex`:

```elixir
projection =
  case Keyword.get(opts, :strict, true) do
    false -> projection
    true -> projection
  end
```

Also `track_query_execution/3` is effectively a no-op (commented-out monitor calls inside try/rescue/catch). Noise that suggests unfinished features.

---

### 11. Global process-dict / named GenServer design

- Performance hooks stored process-wide and snapshotted across tasks.
- `QueryCache`, `MetricsCollector` use fixed registered names.
- Connection pool generates atoms for pool names (`:"selecto_pool_#{hash}"`) — another atom source, though bounded by unique DB configs.

Harder to test in parallel; surprising for library consumers who didn’t opt into global state.

---

### 12. Weak static analysis config

`.credo.exs` enables **warnings only** (no readability / design / consistency). That explains “5464 mods/funs, found no issues” — Credo is not looking.

Recommended enables (at least for `lib/`):
`Credo.Check.Refactor.CyclomaticComplexity`, `Nesting`, `LongQuoteBlocks`, `PipeChainStart` (optional), `DuplicatedCode`, `AliasUsage`, `UnsafeToAtom` in default config.

---

## Potential correctness / reliability problems

| Issue | Why it matters |
|---|---|
| Nested timeout tasks | `TaskSupervisor.async` + `Task.await` with hooks restore is complex; link/nolink mix can leave stray processes on timeout races |
| `Process.exit(connection, :normal)` in pool | Prefer supervised stop / adapter close API |
| `Code.ensure_loaded?/1` + optional modules | Fine, but combined with rescue-all can hide load errors |
| `raise` in configure path on adapter connect failure | API is inconsistent with `{:ok, _}/{:error, _}` execute style |
| `Enum.into(opts, [])` for map→keyword | Non-deterministic keyword order (usually OK, surprising if order-sensitive) |
| Overlapping public APIs | e.g. CTE/lateral overloads with multi-arg dispatch in `selecto.ex` — easy to hit wrong clause |

---

## Idiomatic wins (keep these)

- **Struct + derived Inspect** for the core query type.
- **Iodata SQL building** + `{:param, v}` markers — good for performance and injection resistance when used consistently.
- **Tagged errors** via `%Selecto.Error{}` for the execute path (when not over-rescued).
- **Telemetry spans** around execution.
- **Delegation** of fields/query/tenant into modules (directionally good; finish the job on `Selecto` itself).
- **Adapter callbacks** for quoting / features — right abstraction.

---

## Prioritized recommendations

1. **Security/reliability:** ban new `String.to_atom/1` on external data; fix transformers to default to string keys or existing atoms only; fail closed if Task supervisor is down.
2. **SQL:** treat `raw_sql` as trusted escape hatch with docs/warnings; harden identifier quoting (escape quotes; always use adapter).
3. **Error model:** stop blanket `rescue error`; use typed exceptions; convert only known DB failures.
4. **Structure:** split `prep_selector` and shrink `Selecto` to a pure façade; delete no-ops / dead TODO paths.
5. **Normalize keys** once at domain load.
6. **Turn Credo up** and add Dialyzer CI if not already required on PRs (`precommit` has credo but weak config).
7. **Builders:** replace `list ++ [x]` accumulates with prepend/reverse.

---

## Severity summary

| Severity | Themes |
|---|---|
| **High** | Atom leaks; unsupervised spawn fallback; raw SQL / quote edge cases; over-broad rescue |
| **Medium** | God modules; dual key types; `++` list growth; bare raises; throw/catch; global GenServers |
| **Low** | Dead code; TODOs; weak Credo; inconsistent API raising vs tuples |
