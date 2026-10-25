# ADR-0004: Client-Side Meta-Variable Evaluation Replacing Custom Valkey Modules

- **Status:** Proposed
- **Date:** 2026-07-12
- **Authors:** MDAI Team

## Context

Meta variables (`metaPriorityList`, `metaHashSet`) are implemented as custom Valkey modules written in Rust (`valkey_modules` repository), compiled into a custom Valkey image. Each module stores a *link object* — the list of referenced variable keys — as a module-defined data type, and evaluates it server-side at read time:

- `PRIORITYLIST.GET` iterates the stored refs inside the server, probing each with `TYPE` and reading it with `GET` or `SMEMBERS`, returning the first non-empty value (`prioritylist/src/priority_list.rs`, `get_priority_list_value_from_refs`).
- `HASHSET.LOOKUP` reads the referenced string variable and uses its current value as the field for an `HGET` on the referenced map variable (`hashset/src/hashset.rs`).

The link objects are materialized copies of configuration that already exists elsewhere: `VariableRefs` is a field of the `MdaiHub` CR (`api/v1/mdaihub_types.go`), projected by the operator into the `<hub>-variables-schema` ConfigMap (`variableSchemaConfigMapEntry.VariableRefs` in `internal/controller/hub_adapter.go`), and parsed by the gateway into its `Definition` type (`mdai-gateway/internal/variables/service.go`). Exactly two components consume the module commands: the gateway through `mdai-data-core`'s `Resolve`, and the operator through `GetOrCreateMetaPriorityList`/`GetOrCreateMetaHashSet` during reconciliation. The event-hub neither reads meta variables nor accepts writes to them (`determineCommandType` classifies them as read-only); the gateway REST layer restricts mutations to manual variables.

This arrangement carries costs disproportionate to the logic it hosts:

### 1. Custom image and undeclared runtime configuration

Valkey runs from a custom image (`decisiveai/valkey`) that embeds the compiled modules. The image build in `valkey_modules/Dockerfile` uses base `valkey/valkey:8.1.3` while the deployed tag is `9.0.1` — the build definition and the running artifact have drifted. The `loadmodule` directives (and `notify-keyspace-events`) appear in no production deployment artifact: they exist only in `mdai-operator/test/test-samples/valkey-values.yaml`, in the value syntax of a different (bitnami) chart. The in-house valkey chart's configuration hook (`valkeyConfig`) is unset in `mdai-hub/values.yaml`, and its init script writes only auth, TLS, and replication settings. The configuration the variable system depends on is owned by nothing under version control.

### 2. Persistence coupled to module versioning

Module data types define their own RDB encoding. `type_rdb_load` rejects any encoding version other than 0 by returning null, which fails the RDB load. A module or Valkey upgrade that changes encoding — or a stock Valkey started against a dataset containing module-typed keys — renders the dataset unloadable. Plain strings, hashes, and sets carry no such coupling.

### 3. Process-wide blast radius

A panic in module code aborts the Valkey server process, which also holds audit streams, trace buffers (tracealyzer), and all variable state. A defect in equivalent Go code fails one request with an error.

### 4. Untyped cross-language contract with duplicated invariants

Go invokes the modules through raw command strings (`Arbitrary(PriorityListGetOrCreateCommand)` in `mdai-data-core/variables/adapter.go`); no compile-time check or cross-language contract test exists. Invariants are owned twice: the admission webhook enforces ref immutability (`validateMetaVarRefs` in `internal/webhook/v1/mdaihub_webhook.go`) and the module enforces it again as `REPLACE_FORBIDDEN`. The two owners already disagree: the webhook admits a priority list with a single ref (it checks only non-empty), while the module rejects fewer than two refs (`AT_LEAST_TWO_REFS_REQUIRED`) — an admitted CR fails later at reconcile time.

### 5. A second toolchain for small logic

The evaluation semantics — "first non-empty referenced variable" and "map lookup keyed by another variable's value" — occupy roughly 150 lines of Rust per module plus a nightly-Rust cross-compilation pipeline. The `streams` module in the same repository is an unused prototype, already excluded from the image build.

## Decision

**Evaluate meta variables in the control plane. `mdai-data-core` performs the evaluation client-side from the declared `VariableRefs`; the link objects, the module commands, and the custom Valkey image are removed. Valkey runs a stock upstream image.**

Specifically:

1. **The refs travel with the schema, not the store.** Evaluation consumes `VariableRefs` from where it is already declared and distributed: the `MdaiHub` spec (operator) and the `<hub>-variables-schema` ConfigMap (gateway). The `Reader` interface in `mdai-data-core/variables/resolve.go` drops `GetMetaPriorityList`/`GetMetaHashSet`; `Resolve` accepts the refs and the referenced variables' declared data types.

2. **Evaluation preserves module semantics with an atomic snapshot.**
   - `metaPriorityList`: all refs are read in a single `MULTI/EXEC` transaction — `GET` or `SMEMBERS` selected by each ref's *declared* data type, replacing the module's runtime `TYPE` probe — and the first non-empty value wins. Refs that are missing or not declared in the schema are skipped, matching the module's behavior for unexpected types.
   - `metaHashSet`: `GET` of the selector ref and `HGETALL` of the map ref in one transaction; the field is selected client-side. A null selector yields null, matching `HASHSET.LOOKUP`.
   - A transaction (or Lua script) is required, not a pipelined `DoMulti`: the module evaluates atomically today, and the replacement preserves that snapshot property.

3. **The operator stops writing link objects.** `handleMetaVariable` calls the shared evaluator with the CR's refs; the `GetOrCreateMeta*` calls and the create/replace ceremony against immutable store objects are removed. Serialization into the variables ConfigMap is unchanged.

4. **The webhook becomes the sole owner of ref invariants.** The priority-list minimum of two refs — currently enforced only inside the module — moves into `validateMetaVarRefs`, closing the admitted-but-unreconcilable gap.

5. **The `valkey_modules` repository is retired.** The unused `streams` prototype is deleted with it. The chart's image reference moves to the upstream `valkey/valkey` image.

6. **Migration order: readers first, data second, image last.** Components ship with client-side evaluation while the modules are still loaded (the link keys become unread). The hub-prefixed link keys are then deleted (`DEL` operates on module types; the finalizer's prefix scan already covers the key shape). Only after no module-typed keys remain may a persistent deployment switch to the stock image — an RDB containing module-typed keys does not load without the modules.

## Consequences

### Positive

- **Stock Valkey image.** Upstream upgrades apply directly; the custom build pipeline, the base-image/tag drift, and the undeclared `loadmodule` configuration dependency are eliminated.
- **Persistence decoupled from module versioning.** No RDB encoding ownership; datasets remain loadable across upgrades.
- **Reduced blast radius.** Evaluation defects fail a single request in Go instead of aborting the shared store process.
- **One owner per invariant.** Ref validation lives entirely in the admission webhook; the existing webhook/module divergence on minimum refs is resolved rather than perpetuated.
- **Testable evaluation.** The logic lands beside `resolve_test.go`'s existing fakes with ordinary unit tests, logs, and traces, replacing logic testable only against a modules-enabled Valkey.
- **Smaller writer surface.** The operator's runtime Valkey writes reduce to finalizer cleanup, simplifying the variable store's reader/writer matrix.

### Negative

- **Evaluation ships with each reader.** The gateway and the operator must run `mdai-data-core` versions with compatible evaluation semantics; during a rolling upgrade the two may briefly differ. The semantics are small and stable, and both components already share the library, but version skew becomes a correctness consideration where the store previously guaranteed uniformity.
- **One command becomes one transaction.** Meta reads cost a `MULTI/EXEC` round trip instead of a single command. At control-plane request rates (UI reads, reconciles) this is negligible.
- **A transactional read pattern enters the codebase.** Existing multi-key operations use pipelined `DoMulti` (`handlers/adapter.go`); the evaluator must not regress to that pattern, since a pipeline does not provide the snapshot the module gives today.
- **Migration is ordered and irreversible at the last step.** Persistent deployments must complete link-key deletion before the image swap; skipping the order leaves Valkey unable to start.

### Neutral

- **The gateway REST contract is unchanged.** Octant-ui continues to read variable definitions and resolved values through the same endpoints with the same response shapes; only `Resolve`'s internals change.
- **Meta variables remain read-only** on the REST and event paths; no consumer-visible behavior changes.
- **The event-hub is untouched.** It neither reads nor writes meta variables.
- **The `notify-keyspace-events` dependency is out of scope.** The operator's change-notification channel is addressed separately; this decision only removes the module portion of the undeclared Valkey configuration.

## Alternatives Considered

### A. Keep the modules and harden their operations

Declare `loadmodule` in the production chart, add cross-language contract tests, and gate Valkey upgrades on a module compatibility matrix.

**Rejected because:** The recurring costs remain — every Valkey upgrade re-runs the compatibility work, RDB encoding stays owned by the modules, a module panic still aborts the shared store, and the duplicated invariants remain duplicated. Hardening pays interest on the coupling instead of removing it.

### B. Replace the modules with Lua scripts

Server-side evaluation via `EVAL`, keeping atomicity and single-round-trip reads without a custom binary.

**Rejected because:** A client-side `MULTI/EXEC` transaction already provides the atomic snapshot, so Lua adds no capability — while retaining the drawbacks of out-of-process logic: scripts distributed out-of-band, no type checking, and testing only against a live server.

### C. Materialize meta values on write

Recompute and store each meta variable as a plain key whenever a referenced variable changes.

**Rejected because:** This converts a read-time join into write-time fan-out requiring dependency tracking from every ref to every dependent meta variable — and every writer must participate. Manual variables are documented as writable directly in Valkey by external systems (`docs/variables.md`), and those writes would bypass recomputation, leaving stale meta values.

### D. Move variable storage out of Valkey entirely

Store variables in CRD status or ConfigMaps and evaluate in the operator.

**Rejected because:** This is a larger architectural decision about the variable store itself, with its own trade-offs (API server write rates, external writer contract, gateway read path). Removing the modules neither requires nor precludes it; scoping this decision to evaluation keeps it independently shippable.
