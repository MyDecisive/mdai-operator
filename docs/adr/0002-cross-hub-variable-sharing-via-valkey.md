# ADR-0002: Cross-Hub Variable Sharing via Valkey

- **Status:** Proposed
- **Date:** 2026-03-27
- **Authors:** MDAI Team
- **Supersedes:** None
- **Related:** [ADR-0001: Single-Namespace Scope for Hub and Collectors](0001-single-namespace-scope-for-hub-and-collectors.md)

## Context

[ADR-0001](0001-single-namespace-scope-for-hub-and-collectors.md) establishes that a Hub and all its managed resources must reside in the same namespace. This simplifies ownership, garbage collection, and identity, but raises a question: how do teams share global variables across Hubs in different namespaces?

### The variable pipeline today

Variables in the MDAI operator are not static configuration. They follow a runtime pipeline:

1. **Defined** in the MdaiHub CR spec (type, data type, storage type, serializers, transformers).
2. **Stored** in Valkey under the key pattern `variable/{hubName}/{key}`.
3. **Mutated at runtime** by internal services (e.g., automation rules, computed variables) via Valkey.
4. **Materialized** by the Hub reconciler into ConfigMaps that OpenTelemetryCollectors consume.

Any solution for shared variables must participate in this pipeline. A mechanism that bypasses it -- such as referencing an external ConfigMap directly -- would create a second, inconsistent path for variable resolution and lose the benefits of the type system, serializers, transformers, and runtime mutability.

### Use case

A platform team needs to define organization-wide variables (e.g., environment label, compliance tags, shared API endpoints) that every team's Hub should inherit. Individual teams must be able to override specific variables for their own namespace without affecting other teams.

## Decision

**Allow a Hub to reference variables from other Hubs via Valkey reads. Introduce a `variableRefs` field in the Hub spec that points to source Hubs by name and namespace.**

### Spec change

```yaml
apiVersion: hub.mydecisive.ai/v1
kind: MdaiHub
metadata:
  name: team-a
  namespace: team-a
spec:
  variableRefs:
    - hubName: global
      hubNamespace: platform
  variables:
    - key: team-specific-var
      type: computed
      dataType: string
      storageType: built-in-valkey
      serializeAs:
        - name: TEAM_VAR
```

### Reconciliation behavior

During `ensureVariableSynchronized`, the reconciler:

1. **Reads referenced variables.** For each entry in `variableRefs`, reads variables from the referenced Hub's Valkey key prefix (`variable/{hubNamespace}/{hubName}/*`) using the namespace-qualified key pattern from ADR-0001.
2. **Reads local variables.** Reads the Hub's own variables from Valkey as it does today.
3. **Merges with local precedence.** Local variables override referenced variables when keys conflict. The ordering of `variableRefs` entries determines precedence among multiple references (later entries override earlier ones).
4. **Materializes ConfigMaps locally.** The merged variable set is written to ConfigMaps in the Hub's own namespace, with owner references. No cross-namespace writes.

### What crosses namespace boundaries

Only Valkey reads. Valkey is a shared data store with no namespace concept -- reading another Hub's keys is just reading different key prefixes. No Kubernetes cross-namespace resource ownership, no cross-namespace writes, no cross-namespace owner references.

### What stays the same

- The global Hub is a regular MdaiHub CR with the same variable spec (types, serializers, transformers).
- Internal services modify global variables the same way they modify any variable -- through Valkey, against the global Hub's keys.
- The reconciler remains the single bridge from Valkey state to ConfigMaps.
- `SetControllerReference` is used on all materialized ConfigMaps (same namespace as the Hub).

## Consequences

### Positive

- **Full pipeline participation.** Shared variables go through the same type system, serializers, transformers, and runtime mutation as local variables. No second code path.
- **Familiar operational model.** The global Hub is a standard MdaiHub. Teams manage it the same way they manage any Hub -- via the CR spec, GitOps, and internal services.
- **Clean separation from namespace scoping.** The only cross-namespace interaction is a Valkey key read. All Kubernetes resource ownership stays within a single namespace, consistent with ADR-0001.
- **Explicit and auditable.** The `variableRefs` field makes the dependency on external Hubs visible in the CR spec. Operators can see exactly which Hubs a given Hub inherits from.
- **Composable.** A Hub can reference multiple source Hubs with a clear precedence order, enabling layered configurations (e.g., org-wide defaults, department overrides, team-specific values).

### Negative

- **Implicit runtime dependency.** A Hub depends on the referenced Hub's Valkey keys existing and being populated. If the global Hub is deleted or its variables are cleared, downstream Hubs lose those variables on the next reconciliation.
- **Eventual consistency.** When a global variable changes, downstream Hubs pick it up on their next reconciliation cycle, not immediately. There is no push notification from the global Hub to consumers (though the existing Valkey subscription mechanism could be extended to cover this).
- **Precedence can be surprising.** With multiple `variableRefs` entries and local variables, the effective value of a variable depends on ordering. This needs clear documentation.

### Neutral

- The `variableRefs` field is optional. Hubs without it behave exactly as they do today.
- This decision does not require changes to the Valkey schema beyond the namespace-qualified key prefix introduced in ADR-0001.

## Alternatives Considered

### A. External ConfigMap references

Add a `variableSources` field pointing to ConfigMaps by name/namespace. The Hub reads them and merges with its own variables.

**Rejected because:** ConfigMaps are static. This bypasses the Valkey-backed variable pipeline -- no type system, no runtime mutation by internal services, no serializers or transformers. It creates a second, inconsistent code path for variable resolution.

### B. Cluster-scoped GlobalVariable CRD

Introduce a new cluster-scoped CRD (e.g., `MdaiGlobalVariable`) that all Hubs inherit from automatically.

**Rejected because:** It adds a new CRD, a new controller, and a new reconciliation path for functionality that can be achieved with the existing Hub CRD and Valkey infrastructure. It also couples all Hubs to a single global source with no opt-in mechanism.

### C. Distribute global variables via GitOps tooling

Use Kyverno, ArgoCD, or Flux to sync a global ConfigMap into every Hub namespace.

**Rejected as the sole mechanism because:** This works for static configuration but does not support runtime-mutable variables. It also pushes variable management outside the MDAI operator's domain, making it invisible to the Hub's reconciliation logic and the variable type system. However, this approach remains valid for non-variable shared configuration and can complement the `variableRefs` mechanism.
