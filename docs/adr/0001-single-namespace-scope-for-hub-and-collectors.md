# ADR-0001: Single-Namespace Scope for Hub and Collectors

- **Status:** Proposed
- **Date:** 2026-03-26
- **Authors:** MDAI Team

## Context

The MDAI operator manages several custom resources (MdaiHub, MdaiCollector, MdaiObserver, MdaiIngress, MdaiReplay, MdaiDal). Currently, the MdaiHub controller operates under a cross-namespace model: it discovers OpenTelemetryCollectors across all namespaces via label selectors and creates variable ConfigMaps in each collector's namespace.

This design introduces several problems:

### 1. Orphaned cross-namespace ConfigMaps

When the Hub creates ConfigMaps in a collector's namespace (via `syncComputedConfigMapsAndRestart`), it cannot set a Kubernetes owner reference because `SetControllerReference` requires the owner and owned object to be in the same namespace. These ConfigMaps have no owner and are never garbage collected -- not by Kubernetes, and not by the Hub's finalizer, which only cleans up PrometheusRules and Valkey keys.

### 2. Hub name uniqueness assumed but not enforced

`findHubNamespace` lists all MdaiHubs across all namespaces and returns the first match by name. The code comments acknowledge this assumption:

```go
// Assuming that hub names are unique across namespaces, take the first match
```

If two MdaiHubs share the same name in different namespaces:
- Valkey key-change events route to the wrong Hub.
- `requeueByLabels` triggers reconciliation for the wrong Hub.
- Variable ConfigMaps could be written to unintended namespaces.

Nothing in the CRD, webhook layer, or admission control enforces this uniqueness.

### 3. Cluster-wide list operations

`listOtelCollectorsWithLabel` queries collectors across all namespaces with no scope restriction. `findHubNamespace` lists all MdaiHubs across all namespaces. These are expensive operations that scale poorly with cluster size and increase the blast radius of any permission misconfiguration.

### 4. Redundant and collision-prone RBAC resources

The MdaiCollector controller creates a ClusterRole, ClusterRoleBinding, and ServiceAccount per collector instance. All three are identical across instances -- the ClusterRole grants the same hardcoded read-only permissions (pods, nodes, namespaces, deployments, etc.), the ServiceAccount has no collector-specific configuration, and the ClusterRoleBinding simply links the two.

The cluster-scoped resource names are derived from `collectorCR.Name` without including the namespace (via `getScopedMdaiCollectorResourceName`). Because MdaiCollector is namespaced, two collectors with the same name in different namespaces produce identical ClusterRole and ClusterRoleBinding names, causing them to overwrite each other on every reconciliation. Either collector's finalizer would also delete the other's RBAC resources.

Additionally, these cluster-scoped resources cannot have a namespaced owner reference, and the collector's finalizer does not delete them, resulting in RBAC resource leaks on every MdaiCollector deletion.

### 5. Inconsistency across controllers

MdaiIngress already assumes the referenced OtelCollector is in the same namespace (`Namespace: req.Namespace`). The Hub's cross-namespace model is the outlier. This inconsistency increases cognitive load and makes the codebase harder to reason about.

## Decision

**Require all resources managed by a single MdaiHub -- including OpenTelemetryCollectors, MdaiCollectors, and their dependent objects -- to reside in the same namespace as the Hub.**

Specifically:

1. **Scope collector discovery to the Hub's namespace.** `listOtelCollectorsWithLabel` will filter by `client.InNamespace(c.mdaiCR.Namespace)` instead of listing across all namespaces.

2. **Remove `findHubNamespace`.** The Valkey subscription handler and `requeueByLabels` will resolve the Hub's namespace directly from the reconciliation request or from a namespace-qualified label, eliminating the cluster-wide Hub listing.

3. **Set owner references on all same-namespace resources.** Variable ConfigMaps, PrometheusRules, and any other resources created by the Hub can use `SetControllerReference` since owner and owned object share a namespace. Kubernetes GC handles cleanup automatically.

4. **Consolidate RBAC to shared cluster-scoped resources.** Replace the per-collector ClusterRole, ClusterRoleBinding, and ServiceAccount with shared resources:

   - **One ClusterRole** (`mdai-collector-role`) deployed with the operator (e.g., via Helm chart), containing the read-only permissions all collectors need. The operator does not manage this resource at runtime.
   - **One ClusterRoleBinding** (`mdai-collector-rb`) with a subjects list managed by the collector reconciler. Each collector ensures its namespace's ServiceAccount is present in the subjects list. On deletion, the finalizer removes its entry.
   - **One ServiceAccount per namespace** (`mdai-collector-sa`), shared by all collectors in that namespace. Since all ServiceAccounts are identical and namespaced, there is no collision risk and no reason to create one per collector.

   This eliminates the naming collision bug, the RBAC leak on deletion, and the per-instance duplication of identical resources.

5. **Namespace-qualify Valkey keys.** Change the key pattern from `variable/{hubName}/{key}` to `variable/{namespace}/{hubName}/{key}` so that identically named Hubs in different namespaces do not collide.

## Consequences

### Positive

- **Automatic garbage collection.** Owner references on all namespaced resources mean Kubernetes handles cleanup. No more orphaned ConfigMaps.
- **No global uniqueness assumption.** Hub identity is `namespace/name`, the standard Kubernetes convention. Two teams can independently operate Hubs with the same name in their own namespaces.
- **Reduced ad hoc cluster-wide queries.** `findHubNamespace` and its cluster-wide MdaiHub list are removed. Collector discovery is scoped to the Hub's namespace. Note: the manager's cache and informers remain cluster-scoped (needed to serve Hubs across multiple namespaces), so the operator's RBAC permissions do not change. The improvement is in eliminating unnecessary runtime list calls, not in reducing the operator's permission footprint.
- **No RBAC collisions or leaks.** Shared ClusterRole and ClusterRoleBinding eliminate the per-collector naming collision bug and the resource leak on deletion. One ServiceAccount per namespace removes redundant identical accounts.
- **Simpler code.** `findHubNamespace`, cluster-wide list operations, the cross-namespace ConfigMap creation path, and per-collector ClusterRole/ClusterRoleBinding creation are removed. The collector reconciler's RBAC responsibility reduces to managing a single subjects list entry.
- **Consistent pattern.** All controllers (Hub, Collector, Observer, Ingress, Replay, DAL) follow the same same-namespace convention.

### Negative

- **One Hub per namespace for multi-team setups.** A single Hub can no longer serve collectors in multiple namespaces. Teams that need this must deploy one Hub per namespace.
- **Migration required.** Existing deployments using the cross-namespace model will need to relocate collectors into the Hub's namespace or deploy additional Hubs. Orphaned cross-namespace ConfigMaps from the previous model must be manually cleaned up.
- **Breaking change to Valkey key contract.** The key pattern change from `variable/{hubName}/{key}` to `variable/{namespace}/{hubName}/{key}` is a breaking change for external systems. Manual variables are documented as being written directly to Valkey by external systems and users (see `docs/variables.md`). All external writers must be updated to use the new key pattern. The migration plan must include:
  - Identifying all external services that write manual variables to Valkey.
  - Coordinating the key pattern change with those teams.
  - A key migration strategy (e.g., the operator reads from both old and new prefixes during a transition period, or a one-time migration script renames existing keys).
  - Updating documentation and e2e tests that reference the old key shape.

### Neutral

- The MdaiIngress controller already follows this model and requires no changes.
- The MdaiCollector, MdaiObserver, MdaiReplay, and MdaiDal controllers already create all resources in the CR's own namespace and require no changes to resource creation logic.
- The shared ClusterRole must be deployed with the operator (e.g., as a Helm chart resource). This shifts RBAC definition from runtime to deploy-time, which is standard practice for operators.

## Alternatives Considered

### A. Namespace-qualify labels and keys without enforcing co-location

Add a `mydecisive.ai/hub-namespace` label to OtelCollectors and encode namespace in Valkey keys, but continue allowing cross-namespace operation.

**Rejected because:** This fixes the uniqueness issue (#2) but not the orphaned ConfigMap issue (#1). Cross-namespace resources still cannot have owner references, so the operator would need custom finalizer logic to track and clean up ConfigMaps across arbitrary namespaces -- replicating what Kubernetes GC already provides for same-namespace resources.

### B. Enforce hub name uniqueness via a validating webhook

Deploy an admission webhook that rejects MdaiHub creation if a Hub with the same name exists in another namespace.

**Rejected because:** This adds operational complexity (webhook deployment, TLS certificates, availability concerns) and doesn't fix the underlying design issues. Cross-namespace ConfigMaps would still be orphaned, and the operator would still require cluster-wide list permissions. If the webhook is unavailable, the uniqueness guarantee silently disappears.
