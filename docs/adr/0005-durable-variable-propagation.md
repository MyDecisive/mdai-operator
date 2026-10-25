# ADR-0005: Durable Variable Propagation to the Operator

- **Status:** Proposed
- **Date:** 2026-07-12
- **Authors:** MDAI Team

## Context

The operator learns that variable state changed in Valkey through keyspace notifications: a pattern subscription on `__keyspace@0__:variable/*` feeds a `GenericEvent` channel that enqueues hub reconciliation (`internal/controller/mdaihub_controller.go`, `startValkeySubscription`). Reconciliation then re-renders the `<hub>-variables` ConfigMap and rolls the collectors whose consumed variables changed. This trigger path is the sole low-latency link between a variable write and the data plane, and it is defective on several independent grounds.

### 1. The subscription is single-shot

`valkeyClient.Receive` returns on unsubscribe, client close, context cancellation, or command failure (vendored `valkey-go` contract). The operator invokes it once; on any return the handler logs an error and exits the goroutine. There is no resubscribe loop — the exponential backoff nearby (`initializeValkey`) covers only initial client creation. After a Valkey failover or dropped connection, variable changes stop propagating until the operator pod restarts.

### 2. There is no meaningful backstop

`cmd/main.go` configures no `SyncPeriod`, so the controller-runtime default (approximately ten hours) is the only periodic reconcile. Combined with the single-shot subscription, the staleness window after a dropped connection is unbounded for operational purposes.

### 3. Delivery is fire-and-forget on an unmanaged goroutine

Keyspace notifications carry no durability: a message published while the operator is disconnected is lost. The subscription runs as a bare `go` statement with `context.Background()` on every operator replica, ignoring both leader election and manager shutdown.

### 4. The enabling configuration is owned by nothing

`notify-keyspace-events` appears in no production deployment artifact. It exists only in `mdai-operator/test/test-samples/valkey-values.yaml`, in the value syntax of a different (bitnami) chart. The in-house valkey chart's configuration hook (`valkeyConfig`) is unset in `mdai-hub/values.yaml`, and its init script writes only auth, TLS, and replication settings. The trigger mechanism the data plane depends on is configured outside version control.

### 5. The signal is poorly shaped for the consumer

Notifications fire once per key operation with no payload and no batching: a ten-command automation batch produces ten triggers. Each message costs a cross-namespace hub lookup (`findHubNamespace`) to resolve the reconcile target.

### 6. Not every writer can signal

Variable state has three writers: the event-hub executing automation and REST-initiated commands (mediated, audited), the operator's own reconcile-internal writes, and external systems writing manual variables directly to Valkey — a documented contract (`docs/variables.md`). Any push-based trigger owned by the platform covers only the mediated writes; external direct writes emit nothing the platform controls. Keyspace notifications were the one mechanism that observed all writers, which is why correctness and transport were fused into a single fragile channel.

## Decision

**Propagation correctness comes from a periodic per-hub reconcile floor; propagation latency comes from a commit signal the event-hub writes to the `MdaiHub` resource. The operator's Valkey subscription is removed.**

Specifically:

1. **A reconcile floor bounds staleness for every writer.** Each `MdaiHub` is requeued on a configured interval (default 60s). Reconciliation is already idempotent and diff-based — `diffEnvMapKeys` plus the consumed-variable check restart nothing when state is unchanged — so periodic runs cost a handful of Valkey and cached Kubernetes reads per hub. Worst-case propagation delay for any write, from any writer including external direct writers, is the interval.

2. **The event-hub signals commits for low-latency propagation.** After successfully executing a command batch, the event-hub patches a revision annotation (`mydecisive.ai/variables-rev`) on the target `MdaiHub`. The hub controller's primary watch carries no restricting predicate (`For(&mdaiv1.MdaiHub{})`), so the patch triggers reconciliation through standard watch machinery — durable, deduplicated, leader-aware, and replayed after operator restarts. The signal is batch-granular: one patch per command batch, not one trigger per key operation. The event-hub already constructs the operator clientset (`cmd/mdai-event-hub/deps.go`); its RBAC gains `patch` on `mdaihubs`.

3. **The signal is advisory; the floor is authoritative.** A crash between the Valkey commit and the annotation patch converges within one floor interval. Concurrent patches from event-hub replicas are idempotent (the annotation value is the triggering event ID; conflicts retry). A stripped or mangled annotation causes at most one redundant reconcile.

4. **The subscription path is deleted.** `startValkeySubscription`, the `ValkeyEvents` channel, the `WatchesRawSource` wiring, and the per-message `findHubNamespace` lookup are removed. The operator retains its Valkey client for reads during reconciliation; only the pub/sub dependency goes. With ADR-0004 removing `loadmodule`, no MDAI component requires any Valkey server configuration beyond the defaults, and the undeclared `notify-keyspace-events` dependency disappears rather than being adopted.

5. **The external-writer contract is preserved with an explicit bound.** Direct Valkey writes to manual variables remain supported and converge within the floor interval; `docs/variables.md` states the bound. Mediated writes — the automation loop, where latency matters — propagate at watch latency via the commit signal.

6. **Cutover is direct.** No release runs both mechanisms: the floor bounds the risk of the new path underperforming, and the old path's failure mode (silent, unbounded staleness) is strictly worse than the new path's (bounded delay).

## Consequences

### Positive

- **Bounded staleness replaces unbounded staleness.** Today a dropped subscription silently freezes propagation until a pod restart, with a ten-hour resync as the ceiling; after this change the worst case for any write is the floor interval, and the common case (automation) is watch latency.
- **The operator's trigger inputs become Kubernetes-native.** Watches and periodic requeues, with leadership, shutdown, replay, and deduplication handled by controller-runtime — the unmanaged goroutine and its reconnect edge cases are deleted, not hardened.
- **The undeclared Valkey configuration dependency is removed.** Together with ADR-0004, the platform runs against stock Valkey defaults.
- **Triggering is batch-granular.** One reconcile trigger per command batch replaces one per key operation, and the per-message cross-namespace hub lookup disappears.
- **Composability with ADR-0003.** After both decisions, every hop in the automation loop is either a JetStream durable log or a level-triggered Kubernetes read; no fire-and-forget channel remains.

### Negative

- **External direct writes lose instant propagation.** They converge within the floor interval (default 60s) instead of sub-second. Per `docs/variables.md` these are static or externally managed configuration values; the bound is documented. This is strictly better than the current behavior once the current path's silent-death mode is accounted for, but it is a visible latency change for a working system.
- **The event-hub writes to a resource it does not own.** A revision annotation on the `MdaiHub` CR is an informal signal channel; it requires RBAC (`patch` on `mdaihubs`), documentation, and tolerance for the annotation being stripped (harmless — one redundant reconcile).
- **Periodic reconcile churn.** Every hub reconciles every interval regardless of activity. At current scale (per-hub variable counts and hub counts) the cost is negligible; the interval is configurable if scale changes the calculus.

### Neutral

- The operator keeps its Valkey read client; reconciliation continues to read variable state from Valkey when rendering ConfigMaps.
- Write governance — whether external writers should be required to use the gateway REST API for audit and ACL reasons — is explicitly out of scope. The floor makes propagation correctness independent of that decision, which can proceed on its own merits.
- The restart machinery downstream of the trigger (consumed-variable check, ConfigMap diff, `restartedAt`) is unchanged by this decision.

## Alternatives Considered

### A. Harden the keyspace subscription

Add a resubscribe loop with backoff, run it as a leader-elected Runnable, declare `notify-keyspace-events` in the production chart, and add a floor as backstop.

**Rejected because:** The floor is required anyway; once it exists, the subscription's only contribution is low latency for unmediated writes — precisely the writes least sensitive to latency. In exchange the platform keeps the pub/sub client machinery, its reconnect edge cases, a per-key-operation signal shape, and permanent ownership of a Valkey server configuration setting. Hardening pays maintenance on a channel whose remaining value is marginal.

### B. Reconcile floor only, no fast path

Drop the commit signal and rely solely on the periodic requeue.

**Rejected because:** Automation actuation would pay up to a full interval before collector rollout begins, on top of rollout time — roughly doubling actuation latency for the latency-sensitive path. The commit signal is a small addition (one patch per batch) that preserves current automation responsiveness.

### C. A dedicated revision ConfigMap instead of a CR annotation

The event-hub writes a per-hub revision ConfigMap that the operator watches.

**Rejected because:** It requires an additional object per hub and new watch wiring, while the annotation rides the controller's existing primary watch. Failure modes and semantics are otherwise identical. The ConfigMap variant remains acceptable if writing to the CR proves undesirable in practice.

### D. Trigger through NATS

The operator subscribes to variable events on JetStream and requeues hubs from them.

**Rejected because:** It adds NATS as an operator runtime dependency — rejected on the same grounds in ADR-0003 (Alternative F) — and an at-least-once consumer brings offset and redelivery machinery where the level-based floor already provides correctness.

### E. Close the write surface first

Require all writes through the gateway REST API, making the mediated commit signal cover every writer, then drop the floor to a long-interval safety net.

**Rejected as a prerequisite:** It couples a contract-breaking governance change to a correctness fix that does not need it. The floor covers unmediated writers at bounded cost. Write-surface consolidation remains a worthwhile independent decision for audit completeness and ACL tightening, and this design loses nothing if it lands later.
