# ADR-0003: Replace Alertmanager with Direct Rule-State Evaluation in the Automation Path

- **Status:** Proposed
- **Date:** 2026-07-12
- **Authors:** MDAI Team

## Context

Alert-driven automation currently flows through Alertmanager. The full path is:

1. The operator renders `PrometheusRule` resources from the `MdaiHub` spec (`internal/controller/hub_adapter.go`).
2. Prometheus evaluates the rules, tracking `for`-duration and firing/resolved state.
3. Alertmanager receives firing alerts and delivers them to the gateway's `/alerts/alertmanager` webhook, applying its grouping, batching, retry, and `repeat_interval` semantics.
4. The gateway translates the payload into `MdaiEvent`s and publishes them to NATS JetStream (`mdai-gateway/internal/adapter/promalert.go`).
5. The event-hub consumes the events and applies variable changes.

Alertmanager was adopted for its built-in alert-management features: grouping, deduplication, silences, inhibition, routing, and delivery retry. In practice the automation path uses none of them as features, while its delivery semantics generate work downstream.

### 1. Delivery semantics require three compensating deduplication layers

Alertmanager re-sends alerts on `group_interval` and `repeat_interval`, retries failed deliveries, and batches multiple alerts per payload. The gateway compensates with three stacked mechanisms:

- An in-memory `Deduper` with a peek-at-adaptation / commit-after-publish protocol (`mdai-gateway/internal/adapter/deduper.go`). It carries a `TODO` for a missing TTL (the map grows without bound), resets on gateway restart, and is not shared across gateway replicas.
- Within-payload deduplication via `batchLatest` and staleness checks against `changeTime` (`promalert.go`).
- A deterministic event ID — dedupe key plus change-time nanoseconds (`alertEventID` in `promalert.go`) — used as the NATS message ID against JetStream's deduplication window.

Each layer exists to reconstruct "exactly one event per state transition" from a notification stream designed to repeatedly tell a human "this is still firing."

### 2. The load-bearing features live in Prometheus, not Alertmanager

The automation loop depends on PromQL evaluation, `for`-duration handling, and the firing/resolved lifecycle. All three are provided by Prometheus rule evaluation. Alertmanager contributes only transport — and the transport is the source of the problems above.

### 3. Alert identity is not owned by the platform

Alertmanager fingerprints hash only the label set; `hub_name` travels as an annotation. Two hubs can therefore share a fingerprint, which caused a cross-hub event-routing defect. The gateway now qualifies dedupe keys with the hub name (`dedupeKey` in `promalert.go`), but the identity scheme remains inherited rather than owned, and `changeTime` must be reconstructed from `startsAt`/`endsAt`.

### 4. Loop latency is nondeterministic

End-to-end automation latency includes Alertmanager's `group_wait`, `group_interval`, and webhook retry backoff on top of the Prometheus evaluation interval. These parameters are tuned for human notification and make the control-loop reaction time an emergent property rather than a configured one.

### 5. Silences pause automation invisibly

An Alertmanager silence suppresses automation events, but nothing in the `MdaiHub` resource reflects that state. An operator inspecting the hub CR cannot see that its automation is effectively disabled.

## Decision

**Remove Alertmanager from the automation event path. The event-hub hosts a rule-state evaluator that reads alert state directly from Prometheus and publishes the same `MdaiEvent`s on state transitions. Alertmanager remains available for human notification only.**

Specifically:

1. **Rule generation is unchanged.** The operator continues to render `PrometheusRule` resources from the `MdaiHub` spec; Prometheus remains the evaluation engine for PromQL and `for`-duration state.

2. **The event-hub polls the Prometheus rules API.** A rule-state evaluator in the event-hub queries `/api/v1/rules` on a configured interval (default 15s). The event-hub already holds every dependency the evaluator needs: a NATS publisher wired into its `HandlerAdapter` (`cmd/mdai-event-hub/deps.go`), the audit adapter, the Valkey client, and — decisively — the `<hub>-automation` ConfigMaps it watches via `NewHubConfigMapController`, whose compiled `AlertTrigger` definitions are exactly the filter for which rules feed automation.

3. **Transitions are derived, not delivered, and evaluator state is durable.** The evaluator maintains last-observed state per alert instance in Valkey and emits a firing event on the inactive→firing edge and a resolved event when a previously firing instance leaves the active set. Polling is level-based: missed polls converge on the next read. Because the "was firing" set survives process restarts, a resolution that occurs while the evaluator is down is still detected and emitted on the next poll — a case that is silently lost today once Alertmanager's retry budget is exhausted.

4. **Poll ticks are single-writer; replicas are unchanged.** The event-hub keeps its horizontally scaled consumer-group deployment. Each poll interval, one replica acquires a Valkey `SET NX PX` tick lock and evaluates; state lives in Valkey, so any replica may win any tick. A rare duplicate evaluation is benign: the deterministic event ID deduplicates at the JetStream window.

5. **Alert identity is platform-owned.** The alert instance key is computed from the full label set qualified by hub identity, replacing the Alertmanager fingerprint plus hub-annotation scheme. The transition timestamp is the evaluator's observation of the edge, removing `startsAt`/`endsAt` reconstruction.

6. **Event contract and downstream consumers are unchanged.** The evaluator produces the same `MdaiEvent` shape, subjects, and audit entries, published to JetStream and consumed by the existing alert consumer group — self-published events follow the hop-limit discipline the event-hub already applies to its follow-on events. The deterministic event ID (instance key plus transition time) is retained as the JetStream message ID.

7. **The gateway sheds the machine alert path.** The `Deduper`, batch staleness tracking, and hub-qualified fingerprint handling are deleted once the webhook path is retired. The `/alerts/alertmanager` endpoint remains during migration and is removed afterward. External alert sources with their own webhook contracts (e.g. Datadog) are unaffected. With the per-replica dedup state gone, the single-replica constraint on the gateway deployment is lifted.

8. **Automation pause is an explicit spec field.** A `paused` field on the `MdaiHub` automation entry replaces silences as the mechanism for suspending automation, making the state visible in the CR and independent of notification infrastructure.

## Consequences

### Positive

- **One delivery semantic.** Edge detection happens at a single point that reads authoritative state. The three-layer deduplication stack in the gateway is deleted rather than hardened.
- **Deterministic loop latency.** Reaction time is the Prometheus evaluation interval plus the poll interval — both explicit configuration — instead of notification-pipeline timing.
- **Platform-owned alert identity.** The cross-hub fingerprint collision class is eliminated structurally, not patched.
- **Resolved transitions survive downtime.** Durable last-observed state in Valkey means a fire-and-resolve spanning an evaluator outage still produces the resolved event; today the equivalent notification is lost once Alertmanager's retries are exhausted.
- **Visible automation state.** Pausing automation is a spec-level operation, auditable and observable in the hub CR.
- **Fewer runtime dependencies in the loop.** Alertmanager availability, configuration, and upgrade cycle no longer affect automation correctness.
- **The gateway becomes horizontally scalable.** Its single-replica pin exists only to contain per-replica dedup state; removing the machine alert path removes the constraint.
- **The operator's dependency set is unchanged.** The reconciler remains free of NATS and Prometheus API dependencies, preserving its role as a pure Kubernetes convergence loop.

### Negative

- **Direct Prometheus dependency.** The event-hub requires network access and credentials for the Prometheus rules API; Prometheus unavailability stalls automation events (today the same outage stalls alert delivery one hop earlier). A hung rules-API call must not block event processing: the evaluator runs on its own goroutine with bounded request timeouts.
- **Tick coordination adds a Valkey-dependent code path.** The `SET NX PX` lock makes polling depend on Valkey availability. This introduces no new failure mode — the event-hub cannot process events without Valkey today — but the lock TTL and poll interval must be tuned together to avoid overlapping evaluations.
- **Polling latency floor.** A transition is observed at worst one poll interval after Prometheus records it. The interval is a tradeoff between reaction time and query load.
- **Transitions shorter than one poll interval are not observed.** An alert that fires and resolves entirely between polls produces no event. Level-based rules re-fire while their condition holds, so persistent conditions are not lost; the equivalent loss exists today when the gateway is unreachable beyond Alertmanager's retry budget.
- **CRD and UI change.** The `paused` field requires a `MdaiHub` schema addition and corresponding UI support.
- **Migration steps.** Existing deployments must drop the Alertmanager route to the gateway, enable the evaluator, and — after a transition period — remove the webhook endpoint. Alertmanager configuration shipped by the `mdai-hub` chart is reduced to human-notification routes.

### Neutral

- The `kube-prometheus-stack` dependency continues to ship Alertmanager; it serves human notification (Slack, PagerDuty) outside the control loop.
- The `PrometheusRule` contract, the event-hub's consumer-side processing, JetStream configuration, and audit trail are unchanged.
- The event-hub already publishes events (its `HandlerAdapter` emits follow-on events with hop-limit protection), so producing alert events introduces no new architectural role.

## Alternatives Considered

### A. Harden the existing webhook path

Keep Alertmanager and fix the compensation layer: add a TTL to the `Deduper`, move its state to Valkey for cross-replica sharing, and tighten `group_wait`/`repeat_interval` for lower latency.

**Rejected because:** This treats symptoms. The semantic mismatch — notification stream consumed as a state-transition feed — remains, along with nondeterministic latency and inherited alert identity. Every future defect in this area lands in the same compensation code.

### B. Implement the Alertmanager API in the gateway

Point Prometheus's `alerting` configuration directly at the gateway by implementing the Alertmanager v2 push API, removing the Alertmanager hop without polling.

**Rejected because:** Prometheus re-sends active alerts on its own resend interval, so the deduplication problem survives; the gateway takes on maintaining compatibility with an external API contract; and push delivery still provides notifications rather than readable state, so a missed delivery is lost rather than converged on the next poll.

### C. Embed a PromQL rules engine in the evaluator

Evaluate alert expressions in-process against the Prometheus query API, owning `for`-duration tracking and alert lifecycle directly.

**Rejected because:** This reimplements rule-state machinery Prometheus already provides and that the rules API already exposes, including pending (`for`-in-progress) state. It becomes the right design only if the operator-managed Prometheus is itself removed; the evaluator introduced here isolates that potential future change to one component.

### D. Watch the `ALERTS` metric instead of the rules API

Poll the synthetic `ALERTS{alertstate=...}` series via the query API.

**Rejected because:** The rules API is purpose-built for this read: it returns rule health, evaluation errors, annotations, and pending state without query construction. The `ALERTS` series adds no information and loses rule-health visibility.

### E. Host the evaluator in the gateway

Place the evaluator in the gateway, which receives the Alertmanager webhook today and constructs the alert events.

**Rejected because:** The gateway's deployment is pinned to a single replica solely to contain the per-replica dedup state this ADR deletes (`deployment/templates/deployment.yaml`); hosting a stateful poller there would make that pin permanent and forfeit the gateway's return to horizontal scaling. Its existing Prometheus client serves only the legacy connection code superseded by Octant. The producer machinery it appears to own — publisher, audit, event construction — is `mdai-data-core` library code available identically to the event-hub, and the alert-specific adaptation in `internal/adapter/` is predominantly the compensation layer being removed.

### F. Host the evaluator in the operator

Run the evaluator as a leader-elected Runnable in the operator, next to the rule generation it consumes.

**Rejected because:** It adds NATS as an operator runtime dependency, coupling the Kubernetes convergence loop to app-plane messaging infrastructure. The co-location advantage is illusory: the rule-to-automation mapping the evaluator needs already flows to the event-hub through the `<hub>-automation` ConfigMap, so the operator holds no information the event-hub lacks. The genuine benefits — free singleton semantics via leader election and direct `MdaiHub.status` reflection — are matched by the Valkey tick lock and are separable (the operator can surface firing state in status from the audit stream independently of where evaluation runs).

### G. Dedicated evaluator service

Ship the evaluator as its own deployment with a single replica.

**Rejected because:** The evaluator needs Valkey for durable state and NATS to publish — the event-hub's exact dependency set — so a dedicated service is an event-hub replica with additional packaging: an image, chart, RBAC, and monitoring surface for a poll loop and an edge detector. Hard blast-radius isolation from event processing is the only distinct benefit and does not justify the operational cost; the bounded-timeout goroutine isolation in the chosen design addresses the same risk.
