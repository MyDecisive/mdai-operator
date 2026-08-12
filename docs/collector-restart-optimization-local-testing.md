# Selective Collector Restart — Local Kind Test Plan

Local end-to-end test plan for the operator's selective-restart optimization (commit `3c0724f`). Companion to `mdai-data-core/docs/variable-defaults-local-testing.md` (which covers the variable-defaults feature on the same branch).

## What this plan covers

The optimization changes `syncComputedConfigMapsAndRestart` from "restart every collector whose env-CM namespace changed" to:

1. Compute the env-var ConfigMap **diff** (which keys actually changed) per namespace.
2. For each collector, statically parse `Spec.Config.Yaml()` for `${env:NAME}` references.
3. Restart only collectors whose referenced env vars intersect the changed set.
4. Fall back to wildcard ("always restart") when the config contains indirect refs the parser can't resolve, or when the config YAML can't be parsed at all.

The operator's e2e covers the simplest case: one no-refs collector ignored in one namespace. The fixtures here cover the per-collector decision matrix plus the wildcard fallback path.

## Test gaps not covered elsewhere

| # | Gap | Why it matters |
|---|---|---|
| 1 | Per-collector decisions inside one namespace | The dedup logic computes `changedByNamespace` once per namespace, but evaluates each collector individually. A regression that flips it back to "restart every collector in the namespace" would silently restart non-consumers. |
| 2 | Cross-variable selectivity | Changing variable A must not restart a collector that only consumes variable B. |
| 3 | No-op reconcile | A reconcile that produces an identical CM must not restart anyone. Otherwise the controller thrashes consumers on every reconcile loop. |
| 4 | Wildcard fallback for indirect refs | Without it, a collector using `${env:${PREFIX}_FOO}` silently never restarts on env-CM changes — its real runtime dependency is invisible to the static parser. |

## Prerequisites

- Operator and OpenTelemetry Operator deployed in `mdai`.
- Working directory `mdai-operator/docs/`.
- Helpers:
  ```bash
  HUB=restart-test
  rev() { kubectl get deploy -n mdai "$1-collector" -o jsonpath='{.metadata.annotations.deployment\.kubernetes\.io/revision}'; }
  set_default() {
    local idx=$1 val=$2
    kubectl patch mdaihub "$HUB" -n mdai --type=json \
      -p "[{\"op\":\"replace\",\"path\":\"/spec/variables/$idx/default\",\"value\":$val}]"
    sleep 3
  }
  ```

## Setup

```bash
kubectl apply -f collector-restart-tests/hub.yaml
kubectl apply -f collector-restart-tests/collector-consumer.yaml
kubectl apply -f collector-restart-tests/collector-non-consumer.yaml
kubectl apply -f collector-restart-tests/collector-other-consumer.yaml
kubectl apply -f collector-restart-tests/collector-wildcard.yaml

kubectl wait --for=condition=Available --timeout=120s \
  deploy/consumer-collector \
  deploy/non-consumer-collector \
  deploy/other-consumer-collector \
  deploy/wildcard-collector \
  -n mdai

CONS=$(rev consumer)
NON=$(rev non-consumer)
OTHER=$(rev other-consumer)
WILD=$(rev wildcard)
echo "baseline: consumer=$CONS non-consumer=$NON other-consumer=$OTHER wildcard=$WILD"
```

## Why the wildcard fixture is honest

`extractCollectorEnvRefs` only inspects `Spec.Config.Yaml()` — it deliberately does NOT read `Spec.Env`. The wildcard fixture puts `RATE_PREFIX` in `Spec.Env` (literal `SAMPLED`), so:

- The operator's static parser sees only `RATE_PREFIX` as a resolvable ref. `RATE_PREFIX` is never written to any env-CM, so it never appears in the diff.
- The parser ALSO sees an extra `${env:` (the outer of `${env:${env:RATE_PREFIX}_RATE:-100}`) that can't strict-match → wildcard branch returns `true`.
- Without the wildcard fallback, this collector would never restart on any env-CM change.
- With the wildcard fallback, it restarts on every env-CM change. That's what we assert below.

The `:-100` default keeps the OTel collector healthy regardless of whether `SAMPLED_RATE` is in the env-CM, so the deployment-revision check is deterministic.

## Section A — Per-collector decisions (gap 1, 2, 4)

Change `sampled` (index 0).

```bash
set_default 0 250

[[ $(rev consumer)       != "$CONS"  ]] && echo "consumer restarted: ✓"        || echo "consumer NOT restarted: ✗"
[[ $(rev non-consumer)   == "$NON"   ]] && echo "non-consumer skipped: ✓"      || echo "non-consumer restarted: ✗"
[[ $(rev other-consumer) == "$OTHER" ]] && echo "other-consumer skipped: ✓"    || echo "other-consumer restarted: ✗"
[[ $(rev wildcard)       != "$WILD"  ]] && echo "wildcard restarted: ✓"        || echo "wildcard NOT restarted: ✗"

CONS=$(rev consumer); NON=$(rev non-consumer); OTHER=$(rev other-consumer); WILD=$(rev wildcard)
```

Pass criteria: exactly two restarts — `consumer` (real consumer of `SAMPLED_RATE`) and `wildcard` (defensive restart, parser can't follow indirection).

## Section B — Independent variable doesn't fan out (gap 2, 4)

Change `other` (index 1).

```bash
set_default 1 7

[[ $(rev consumer)       == "$CONS"  ]] && echo "consumer skipped: ✓"          || echo "consumer restarted: ✗"
[[ $(rev non-consumer)   == "$NON"   ]] && echo "non-consumer skipped: ✓"      || echo "non-consumer restarted: ✗"
[[ $(rev other-consumer) != "$OTHER" ]] && echo "other-consumer restarted: ✓"  || echo "other-consumer NOT restarted: ✗"
[[ $(rev wildcard)       != "$WILD"  ]] && echo "wildcard restarted: ✓"        || echo "wildcard NOT restarted: ✗"

CONS=$(rev consumer); NON=$(rev non-consumer); OTHER=$(rev other-consumer); WILD=$(rev wildcard)
```

Pass criteria: exactly two restarts — `other-consumer` (real consumer of `OTHER_RATE`) and `wildcard` (defensive). `consumer` does not restart even though the change happened in the same hub.

## Section C — No-op reconcile (gap 3)

A reconcile that produces an identical env-CM must restart zero collectors — including the wildcard one. The wildcard branch only forces a restart when the diff is non-empty.

Do NOT re-apply `hub.yaml` here: sections A and B already patched the defaults away from the file's values, so a re-apply is a real change. Instead, force a reconcile by bumping an annotation that doesn't touch any variable:

```bash
kubectl annotate mdaihub restart-test -n mdai test-tick=$(date +%s) --overwrite
sleep 3

[[ $(rev consumer)       == "$CONS"  ]] && echo "consumer no-op: ✓"        || echo "consumer restarted: ✗"
[[ $(rev non-consumer)   == "$NON"   ]] && echo "non-consumer no-op: ✓"    || echo "non-consumer restarted: ✗"
[[ $(rev other-consumer) == "$OTHER" ]] && echo "other-consumer no-op: ✓"  || echo "other-consumer restarted: ✗"
[[ $(rev wildcard)       == "$WILD"  ]] && echo "wildcard no-op: ✓"        || echo "wildcard restarted: ✗"
```

Pass criteria: zero restarts.

## Cleanup

```bash
kubectl delete -f collector-restart-tests/collector-wildcard.yaml
kubectl delete -f collector-restart-tests/collector-other-consumer.yaml
kubectl delete -f collector-restart-tests/collector-non-consumer.yaml
kubectl delete -f collector-restart-tests/collector-consumer.yaml
kubectl delete mdaihub restart-test -n mdai
```

## Mapping back to source

| Section | Function exercised |
|---|---|
| A, B | `extractCollectorEnvRefs` strict refs + `anyKeyInSet` + `diffEnvMapKeys` happy paths |
| A, B (wildcard column) | `extractCollectorEnvRefs` wildcard branch when diff is non-empty |
| C | `diffEnvMapKeys` empty-diff short-circuit (`len(changed) == 0`) gates wildcard, too |

The pure parser is additionally unit-tested at `internal/controller/hub_adapter_test.go:1240` (`TestExtractCollectorEnvRefs`), including the `"indirection falls back to wildcard"` subcase at line 1283.
