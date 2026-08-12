# Variable Injection Patterns — Local Kind Test Plan

End-to-end validation that the three recommended injection patterns work against a real OpenTelemetry Collector, plus a guard test that the deprecated map-with-typed-numerics anti-pattern fails clearly.

## The three patterns under test

| Pattern | Use case | Variable dataType | Template position |
|---|---|---|---|
| 1 — scalar | single typed field | `int`, `float`, `boolean`, `string` | inline scalar |
| 2 — string-string map | grouped string values (headers, labels, attributes) | `map` | inline mapping |
| 3 — string with JSON | mixed-type processor config blob | `string` | whole-mapping substitution |

Path A (drop the map renderer's int/float reparse) means map values now render as YAML strings. Mixed-type processor config must use pattern 3, not pattern 2.

## Prerequisites

- Operator and OpenTelemetry Operator deployed in `mdai`.
- Working directory `mdai-operator/docs/`.

## Setup

```bash
kubectl apply -f variable-injection-patterns-tests/hub.yaml
kubectl apply -f variable-injection-patterns-tests/collector.yaml

kubectl wait --for=condition=Available --timeout=120s \
  deploy/injection-test-collector -n mdai

kubectl get pod -n mdai -l app.kubernetes.io/name=injection-test-collector -o name
```

## Section 1 — Patterns 1, 2, 3 against a working collector

The combined `collector.yaml` exercises all three patterns in one OTel collector spec:

| Field in collector config | Pattern | Variable | What it proves |
|---|---|---|---|
| `processors.probabilistic_sampler.sampling_percentage: ${env:SAMPLING_RATE}` | 1a (scalar→float) | `sampling_rate` (int, default 100) | Scalar substitution into a typed numeric field |
| `processors.filter/noisy.traces.span[0]: 'resource.attributes["service.name"] == "${env:NOISY_SERVICE}"'` | 1b (scalar→string in OTTL) | `noisy_service` (string, default "checkout") | Scalar substitution inside a quoted OTTL expression |
| `exporters.otlphttp/tenant.headers: ${env:TENANT_HEADERS}` | 2 (string-string map) | `tenant_headers` (map, default `{X-Tenant-Id: "01234", X-Build: "v1.2.3"}`) | Map renders as YAML strings; leading zero preserved |
| `processors.batch: ${env:BATCH_CONFIG}` | 3 (string with JSON) | `batch_config` (string, default `{"send_batch_size":100,"timeout":"15s"}`) | JSON-in-string substitutes into a typed mapping; collector resolves types from JSON syntax |

### 1.1 Collector starts cleanly

```bash
kubectl rollout status -n mdai deploy/injection-test-collector --timeout=60s
kubectl get pod -n mdai -l app.kubernetes.io/name=injection-test-collector
# expect: STATUS=Running, READY=1/1, no restarts
```

Pass criteria: pod Ready. Any startup failure here means at least one of the four patterns broke during config decode.

### 1.2 Sampling rate is parsed as a float (Pattern 1a)

Three levels of assertion: **Smoke** (decode succeeded), **Verification** (the substituted value is exactly what we expect), **Pipeline** (the running collector applies it as intended).

**Smoke** — env var rendered, no decode error.
```bash
kubectl get cm injection-test-variables -n mdai -o jsonpath='{.data.SAMPLING_RATE}'
# expect: 100

kubectl logs -n mdai deploy/injection-test-collector --tail=200 | \
  grep -iE 'probabilistic_sampler|sampling_percentage|cannot decode|invalid' | head
# expect: no error lines mentioning sampling_percentage
```

**Verification** — read the environment of the running collector process (PID 1). OTel does env substitution in-memory at startup, never writing the resolved YAML to disk, so the only way to see what the binary actually got is `/proc/1/environ`.
```bash
POD=$(kubectl get pods -n mdai -l app.kubernetes.io/name=injection-test-collector -o name | head -1)
kubectl debug -it -n mdai "$POD" \
  --image=busybox:1.36 --target=otc-container \
  --profile=general -- sh -c 'cat /proc/1/environ | tr "\0" "\n" | grep "^SAMPLING_RATE="'
# expect: SAMPLING_RATE=100
```

**Pipeline** — run a one-shot telemetrygen Job and confirm every span flows through the debug exporter (sampling_percentage=100 means everything passes).
```bash
# The collector is operator-managed (rollout restart is reverted) and its log accumulates across
# runs, so isolate THIS run by timestamp (--since-time) instead of restarting.
T0=$(date -u +%Y-%m-%dT%H:%M:%SZ)

kubectl run telemetrygen --restart=Never -n mdai \
  --image=ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:latest \
  -- traces --otlp-endpoint injection-test-collector.mdai.svc.cluster.local:4317 \
            --otlp-insecure --traces 5 --rate 1
kubectl wait pod telemetrygen -n mdai --for=condition=Ready=false --timeout=30s 2>/dev/null

# 5 traces x 2 spans = 10. Poll until the batch processor flushes all of them (a fixed sleep lands
# on the flush boundary and undercounts).
for i in $(seq 1 12); do
  sleep 5
  n=$(kubectl logs -n mdai deploy/injection-test-collector --since-time="$T0" | grep -c 'Span #')
  [ "$n" -ge 10 ] && break
done
echo "spans this run: $n"   # expect: 10

kubectl delete pod telemetrygen -n mdai
```

### 1.3 Filter expression substitution (Pattern 1b)

The filter processor's `span` field is `[]string` of OTTL expressions. The template wraps `${env:NOISY_SERVICE}` in single quotes, so substituting `checkout` produces `'attributes["service.name"] == "checkout"'`. The collector parses the OTTL expression and applies it.

**Smoke** — env var rendered, no OTTL parse error.
```bash
kubectl get cm injection-test-variables -n mdai -o jsonpath='{.data.NOISY_SERVICE}'
# expect: checkout

kubectl logs -n mdai deploy/injection-test-collector --tail=200 | \
  grep -iE 'filter/noisy|ottl|cannot decode|parse error' | head
# expect: no lines
# (otlphttp/tenant retry errors for `example.invalid` are unrelated and expected — see Section 1.4.)
```

**Verification** — same `/proc/1/environ` reading, scoped to `NOISY_SERVICE`. The on-disk YAML still carries `${env:NOISY_SERVICE}` because OTel substitutes in-memory.
```bash
POD=$(kubectl get pods -n mdai -l app.kubernetes.io/name=injection-test-collector -o name | head -1)
kubectl debug -it -n mdai "$POD" \
  --image=busybox:1.36 --target=otc-container \
  --profile=general -- sh -c 'cat /proc/1/environ | tr "\0" "\n" | grep "^NOISY_SERVICE="'
# expect: NOISY_SERVICE=checkout
```

**Pipeline** — send spans whose `service.name` matches and doesn't match; only the non-match should reach the debug exporter.
```bash
kubectl run telemetrygen-match --restart=Never -n mdai \
  --image=ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:latest \
  -- traces --otlp-endpoint injection-test-collector.mdai.svc.cluster.local:4317 \
            --otlp-insecure --traces 3 --service checkout

kubectl run telemetrygen-nomatch --restart=Never -n mdai \
  --image=ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:latest \
  -- traces --otlp-endpoint injection-test-collector.mdai.svc.cluster.local:4317 \
            --otlp-insecure --traces 3 --service api

kubectl wait pod telemetrygen-match telemetrygen-nomatch -n mdai --for=condition=Ready=false --timeout=30s 2>/dev/null
# Wait past the batch processor's 15s timeout so the buffered batch flushes.
sleep 20

# Filter drops matched spans; "checkout" spans are dropped, "api" spans pass.
kubectl logs -n mdai deploy/injection-test-collector --tail=400 | grep -c 'service.name: Str(api)'
# expect: 3
kubectl logs -n mdai deploy/injection-test-collector --tail=400 | grep -c 'service.name: Str(checkout)'
# expect: 0

kubectl delete pod telemetrygen-match telemetrygen-nomatch -n mdai
```

### 1.4 Map dataType preserves string-only values, including leading zeros (Pattern 2)

**Smoke** — env-CM rendered with the leading-zero value quoted; collector decoded the headers without error.
```bash
kubectl get cm injection-test-variables -n mdai -o jsonpath='{.data.TENANT_HEADERS}'
# expect (literal output):
#   X-Build: v1.2.3
#   X-Tenant-Id: "01234"

kubectl logs -n mdai deploy/injection-test-collector --tail=200 | \
  grep -iE 'otlphttp|tenant|headers|cannot decode' | head
# expect: no decode error mentioning headers
```

`X-Build` is unquoted because `v1.2.3` is unambiguously a string. `X-Tenant-Id` must be quoted because the bare `01234` would be parsed back as int and the leading zero lost.

**Verification** — read `TENANT_HEADERS` from the collector process's environment. The rendered YAML value contains a newline and the quoted leading-zero ID; both must survive intact.
```bash
POD=$(kubectl get pods -n mdai -l app.kubernetes.io/name=injection-test-collector -o name | head -1)
kubectl debug -it -n mdai "$POD" \
  --image=busybox:1.36 --target=otc-container \
  --profile=general -- sh -c 'cat /proc/1/environ | tr "\0" "\n" | sed -n "/^TENANT_HEADERS=/,/^[A-Z_]*=/p" | head -3'
# expect:
#   TENANT_HEADERS=X-Build: v1.2.3
#   X-Tenant-Id: "01234"
```

**Pipeline** — exercise the exporter against a mock HTTP receiver (e.g. `mockbin`, `nginx` with logging) to confirm the outbound request carries the literal header value. Optional; skip unless you want to chase a live HTTP round-trip.

### 1.5 String-with-JSON resolves into typed processor config (Pattern 3)

The batch processor expects `send_batch_size: int` and `timeout: time.Duration`. The env value is the literal JSON `{"send_batch_size":100,"timeout":"15s"}`. After substitution the collector's YAML parser reads the flow-style mapping and resolves types from JSON syntax: `100` → int, `"15s"` → string → Duration.

**Smoke** — env-CM rendered, batch decoded.
```bash
kubectl get cm injection-test-variables -n mdai -o jsonpath='{.data.BATCH_CONFIG}'
# expect: {"send_batch_size":100,"timeout":"15s"}

kubectl logs -n mdai deploy/injection-test-collector --tail=200 | \
  grep -iE 'batch|send_batch_size|cannot decode' | head
# expect: no decode error mentioning send_batch_size or timeout
```

**Verification** — read `BATCH_CONFIG` from the collector process's environment.
```bash
POD=$(kubectl get pods -n mdai -l app.kubernetes.io/name=injection-test-collector -o name | head -1)
kubectl debug -it -n mdai "$POD" \
  --image=busybox:1.36 --target=otc-container \
  --profile=general -- sh -c 'cat /proc/1/environ | tr "\0" "\n" | grep "^BATCH_CONFIG="'
# expect: BATCH_CONFIG={"send_batch_size":100,"timeout":"15s"}
```

**Pipeline** — send a burst of spans with `--rate` low enough that the batch processor's timeout (15s) fires before send_batch_size (100) does. The debug exporter then logs a single batch flush per 15s window.
```bash
kubectl run telemetrygen-batch --rm -i --restart=Never -n mdai \
  --image=ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:latest \
  -- traces --otlp-endpoint injection-test-collector.mdai.svc.cluster.local:4317 \
            --otlp-insecure --duration 30s --rate 1
# expect: debug-exporter log shows batched output every ~15s, not on every span
```

## Section 2 — Anti-pattern guard (map → typed field fails clearly)

The anti-pattern is to inject a typed processor config via a map variable. Under Path A this is expected to fail at the collector's config-decode step because the int-typed field receives a string.

```bash
kubectl apply -f variable-injection-patterns-tests/collector-antipattern.yaml
sleep 5

kubectl get pod -n mdai -l app.kubernetes.io/name=injection-antipattern-collector
# expect: STATUS=CrashLoopBackOff or Pending (not Ready)

kubectl logs -n mdai deploy/injection-antipattern-collector --tail=30 | \
  grep -iE 'send_batch_size|batch.*processor|cannot decode|expected.*type'
# expect: 'send_batch_size' expected type 'uint32', got unconvertible type 'string'
```

Pass criteria: collector pod is not Ready AND the logs reference a typed-decode failure for `send_batch_size`. This is the evidence that:

- Path A correctly renders map values as strings.
- OTel's `confmap` decoder (with `WeaklyTypedInput: false`) refuses to coerce string → int.
- The user gets a loud error telling them to migrate to Pattern 3 (string+JSON).

## Section 3 — Migration verification

Apply a fixed version that swaps the offending map for a string-with-JSON variable. The collector should recover.

```bash
kubectl patch mdaihub injection-antipattern -n mdai --type=json -p='[
  {"op": "replace", "path": "/spec/variables/0", "value": {
    "key": "batch_config_legacy",
    "type": "manual",
    "dataType": "string",
    "default": "{\"send_batch_size\":100,\"timeout\":\"15s\"}",
    "serializeAs": [{"name": "BATCH_CONFIG_LEGACY"}]
  }}
]'

kubectl rollout restart deploy/injection-antipattern-collector -n mdai
kubectl rollout status   deploy/injection-antipattern-collector -n mdai --timeout=60s
```

Pass criteria: pod becomes Ready after the patch + restart. This is the evidence that Pattern 3 is a working migration path for the anti-pattern.

## Cleanup

```bash
kubectl delete -f variable-injection-patterns-tests/collector.yaml
kubectl delete -f variable-injection-patterns-tests/hub.yaml
kubectl delete -f variable-injection-patterns-tests/collector-antipattern.yaml
```

## Mapping back to source

| Section | Code path exercised |
|---|---|
| 1.1, 1.2 | `applySerializerToString` for scalar variables; OTel env substitution into typed numeric field |
| 1.3 | scalar string variable substitution into a quoted YAML position (template-side quoting) |
| 1.4 | `renderMapForCollectorEnv` rendering `map[string]string` with `yaml.Marshal` (no reparse); OTel ingesting a YAML mapping for `headers: map[string]string` |
| 1.5 | scalar string variable substitution into a YAML flow-mapping position; OTel resolving types from literal JSON syntax |
| Section 2 | Path A's removal of int/float reparse forces the legacy map+typed-config pattern to surface as a decode error |
| Section 3 | string-with-JSON migration for users hitting Section 2 |

## What's intentionally out of scope

- **Pattern 2 with bool/null-looking string values.** `yaml.Marshal` of `map[string]string{"x":"true"}` produces `x: "true"` (quoted) under YAML 1.2 semantics, which the collector parses as string. Worth a separate test in the canonicalizer Go suite but not necessary against a live collector.
- **Pattern 3 with multi-line indented JSON.** Flow-style YAML (anything inside `{…}`) is whitespace-insensitive, so multi-line JSON substitutes cleanly. Single-line JSON in the fixture is the tighter test.
- **Pattern 1 leading-zero risk in scalar string variables.** A scalar string value `"01234"` substituted into a YAML position where the collector's parser auto-coerces (e.g., a string field for which the YAML parser does not know the target type) can still lose the leading zero. The user fix is the same as for Pattern 3: wrap with quotes in the template (`"${env:ID}"`). Worth documenting in `variables.md` but doesn't change the renderer.
