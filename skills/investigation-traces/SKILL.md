---
name: investigation-traces
description: "Trace-specific RCA discipline that complements the core ``investigation`` skill. Activate when the investigation has access to distributed traces (spans with parent-child links and per-span durations). Encodes the reasoning steps that REQUIRE trace structure: self-time decomposition, cross-component path-latency D vs D' check, span-gap position semantics, and latency-distribution shape discrimination (bimodal-within-trace, point-mass, unimodal). Includes an OpenTelemetry trace-conventions reference (span.kind, service.name, http.*, rpc.*, db.*, span events / status code) so the rules can be applied directly against OTel-emitted spans. Without trace data this skill cannot apply; with trace data, several common misclassifications (originator vs observer, CPU vs socket, query-regression vs injected-delay) become tractable only via the rules below."
metadata:
  version: 1.1.0
  tags: [rca, traces, observability, distributed-systems, methodology, opentelemetry]
---

# Investigation — Traces

## When to use

Activate when ALL of:

- The core ``investigation`` skill is already in scope.
- The investigation has trace data with at least span identity, parent-child linkage, per-span duration, and a service / component identifier per span.
- A latency anomaly is being investigated (this skill says little about correctness or volume anomalies).

Skip when:

- Only metrics + logs are available (no spans).
- Spans exist but are flat (no parent-child structure recorded) — you can still use the per-span variance ideas but the causal-walk rules degrade.

## Why traces change the analysis

Three questions become answerable that pure metrics cannot answer:

1. **Where is the wall-clock time spent inside a single request?** Self-time decomposition.
2. **Is the slow leaf actually the cause, or is the time on the path to it?** Cross-component D vs D' check.
3. **What kind of wait is happening?** Latency-distribution shape across many same-operation spans.

Each is an independent rule below. None of them depends on knowing the failure mode in advance — they discriminate **after** the candidate set is built (Phase B of the core skill).

---

## T.1 — Self-time decomposition (the most-skipped rule in failed RCAs)

For any span, define:

```
self_time(span) = duration(span) − Σ duration(direct children of span)
```

Self-time is the wall-clock the span spent doing its own work — neither in a logged child call nor in a downstream service.

Two observations follow:

- **A service is the originator of latency, not a victim, when its own spans show large self-time inflation in the anomaly window**, regardless of what its children look like and regardless of whether it sits at the root or at a leaf in the call tree.
- **A service whose total latency rises but whose self-time stays flat is an observer** — its inclusive duration grew because something it called grew. The cause is downstream.

The recurring failure mode this rule prevents: ranking candidates by total latency or p95 of full span duration. The deepest leaf in a call tree always *looks* slow because its inclusive duration includes everything below it, but the originator might be three hops up where self-time exploded while children stayed fast.

**Always rank Phase B candidates by self-time delta vs baseline, not by total duration delta.**

When self-time decomposition is impossible (no full child-span coverage, sampling gaps), fall back to: largest p95 deviation × per-span call count, but mark the result as lower confidence and verify with one of the other trace rules below.

---

## T.2 — Span-gap position semantics

Within a single span's duration, the model has four kinds of "where time went":

1. **Pre-IO gap**: time between span start and the first child / first I/O event.
2. **Between-IO gap**: time between two adjacent child spans.
3. **Post-IO gap**: time between the last child and span end.
4. **Inside-children**: covered by self-time of children (recurse).

Each gap class points at a different cause family:

| Gap location | Likely cause family | Why |
|---|---|---|
| Large pre-IO | Connection acquisition, DNS, TLS handshake, SYN retransmit, thread-pool wait, queue ingress | Wait happens before the first downstream I/O fires |
| Large between-IO | GC pause, lock contention, scheduler delay, CPU throttling | Wait happens between two downstream calls within the same handler |
| Large post-IO | Response serialization, application-side compute, log-flush, blocking write | Wait happens after the last downstream call returns |
| Inside-children | Recurse: this is just the next level's self-time — apply T.1 again |

**Always identify which class of gap is dominant before naming a mechanism.** "The span took N seconds and only X of it was downstream" is incomplete diagnosis until you record *where* the missing time sat.

The recurring failure mode this prevents: defaulting to the most familiar mechanism for "unaccounted span time". Reflection logs repeatedly show "I attributed this to GC / connection-pool" without first checking whether the gap was pre-IO (connection-side), between-IO (in-process), or post-IO (response-side).

---

## T.3 — Cross-component path-latency check (D vs D')

When a parent span's outbound call to a downstream span takes wall-clock duration `D` measured at the parent, and the downstream's inbound / processing measurement is `D'` at the callee:

- If `D ≈ D'`: the time is on the callee, attribute downstream.
- If `D ≫ D'`: the gap `D − D'` is on the wire (network path) OR is the callee's queue / ingress time before its own measurement starts. **Promote the path or the callee's ingress** — neither endpoint's processing time is the cause.

This is the single most reliable defense against **originator-observer confusion**. A parent that "looks slow" because of a slow outbound call is a victim; the actual time-eater is between the two endpoints or in the callee's pre-processing queue.

**Always perform this check before finalizing a candidate that sits upstream in a slow call chain.** Reflection logs repeatedly show "I assumed the downstream's slowness propagated up" — this rule is the test that decides whether the upstream was passively waiting (observer) or actively slow (originator).

The same principle extends to errors, not just latency. A service whose logs only contain receive-side exceptions (`HttpServerErrorException`, `RestClientException`, gRPC `UNAVAILABLE`/`DEADLINE_EXCEEDED`, any "I called X and X failed" pattern) is by definition an observer; the originator is downstream. Pull the failing `traceID`s, walk to the deepest descendant span whose `status.code = ERROR` (or whose span events carry the exception) — that span's `service.name` is the emitter. If trace data is unavailable, run the same exception scan symmetrically on each synchronous downstream and pick the one whose logs carry the *generated* exception, not the propagated one.

---

## T.4 — Latency-distribution shape across many same-operation spans

For a single (serviceName, operationName) pair across the anomaly window, the **shape** of the duration distribution discriminates failure-mode families that point estimates (avg, p95) cannot:

| Shape | Diagnostic interpretation |
|---|---|
| **Point mass** at a round value (p50 ≈ p95 ≈ p99 ≈ 200ms / 500ms / 1s, very low variance) | Injected delay / artificial wait / fixed throttle / sleep. Organic mechanisms (slow query, GC, contention) produce variance; only an imposed wait collapses to a delta. |
| **Bimodal within a single trace** (sibling calls to the same op show fast mode + slow mode in the same trace) | Discrete event class — **not** steady-state saturation. Candidate causes: connection-pool checkout race, TCP retransmit, GC pause, lock acquisition, half-open socket. Saturation produces unimodal smearing; bimodal-within-trace is the fingerprint of a per-call discrete wait. |
| **Bimodal across traces** with peaks near TCP RTO multiples (~200ms / ~1s / ~3s) | Packet loss / retransmit-driven timeouts. RTO doubles, so clusters at 200/600/1400 ms are decisive. |
| **Unimodal right-shift** (whole distribution moves up, similar variance ratio) | Resource contention or load — affects all calls proportionally. |
| **Heavy right tail** (p50 stable, p99 explodes) | Cache miss / GC / single-trace tail — check whether the slow tail correlates with a particular caller, payload, or input size. |

**Always plot the shape before naming a mechanism.** Reflection logs repeatedly show "I had p50 and p95 but never looked at distribution shape" — by the time the report is written, the model has the data to do this but does not run the histogram.

A point-mass distribution is by far the highest-confidence shape: if p50 ≈ p95 ≈ p99 at a round number, naming any mechanism other than "injected delay" requires explicit falsification.

---

## T.5 — Ranking criterion: relative self-time deviation × call count

Combine T.1 and the core skill's relative-deviation rule into the canonical ranking score:

```
score(service, op) = (anomaly_self_time_p95 / baseline_self_time_p95)
                     × (calls_in_anomaly_window / max_calls_across_services)
```

The first factor is what a service did **in its own span** — the candidate quality. The second factor is **throughput weight** — a 100× deviation on a service with 5 calls per window matters less than a 5× deviation on a service with 50 000 calls per window.

The recurring failure mode this prevents: "leaf-of-call-tree bias" (service with the deepest visible span gets blamed) and "loudest-log bias" (service with the most error messages gets blamed) — neither tracks self-time × throughput, the actual user-facing impact axis.

---

## Hard constraints (trace-specific)

These add to the core ``investigation`` skill's constraints; they do not replace them.

- NEVER rank candidates by total span duration when self-time decomposition is available; rank by self-time delta.
- NEVER attribute a parent's latency to its child without performing T.3 (D vs D'). A parent waiting on a child is an observer until proven otherwise.
- NEVER name a mechanism from a single point estimate (avg or p95) when the anomaly window has enough same-operation spans to plot a distribution. The shape is the discriminator.
- NEVER conflate "leaf in the call tree" with "is the originator". Leaves can be victims and roots can be originators; topology is orthogonal to causation.
- NEVER ignore a point-mass latency distribution; it is the single most decisive shape and points at injected delay before any other mechanism.

---

## Outputs the consumer should produce (trace-specific additions)

In addition to whatever the core skill produces:

1. **Self-time delta** for each named candidate, baseline vs anomaly window. If unavailable, say so explicitly.
2. **Gap-position attribution** (pre-IO / between-IO / post-IO) for the anomalous self-time, with at least one example trace cited.
3. **Distribution shape** of the dominant slow operation (point mass / bimodal-within-trace / bimodal-RTO / unimodal right-shift / heavy tail), with the specific p50 / p95 / p99 values that classify it.
4. **D vs D' result** for any cross-component call where the candidate is upstream of the suspected slow leaf or the suspected error emitter.

If any of these is impossible to compute on the available data, mark it explicitly rather than skipping silently — silent skipping is the mechanism by which trace evidence gets defaulted away in favour of less specific hypotheses.

---

## Platform-specific reference — OpenTelemetry trace conventions

The rules above are platform-agnostic. The field-name examples below are **specific to OpenTelemetry semantic conventions** for spans. On other formats (Zipkin, X-Ray, Jaeger native, proprietary tracers) substitute the equivalent attributes — the discrimination logic does not change, only the attribute keys.

### Identity / topology attributes

| What you need | OTel attribute | Notes |
|---|---|---|
| Service identity | `service.name` (resource attribute), `service.namespace`, `service.instance.id` | Use as the entity grouping key for self-time aggregations and ranking. |
| Span kind | `span.kind` ∈ `SERVER`, `CLIENT`, `PRODUCER`, `CONSUMER`, `INTERNAL` | Critical for D vs D' (T.3): a `CLIENT` span on the parent + a matching `SERVER` span on the callee gives you the two endpoints of the same call; `D − D'` is the wire/queue gap. |
| Causal linkage | `trace_id`, `span_id`, `parent_span_id`, `links[]` | Standard parent-child structure used by T.1 / T.2 / T.3. |
| Operation name | `name` (the span name), plus protocol-specific operation attrs (e.g. `http.route`, `rpc.method`, `db.operation.name`) | Use the protocol-specific attribute for the (service, operation) pair in T.4 / T.5; the bare `name` field is often too coarse or too fine depending on the SDK. |

### Status / error attributes

- `status.code` ∈ `UNSET`, `OK`, `ERROR` plus `status.message`. An `ERROR` status is the canonical "this span failed" signal — pair with span events (below) to recover the underlying exception.
- Span events (`events[]`): an `exception` event carries `exception.type`, `exception.message`, `exception.stacktrace`. Treat these as **structured log lines bound to the span** — they answer "what failed inside this span" without joining to a separate logs index.

### HTTP / RPC / DB / messaging attributes (for self-time and gap attribution)

- HTTP: `http.request.method`, `http.response.status_code`, `http.route`, `url.path`, `server.address`, `server.port`, `client.address`. Use `http.route` (not `url.path`) as the operation grouping key — `url.path` cardinality explodes on path parameters.
- RPC: `rpc.system` (e.g. `grpc`, `aws-api`), `rpc.service`, `rpc.method`, `rpc.grpc.status_code`. The (`rpc.service`, `rpc.method`) tuple is the operation key.
- Database: `db.system.name` (e.g. `postgresql`, `mongodb`), `db.namespace`, `db.operation.name`, `db.query.text` (sanitized), `db.collection.name`. For T.4 distribution-shape work, group by (`service.name`, `db.system.name`, `db.operation.name`) — the operation name discriminates `find` vs `aggregate` vs `update` whose tail behaviors differ qualitatively.
- Messaging: `messaging.system`, `messaging.destination.name`, `messaging.operation.type` ∈ `publish`, `receive`, `process`. A `process`-kind span's self-time is the consumer's handler time; pre-IO gap on a `process` span is queue dwell + dispatch.
- Network peer: `network.peer.address`, `network.peer.port`, `network.transport`. Used to identify the wire endpoint when applying T.3 (D − D' is on the wire).

### Resource attributes (link spans back to the resource-saturation skill)

When the same OTel pipeline emits both spans and resource metrics, the resource attributes on a span tell you which entity's resource counters to pull:

- `service.name` / `service.instance.id` (the workload).
- `host.name`, `host.id` (the node).
- `k8s.pod.name`, `k8s.namespace.name`, `k8s.container.name`, `container.id` (the pod / container).
- `process.pid`, `process.runtime.name`, `process.runtime.version` (the JVM / Node / Python process).

Use these to bridge a trace-shape diagnosis (T.4 says "bimodal RTO clusters") to the matching resource-axis probe in `investigation-resource-saturation` (run the network-loss probe filtered to `service.name = <originator>` and the matching `k8s.pod.name`).

### Counter vs gauge does NOT apply to spans

Spans are **events**, not counters or gauges — treat each span as a discrete observation with its own `start_time`, `end_time`, attributes, and events. Aggregations (rate of spans, percentiles of `end_time − start_time`) happen at query time, not in the span itself. Cumulative-counter rate logic from the resource-saturation skill does not apply here.

---

## Illustrative pattern matches (NOT prescriptive)

These map common reflection-log failure modes to which trace rule would have prevented the misclassification. They are diagnostic illustrations, not exhaustive.

- "Ranked the deepest-leaf service as root cause" → **T.1** (self-time at the upstream service inflated; leaf was a victim of upstream queueing).
- "Said it was MongoDB lock contention but every call took ~200ms exactly" → **T.4** (point-mass distribution rules out organic contention; injected delay is the higher-prior hypothesis).
- "Said it was CPU saturation but sibling calls in the same trace had bimodal latency" → **T.4** (CPU saturation smears unimodally; bimodal-within-trace points at discrete waits — connection pool, lock, GC).
- "Said it was 'thread-pool stall' but the stall was entirely before the first downstream I/O" → **T.2** (pre-IO gap = connection acquisition / DNS / SYN, not in-process thread-pool — different class of cause).
- "Said the upstream service was the originator but its self-time was flat and only its child got slow" → **T.1** + **T.3** (upstream is the observer; promote the downstream or the path between).
