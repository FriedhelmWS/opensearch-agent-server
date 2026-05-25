---
name: investigation
description: >
  General-purpose Root Cause Analysis methodology for distributed systems and
  any incident investigation backed by observability data. Telemetry-agnostic
  and failure-mode-agnostic: the skill encodes the DISCIPLINE of investigation
  (phases, gates, originator-vs-observer reasoning, direct-vs-symptom evidence,
  cognitive-bias guards) without prescribing a query language, telemetry
  backend, taxonomy of failure modes, or output artifact format. Activate when
  an agent is debugging a specific incident from any combination of traces,
  logs, and metrics, and needs a rigorous flow that resists premature
  commitment, magnitude bias, and symptom-as-cause misclassification.
version: 2.0.0
tags: [rca, investigation, observability, methodology, distributed-systems]
---

# Investigation Skill

## Overview

A reusable Root Cause Analysis discipline for any agent investigating an incident from observability data. It encodes the lessons that survive across platforms, telemetry backends, and failure taxonomies: where bias enters an investigation, which signals systematically lie, and which decision principles hold up regardless of what's broken.

The skill produces two outcomes:

1. An **originator** — the service / component / boundary whose anomaly _originates_ the failure, distinguished from those that are downstream victims.
2. A **named failure mode** — chosen from candidates _appropriate to the system under investigation_, backed by _direct_ evidence (not symptom logs, not field-name guesses, not familiarity).

The skill is deliberately telemetry-agnostic AND failure-mode-agnostic. It does not enumerate "the 6 ways things break" — that list is a property of the system being investigated, not of RCA. Instead, the skill provides the framework an agent uses to _construct_ its own candidate-mode list and its own direct-indicator queries at investigation time.

## When to use

Activate when ALL of:

- An agent is investigating a specific incident (latency spike, error rate, anomaly, outage, regression)
- Telemetry is available — any combination of traces, logs, metrics; structured or unstructured
- The investigation needs to identify a root-cause component AND name what failed

Do NOT use when:

- The failure mode is already known and the user wants a fix (this is a diagnostic skill, not a remediation one)
- The task is capacity planning, benchmarking, or trend analysis (no incident, no anomaly window)
- Only one signal type is available with no per-component granularity AND no baseline (the methodology requires comparisons)

## What this skill does NOT prescribe

To stay general:

- **Query language / DSL** — OpenSearch DSL, PromQL, LogQL, SPL, X-Ray, Tempo, Datadog query, kubectl, cloud-native APIs all work.
- **Telemetry backend.**
- **Taxonomy of failure modes.** The skill does not assume the failure is "one of {cpu, memory, disk, network, socket, slow query}" or any other fixed list. The candidate modes for a given investigation depend on the system: a database might fail via lock contention, plan regression, replication lag, or schema drift; a frontend might fail via bundle-size regression, third-party-script latency, or CDN routing; a payments system might fail via idempotency-key collision, rate-limit at a partner, or a cert-rotation regression. The agent enumerates plausible modes for _this_ system; the skill provides the framework for testing each one rigorously.
- **Specific thresholds.** "≥ 2x" and "≥ 80%" are illustrative defaults — actual thresholds depend on the system's normal variance.
- **Output artifact format** — JSON, prose, ticket comment, dashboard, the consumer chooses.
- **Field names or platform-specific indicators.** Examples in this skill are clearly labeled; substitute your platform's equivalents.

A consuming agent layers its own scaffolding on top: query templates for its backend, a candidate-mode list relevant to the system, an output contract for its caller.

---

## The methodology

Three phases. Do not advance to the next phase until the current phase's gates are satisfied. Most failed RCA runs come from racing through phases or skipping gates.

### Phase A — Understand the data (before searching it for evidence)

#### A.1 — Schema discovery

For each telemetry source in scope, sample one document. Record:

- **Time field and unit** for each source (sources frequently disagree).
- **Identifier fields** that link traces, logs, and metrics to a component. These are usually three different field names. The trace's `serviceName` is rarely the log's service identifier.
- **Trace-shape fields**: how a span identifies its parent, how a root span is marked, where errors are recorded (try multiple paths — error fields often differ between instrumentations on the same system).
- **Duration units**. Off-by-1000 errors from confusing ms / µs / ns are a recurring silent failure.

If any of the above is ambiguous, plan now to verify it before relying on it. A query that returns zero hits is much more often a wrong field name than a real absence.

**Gate before advancing**: explicitly state, for each source, which fields you will use as time, identifier, error-path, and (for traces) duration + parent linkage.

#### A.2 — Signal inventory

For each source, enumerate what's actually available — not what you expect or remember.

- For metrics: list every numeric field. Group at a coarse level (compute, memory, IO, network, queue/concurrency, application-domain). The grouping is descriptive, not exhaustive.
- For logs: identify which fields are structured vs free-text, what severity levels exist, and what cardinality the message field has.
- For traces: span attributes vary by instrumentation library; note which categories of attributes (db._, http._, rpc._, messaging._, custom domain attributes) are populated.

**Gate before advancing**: state which signal categories ARE present and which are absent. Premature narrowing here ("I'll just look at CPU and memory") is the most common cause of missing the actual root cause when it lives in a category the agent didn't scan.

---

### Phase B — Find the originator (not the observer)

Goal: identify the component whose anomaly _originates_ the failure, separating it from components that are visible victims.

#### B.1 — Counter vs gauge discipline

Every numeric metric is one of:

- **Counter**: monotonic since process start. Absolute value is uptime, not signal. **Counters require two samples per window and a rate computation** before any ratio is meaningful.
- **Gauge**: instantaneous sample. Diff directly.

Mixing these up is the most common quantitative error in metric analysis. A "huge counter value" tells you nothing; a counter that didn't change between two samples has rate zero regardless of magnitude.

**Recognising a counter vs gauge before you query.** During Phase A schema discovery, classify each numeric field BEFORE you write the Phase B query. Use these heuristics, in order:

1. **Naming convention.** Prometheus and many cgroup exporters end counters with `-total` (e.g., `*-seconds-total`, `*-bytes-total`, `*-failures-total`). These are almost always cumulative counters. Gauges typically end in nouns describing the current state (`*-bytes`, `*-set-bytes`, `*-rss`, `*-cache`, `-current`, queue lengths).
2. **Sampling pattern.** Pull two consecutive sample values for the field (Phase A.1 already samples one document; sample one more from the next timestamp). If the value is monotonically non-decreasing, it is a counter. If it fluctuates around a mean, it is a gauge — even if its name ends in `-total` (some exporters emit pre-rated samples).
3. **Magnitude.** "An instantaneous CPU reading of 17.78 seconds with a 1-second scrape interval is impossible" — that magnitude only makes sense as elapsed CPU-time, i.e. a counter. If the absolute value is much larger than what an instant reading should be, treat as counter.

**Common saturation traps that follow.** Once you have classified the fields, the wrong direct comparison gives nonsense:

- **CPU usage (counter) vs CPU quota (constant)**: dividing the average of `*-cpu-usage-seconds-total` by the average of `*-spec-cpu-quota` yields saturation values of hundreds or thousands of percent — wrong by orders of magnitude. Compute `rate = (max - min) / window_seconds`, then `saturation = rate / (quota_us / 100_000)` for cgroup v1 quotas in microseconds-per-100ms.
- **Memory usage (gauge) vs memory limit (constant)**: this division IS valid. `avg(working-set-bytes) / avg(spec-memory-limit-bytes)` is a real saturation ratio.
- **Disk / IO counters (`*-fs-reads-bytes-total`, `*-fs-writes-bytes-total`, `*-blkio-device-usage-total`)**: counters. To detect a write storm, compute the rate over the window, not the absolute total. A storm shows as a rate that is ≥ several × baseline rate, not as a large absolute value.

The PPL idioms for computing rate and saturation correctly — and the indices in this ecosystem where `istio-*-total` is unusually pre-rated rather than cumulative — are documented in the `ppl-cookbook` skill (section "Counters vs gauges"). Consult it when about to write a saturation-pair query.

#### B.2 — Rank by relative deviation, not absolute magnitude

Compare each metric in the anomaly window to its baseline. Rank candidates by **how much they deviated from their own normal**, not by absolute value.

A signal going `5 → 70` is a 14x anomaly and almost certainly more important than one going `900 → 1000`, even though the second has a much bigger raw number. Magnitude bias — ranking the most familiar-looking large number first — is one of the most reliable ways to misidentify the originator.

The threshold for "anomalous" depends on the system. For metrics with low normal variance, 1.3x may be significant; for chatty noisy metrics, 3x may be background. Threshold calibration is the consumer's responsibility; the principle (relative deviation, not absolute) is universal.

#### B.3 — Saturation pair check

Some resources are anomalous at a constant ratio because they were already pinned at the limit. Whenever a usage/limit pair exists for a resource, compute saturation in both windows and flag pre-saturated resources even when their ratio is ≈ 1.0.

The pattern: usage/limit ≥ high-percentage in _both_ windows. The resource was already at the wall; the anomaly is that work is now contending for it.

#### B.4 — Pre-existing-condition filter

A component whose baseline anomaly-signal value is already a substantial fraction of its anomaly-window value is _pre-existing_, not freshly anomalous. Discard it from candidates UNLESS its anomaly-window value is also a large multiple of its baseline.

Past failures at this step look like ranking the consistently-slow component as #1 simply because it has the highest absolute latency in the anomaly window — when it had the same latency yesterday.

#### B.5 — Per-component variance and self-time

For each component, compute over the anomaly window:

- Latency distribution (p50, p95, count) over time-bucketed windows
- Inflection points, progressive degradation, sustained elevation, request-rate collapse
- For traces: per-span "self-time" — time the span itself consumed, excluding waiting on its children. Aggregate across multiple traces; one trace's self-time can mislead because trace structure varies.

Components with high p95/p50 ratio (large variance) are deep-dive candidates regardless of their magnitude rank. Components with high self/total ratio in their spans are originator candidates regardless of absolute span duration.

#### B.6 — Candidate set

A component enters the candidate set if ANY of:

1. It has the earliest onset of degradation across the time-bucketed view
2. Its p95/p50 (variance) is anomalous
3. It has a unique-to-itself signal (not shared across multiple components)
4. It exhibits high self/total time on its own spans
5. Its per-span p95 is uniformly elevated across many distinct callers (downstream slowness regardless of who calls it)
6. It dominates the top of cumulative-duration / cumulative-self-time lists (weakest signal — duration alone never wins ties)

Rank by some form of `relative_anomaly × throughput / pre_existing_factor`. Moderate anomaly on a high-throughput component beats severe anomaly on a low-throughput one; pre-existing elevated components are down-weighted.

#### B.7 — Causal-direction walk (the most-skipped step in failed runs)

**Even when the #1 candidate has strong evidence, walk its outbound dependencies before committing.** Past failed runs almost always skipped this step.

For the current #1, find the components it calls. For each downstream:

1. Compute its own anomaly-vs-baseline ratio on its own spans / metrics.
2. Check whether it has a unique signal the parent lacks.
3. Check whether the parent's apparent self-time is dominated by waiting on this downstream.
4. **Cross-component path latency** — when the parent's outbound call to the downstream takes `D` but the downstream's inbound/processing measurement is `D'` with `D ≫ D'`, the gap (`D − D'`) is on the path TO the downstream, OR is the downstream's queue time. Promote the downstream — its ingress is where the contention lives.

Promote a downstream over the parent when ANY of (a)–(d) is true. Descend up to a few hops. Prefer unique downstream anomalies over cascading shared signals.

The single recurring symptom of this step being skipped: H1 names a service that appears slow because it was waiting on a downstream that is the actual originator.

**Gate before advancing**: state explicitly which dependency walk you performed and whether you promoted a downstream. Do this even when no promotion happens.

---

### Phase C — Name the failure mode (from direct evidence only)

Goal: name what failed, using only direct indicators. Phase C is where most past runs failed by defaulting to the most familiar-sounding category.

#### C.1 — Direct evidence vs symptom evidence

This distinction is universal across failure types and platforms.

- **Direct evidence**: evidence whose presence implies the mode. A resource gauge near its limit, a log message that names a specific failure class, a structural signature in trace shape, a distinctive sequence of state transitions. Direct evidence on its own can name a mode.
- **Symptom evidence**: evidence that is consistent with the mode but also consistent with several other modes. Generic error / exception log lines, "downstream timed out", "connection failed" — these admit multiple root causes. Symptom evidence is **context only**; it cannot name a mode by itself.

The single most common Phase C failure: using the loudest symptom log line as primary mode-naming evidence. "Connection reset by peer" is consistent with network loss, with CPU starvation on either endpoint, with socket exhaustion, with a forced restart, and with a deliberate close; using it to name "network loss" without checking the other possibilities is unsafe.

#### C.2 — Enumerate candidate modes for THIS system

The skill does not provide a fixed mode list. Build the candidate-mode list at investigation time, conditioned on:

- The kind of system (database vs frontend vs message broker vs ML training job vs payment processor — each has its own characteristic failure modes)
- The signals present in the inventory
- The shape of the anomaly observed

If your investigation only considers the modes from a familiar checklist, you will systematically miss modes outside that checklist. The candidate list should be wide enough to include modes you don't initially consider likely. It is much cheaper to rule a mode out with a single targeted query than to overlook it entirely.

#### C.3 — For each candidate mode, identify its direct indicator(s)

For each candidate mode, ask: **what evidence, if present, would directly imply this mode (not merely be consistent with it)?**

Direct indicators take a small number of structural shapes. Use these as a checklist when constructing the indicator queries:

- **Resource saturation against a limit** (usage / limit at high percentage in the anomaly window). Requires identifying the limit.
- **A monotonic resource counter (count of in-use items) at an anomalous level** — counts of in-flight things, not rates.
- **A specific log class** — a message that names the failure mode unambiguously, distinct from generic error/exception text.
- **A structural signature in trace shape** — e.g., a span whose duration is dominated by self-time with no compute activity (characteristic of waiting on the network); a leaf span much slower than baseline with no resource pressure on either endpoint.
- **A causal-state transition** — restart, crash, leadership change, reconfiguration event recorded somewhere.
- **A counter rate that collapses or reverses across two anomaly samples** — implies stall or restart, not "recovery".

For each candidate mode, write down (or query for) at least one such direct indicator. **A candidate with no direct indicator cannot be named as the mode.** It can only be reported as "not ruled in".

#### C.4 — Causal precedence when multiple modes show signals

When more than one candidate mode shows positive direct evidence, **prefer the upstream cause in the causal chain**. The downstream phenomenon often has its own direct indicators that are real but secondary.

This is not specific to any platform. The general pattern:

- A resource exhaustion typically causes downstream connection / IO failures, which are themselves real but secondary.
- A capacity limit (gauge near its ceiling) typically causes rate-based counters to spike (the rate of work failing because it can't get the resource); the gauge wins because the rate is its consequence.
- An upstream cause that explains the downstream effects is preferred over the effects in isolation.

When constructing the candidate-mode set in C.2, include the _upstream candidates_ of any obvious downstream symptom. The most common Phase C error: naming the downstream symptom as the mode and stopping.

#### C.5 — Gauge-before-counter, in general

When a state-of-the-system signal (gauge: in-use count, queue depth, working set, current saturation) and a rate-of-events signal (counter: errors per second, retries per second, drops per second) both fire, the gauge usually wins. The rate is almost always a consequence of the state.

Counter-only evidence with a clean gauge typically means the _event-producing_ rule is something other than the state the gauge measures.

#### C.6 — When no rule fires

If no candidate mode has direct evidence after C.2–C.5, name the result honestly: "anomaly without identified mode" or "delay / latency injection / unspecified", with explicit downgrading of confidence. Do **not** default to the most familiar-sounding category.

This step is essential. The "default to familiar" failure is responsible for many wrong RCAs: the agent finds no clean direct indicator, picks the category that "feels right" from prior experience, and ships a wrong answer with high stated confidence.

---

## Cognitive biases to guard against (the cross-cutting failures)

The misclassifications in past failed runs cluster under a small set of cognitive biases. These guards apply regardless of the failure type or platform.

| Bias                               | Symptom                                                                                                | Guard                                                                                                               |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------- |
| **Magnitude bias**                 | Ranking the largest absolute value as the anomaly                                                      | Always rank by relative deviation against the component's own baseline                                              |
| **Familiarity bias**               | Defaulting to the most familiar-sounding mode when evidence is ambiguous                               | Require direct evidence per C.3; "no direct evidence" is a valid output                                             |
| **First-anomaly bias**             | Stopping at the first anomalous component without walking outbound dependencies                        | B.7 is mandatory, even when #1 already has strong evidence                                                          |
| **Symptom-as-cause bias**          | Using a generic symptom log ("connection refused", "timeout", "error") as primary mode-naming evidence | C.1: symptom logs are context only                                                                                  |
| **Field-name bias**                | Naming a mode from a category keyword in a field name (the field "memory_X" must be a memory issue)    | Mode is named only by the structured indicator check, not by field naming convention                                |
| **Originator-observer confusion**  | Attributing path latency to the caller when it belongs to the destination                              | Cross-component path-latency check in B.7 (D vs D′)                                                                 |
| **Pre-existing-as-anomaly**        | Ranking the chronically slow component first                                                           | B.4 filter                                                                                                          |
| **Counter-absolute-value bias**    | Treating a counter's magnitude as a signal                                                             | B.1: counters require rate computation                                                                              |
| **Magnitude-of-severity bias**     | Ranking by log severity rather than by sustained impact                                                | Sustained silent latency degradation usually outranks a self-recovering ERROR                                       |
| **Symptom-service-named-as-cause** | An error message names component X; agent reports X as root cause                                      | The originator is the component whose _spans / metrics_ show the anomalous pattern, not the one named in error text |
| **Counter-collapse-as-recovery**   | A rate falling across the anomaly window is misread as the system recovering                           | A counter rate that collapses or reverses indicates stall / restart, a strong originator signal                     |
| **Confirmation bias**              | Forming a hypothesis early in Phase A or B and then selectively interpreting later evidence            | Phase ordering and gates exist to prevent this; do not advance phases out of order                                  |

---

## Hard constraints

- NEVER form a hypothesis before Phase A and Phase B complete.
- NEVER rank a metric anomaly by absolute value; rank by relative deviation against the component's own baseline.
- NEVER compute a ratio on a counter's absolute value; counters require two samples and a rate.
- NEVER narrow to a subset of signal categories before Phase A.2 completes.
- NEVER miss a pre-saturated resource because its ratio is ≈ 1.0; saturation against a limit is itself a signal.
- NEVER name a mode from a single category keyword in a field name.
- NEVER skip the causal-direction walk in B.7, even when #1 already has strong evidence.
- NEVER use a downstream-symptom log line as primary mode-naming evidence.
- NEVER name a mode without at least one direct indicator (C.3); honestly report "no direct evidence" instead of defaulting to the most familiar category.
- NEVER attribute network-path latency to the caller when caller D ≫ callee D′; the loss is on the path TO the callee.
- NEVER name the component mentioned in an error message as the root cause; the originator is the component whose spans / metrics show the anomalous pattern.
- NEVER skip the log sweep / direct-indicator check for any candidate; "no signal found" is the explicit output, not silence.
- NEVER conclude "no errors" from a single error-field query; error fields vary across instrumentations on the same system.
- NEVER let the final hypothesis disagree with the top-ranked candidate from Phase B. If they conflict, redo B.7 — the candidate ranking is authoritative.
- NEVER use familiarity ("this looks like a CPU problem") as a substitute for direct evidence.

---

## Outputs the consumer should produce

Whatever artifact format the consumer chooses, it must contain:

1. **Top candidate originator(s)** with the criteria from B.6 that placed them.
2. **Named failure mode** if a direct indicator was found in C.3, OR an explicit "no direct evidence; mode unidentified" with reduced confidence.
3. **Direct-evidence citations** — the specific indicator(s) backing the named mode. Latency alone is never sufficient.
4. **Causal chain** — if a downstream was promoted in B.7, the parent → downstream relationship and the promotion criterion.
5. **Final-hypothesis-to-#1-candidate alignment**. If they disagree, redo B.7 — the candidate ranking wins.

---

## Illustrative examples (NOT prescriptive)

The skill's principles are abstract by design. To make them concrete, here are examples drawn from cgroup / Kubernetes / Linux observability — these are _examples of what direct indicators can look like_, not a list of modes the agent must check. Substitute the equivalents from your own platform.

**Example: a usage/limit pair (B.3 saturation pattern)**
On Linux cgroup-v1 metrics, `memory_usage_bytes` ↔ `memory_limit_bytes` is one such pair; on Windows containers it is different; on cloud-managed runtimes (Lambda, Fargate) the limit may not be exposed at all. The principle — find the pair, check saturation in both windows — is universal. The field names are not.

**Example: a direct-indicator log class (C.1 direct evidence)**
A log line `OOMKilled` directly names the failure mode. A log line `connection refused` does not — it is consistent with several modes. The distinction between the two is a property of the message, not of the platform.

**Example: a structural trace signature (C.3 indicator)**
A leaf span (no children) whose duration is much greater than its baseline AND whose host process shows no compute activity AND no error logs — the entire span duration is wait time on something external. If neither endpoint shows resource pressure, the latency is on the path between them. The pattern works for HTTP, gRPC, message-queue consumes, database calls, RPC over any transport.

**Example: a causal chain (C.4)**
A capacity exhaustion (gauge near its ceiling) often produces downstream rate-based failures (counters of dropped / rejected / failed events). The gauge is the upstream cause; the counters are the downstream effects. The general pattern — gauge wins over counter — applies to many resource families: connection pools, file descriptors, thread pools, queue capacity, memory, disk space. The list of resources that exhibit this pattern is system-specific; the principle is not.

**Example of what NOT to do**
Do not transcribe the example field names above into a decision table and treat that as the methodology. They are illustrations of one platform. The methodology is the discipline of (a) discovering schema, (b) finding the originator by relative-deviation ranking and causal walk, (c) naming the mode only from direct indicators appropriate to the system at hand.
