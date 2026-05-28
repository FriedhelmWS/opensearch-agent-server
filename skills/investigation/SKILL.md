---
name: investigation
description: "General-purpose Root Cause Analysis methodology for distributed systems and any incident investigation backed by observability data. Telemetry-agnostic, failure-mode-agnostic, and platform-agnostic: encodes the DISCIPLINE of investigation (phases, gates, originator-vs-observer reasoning, direct-vs-symptom evidence, counter-vs-gauge, cognitive-bias guards) without prescribing a query language, telemetry backend, taxonomy of failure modes, OTel/Prometheus/StatsD/proprietary naming, or output artifact format. Includes a mandatory breadth-first cross-axis Δ ranking gate (Phase B.0) that must complete before any hypothesis is formed. Activate whenever an agent is debugging a specific incident from any combination of traces, logs, and metrics. Pair with investigation-traces when trace data is available, and with investigation-resource-saturation when per-component resource metrics are available — those companions hold the platform-specific axis catalogs, OTel semantic-convention mappings, trace-shape discriminator matrices, and resource-vs-trace symptom rows."
metadata:
  version: 4.0.0
  tags: [rca, investigation, observability, methodology, distributed-systems]
---

# Investigation Skill

## Overview

A reusable Root Cause Analysis discipline for any agent investigating an incident from observability data. It encodes the lessons that survive across platforms, telemetry backends, and failure taxonomies: where bias enters an investigation, which signals systematically lie, and which decision principles hold up regardless of what's broken.

The skill produces two outcomes:

1. An **originator** — the service / component / boundary whose anomaly _originates_ the failure, distinguished from those that are downstream victims.
2. A **named failure mode** — chosen from candidates _appropriate to the system under investigation_, backed by _direct_ evidence (not symptom logs, not field-name guesses, not familiarity).

The skill is deliberately telemetry-agnostic AND failure-mode-agnostic. It does not enumerate "the 6 ways things break" — that list is a property of the system being investigated, not of RCA. Two companion skills add specifics on demand:

- ``investigation-traces`` — rules that require span / parent-child structure (self-time decomposition, cross-component path latency, latency-shape discrimination), plus the OTel trace conventions and the trace-shape discriminator matrix. Activate when trace data is in scope.
- ``investigation-resource-saturation`` — the resource-axis sweep, the CPU-as-outcome disambiguation, the demand-side / supply-side pair, and the OTel / cgroup / Prometheus naming reference. Activate when per-component resource metrics are in scope.

This file stays universal; activate one or both companion skills when their preconditions hold.

## When to use

Activate when ALL of:

- An agent is investigating a specific incident (latency spike, error rate, anomaly, outage, regression).
- Telemetry is available — any combination of traces, logs, metrics; structured or unstructured.
- The investigation needs to identify a root-cause component AND name what failed.

Do NOT use when:

- The failure mode is already known and the user wants a fix (this is a diagnostic skill, not a remediation one).
- The task is capacity planning, benchmarking, or trend analysis (no incident, no anomaly window).
- Only one signal type is available with no per-component granularity AND no baseline (the methodology requires comparisons).

## What this skill does NOT prescribe

To stay general:

- **Query language / DSL.** OpenSearch DSL, PromQL, LogQL, SPL, X-Ray, Tempo, Datadog query, kubectl, cloud-native APIs all work.
- **Telemetry backend or naming convention.** OpenTelemetry semantic conventions, Prometheus exposition, StatsD, vendor-specific schemas — all are valid; concrete field names live in the companion skills.
- **Taxonomy of failure modes.** The skill does not assume the failure is one of any fixed list. The candidate modes for a given investigation depend on the system: a database might fail via lock contention, plan regression, replication lag, or schema drift; a frontend might fail via bundle-size regression, third-party-script latency, or CDN routing; a payments system might fail via idempotency-key collision, rate-limit at a partner, or a cert-rotation regression. The agent enumerates plausible modes for _this_ system; the skill provides the framework for testing each one rigorously.
- **Specific axes.** The B.0 cross-axis sweep below names only the universal observability axes (latency, throughput, error rate). Domain-specific axes (resource saturation, trace self-time, etc.) are added by companion skills when their preconditions hold.
- **Specific thresholds.** "≥ 2x" and "≥ 80%" are illustrative defaults — actual thresholds depend on the system's normal variance.
- **Output artifact format** — JSON, prose, ticket comment, dashboard, the consumer chooses.

A consuming agent layers its own scaffolding on top: query templates for its backend, a candidate-mode list relevant to the system, an output contract for its caller.

---

## The methodology

Three phases. Do not advance to the next phase until the current phase's gates are satisfied. Most failed RCA runs come from racing through phases or skipping gates.

### Phase A — Understand the data (before searching it for evidence)

#### A.1 — Schema discovery

For each telemetry source in scope, sample one document. Record:

- **Time field and unit** for each source (sources frequently disagree).
- **Identifier fields** that link components across sources. These are usually three different field names — the trace's service identifier rarely matches the log's container identifier or the metric's entity prefix.
- **Trace-shape fields** (when traces are present): how a span identifies its parent, how a root span is marked, where errors are recorded. Try multiple paths — error fields often differ between instrumentations on the same system.
- **Duration units.** Off-by-1000 errors from confusing ms / µs / ns are a recurring silent failure.

If any of the above is ambiguous, plan now to verify it before relying on it. A query that returns zero hits is much more often a wrong field name than a real absence.

**Gate before advancing**: explicitly state, for each source, which fields you will use as time, identifier, error-path, and (for traces) duration + parent linkage.

#### A.2 — Signal inventory

For each source, enumerate what's actually available — not what you expect or remember.

- For metrics: list every numeric field. Group at a coarse level by what the field is *measuring*, not by what you assume the failure is. The grouping is descriptive, not exhaustive.
- For logs: identify which fields are structured vs free-text, what severity levels exist, and what cardinality the message field has.
- For traces: span attributes vary by instrumentation library; note which categories of attributes are populated.

**Gate before advancing**: state which signal categories ARE present and which are absent. Premature narrowing here ("I'll just look at the most familiar axis") is the most common cause of missing the actual root cause when it lives in a category the agent didn't scan. Phase B.0 will then enforce a breadth-first Δ ranking across every axis the inventory shows is available; companion skills (when activated) tighten their respective subsets of that sweep.

---

### Phase B — Find the originator (not the observer)

Goal: identify the component whose anomaly _originates_ the failure, separating it from components that are visible victims.

#### B.0 — Cross-axis Δ ranking sweep (mandatory before any hypothesis)

Before reading any traces, before reading any logs in depth, before forming any hypothesis: rank every component on every available axis by **delta vs baseline**, breadth-first. The single most reliable failure pattern across past failed runs is the agent latching onto the loudest visible signal (a dramatic latency span, a vivid error log, the most familiar metric) and skipping the breadth-first scan that would have surfaced the actual originator on a quieter axis.

For each component (service / pod / instance / partition) compute Δ = `(value in anomaly window) − (value in baseline window)`, normalised to relative change where appropriate, on each axis the data supports.

The **universal axis set** — present in any observability stack:

| # | Axis | What to compute | What counts as a hit |
|---|---|---|---|
| 1 | **Latency** | p50, p95, p99 deltas per (component, operation) | Sustained right-shift; especially p99 deltas ≫ p50 deltas (heavy-tail signal) |
| 2 | **Throughput / request rate** | Δ in requests-per-second per component | Either a spike (caller retry storm) OR a sudden drop (component became unreachable — silent loss) |
| 3 | **Error rate / error content** | Δ in error responses / non-success / span-error per component, AND new or surging exception classes / status codes / message keywords in logs and span events | Note: many real failures don't increment error counters at all; silence here is NOT exoneration. A NEW exception class (zero baseline → non-zero anomaly) on a component is a first-class hit on its own, even when the rate axis looks flat. Note who **generates** vs **receives** the exception — receive-side exceptions name the caller's surface, not the cause |

These three are the "Golden Signals" subset that applies to every system. The companion skills extend this set when their preconditions hold:

- The ``investigation-resource-saturation`` companion adds the resource-saturation axes (compute / memory / storage I/O / network / connections / GC) and provides the OTel / Prometheus / cgroup naming reference that maps each axis to actual field names.
- The ``investigation-traces`` companion adds a self-time axis (T.1) and the latency-shape axis (T.4), both of which are decisive when present.

When a companion is active, its axes are added to B.0; absence of data is recorded as "indicator unavailable", never silently skipped.

**The output of B.0 is a ranked anomaly table**, one row per (component, axis) hit, sorted by Δ magnitude (relative). The candidate set in B.6 will be drawn from this table. A component does NOT need to score on multiple axes to be a candidate — a single decisive axis hit is sufficient to enter the candidate set.

**Coverage requirement**: every axis above for which data is available must produce at least one ranked entry (or be explicitly marked "indicator unavailable"). Reflection logs from past failed runs cluster on the same partial-coverage anti-patterns:

- "Anchored on the loudest single axis; never expanded to the full set" → the originator's deviation was on a smaller-magnitude but more decisive axis.
- "Anchored on the most verbose error log" → the originator emitted no error log at all; it failed silently on throughput or self-time.
- "Checked the familiar axes; concluded no cause" → the unfamiliar axis (the one the agent doesn't normally look at) was where the cause lived. The companion-skill-specific instance of this anti-pattern lives in the relevant companion.

**Gate before advancing to B.1**: present the ranked anomaly table. If any axis with available data is absent from the table, do not proceed. If a candidate's only hit is on an unexpected axis, do not down-weight it on that basis — record it at the rank its Δ earns.

The discipline B.0 enforces is **breadth before depth**. Once the ranked table is in hand, B.1–B.7 sharpen the candidates; without the table, B.1–B.7 cannot correct for the wrong starting set.

#### B.1 — Counter vs gauge discipline

Every numeric metric is one of:

- **Counter**: monotonic since process start. Absolute value is uptime, not signal. **Counters require two samples per window and a rate computation** before any ratio is meaningful.
- **Gauge**: instantaneous sample. Diff or average directly.

Mixing these up is the most common quantitative error in metric analysis. A "huge counter value" tells you nothing; a counter that didn't change between two samples has rate zero regardless of magnitude.

**Recognising counter vs gauge before you query**, in order of decreasing reliability:

1. **Sampling pattern.** Pull two consecutive sample values. If monotonically non-decreasing across many samples, it is a counter. If it fluctuates around a mean, it is a gauge. This test is decisive — naming conventions can lie (some exporters emit pre-rated values under counter-style suffixes), but two consecutive samples cannot.
2. **Naming convention.** Many exporters end counters with `-total` / `_total` / `.count`; gauges typically end in nouns describing current state. Use as a hint, verify with sampling. (Platform-specific naming maps live in ``investigation-resource-saturation``.)
3. **Magnitude.** A reading whose absolute value only makes sense as elapsed time / bytes since process start (e.g., a "CPU reading" of double-digit seconds at a 1s scrape interval) implies counter, not gauge.

#### B.2 — Rank by relative deviation, not absolute magnitude

Compare each metric in the anomaly window to its baseline. Rank candidates by **how much they deviated from their own normal**, not by absolute value.

A signal going `5 → 70` is a 14× anomaly and almost certainly more important than one going `900 → 1000`, even though the second has a much bigger raw number. Magnitude bias — ranking the most familiar-looking large number first — is one of the most reliable ways to misidentify the originator.

The threshold for "anomalous" depends on the system. For metrics with low normal variance, 1.3× may be significant; for chatty noisy metrics, 3× may be background. Threshold calibration is the consumer's responsibility; the principle (relative deviation, not absolute) is universal.

#### B.3 — Saturation-pair check (whenever a usage / limit pair exists)

Some resources are anomalous at a constant ratio because they were already pinned at the limit. Whenever a usage / limit pair exists for any resource, compute saturation in both windows and flag pre-saturated resources even when their ratio is ≈ 1.0.

The pattern: usage / limit ≥ high-percentage in _both_ windows. The resource was already at the wall; the anomaly is that work is now contending for it. The companion ``investigation-resource-saturation`` skill (rule R.3) operationalizes this with the additional requirement to pair every usage gauge with the corresponding throttle / wait counter, and provides the platform-specific saturation-pair conventions.

#### B.4 — Pre-existing-condition filter

A component whose baseline anomaly-signal value is already a substantial fraction of its anomaly-window value is _pre-existing_, not freshly anomalous. Discard it from candidates UNLESS its anomaly-window value is also a large multiple of its baseline.

Past failures at this step look like ranking the consistently-slow component as #1 simply because it has the highest absolute latency in the anomaly window — when it had the same latency yesterday.

#### B.5 — Per-component variance

For each component, compute over the anomaly window:

- Latency distribution (p50, p95, count) and how it changed vs baseline.
- Inflection points, progressive degradation, sustained elevation, request-rate collapse.
- Variance shape — large p95/p50 ratio is a generic anomaly signal; the specific shape catalog (point-mass / bimodal-within-trace / bimodal-RTO / unimodal right-shift / heavy tail) and what each shape diagnoses live in ``investigation-traces`` (rule T.4) when trace data is available.

Components with high p95/p50 ratio are deep-dive candidates regardless of their magnitude rank.

#### B.6 — Candidate set

The input to B.6 is the ranked anomaly table from B.0, refined by B.1–B.5.

A component enters the candidate set if ANY of:

1. It has the earliest onset of degradation across the time-bucketed view.
2. Its variance shape (p95/p50, distribution shape) is anomalous.
3. It has a unique-to-itself signal (not shared across multiple components) — **including a single decisive axis hit from B.0** with no peer comparable. The axis on which the unique signal lands does not down-rank it; what matters is that the deviation is real and large relative to the component's own baseline.
4. It exhibits high self/total time on its own spans (when trace data is available — see ``investigation-traces`` T.1).
5. Its per-call distribution is uniformly elevated across many distinct callers (downstream slowness regardless of who calls it).
6. It dominates the top of cumulative-duration / cumulative-self-time lists (weakest signal — duration alone never wins ties).

Rank by some form of `relative_anomaly × throughput / pre_existing_factor`. Moderate anomaly on a high-throughput component beats severe anomaly on a low-throughput one; pre-existing elevated components are down-weighted.

**Do not silently demote candidates because their hit is on an "unfamiliar" axis.** A service whose only B.0 hit lands on an axis the agent does not normally look at is still a candidate; defaulting to a more familiar-axis candidate is exactly the pattern reflection logs identify as the dominant Phase B failure.

#### B.7 — Causal-direction walk (the most-skipped step in failed runs)

**Even when the #1 candidate has strong evidence, walk its outbound dependencies before committing.** Past failed runs almost always skipped this step.

For the current #1, find the components it calls. For each downstream:

1. Compute its own anomaly-vs-baseline ratio on its own spans / metrics.
2. Check whether it has a unique signal the parent lacks.
3. Check whether the parent's apparent self-time is dominated by waiting on this downstream.

Promote a downstream over the parent when any of (1)–(3) is true. Descend up to a few hops. Prefer unique downstream anomalies over cascading shared signals.

When trace data is available, the companion ``investigation-traces`` skill (rule T.3) adds the cross-component D vs D' check — the most reliable defense against attributing path latency to the caller when it belongs between caller and callee, or to the callee's ingress queue.

The single recurring symptom of this step being skipped: H1 names a service that appears slow because it was waiting on a downstream that is the actual originator.

**Gate before advancing**: state explicitly which dependency walk you performed and whether you promoted a downstream. Do this even when no promotion happens.

---

### Phase C — Name the failure mode (from direct evidence only)

Goal: name what failed, using only direct indicators. Phase C is where most past runs failed by defaulting to the most familiar-sounding category.

#### C.1 — Direct evidence vs symptom evidence

This distinction is universal across failure types and platforms.

- **Direct evidence**: evidence whose presence implies the mode. A resource gauge near its limit, a log message that names a specific failure class, a structural signature in trace shape, a distinctive sequence of state transitions. Direct evidence on its own can name a mode.
- **Symptom evidence**: evidence that is consistent with the mode but also consistent with several other modes. Generic error / exception log lines, "downstream timed out", "connection failed" — these admit multiple root causes. Symptom evidence is **context only**; it cannot name a mode by itself.

The single most common Phase C failure: using the loudest symptom log line as primary mode-naming evidence. A generic message like "connection reset by peer" is consistent with many distinct underlying mechanisms; using it to name any one of them without checking the others is unsafe.

#### C.2 — Enumerate candidate modes for THIS system

The skill does not provide a fixed mode list. Build the candidate-mode list at investigation time, conditioned on:

- The kind of system (database vs frontend vs message broker vs ML training job vs payment processor — each has its own characteristic failure modes).
- The signals present in the inventory.
- The shape of the anomaly observed.

If your investigation only considers the modes from a familiar checklist, you will systematically miss modes outside that checklist. The candidate list should be wide enough to include modes you don't initially consider likely. It is much cheaper to rule a mode out with a single targeted query than to overlook it entirely.

#### C.3 — For each candidate mode, identify its direct indicator(s)

For each candidate mode, ask: **what evidence, if present, would directly imply this mode (not merely be consistent with it)?**

Direct indicators take a small number of structural shapes:

- **Saturation against a limit** — a resource gauge / occupancy meter pinned at or near its capacity, paired with the corresponding pressure / wait / throttle counter rising. Requires identifying the limit; companion skills carry the platform-specific saturation-pair conventions.
- **A monotonic resource counter (count of in-use items) at an anomalous level** — counts of in-flight things, not rates.
- **A structural signature in trace shape** — the catalog of shapes (point-mass, bimodal-within-trace, bimodal-RTO, unimodal right-shift, heavy tail) and what each diagnoses lives in ``investigation-traces`` (T.4).
- **A causal-state transition** — restart, crash, leadership change, reconfiguration event recorded somewhere.
- **A counter rate that collapses or reverses across two anomaly samples** — implies stall or restart, not "recovery".

For each candidate mode, write down (or query for) at least one such direct indicator. **A candidate with no direct indicator cannot be named as the mode.** It can only be reported as "not ruled in". The reverse also holds: **the absence of a non-specific signal does not falsify a mode.** A keyword's non-appearance in a log corpus rules out that exact keyword, not the underlying mode — many degradations produce no distinctive log line at all.

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

The misclassifications in past failed runs cluster under a small set of cognitive biases. These guards apply regardless of the failure type or platform. Companion skills carry their own additional bias rows specific to their domains (e.g., resource-axis-narrowing in ``investigation-resource-saturation``; trace-leaf-bias in ``investigation-traces``).

| Bias                               | Symptom                                                                                                | Guard                                                                                                               |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------- |
| **Magnitude bias**                 | Ranking the largest absolute value as the anomaly                                                      | Always rank by relative deviation against the component's own baseline                                              |
| **Familiarity bias**               | Defaulting to the most familiar-sounding mode when evidence is ambiguous                               | Require direct evidence per C.3; "no direct evidence" is a valid output                                             |
| **First-anomaly bias**             | Stopping at the first anomalous component without walking outbound dependencies                        | B.7 is mandatory, even when #1 already has strong evidence                                                          |
| **Symptom-as-cause bias**          | Using a generic symptom log ("connection refused", "timeout", "error") as primary mode-naming evidence | C.1: symptom logs are context only                                                                                  |
| **Field-name bias**                | Naming a mode from a category keyword in a field name (the field "memory_X" must be a memory issue)    | Mode is named only by the structured indicator check, not by field naming convention                                |
| **Originator-observer confusion**  | Attributing path latency to the caller when it belongs to the destination                              | Cross-component path-latency check in B.7 (companion ``investigation-traces`` rule T.3)                              |
| **Pre-existing-as-anomaly**        | Ranking the chronically slow component first                                                           | B.4 filter                                                                                                          |
| **Counter-absolute-value bias**    | Treating a counter's magnitude as a signal                                                             | B.1: counters require rate computation                                                                              |
| **Magnitude-of-severity bias**     | Ranking by log severity rather than by sustained impact                                                | Sustained silent latency degradation usually outranks a self-recovering ERROR                                       |
| **Symptom-service-named-as-cause** | An error message names component X; agent reports X as root cause                                      | The originator is the component whose _spans / metrics_ show the anomalous pattern, not the one named in error text |
| **Counter-collapse-as-recovery**   | A rate falling across the anomaly window is misread as the system recovering                           | A counter rate that collapses or reverses indicates stall / restart, a strong originator signal                     |
| **Axis-narrowing bias**            | Concluding "no cause" after checking only the most familiar axes                                       | B.0 coverage requirement — every available axis produces a ranked entry; the unfamiliar axis is where cause hides   |
| **Confirmation bias**              | Forming a hypothesis early in Phase A or B and then selectively interpreting later evidence            | Phase ordering and gates exist to prevent this; do not advance phases out of order                                  |

---

## Symptom-shape → discriminating-axis matrix (universal rows)

Loud-but-ambiguous symptoms look the same across very different failure modes. Resolving each symptom requires querying a **specific discriminating axis** — usually NOT the axis the symptom appeared on.

This matrix carries only the rows that are universal across observability stacks. Trace-shape rows (parent-span ≫ children, anomalously short span, bimodal latency clusters, etc.) live in ``investigation-traces``; resource-axis rows (CPU drop + throughput drop, memory-failure + I/O, four-axis decisions) live in ``investigation-resource-saturation``. Companion-specific rows are added on top of these when the companion is active.

| Symptom shape (what you see first) | Naive interpretation (often wrong) | Discriminating axes to query (in order) | Decisive distinguishing fingerprints |
|---|---|---|---|
| **A downstream returns null / empty / "not allowed" / "status=0"** | "Downstream is the bug" | (1) the *upstream* dependency's own latency, error-rate, throughput, and resource axes; (2) the upstream's restart / health-state / crash events | Null / empty responses are usually the **silent failure** signature of the upstream that the downstream depends on (auth couldn't validate, data-service couldn't return data, route-service was unreachable). The error-emitting service is rarely the originator; the service whose data is missing is. |
| **An error message names another service** ("UnknownHost: ts-X", "connection refused to ts-X") | "Service ts-X is the cause" | (1) the *emitting* service's own egress / network metrics, (2) the *named* service's own health on a neutral health-check probe | The originator is the component whose **spans / metrics** show the anomalous pattern, not the one named in error text. A connection-refused error to ts-X is consistent with: (a) ts-X being down, (b) the emitter's egress being broken, (c) the network path being broken. The emitter's own egress metrics decide. |
| **Loud cascading errors in many downstream services + one quiet upstream** | "All those services are degraded" | (1) earliest-onset timestamp per component, (2) self-time / inclusive-duration Δ on the quiet upstream (T.1 when traces present), (3) request-rate / error-rate on the quiet upstream | The earliest-onset component is structurally more likely to be the originator than the loudest-onset component. Cascade volume is a function of fan-in, not causation. |
| **A service shows large absolute counter values** | "That's a huge number, must be the cause" | (1) compute the **rate** (Δ ÷ window) and the **relative deviation** vs baseline; rank by relative Δ, not absolute | Absolute counter magnitude is uptime, not signal. The counter that grew 22 000× from a small base beats one whose absolute is large but stable. Always rank by Δ, not value. |
| **An obvious mechanism explains everything** ("missing index", "GC thrash", "lock contention", "the obvious one") | "I have my answer" | (1) one query that **falsifies** the obvious mechanism on a different axis (saturation pair, distribution shape, throughput direction) | Familiar mechanisms are anchored to early. The discipline: before committing, write down what evidence would falsify the mechanism, then query for it. If the obvious mechanism is right, the falsification query simply confirms it; if wrong, it reveals the actual axis. |

**How to use the matrix**: when you observe one of the symptom shapes in the left column, the discriminating axes in column 3 are MANDATORY — not optional follow-ups. The naive interpretation in column 2 is the trap reflection logs document; column 4 lists the fingerprint that distinguishes the right answer from the trap.

The matrix is not exhaustive. When a symptom doesn't match any row, fall back to the B.0 sweep in full and let the ranked table speak. Companion skills add their own symptom-shape rows when activated.

---

## Hard constraints

- NEVER form a hypothesis before Phase A and Phase B complete.
- NEVER skip the B.0 cross-axis Δ ranking sweep. The ranked anomaly table — covering every axis for which data is available — is mandatory before B.1; without it, the candidate set in B.6 is drawn from whichever axis the agent happened to look at first.
- NEVER rank a metric anomaly by absolute value; rank by relative deviation against the component's own baseline.
- NEVER compute a ratio on a counter's absolute value; counters require two samples and a rate.
- NEVER narrow to a subset of signal categories before Phase A.2 completes.
- NEVER miss a pre-saturated resource because its ratio is ≈ 1.0; saturation against a limit is itself a signal.
- NEVER name a mode from a single category keyword in a field name.
- NEVER skip the causal-direction walk in B.7, even when #1 already has strong evidence.
- NEVER use a downstream-symptom log line as primary mode-naming evidence.
- NEVER name a mode without at least one direct indicator (C.3); honestly report "no direct evidence" instead of defaulting to the most familiar category.
- NEVER name the component mentioned in an error message as the root cause; the originator is the component whose spans / metrics show the anomalous pattern.
- NEVER treat a null / empty / "not allowed" / "status=0" downstream response as evidence the downstream service is the originator. Such responses are the silent-failure signature of an upstream dependency; investigate the upstream before naming the downstream.
- NEVER demote a candidate because its only B.0 hit is on an "unfamiliar" axis. The unfamiliar axis is exactly where reflection logs show the actual originator hides.
- NEVER treat the absence of a generic-keyword match in logs as falsification of a candidate mode; absence of a non-specific signal proves nothing about the mode.
- NEVER conclude "no errors" from a single error-field query; error fields vary across instrumentations on the same system.
- NEVER let the final hypothesis disagree with the top-ranked candidate from Phase B. If they conflict, redo B.7 — the candidate ranking is authoritative.
- NEVER use familiarity as a substitute for direct evidence.

Companion skills carry their own additional constraints — when activated, they apply on top of these.

---

## Outputs the consumer should produce

Whatever artifact format the consumer chooses, it must contain:

1. **B.0 ranked anomaly table** — one row per (component, axis) hit, with Δ vs baseline and the value in each window. Axes with no available indicator are listed as "unavailable", not omitted.
2. **Top candidate originator(s)** with the criteria from B.6 that placed them, and which row(s) of the B.0 table they were drawn from.
3. **Named failure mode** if a direct indicator was found in C.3, OR an explicit "no direct evidence; mode unidentified" with reduced confidence.
4. **Direct-evidence citations** — the specific indicator(s) backing the named mode. Latency alone is never sufficient.
5. **Causal chain** — if a downstream was promoted in B.7, the parent → downstream relationship and the promotion criterion.
6. **Final-hypothesis-to-#1-candidate alignment**. If they disagree, redo B.7 — the candidate ranking wins.

Companion skills add their own required outputs — see ``investigation-traces`` (self-time deltas, shape classifications, gap-position attribution) and ``investigation-resource-saturation`` (resource-axis sweep results, CPU disambiguation, demand-vs-supply alternative).
