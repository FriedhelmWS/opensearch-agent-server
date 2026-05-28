---
name: investigation-resource-saturation
description: "Resource-saturation discipline that complements the core ``investigation`` skill. Activate when per-component metrics are available (CPU, memory, filesystem/disk, network/socket, connection pools, file descriptors, etc.). Encodes the rules that prevent the most common metric-side miss patterns: failing to sweep all four resource axes before naming a mechanism, treating CPU as a mechanism rather than an outcome, and defaulting to the familiar saturation type when latency rises silently. Includes a reference of OpenTelemetry semantic-convention metric names (system.*, container.*, process.*, jvm.*, db.client.*) alongside cgroup-v1 / Prometheus field names — substitute for CloudWatch / Datadog / non-container deployments."
metadata:
  version: 1.1.0
  tags: [rca, metrics, saturation, observability, methodology, opentelemetry]
---

# Investigation — Resource Saturation

## When to use

Activate when ALL of:

- The core ``investigation`` skill is already in scope.
- Per-component resource metrics are available — at minimum, two of {CPU, memory, disk I/O, network / connections}.
- The investigation involves a latency, throughput, or saturation anomaly (this skill says little about correctness or routing anomalies).

Skip when:

- The data sources are pure logs + traces with no per-component resource counters.
- The system has no resource limits to compare against (raw VM metrics with no quota / no peer baseline).

## Why a separate skill

The core ``investigation`` skill teaches direct-vs-symptom evidence and originator-vs-observer reasoning. It does not teach **which families of resource saturation to enumerate** before declaring "no resource cause found". Reflection logs from past failed runs cluster overwhelmingly on two patterns this skill addresses:

1. **Partial resource sweep**: "I checked CPU and memory, but never queried disk I/O" / "I never queried network counters" / "I never queried connection-pool gauges".
2. **CPU-as-mechanism confusion**: "CPU was high so I named it compute saturation" — but CPU is consumed equally by compute work, iowait spinning, syscall churn, busy-wait on locks, and connection-retry loops. CPU is an **outcome**, not a mechanism.

The rules below operationalize "always sweep all four axes" and "disambiguate CPU before naming a cause" so they cannot be silently skipped.

---

## R.1 — The four-axis sweep is non-optional

Before naming any resource saturation as the failure mode (or declaring "no resource cause"), explicitly probe all four axes for the candidate component:

| Axis | What to probe | Direct-indicator shape |
|---|---|---|
| **Compute** | CPU usage rate, throttle counters, run-queue length, scheduling delay | Usage at limit AND throttled-time counter rising; OR run-queue depth > core count sustained |
| **Memory** | Working-set / RSS vs limit, swap, allocation-failure counter, GC pause time (if managed runtime) | Working-set ≥ high% of limit; OR allocation-failure rate > 0; OR GC pause time fraction climbing |
| **Storage I/O** | Read / write bytes rate, IOPS rate, I/O-time fraction (await %), filesystem usage / inode usage | I/O-time fraction climbing OR write-rate ≫ baseline rate; saturated FS or inode count |
| **Network / connections** | NIC errors / drops, TCP retransmits, socket counts (ESTABLISHED / TIME_WAIT / CLOSE_WAIT), connection-pool active / pending / wait-time, file-descriptor usage vs limit | Retransmit rate climbs; pool wait-queue non-empty; FD usage near limit; sockets piling in TIME_WAIT or CLOSE_WAIT |

**A saturation hypothesis is not falsified by checking one axis and finding it clean.** "Not CPU" does not mean "not saturated"; it means "not compute-bound — check the other three". This is the single most repeated reflection-log finding: "silent latency + clean CPU/memory" is the **signature of disk or network saturation**, not the absence of saturation.

**Required outputs** when ranking saturation as a candidate or ruling it out:

- For each of the four axes: at least one direct-indicator query, with the value in baseline and anomaly windows.
- When an axis lacks an exposed indicator (e.g. cloud-managed runtime hides the limit), say so explicitly — do not silently skip.

---

## R.2 — CPU is an outcome, not a mechanism

A CPU-rate spike is consistent with all of:

- **Compute work** — actual computation (BCrypt, encryption, big aggregations).
- **iowait spinning** — threads parked on disk I/O; depending on cgroup accounting, time spent in iowait may be charged to the container's CPU counter.
- **Connection / DNS / TLS retry loops** — busy-wait on a broken socket burns user-space CPU without doing useful work.
- **Lock contention with spin-then-park** — initial contention spins on CPU before parking; many fast lock acquisitions look like compute load.
- **GC thrash** — high allocation pressure that triggers frequent GC fires; the GC threads consume CPU, the worker threads stall.

**Never name "CPU saturation" as the mechanism without disambiguating which of the above is producing the CPU consumption.** The discriminating evidence comes from somewhere else, typically:

- **Throttled-time counter** rising → CFS throttling against a quota → genuine compute saturation against the configured cap.
- **iowait CPU mode** rising independently of user-mode CPU → blocked on storage / disk.
- **Per-request CPU-time** rising while throughput stays flat → per-request compute cost up (legitimate compute regression).
- **Per-request CPU-time** flat while wall-clock-per-request rises → wait, not compute (most often disk or network).
- **Throughput collapsing while CPU stays elevated** → busy-wait / retry loop, not productive work.

The single test that most often discriminates: **(per-request CPU-time-delta) / (per-request wall-clock-delta)**. If wall-clock grew 10× but CPU-time-per-request stayed flat, the added latency is non-compute. If both grew proportionally, it is compute-bound.

---

## R.3 — Saturation-against-limit is a signal even when the ratio is constant

When usage / limit is at a high ratio in **both** baseline and anomaly windows, the resource is **already pinned**. The anomaly cannot show up as a rising ratio because the ratio is censored at the ceiling. Instead it shows up as:

- A throttled-time / wait-time / queue counter rising (if the limit is enforced by a counter-emitting mechanism).
- Latency rising on the consumers of this resource.
- Throughput collapsing as workers queue.

**Treat usage/limit ≥ ~80% in both windows as evidence FOR a capacity-induced cause, not evidence against one.** The most common reflection-log mistake under this rule: "CPU was already at 200% of quota in baseline and stayed there in anomaly, so CPU is not the anomaly" — wrong; the anomaly moved into queue-depth and per-request latency, not into the headroom-zero usage gauge.

For each saturation pair (usage, limit) you check, **always pair it with the corresponding pressure / throttle / wait counter** in the same query. The pressure counter is what moves once the limit is pinned.

---

## R.4 — Apparent improvement is not neutral

A counter going down, a rate dropping to zero, or a usage gauge falling **is itself a signal**. It can mean:

- The component recovered (rare, and only if other indicators agree).
- The component **stalled** — no work is reaching the resource because threads are blocked elsewhere (network, lock, GC).
- Measurement loss — the counter source crashed / scrape gap / counter reset.
- Throttling — admission rate dropped externally.

**When the leading candidate "looks better" on one axis, you must actively probe whether other axes are picking up the slack.** Reflection logs repeatedly show CPU dropping to half of baseline being read as exoneration, when it was actually the symptom of the worker threads being parked on a slow socket — same workload, no CPU spent on it.

The diagnostic test: **what happened to throughput?** Throughput collapsed AND CPU dropped → workers stalled. Throughput stable AND CPU dropped → genuine recovery or efficiency win.

---

## R.5 — Distinguishing demand-side and supply-side saturation

When a component shows resource saturation, two distinct stories produce identical fingerprints:

- **Demand-side**: the component itself is doing more work — driving more load against a downstream resource (more queries, more allocations, more network calls).
- **Supply-side**: the resource the component depends on has degraded — same workload pattern, but each unit of work is taking longer on the resource side.

These two cannot be discriminated from the consumer's metrics alone. The discriminating evidence comes from:

- The **resource side's own metrics** (sidecar database, network path, disk node).
- **Per-call latency vs call rate**: demand-side increases call rate × call cost together; supply-side keeps call rate flat or down while call cost rises.
- **Other consumers of the same resource** — supply-side degradation hits multiple consumers symmetrically; demand-side stays localized to the one consumer.

**Always include the symmetric supply-side / demand-side pair in `mechanism_alternatives`.** When the leading mechanism is "service X driving heavy I/O", the alternative is "the I/O subsystem X depends on is itself slow"; when the leading mechanism is "service X out of memory", the alternative is "the allocator / kernel reclaim path is degraded for X". The two produce the same usage curves; only the discriminating evidence above tells them apart.

---

## R.6 — Latency-shape × resource-shape decision matrix

Combine the trace-shape rules from ``investigation-traces`` (T.4) with resource-axis observations to discriminate cause families. This is a **default mapping**, not an exhaustive one — substitute domain-specific signatures from active skills.

| Latency shape | Resource pattern | Likely cause family |
|---|---|---|
| Point mass at round value | Resources flat / down | Injected delay / artificial throttle / fixed sleep |
| Bimodal-within-trace | Resources flat | Connection pool checkout, lock contention, GC pause |
| Bimodal RTO-spaced (200ms / 1s / 3s clusters) | Resources flat | Packet loss / TCP retransmit |
| Unimodal right-shift | Saturation pair pinned, throttle rising | Resource saturation (compute / IO / network depending on which axis is pinned) |
| Heavy tail (p50 stable, p99 explodes) | Periodic spikes correlating with the tail | GC, log rotation, fsync, periodic flush |
| Latency up, throughput down, CPU down | Network / connection / disk wait | Workers parked on a finite-resource wait |
| Latency up, throughput stable, CPU up proportionally | Per-call cost up | Genuine compute regression |
| Latency up, throughput stable, CPU up disproportionately, no work shape change | Busy-wait / retry loop | Connection retry, DNS retry, spin-then-park lock |

---

## Hard constraints (saturation-specific)

These add to the core ``investigation`` skill's constraints and the ``investigation-traces`` constraints; they do not replace either.

- NEVER rule out resource saturation after checking a subset of {CPU, memory, disk, network/connection} axes — the unchecked axis is exactly where reflection logs show the cause hides.
- NEVER name "CPU saturation" as the mechanism without disambiguating compute vs iowait vs busy-wait vs GC vs lock-spin.
- NEVER treat usage/limit ≈ 1.0 in both windows as exoneration; pair every usage gauge with the corresponding throttle / wait / queue counter.
- NEVER read "CPU dropped" or "request rate dropped" as recovery without checking throughput. A dropped rate paired with collapsed throughput is workers stalled, not workers idle.
- NEVER omit the symmetric demand-side / supply-side alternative from `mechanism_alternatives`. Their fingerprints are identical at the consumer; the discriminating evidence is on the resource side.
- NEVER conclude "no resource cause" from a clean working-set ratio; OOM-by-allocation-failure can fire well below the working-set ceiling, and disk/network resources are entirely orthogonal to memory.

---

## Outputs the consumer should produce (saturation-specific additions)

In addition to whatever the core skill produces:

1. **Four-axis sweep result**: for each of {CPU, memory, disk I/O, network/connections}, the value baseline-vs-anomaly and the direct-indicator query that produced it. Missing axes must be marked "indicator unavailable", not omitted.
2. **CPU disambiguation result** if CPU is named: which of compute / iowait / busy-wait / GC / lock-spin, and the discriminating signal that classifies it.
3. **Saturation-pair check** for every usage/limit pair: ratio in both windows AND the throttle / wait counter for the same resource.
4. **Demand-vs-supply pair** in `mechanism_alternatives` whenever the proposed mechanism is a saturation: the symmetric alternative and the discriminating evidence (or its absence).

---

## Platform-specific reference

The rules above are platform-agnostic. The field-name examples below are **specific to two ecosystems** — OpenTelemetry semantic conventions and cgroup-v1 + Prometheus container metrics. On other platforms (CloudWatch, Datadog, Azure Monitor, on-prem-non-Prom) substitute the equivalent fields — the rules and ratios are the same, only the labels change.

### OpenTelemetry semantic conventions (system / container / process / jvm)

OTel publishes a stable set of metric names under the `system.*`, `container.*`, `process.*`, `jvm.*`, and `db.client.*` namespaces. Map the four-axis sweep to these names when the data source is an OTel collector:

| Axis | OTel metric (instrument kind) | Notes |
|---|---|---|
| **Compute** | `system.cpu.utilization` (gauge, 0–1 per state) | Has a `system.cpu.state` attribute: `user`, `system`, `iowait`, `idle`, `steal`, `nice`. The iowait state is the OTel equivalent of cgroup's iowait CPU mode. |
| | `system.cpu.time` (counter, seconds) | Cumulative — rate it. Same `state` attribute as above. |
| | `container.cpu.time` (counter, seconds) | Container-scoped CPU time. Pair with quota — OTel does not yet have a stable throttled-time metric, so for cgroup-throttle data fall back to the Prometheus `container_cpu_cfs_throttled_*` fields below. |
| | `process.cpu.utilization`, `process.cpu.time` | Per-process; useful when one container hosts multiple processes. |
| **Memory** | `system.memory.usage` (gauge, bytes) with `system.memory.state` ∈ `used`, `free`, `cached`, `buffered` | Sum across states ≈ total. |
| | `system.memory.utilization` (gauge, 0–1) | Saturation ratio directly. |
| | `container.memory.usage` (gauge, bytes), `container.memory.utilization` (gauge, 0–1) | Pair with the limit (often emitted as a separate `container.memory.limit`-style attribute or surfaced via the orchestrator). |
| | `process.memory.usage`, `process.memory.virtual` | Per-process RSS / virtual sizes. |
| | `jvm.memory.used`, `jvm.memory.committed`, `jvm.memory.limit` (with `jvm.memory.type` = `heap` / `non_heap`, `jvm.memory.pool.name` for generations) | Heap pressure ratio = `jvm.memory.used{type=heap} / jvm.memory.limit{type=heap}`. |
| | `jvm.gc.duration` (histogram, seconds) with `jvm.gc.action` and `jvm.gc.name` attributes | Sum of pause durations / window-seconds is the GC overhead fraction. |
| **Storage I/O** | `system.disk.io` (counter, bytes) with `system.device` and `direction` ∈ `read`, `write` | Rate per direction. |
| | `system.disk.operations` (counter, ops) | IOPS. |
| | `system.disk.io_time` (counter, seconds) | Closest OTel analogue to await %. |
| | `system.filesystem.usage` (gauge, bytes), `system.filesystem.utilization` (gauge, 0–1) with `system.filesystem.state` ∈ `used`, `free`, `reserved` | Saturation pair. |
| | `container.disk.io` | Container-scoped I/O. |
| **Network / connections** | `system.network.io` (counter, bytes), `system.network.packets` (counter), `system.network.errors` (counter), `system.network.dropped` (counter) — all with `direction` ∈ `receive`, `transmit` | Rate per direction; errors + dropped are the direct loss indicators. |
| | `system.network.connections` (gauge) with `system.network.state` ∈ `established`, `time_wait`, `close_wait`, `listen` | The OTel equivalent of `node_sockstat_TCP_*`. |
| | `process.open_file_descriptors` (gauge) | FD usage; pair with a limit attribute or `process.max_file_descriptors` when emitted. |
| | `db.client.connections.usage` (gauge), `db.client.connections.idle.max`, `db.client.connections.idle.min`, `db.client.connections.max`, `db.client.connections.pending_requests`, `db.client.connections.timeouts` (counter), `db.client.connections.use_time` (histogram) — all with `db.client.connections.pool.name` | Standard OTel mapping for HikariCP / pgbouncer / generic JDBC pools. `pending_requests` rising and `timeouts` rate non-zero is the discriminator for pool exhaustion. |

OTel naming is **dotted, lowercased, and stateful via attributes** (e.g. `system.cpu.state=iowait`) — Prometheus exposition often *flattens* the same data into `system_cpu_time_seconds_total{state="iowait"}` after collector translation, so when querying a Prometheus-backed OTel pipeline you may see either form. Treat `*.time`, `*.io`, `*.errors`, `*.dropped`, and any name containing `count` / `total` as **counters** that need rating; treat `*.usage`, `*.utilization`, `*.connections`, `*.open_file_descriptors` as **gauges**.

### cgroup v1 / Prometheus / Kubernetes

The general counter-vs-gauge rule lives in `investigation` B.1; this section adds only ecosystem-specific gotchas. **Naming convention here:** `-total` suffix indicates a cumulative counter; everything else (`-bytes`, `-rss`, `-cache`, `-current`) is a gauge. **Known exception:** Istio's `istio-request-total` is emitted pre-rated despite the suffix — always verify monotonicity with two consecutive samples before treating any `-total` field as cumulative.

### CPU saturation against a cgroup v1 quota

Compute the rate, then divide by the quota expressed in cores:

```
cpu_rate_cores = (max(cpu-usage-seconds-total) − min(cpu-usage-seconds-total)) / window_seconds
saturation     = cpu_rate_cores / (spec-cpu-quota / 100_000)
```

`spec-cpu-quota` is in microseconds-per-100ms in cgroup v1 (so 40000 = 0.4 cores, 100000 = 1.0 core). On cgroup v2 the equivalent is `cpu.max` in microseconds-per-period.

**Direct throttle indicator**: `container_cpu_cfs_throttled_periods_total` and `container_cpu_cfs_throttled_seconds_total`. A non-zero rate of throttled seconds is the **direct** signal that CPU saturation is the cause; the quota-relative ratio above is the **indirect** signal. Always check both.

### Memory saturation

Direct division IS valid (memory usage is a gauge): `working-set-bytes / spec-memory-limit-bytes`. The direct OOM-kill indicator is in kubelet logs (`OOMKilled` reason on pod status, or `oom_killer` kernel ring buffer messages). Allocation failures show up at `container-memory-failures-total` (counter — rate it).

### Disk / filesystem

Counters: `container-fs-reads-bytes-total`, `container-fs-writes-bytes-total`, `container-fs-reads-total`, `container-fs-writes-total`, `container-blkio-device-usage-total`. Rate them.

I/O-time fraction (the closest thing to await %) is `container_fs_io_time_seconds_total` rate / window.

Node-level disk metrics on the underlying host: `node_disk_io_time_seconds_total`, `node_disk_io_time_weighted_seconds_total`, `node_disk_*_completed_total`.

### Network / connections / FDs

Per-pod NIC: `container_network_receive_packets_dropped_total`, `*_transmit_packets_dropped_total`, `*_receive_errors_total`, `*_transmit_errors_total`. TCP-level on the node: `node_netstat_Tcp_RetransSegs`, `node_sockstat_TCP_inuse`, `node_sockstat_TCP_tw`, `node_sockstat_TCP_orphan`.

Sockets / FDs / pools: `container_sockets`, `process_open_fds`, `process_max_fds`. Connection pool gauges depend on the framework — common names include `hikaricp_connections_pending`, `hikaricp_connections_active`, `*_pool_active`, `*_pool_pending`, `*_pool_wait_time`.

### Cross-reference with PPL

For PPL query templates against this metric naming convention — including the rate-computation idiom, sort-by-alias trap, and wide-format projection requirement — see the ``ppl-cookbook`` skill. The methodology in this file specifies **what** to measure; the cookbook specifies **how to write the query** in this ecosystem's PPL flavor.
