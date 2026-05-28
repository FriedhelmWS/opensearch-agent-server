---
name: ppl-probes
description: "Discriminator-question reference for each registered mechanism_class (network-loss, socket-exhaustion, disk-io-saturation, cpu-compute-saturation, memory-pressure, gc-pause, dependency-degradation, injected-delay). Activate ONLY after the reflector has committed to a mechanism_class. Each entry names the question that distinguishes the class from its near-twins and the field-name / keyword tokens the orchestrator's finalize gate will scan for — record those tokens VERBATIM in the resulting [direct] / [deviation] fact. Field-name examples are illustrative (cgroup-v1 / Prometheus / OTel naming); on other platforms read the equivalent off ``describe <metrics_index>`` and substitute."
---

# Mechanism-class discriminator probes

When you commit to a `mechanism_class`, the finalize gate requires a `[direct]`
or `[deviation]` fact whose body contains a verbatim field-name / keyword
token from the corresponding entry below. Paraphrases ("disk was busy") fail
the gate.

Window-split discipline (from ``ppl-cookbook``): every probe runs once in the
anomaly window, once in the baseline window. Counters require two boundary
samples per window then `(last - first)` for the rate; gauges can be averaged.
The class registry is in ``mechanism_discriminators.py``; substitute platform
equivalents — the gate accepts common variants (`iowait`, `disk.await`,
`enospc`, `oomkilled`, etc.).

---

## `network-loss`

**Q.** Is there a non-zero packet-drop / TCP-retransmit rate, or a
latency distribution clustered at TCP RTO multiples (~200ms / ~1s / ~3s)?

**Direct fact tokens.** `*packets_dropped*`, `*Tcp_RetransSegs*`,
`retransmit`, `RTO`, `bimodal`. Latency-shape companion: bucket
`durationInNanos / 1e8` and look for clusters near bucket 2 / 10 / 30.

---

## `socket-exhaustion`

**Q.** Is sockets-in-use, FDs open, or a connection-pool active gauge
saturated against its limit?

**Direct fact tokens.** `*sockets*`, `*tcp_inuse*`, `*open_fds*` /
`*max_fds*` (with saturation = used / limit), or framework pool gauges
(`*pool_active*`, `*pool_pending*`, `*hikaricp_connections_*`).

---

## `disk-io-saturation`

**Q.** Is the device's `io_time` rate climbing AND `cpu mode=iowait`
rising? (iowait is the discriminator that separates this from
compute-saturation.)

**Direct fact tokens.** `*fs_io_time*`, `iowait`, `disk.await`. Filesystem
ENOSPC / EIO / readonly variants are a separate decisive signal.

---

## `cpu-compute-saturation`

**Q.** Is the CFS throttled-seconds rate non-zero?

CFS throttling is what distinguishes quota-bound compute starvation from
its near-twins (iowait spinning, busy-wait on a half-open socket, GC
thrash). A "CPU is high" fact alone does not.

**Direct fact tokens.** `*cfs_throttled*`. Decisive cross-check: compare
per-request CPU-time vs wall-clock — if wall inflates but cpu-time
doesn't, the latency is non-compute.

---

## `memory-pressure`

**Q.** Is `working_set / memory_limit` near 1.0 AND the major-fault /
memory-failures rate rising? (For JVMs, also check `heap_used / heap_max`
— GC pauses with a stable working_set are `gc-pause`, not this class.)

**Direct fact tokens.** `*working_set_bytes*`, `*memory_failures_total*`,
`*pgmajfault*`, `*jvm_memory_used*`. The kernel/runtime keywords
`OOMKilled`, `OutOfMemoryError`, `Killed (exit 137)`, `Allocation failure`
are each a `[direct]` fact on their own.

---

## `gc-pause`

**Q.** Is `gc_pause_seconds_sum` rate > ~5% of wall-clock, with
working_set stable? (Rising working_set + rising GC is `memory-pressure`,
not this class.)

**Direct fact tokens.** `*jvm_gc_pause*`, `*jvm_gc_collection*`,
`stop-the-world`.

---

## `dependency-degradation`

**Q.** Is the deviation on the **peer** at least as large as on the
candidate, AND is the candidate's self-time flat?

This class is identified by evidence on the peer, not the candidate.
Without the peer measurement, you can't tell demand-side ("candidate
doing more work") from supply-side ("peer got slower"). Run the
window-split per-entity comparison (``ppl-cookbook``) on `peer.service` /
the callee.

**Direct fact tokens.** `peer.service`, `callee`, `self-time flat`,
`D − D'` (when the cross-component check from
``investigation-traces`` T.3 is recorded).

---

## `injected-delay`

**Q.** Is the latency distribution a point mass at a round value
(`p50 ≈ p95 ≈ p99` near 100ms / 200ms / 500ms / 1s)?

Organic mechanisms produce variance; only an imposed wait collapses to
a delta.

**Direct fact tokens.** `point-mass`, `p50≈p95`, the round latency value
itself.
