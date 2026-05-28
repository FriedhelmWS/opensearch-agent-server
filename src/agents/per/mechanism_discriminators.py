"""Mechanism-class registry + discriminator-token catalog.

Background — why this module exists. The PER pipeline's finalize gate
historically asked "is there a [direct] fact for the leading candidate?"
and accepted any answer. Benchmark reflections consistently showed the
reflector picking a plausible-sounding mechanism (e.g. "compute
saturation") backed by a fact that was directionally consistent with
that mechanism but did NOT discriminate it from the leading
alternatives — a high CPU number alone cannot tell compute-bound work
apart from iowait spinning, busy-wait on a half-open socket, or GC
thrash. The gate let those finalizations through and the resulting
``failure_reason`` was wrong roughly half the time on the loss / disk /
socket / cpu categories.

This module is the single source of truth for "what fact would
DISCRIMINATE this mechanism from its near-twins". The reflector
declares which mechanism class it is committing to via the
``mechanism_class`` field on its structured output; the orchestrator's
finalize gate then refuses to finalize unless a fact carrying at least
one of the registered discriminator tokens is present in
KNOWN_FACTS. The discriminator tokens are the OpenSearch / Bedrock
field names and log keywords that the ``ppl-probes`` skill teaches
the executor to query — so the gate and the probe templates reference
the same vocabulary, not divergent prose.

Adding a new mechanism class requires:

  1. an enum entry below;
  2. a discriminator entry in ``MECHANISM_DISCRIMINATORS`` listing the
     tokens (lowercased) that must appear in at least one [direct] /
     [deviation] fact for the gate to pass;
  3. a matching probe template in ``skills/ppl-probes/SKILL.md`` so
     the executor knows how to actually generate such a fact.

Tokens are matched as case-insensitive substrings against the fact
text (after the bullet's leading tag); a fact like
``[direct] container_network_receive_packets_dropped_total +47x`` and
a fact like ``[direct] log: 'connection reset by peer' x312`` both
satisfy the ``network-loss`` requirement because each contains at
least one registered token. We deliberately keep the token list
broad — a missed signature is much worse than an over-broad one,
since the only consequence of an over-broad token is a finalize that
the regular gates would have caught anyway.
"""

from __future__ import annotations

from enum import Enum


class MechanismClass(str, Enum):
    """Coarse mechanism families the finalize gate knows how to verify.

    The taxonomy is intentionally short — these are the *discrimination
    boundaries* that have shown up as failure modes in benchmark
    reflections, not a comprehensive list of every possible failure.
    Anything that does not fit cleanly should be tagged ``OTHER``,
    which leaves the gate without a discriminator requirement (the
    reflector falls back to the rule-6 mechanism-evidence requirement
    alone). Picking ``OTHER`` is a legitimate choice when the active
    skill or the evidence does not match a registered class; picking a
    specific class is a commitment to back it with the corresponding
    discriminator.
    """

    NETWORK_LOSS = "network-loss"
    SOCKET_EXHAUSTION = "socket-exhaustion"
    DISK_IO_SATURATION = "disk-io-saturation"
    CPU_COMPUTE_SATURATION = "cpu-compute-saturation"
    MEMORY_PRESSURE = "memory-pressure"
    GC_PAUSE = "gc-pause"
    LOCK_CONTENTION = "lock-contention"
    DEPENDENCY_DEGRADATION = "dependency-degradation"
    INJECTED_DELAY = "injected-delay"
    OTHER = "other"


# Discriminator tokens per mechanism class. A fact tagged ``[direct]``
# or ``[deviation]`` whose lowercased text contains AT LEAST ONE of
# these tokens satisfies the gate for that class.
#
# The vocabulary mixes three sources:
#   - Bedrock / cAdvisor / Prometheus metric field names (the executor
#     pulls these via the ppl-probes templates);
#   - OS / kernel log fragments (the executor pulls these via filtered
#     log queries);
#   - distribution-shape descriptors (e.g. "rto", "point_mass") that
#     show up only when the executor explicitly bucketed latency.
#
# All tokens are stored lowercased and matched case-insensitively
# against fact text.
MECHANISM_DISCRIMINATORS: dict[MechanismClass, tuple[str, ...]] = {
    MechanismClass.NETWORK_LOSS: (
        "packets_dropped",
        "packets_drop",
        "tcp_retransmit",
        "retranssegs",
        "rto",
        "retransmit",
        "tcp_lost",
        "tcptimeout",
        "rtomult",
        "bimodal",
        "200ms",
        "1s cluster",
        "3s cluster",
        "connection reset",
        "broken pipe",
        "eof",
        "context deadline",
        "no route to host",
    ),
    MechanismClass.SOCKET_EXHAUSTION: (
        "sockets",
        "container_sockets",
        "tcp_inuse",
        "tcp_tw",
        "time_wait",
        "close_wait",
        "fd_usage",
        "process_open_fds",
        "process_max_fds",
        "emfile",
        "too many open files",
        "hikari",
        "pool_active",
        "pool_pending",
        "pool_wait",
        "checkout-timeout",
        "connection pool",
    ),
    MechanismClass.DISK_IO_SATURATION: (
        "fs_io_time",
        "io_time",
        "iowait",
        "fs_reads_bytes",
        "fs_writes_bytes",
        "blkio",
        "node_disk_io_time",
        "disk.await",
        "fsync",
        "enospc",
        "no space left",
        "i/o error",
        "eio",
        "readonly filesystem",
        "wiredtiger",
    ),
    MechanismClass.CPU_COMPUTE_SATURATION: (
        "cfs_throttled",
        "throttled_periods",
        "throttled_seconds",
        "runqueue",
        "run_queue",
        "scheduling_delay",
        "per-request cpu-time",
        "cpu-time per request",
        "user-cpu",
        "system-cpu",
        "cpu_user",
        "cpu_system",
    ),
    MechanismClass.MEMORY_PRESSURE: (
        "working_set_bytes",
        "container_memory_working_set",
        "memory_failures_total",
        "pgmajfault",
        "major_page_faults",
        "container_memory_rss",
        "jvm_memory_used",
        "heap_used",
        "outofmemoryerror",
        "oom",
        "oomkilled",
        "allocation failure",
        "killed (exit 137)",
    ),
    MechanismClass.GC_PAUSE: (
        "jvm_gc_pause",
        "gc_pause",
        "gc.pause",
        "jvm_gc_collection",
        "gc overhead",
        "stop-the-world",
        "stw",
        "g1 old",
        "full gc",
        "promotion failed",
    ),
    MechanismClass.LOCK_CONTENTION: (
        "lock_wait",
        "lock contention",
        "blocked_thread",
        "monitor wait",
        "synchronized",
        "mutex",
        "deadlock",
        "thread_state=blocked",
    ),
    MechanismClass.DEPENDENCY_DEGRADATION: (
        # Supply-side / dependency degradation is identified by the
        # peer's OWN [direct]/[deviation] fact, plus a self-time-flat
        # claim on the caller. We accept either kind of fact body here.
        "peer.service",
        "callee",
        "downstream",
        "self_time_flat",
        "self-time flat",
        "d_minus_d_prime",
        "d - d'",
        "callee_p95",
        "client.duration",
    ),
    MechanismClass.INJECTED_DELAY: (
        "point_mass",
        "point-mass",
        "p50≈p95",
        "p50 ≈ p95",
        "fixed-delay",
        "fixed delay",
        "sleep(",
        "thread.sleep",
        "200ms exact",
        "round-number latency",
    ),
    MechanismClass.OTHER: (),
}


def discriminator_violations(
    mechanism_class: MechanismClass,
    facts_blob: str,
) -> list[str]:
    """Return finalize-gate violation messages for a discriminator miss.

    ``facts_blob`` is the joined text of every [direct] / [deviation]
    fact KNOWN_FACTS contains; case is ignored. Empty list means the
    declared mechanism class is backed by at least one discriminator
    token, so finalize is allowed to proceed (subject to the other
    gates).

    ``OTHER`` always returns ``[]`` — by selecting it, the reflector is
    explicitly saying "no registered class fits", and we delegate to
    the regular mechanism-evidence rule. That escape hatch keeps the
    gate from blocking finalize when the active skill defines a
    mechanism the registry doesn't know about yet (the right fix is
    then to add it here, not to bypass discrimination entirely).
    """
    if mechanism_class == MechanismClass.OTHER:
        return []
    tokens = MECHANISM_DISCRIMINATORS.get(mechanism_class, ())
    if not tokens:
        return []
    blob = facts_blob.lower()
    if any(tok in blob for tok in tokens):
        return []
    sample = ", ".join(tokens[:6])
    return [
        f"mechanism_class is '{mechanism_class.value}' but no [direct] / "
        "[deviation] fact contains a discriminator token for that "
        f"class (looked for any of: {sample}, …). Per the discriminator "
        "gate, picking a specific mechanism class commits to producing "
        "a fact whose body cites at least one such token — typically "
        "by running the corresponding probe template from the "
        "ppl-probes skill. Either run that probe and record the "
        "resulting fact, or change mechanism_class to 'other' if the "
        "active domain skill defines a mechanism this registry does "
        "not yet cover."
    ]
