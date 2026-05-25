---
name: ppl-cookbook
description: >
  Battle-tested PPL idioms and known engine traps for OpenSearch — patterns
  that consistently work, and the specific syntactic / engine pitfalls that
  consistently fail. Activate alongside ppl-reference whenever about to write
  a PPL query involving date ranges, aggregations with sorting, large result
  sets, wide-format indices, or text fields. Examples use placeholder field
  names (`<entity_field>`, `<date_field>`, `<value_field>`); substitute the
  actual names from your Phase A schema discovery.
---

# PPL Cookbook

Practical patterns for writing PPL queries against OpenSearch indices, plus
the shapes that consistently fail and require retries. The traps below are
properties of the PPL engine and OpenSearch behaviour, not of any specific
dataset — they reproduce across schemas.

This skill assumes you already know basic PPL syntax (see `ppl-reference`).
The goal here is to spare round trips spent rediscovering the same handful
of pitfalls.

Examples use placeholder names like `<index>`, `<date_field>`,
`<entity_field>`, `<value_field>`. Always replace these with the actual
names from Phase A schema discovery.

---

## Date / time field comparisons

### The trap

A field declared as `date` or `timestamp` in the index mapping cannot be
compared against a raw long literal:

```ppl
| where <date_field> >= <epoch_long>          # FAILS
```

returns `ExpressionEvaluationException [TIMESTAMP, LONG]`.

### What works

Use a `timestamp(...)` literal with `'YYYY-MM-DD HH:MM:SS'` format:

```ppl
| where <date_field> >= timestamp('<window_start>')
  AND <date_field> <  timestamp('<window_end>')
```

If the schema also exposes a long-typed sibling (e.g. an `<x>Millis` date
field plus an `<x>` long with the same value in different units), you can
compare against the long sibling and convert your bounds to its units.

### Confirm the unit during Phase A

Sample one document and read the magnitude:

| Magnitude observed | Likely unit |
|---|---|
| ~1.7e9 | seconds since epoch |
| ~1.7e12 | milliseconds since epoch |
| ~1.7e15 | microseconds since epoch |
| ~1.7e18 | nanoseconds since epoch |

Two date-typed fields in the same document with different magnitudes
typically means one is ms and the other is µs (or ns). Pick the one
whose unit you understand and stick with it across queries.

---

## Sorting by an aggregated field

### The trap

```ppl
| stats count() by <field> | sort - count        # FAILS
```

After `stats count() by ...` the result column is named `count()` (with
the parentheses), not `count`. Symbol resolution fails.

### What works

Always alias aggregations and sort by the alias:

```ppl
| stats count() as cnt by <field> | sort - cnt | head 20
```

Rule of thumb: any time you write a `stats` clause, alias every aggregate
output, even if you don't think you'll need to reference it later.

---

## Window-split (anomaly vs baseline) per-entity comparison

### Pattern

```ppl
source=<index>
| where <date_field> >= timestamp('<window_start>')
  AND <date_field> <  timestamp('<window_end>')
| stats count() as cnt,
        avg(<value_field>) as avg_v,
        percentile(<value_field>, 50) as p50,
        percentile(<value_field>, 95) as p95,
        percentile(<value_field>, 99) as p99
  by <entity_field>
| sort - p95
```

Run this once for the anomaly window, once for the baseline window, and
join in post-processing to compute per-entity ratios. Do NOT try to
combine into a single PPL query with a per-row case() that branches on
the timestamp — it tends to time out on million-row indices.

### Splitting a single window in half

When the original window is bisected (e.g., the available data range is
shorter than expected and you want to compare its first vs second half):

```ppl
| where <date_field> < timestamp('<midpoint>')      # first half
| where <date_field> >= timestamp('<midpoint>')     # second half
```

Compute the midpoint by epoch math, then format as a timestamp literal.

---

## Root vs non-root span split (for trace indices)

### Pattern

```ppl
| eval is_root = if(<parent_field> = '', 'root', 'child')
| stats count() as spans,
        percentile(<duration_field>, 95) as p95,
        percentile(<duration_field>, 99) as p99
  by <service_field>, is_root
```

Notes:
- The convention for marking a root span varies between trace indices.
  Common ones: empty string `''`, `null` / `is null`, a missing field, or
  an explicit boolean. Confirm with `head 5` during Phase A which
  convention applies, and adjust the `eval if(...)` predicate accordingly.
- An eval-derived keyword like `is_root` works in `by` clauses; raw
  predicates directly in `by` may not — `eval` it first.

### Interpretation

| pattern | meaning |
|---|---|
| `root_p95 ≫ child_p95` | parent waits on something downstream — observer of someone else's slowness |
| only children, no roots | leaf service called by others — possible originator |
| `root_p95 ≈ child_p95` | local processing time dominates, neither pattern |

---

## Wide-format / pivoted indices

### The trap

A wide-format metric index has hundreds or thousands of columns
(one per `<entity>_<metric>` pair) instead of long-format
`(time, entity, metric, value)` rows. On these indices:

```ppl
source=<wide_index> | head 1                              # may fail
```

with `too_complex_to_determinize_exception` because every column has to
be deserialised. Worse, `stats avg(value) by <metric_name_field>` does
not apply because there is no `metric_name` / `value` pair.

### What works

1. Identify wide-format with `describe <index>` — if you see hundreds
   of `<prefix>_<suffix>` columns and no `metric_name` / `value` pair,
   it is wide-format.

2. Sample with explicit field projection (a small handful of columns):
   ```ppl
   source=<wide_index>
   | fields <date_field>, `<entity1>_<metric>`, `<entity2>_<metric>`
   | head 5
   ```

3. Group columns into families by parsing the column name (typically
   the prefix before the first `_` is the entity; the rest is the
   metric family — but verify with the actual schema).

4. For aggregations, project the columns you want and run
   `stats avg(...), max(...)` over each — one aggregate output per
   (entity, metric) pair. `stats` cannot dynamically pivot; you must
   enumerate columns explicitly.

5. For per-window comparison, run TWO queries (anom window, base window)
   each producing the per-column averages, then divide post-hoc.

---

## Heavy aggregations time out via PPL — fall back to OpenSearch DSL

### The trap

PPL stats over indices with > ~1M documents — particularly with multiple
percentiles, branching `case(...)` clauses, or wide group-bys — can hit
the PPL plugin's connection timeout. Symptom: `ConnectionTimeout` or
hanging request.

### What works

Switch to the native search API with a `date_histogram` aggregation:

```
POST /<index>/_search?size=0
{
  "query": {"range": {"<date_field>": {
      "gte": "<start>", "lt": "<end>",
      "format": "strict_date_optional_time||epoch_millis"
  }}},
  "aggs": {
    "buckets": {
      "date_histogram": {
        "field": "<date_field>",
        "fixed_interval": "<bucket_size>",
        "offset": "<alignment_offset>"
      },
      "aggs": {
        "by_entity": {
          "terms": {"field": "<entity_field>", "size": <max_entities>},
          "aggs": {
            "p95": {"percentiles": {"field": "<value_field>", "percents": [95]}}
          }
        }
      }
    }
  }
}
```

This is far faster than the equivalent PPL `stats ... by span(<date>, ...),
<entity> | percentile(...)` and won't time out.

The `offset` lets you align bucket edges to a meaningful clock (e.g., a
specific anomaly start timestamp) so buckets line up with your
investigation boundary rather than with epoch-zero alignment.

---

## Text fields and aggregation

### The trap

Wrapping a text-field substring match inside an aggregation that uses a
per-row branch (e.g. `count(case(like(<text_field>, '%X%'), 1, 0))`-style
sums) fails with "Text fields are not optimised for operations that
require per-document field data and scripting" / "fielddata is disabled
on this text field". The same applies to grouping or counting on a
`text`-typed field directly.

### What works

Three alternatives, in order of preference:

1. Filter, then count — push the substring match into a `where`:
   ```ppl
   | where like(<text_field>, '%<keyword>%')
   | stats count() as hits by <entity_field>
   ```

2. OpenSearch DSL `match_phrase` filter sub-aggs — clean way to get
   per-(entity, keyword) counts in one request:
   ```json
   {"aggs": {
     "by_entity": {"terms": {"field": "<entity_field>", "size": <n>},
                   "aggs": {
                     "kw_a": {"filter": {"match_phrase": {"<text_field>": "<keyword_a>"}}},
                     "kw_b": {"filter": {"match_phrase": {"<text_field>": "<keyword_b>"}}}
                   }}
   }}
   ```

3. Use a `keyword` sibling field if one exists (e.g. `<text_field>.keyword`
   for short message bodies) — but most OpenSearch defaults don't expose
   one for long-text bodies.

### `patterns` command is lossy

```ppl
| patterns <text_field> | stats count() by <entity_field>, patterns_field
```

`patterns` strips alphanumerics and produces punctuation skeletons. It
is useful for clustering similar-shaped log lines but cannot
distinguish, e.g., one error class from another — the alphanumerics that
carry semantic content are exactly what gets stripped. For mode-naming
purposes, prefer match_phrase filter sub-aggs (option 2 above) with a
curated keyword list.

---

## Datasource qualifier

### The trap

Some PPL plugin builds reject a datasource-prefixed source name in
`source=`:

```ppl
source=<datasource>.<index>       # IndexNotFoundException
```

even though `describe <datasource>.<index>` may succeed, and even though
catalog metadata may report `TABLE_CAT="<account>:<datasource>"`.

### What works

Use the bare index name in `source=`:

```ppl
source=<index>
```

This is consistent across describe, search, and aggregation operations.

---

## QUERY_STRING and field arrays

### The trap

```ppl
| where QUERY_STRING(['<text_field>'], '<keyword_a> OR <keyword_b>')   # FAILS
```

Some OpenSearch builds reject the bracket-array form of QUERY_STRING.

### What works

Use `like()` patterns OR'd together:

```ppl
| where like(<text_field>, '%<keyword_a>%')
     OR like(<text_field>, '%<keyword_b>%')
```

or fall back to OpenSearch DSL `query_string`:

```json
{"query_string": {"default_field": "<text_field>",
                  "query": "<keyword_a> OR <keyword_b>"}}
```

### `like()` matches via the analyzer

`like(<text_field>, '%<keyword>%')` matches documents containing the
TOKEN `<keyword>` after the analyzer runs. For a standard analyzer this
is case-insensitive (it lowercases tokens), so `'%FOO%'` and `'%foo%'`
typically return the same docs. Don't rely on case to distinguish, e.g.,
a structured severity-level value from the literal word appearing
elsewhere in body text — they tokenize the same.

---

## Counters vs gauges — never compare a counter's absolute value

### The trap

A field named like `<entity>_<resource>-seconds-total` or
`<entity>_<resource>-bytes-total` looks like a current reading. It is
NOT — it is a cumulative counter (total seconds / total bytes since
process start). Direct division against a per-period quota gives
nonsense:

```ppl
| stats avg(`<entity>_cpu-usage-seconds-total`) as cpu,
        avg(`<entity>_cpu-quota`) as quota
| eval saturation = cpu / quota         # WRONG: order-of-magnitude off
```

Saturation values of hundreds or thousands of percent are the symptom
of having mistaken a counter for a rate.

### How to recognise a counter (do this in Phase A)

Three heuristics, applied in order:

1. **Naming convention.** Field names ending in `-total` are almost
   always cumulative counters (Prometheus convention). Names that
   describe a state — `*-bytes`, `*-set-bytes`, `*-rss`, `*-cache`,
   `*-current`, queue depths — are usually gauges.

2. **Sampling pattern.** Pull two consecutive sample values from
   adjacent timestamps. If the value is monotonically non-decreasing,
   it is a counter. If it fluctuates around a mean, it is a gauge —
   even if its name ends in `-total`. Some emitters (rate-mode
   exporters, some service-mesh sidecars) emit pre-rated samples
   despite a counter-shaped name; only the sampling pattern can tell
   you for sure.

3. **Magnitude sanity check.** "An instantaneous reading much larger
   than what an instant reading should be" indicates a counter. E.g.,
   a CPU value of 17 seconds with a 1-second scrape interval is only
   meaningful as elapsed time, not as instantaneous use.

### How to convert a counter to a rate

The rate is `(value_at_t2 - value_at_t1) / (t2 - t1)`. In PPL:

```ppl
| stats max(<counter>) as last_v, min(<counter>) as first_v,
        max(<date_field>) as last_t, min(<date_field>) as first_t
  by <entity_field>
| eval rate = (last_v - first_v) / ((last_t - first_t) / 1000)
                                               # if <date_field> is in ms
```

For rate-based saturation against a cgroup v1 CPU quota:

```
rate (in cores)  = delta_cpu_seconds / window_seconds
saturation        = rate / (quota_microseconds / 100_000)
```

(`spec-cpu-quota` in cgroups v1 is microseconds-per-100ms; divide by
100_000 to get the equivalent core fraction.)

### Gauges are different — direct comparison works

Fields like `<entity>_memory-usage-bytes` or
`<entity>_memory-working-set-bytes` are gauges (instantaneous samples).
Direct `avg()` over a window gives a meaningful answer; direct division
against `<entity>_memory-limit-bytes` gives a real saturation fraction.

| name pattern (typical convention) | likely kind | comparison method |
|---|---|---|
| ends with `*-total` (Prometheus) | counter | rate(value) over window |
| `*-bytes`, `*-set-bytes`, `*-rss`, `*-cache` | gauge | avg(value) over window |
| `*-seconds-total` | counter (CPU/wall time) | rate as fraction of cores |
| `*-quota`, `*-limit-*` | constant configuration | direct denominator |
| pre-computed percentile (e.g. `*-p95`, `*-99`) | gauge | avg(value) over window |

The "counter vs gauge" classification trumps the name. **Always verify
with consecutive samples in Phase A** before writing a Phase B
saturation query — getting this wrong silently produces orders-of-
magnitude wrong saturation values and can swing the originator
attribution.

---

## Quick-reference idiom checklist

Before submitting a PPL query, mentally verify:

- [ ] Date / timestamp comparisons use `timestamp('YYYY-MM-DD HH:MM:SS')` literals, not raw longs
- [ ] Every `stats` aggregate output has an alias with `as <name>`
- [ ] Every `sort` references an aliased field, not a function call
- [ ] Wide-format indices: explicit column projection in `fields`, not `head`
- [ ] Heavy queries on > 1M docs: switch to OpenSearch DSL `date_histogram` if PPL hangs
- [ ] Text-field aggregation: filter-then-count, or DSL filter sub-aggs
- [ ] `source=<bare_index_name>`, never `source=<datasource>.<index>`
- [ ] Cumulative counters (`*-total`) require rate computation; never raw avg / quota
- [ ] Multi-keyword OR: `like(...) OR like(...)` or DSL `query_string`, not bracket-array `QUERY_STRING`
- [ ] Counter / gauge classification confirmed by consecutive-sample check, not just by field name
