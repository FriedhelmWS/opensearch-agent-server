---
name: ppl-cookbook
description: >
  Battle-tested PPL idioms and known engine traps for OpenSearch — patterns
  that consistently work, and the specific syntactic / engine pitfalls that
  consistently fail. Strictly PPL-engine and OpenSearch-API specific:
  methodology (counter vs gauge classification, four-axis resource sweep,
  trace self-time decomposition) lives in the ``investigation-resource-saturation``
  and ``investigation-traces`` skills; this cookbook covers only how to
  write the corresponding PPL or DSL queries. Activate alongside
  ppl-reference whenever about to write a PPL query involving date ranges,
  aggregations with sorting, large result sets, wide-format indices, or
  text fields. Mechanism-class discriminator probe templates (network-loss,
  cpu-compute-saturation, disk-io-saturation, etc.) live in the separate
  ``ppl-probes`` skill — activate that one only AFTER committing a
  ``mechanism_class``. Examples use placeholder field names (`<entity_field>`,
  `<date_field>`, `<value_field>`); substitute the actual names from your
  Phase A schema discovery.
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

For how to interpret the resulting root vs child p95 patterns
(originator vs observer, self-time decomposition), see the
``investigation-traces`` skill (rules T.1, T.3, T.5). This cookbook
covers only the PPL writing pattern; the methodology lives there.

---

## Wide-format / pivoted indices

If `describe <index>` shows hundreds of `<entity>_<metric>` columns and no
`metric_name` / `value` pair, the index is wide-format. `head 1` may fail
with `too_complex_to_determinize_exception`; project the specific columns you
want with `| fields <date_field>, \`<entity>_<metric>\`, ...` instead. `stats`
cannot pivot dynamically — enumerate the columns of interest, then divide
anomaly / baseline post-hoc.

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

## Counter rate idiom

The counter-vs-gauge classification rule lives in ``investigation`` B.1;
platform-specific field references and saturation formulas (cgroup v1
quota, Prometheus naming, throttle counters, OTel `system.*` /
`container.*` / `jvm.*` mappings) live in the
``investigation-resource-saturation`` skill's "Platform-specific
reference" section. This cookbook only covers the PPL writing pattern.

### The trap

```ppl
| stats avg(<counter_field>) as v
| eval saturation = v / <quota>          # WRONG if <counter_field> is cumulative
```

Direct `avg()` of a counter is meaningless — it averages "total work
since process start", which keeps growing as the window slides.
Saturation values in the hundreds or thousands of percent are the
symptom of having mistaken a counter for a rate.

### What works — compute rate from two boundary samples

```ppl
| stats max(<counter_field>) as last_v, min(<counter_field>) as first_v,
        max(<date_field>)    as last_t, min(<date_field>)    as first_t
  by <entity_field>
| eval rate = (last_v - first_v) / ((last_t - first_t) / 1000)
                                                # if <date_field> is in ms
```

Then divide the rate by whatever per-period denominator applies
(quota, limit, cores) to get a real saturation fraction. The
denominator math depends on the resource and the platform —
consult ``investigation-resource-saturation`` for those formulas.

For gauges (`*-bytes`, `*-set-bytes`, `*-rss`, `*-cache`, pre-computed
percentile fields like `*-p95`), direct `avg()` over a window IS valid.
Verify counter vs gauge with two consecutive sample values during
Phase A before writing the saturation query.

---

## Mechanism-class discriminator probes

The canonical query templates for each registered `mechanism_class`
(network-loss, socket-exhaustion, disk-io-saturation,
cpu-compute-saturation, memory-pressure, gc-pause,
dependency-degradation, injected-delay) live in the separate
``ppl-probes`` skill. Activate that skill ONLY after the reflector has
committed to a class — until then those probes are not relevant and
loading them would just bloat the executor's context. The orchestrator's
finalize gate enforces that the resulting `[direct]` / `[deviation]`
KNOWN_FACTS contain the field-name / keyword tokens those probes
produce, so paraphrased facts ("disk was busy") will fail the gate;
record the verbatim field name from the probe template.

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
