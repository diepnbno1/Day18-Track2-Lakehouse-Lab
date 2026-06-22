# Architecture Brief: LLM Observability Lakehouse at 1B Requests/Day

Student: Nguyễn Bách Điệp  
Student ID: 2A202600535

## Problem Statement

A foundation-model API platform logs 1B requests/day. Each request produces about
5 KB of raw event data, so the system receives about 5 TB/day before compression,
with peak traffic around 60K events/sec. The platform needs per-tenant cost,
latency, error, and safety dashboards refreshed every 5 minutes. Full
prompt/response payloads are needed for incident review for 7 days only; after
that, only aggregates should remain for 1 year. PII must be tokenized before any
human-readable table is exposed. Storage spend must stay under $5K/month, and
the design must survive replay, schema drift, tenant hot spots, and accidental
PII exposure without losing auditability.

## Architecture Diagram

```text
API gateway / model runtime
        |
        | JSON event: tenant_id, request_id, model, usage, latency, status,
        | prompt, response, pii_candidates, trace_id
        v
Kafka / Redpanda topics, 24h replay, request_id key
        |
        v
Bronze Delta: s3://lake/bronze/llm_events_raw
  partition: event_date/hour, tenant_hash_bucket
  actions: schema capture, HMAC tokenization, encryption, append-only
  retention: 7 days for prompt/response, 30 days for non-payload raw metadata
        |
        | Structured Streaming + CDF + expectations
        v
Silver Delta: s3://lake/silver/llm_events
  typed columns, dedup by request_id, malformed rows quarantined,
  no plaintext PII, prompt/response redacted/tokenized
  OPTIMIZE hourly, ZORDER BY tenant_id, model, event_ts
        |
        +--------------------+
        |                    |
        v                    v
Gold Delta             Quarantine Delta
cost_latency_5m        bad_schema, pii_violation,
tenant_daily_cost      replay_conflict, tool_error
model_error_rates
        |
        v
Dashboards / alerts / FinOps reports
Trino, Spark SQL, BI warehouse, incident notebooks
```

## Key Decisions And Rejected Alternatives

### 1. Table Format

I chose Delta Lake for Bronze, Silver, Gold, and quarantine tables. Delta gives
ACID appends, MERGE for request-level dedup, time travel for rollback, Change
Data Feed for replaying downstream aggregates, and mature Spark support.

I rejected raw Parquet because it has no transaction log, so replay and partial
writes would create silent duplicates. I rejected a warehouse-only design because
keeping 5 TB/day of raw payloads in a warehouse would be expensive and would
couple incident review retention to BI storage.

### 2. Medallion Layout

I chose a strict Bronze -> Silver -> Gold contract. Bronze is the immutable audit
landing zone. Silver is typed, deduplicated, tokenized, and queryable. Gold is
dashboard-shaped: 5-minute tenant/model aggregates and daily FinOps rollups.

I rejected "one giant events table" because dashboard queries would scan raw
payload columns and compete with incident review. I rejected writing only Gold
aggregates because incident response needs request-level traceability for 7
days, including prompt/response evidence.

### 3. Partitioning And Clustering

I chose partitioning by `event_date`, `hour`, and `tenant_hash_bucket`, then
ZORDER/clustering on `tenant_id`, `model`, and `event_ts` in Silver and Gold.
This keeps object counts bounded while making the hot path, "show tenant X for
the last N minutes," file-skippable.

I rejected partitioning directly by `tenant_id` because 200K tenants would create
small-file and metadata pressure. I rejected date-only partitioning because a
single tenant dashboard would still scan too many files during high-traffic
hours.

### 4. PII Handling

I chose deterministic HMAC tokenization at Bronze ingestion. Plaintext PII is
never written to analyst-readable tables. The token vault key is in KMS/HSM,
rotated quarterly, and access to reverse-tokenization requires break-glass
approval plus an audit record.

I rejected "redact in BI" because raw PII would already be present in shared
storage. I rejected random per-event tokens because incident review and abuse
investigation need stable joins across retries, traces, and tenants.

### 5. Lifecycle And Retention

I chose 7-day hot retention for full payloads, 30-day retention for typed
request-level metadata, and 1-year retention for Gold aggregates. A daily VACUUM
policy physically removes expired payload columns after legal hold checks.

I rejected keeping raw prompts for 1 year because 5 TB/day raw becomes 1.8 PB/year
before compression and violates the budget. I rejected deleting all request-level
data after 7 days because SLO regressions often need 14-30 days of typed metadata
without full payloads.

### 6. Catalog, Governance, And Lineage

I chose a governed catalog with table owners, column tags (`pii_token`,
`payload_7d`, `finance_metric`), row-level tenant policies, and OpenLineage
events emitted from each streaming job. Every human read of incident tables is
logged.

I rejected a bare Hive metastore because ownership, column policies, and audit
reviews become conventions rather than controls. I rejected dashboard-only
lineage because the risky breakages happen upstream, at schema evolution and
streaming MERGE boundaries.

## Failure Modes

### PII Tokenization Regression

At 3 AM, a new SDK field `customer_phone_raw` appears and bypasses the tokenizer.
Detection: Bronze expectation checks sample payload keys and fails closed when a
new field matches PII classifiers. Rollback: quarantine the new schema version,
time travel Silver to the last clean version, rotate affected Gold partitions
from CDF after the tokenizer rule is patched.

### Small-File Explosion

A retry storm creates thousands of tiny micro-batches for a few hot tenants.
Detection: table health checks alert when average file size drops below 64 MB or
`numFiles` grows faster than row count. Rollback: pause Gold refresh for the
affected hour, run OPTIMIZE on Silver partitions, then replay Gold from CDF.

### Bad Pricing Table

Finance updates model prices with an incorrect output-token multiplier, making
tenant cost dashboards 10x too high. Detection: Gold cost anomaly check compares
new aggregate deltas against token-volume deltas. Rollback: restore the pricing
dimension table to the previous Delta version and recompute Gold partitions for
the affected time range.

### Duplicate Replay From Kafka

An ingestion job restarts from an old checkpoint and replays 20 minutes of
traffic. Detection: Silver MERGE metrics show duplicate request IDs above the
expected retry baseline. Rollback: no data rollback is required because Silver is
idempotent on `request_id`; Gold is rebuilt from Silver CDF for that window.

### Schema Evolution Breaks Readers

A model runtime changes `usage.output` from integer to object. Detection: Bronze
captures the raw record, but Silver expectations reject incompatible typed
writes. Rollback: schema version is quarantined; readers continue from the last
valid Silver Delta version until a compatible parser is deployed.

## Cost Back-Of-Envelope

Raw input: 5 TB/day. With Parquet + ZSTD and column pruning, assume 4:1
compression for payload-heavy Bronze, so 1.25 TB/day compressed.

Hot payload retention: `1.25 TB/day x 7 days = 8.75 TB`. S3 Standard at about
`$23/TB-month` costs about `$201/month`.

Typed Silver metadata after payload stripping: assume 0.35 TB/day compressed for
30 days: `10.5 TB x $23/TB-month = $242/month`.

Gold aggregates: 5-minute windows by tenant/model/status, about 20 GB/day
compressed. One year is about 7.3 TB. Store in Standard-IA at about
`$12.5/TB-month`: about `$91/month`.

Quarantine, logs, and lineage overhead: reserve 5 TB Standard, about
`$115/month`.

Storage subtotal is about `$650/month`, leaving margin under the `$5K/month`
storage cap for request charges, cross-region copies for regulated tenants, and
temporary backfill tables.

Compute estimate: two streaming jobs with 8 workers during peak and autoscale at
off-peak, about `$1.8K/month`; hourly OPTIMIZE and Gold rebuild jobs, about
`$600/month`; BI warehouse and incident notebooks, about `$900/month`. Compute
subtotal is about `$3.3K/month`. Combined storage plus compute is about
`$3.95K/month` before support overhead.

## One-Week MVP Slice

The MVP should prove the riskiest path, not the prettiest dashboard:

1. Ingest one high-volume tenant and one low-volume tenant from Kafka into
   Bronze Delta with deterministic tokenization.
2. Build Silver with MERGE-based dedup on `request_id`, malformed JSON
   quarantine, and no plaintext PII columns.
3. Build Gold 5-minute aggregates for cost, p50/p95 latency, error rate, and
   token volume.
4. Add table-health checks: duplicate rate, new schema fields, file count,
   average file size, and PII leakage classifier.
5. Run a forced replay and a bad-schema incident, then prove recovery with Delta
   time travel and CDF-based Gold rebuild.

Success for the MVP means the dashboard refreshes within 5 minutes, replay does
not duplicate Silver rows, the PII canary is quarantined, and a wrong Gold
aggregate can be rebuilt from a known clean Delta version.
