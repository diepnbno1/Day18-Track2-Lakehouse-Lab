The anti-pattern our team is most likely to hit is the small-file swamp. LLM
observability traffic looks like an append-only firehose: many tenants, many
models, retries, errors, and near-real-time dashboards. If every micro-batch or
tenant shard lands as its own object, the lakehouse still "works" at first, but
queries slowly become metadata-bound instead of data-bound. That is dangerous
because teams often notice only after dashboards are already trusted.

The fix I would enforce is an explicit compaction and clustering contract:
stream into Bronze with bounded micro-batches, dedupe and repartition in Silver,
then run scheduled OPTIMIZE/ZORDER on the dashboard hot paths such as date,
tenant, and model. The lab made this concrete: NB2 started with many small files
and only became fast after OPTIMIZE+ZORDER, while NB4 showed why Gold tables
should be small, typed, and dashboard-shaped rather than raw-log-shaped.
