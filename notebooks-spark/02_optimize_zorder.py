# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # NB2 — Small-File Problem & OPTIMIZE + ZORDER
#
# **Mục tiêu:** prove the 3–10× speedup claim from slide §6 (Storage Optimization).
# Maps to deliverable bullet 2.

# %%
import sys, time, random
sys.path.append("/workspace/scripts")
from spark_session import get_spark
from delta.tables import DeltaTable

spark = get_spark("nb2_optimize_zorder")
path = "s3a://lakehouse/events_smallfiles"

# %% [markdown]
# ## 0. Reset path (idempotent re-run)
#
# Each run starts fresh — otherwise repeated appends keep growing the table
# and the benchmark drifts.

# %%
spark.sql(f"DROP TABLE IF EXISTS delta.`{path}`")
# Best-effort: the DROP above unregisters the catalog entry, but Delta files
# may persist in MinIO. Overwrite below resets the data.

# %% [markdown]
# ## 1. Manufacture the small-file problem
#
# Append 200 tiny batches → 200 small files. Realistic streaming-ingestion shape.

# %%
for batch in range(200):
    rows = [(i, random.choice(["click", "view", "scroll", "purchase"]),
             random.randint(1, 10000))
            for i in range(batch * 500, (batch + 1) * 500)]
    df = spark.createDataFrame(rows, ["event_id", "kind", "user_id"])
    mode = "overwrite" if batch == 0 else "append"
    df.write.format("delta").mode(mode).save(path)

detail_before = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").select("numFiles", "sizeInBytes").first()
files_before = detail_before["numFiles"]
bytes_before = detail_before["sizeInBytes"]
print(f"Files before OPTIMIZE: {files_before}  (target >= 100)")
print(f"Size before OPTIMIZE:  {bytes_before:,} bytes")
assert files_before >= 100, f"expected >=100 small files, got {files_before}"

# %% [markdown]
# ## 2. Benchmark BEFORE optimize

# %%
def bench(label):
    # Warm-up read so we measure query, not cold metadata fetch
    spark.read.format("delta").load(path).limit(1).count()
    t0 = time.time()
    n = (spark.read.format("delta").load(path)
            .where("user_id = 4242 AND kind = 'purchase'").count())
    dt = time.time() - t0
    print(f"{label:25s}  count={n}  time={dt:.2f}s")
    return dt

before = bench("BEFORE OPTIMIZE+ZORDER")

# %% [markdown]
# ## 3. OPTIMIZE + ZORDER

# %%
opt_metrics = spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (user_id)")
opt_metrics.selectExpr(
    "metrics.numFilesRemoved as numFilesRemoved",
    "metrics.numFilesAdded as numFilesAdded",
    "metrics.totalConsideredFiles as totalConsideredFiles",
).show(truncate=False)

# %% [markdown]
# ## 4. Benchmark AFTER

# %%
after = bench("AFTER OPTIMIZE+ZORDER")
speedup = before / max(after, 1e-6)
print(f"\nSpeedup: {speedup:.1f}×  (target ≥ 3×)")

# %% [markdown]
# ## 5. Inspect file count change

# %%
detail_after = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").select("numFiles", "sizeInBytes").first()
files_after = detail_after["numFiles"]
bytes_after = detail_after["sizeInBytes"]
print(f"Files after OPTIMIZE+ZORDER: {files_after}  (was {files_before})")
print(f"Size after OPTIMIZE+ZORDER:  {bytes_after:,} bytes")
print(f"File reduction: {files_before} -> {files_after}  ({files_before / max(files_after, 1):.1f}x fewer)")
assert files_after < files_before, "OPTIMIZE should reduce file count"

# %% [markdown]
# ## ✅ Deliverable check
# - [ ] Speedup ≥ 3×
# - [ ] `numFiles` dropped substantially after OPTIMIZE
# - [ ] Screenshot the printed comparison

# %%
spark.stop()
