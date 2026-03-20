# Case Study 2 — SantéFlux Privacy Crisis

**Domain:** Healthcare · GDPR Compliance
**Platform:** Databricks · Unity Catalog · Serverless Compute
**Status:** Complete · March 2026

---

## Background

SantéFlux is a French health-tech company collecting millions of bio-pings daily from smart watches. The Chief Privacy Officer blocked analytics access after finding full names and raw user IDs in shared folders. The lead data engineer had to build a GDPR-compliant pipeline and deliver an interactive heart rate report to the board — fast.

**Two files:**

| File | Rows | Contains |
|------|------|----------|
| `vitals.csv` | 750,000 | User_ID, Full_Name, Timestamp, ville, Heart_Rate, Status |
| `subs.csv` | 10,000 | User_ID, Subscription_Type (Free / Premium) |

---

## Pipeline Steps

### Step 1 — Ingest

```python
df_vitals = spark.read.csv(".../vitals.csv", header=True, inferSchema=True)
df_subs   = spark.read.csv(".../subs.csv",   header=True, inferSchema=True)
```

---

### Step 2 — PII Masking

Full names masked immediately at ingestion. Raw names never travel through the pipeline.

```python
df_masked = df_vitals.withColumn(
    "Full_Name",
    regexp_replace("Full_Name", r"^(\w+)\s(\w)(.*)", "$1 $2****")
)
# Pierre Dubois → Pierre D****
```

---

### Step 3 — Salted Hashing

Salt prepended before hashing stops rainbow table attacks. Same salt applied to both tables so the join works.

```python
salt = "SanteFlux_Salt_2025"

df_hashed = df_masked.withColumn(
    "hashed_name",
    sha2(concat(col("User_ID"), lit(salt)), 256)
)

df_subs_upd = df_subs.withColumn(
    "hashed_name",
    sha2(concat(col("User_ID"), lit(salt)), 256)
).select("hashed_name", "Subscription_Type")
```

!!! note "Trade-off"
    Using a fixed company salt means every team joining on this data must use the same salt. Salting adds security but requires coordination. See [Hashing & Salting](../concepts/hashing.md) for the full trade-off discussion.

---

### Step 4 — Frozen Sensor Detection

Some sensors transmitted the same heart rate for hours. Grouped by user + 10-minute window, flagged windows where min == max Heart_Rate.

```python
df_windowed = df_hashed.withColumn(
    "Time_Window", window(col("Timestamp"), "10 minutes")
).select("hashed_name", "Time_Window", "Heart_Rate", "ville")

df_agg = df_windowed.groupBy("hashed_name", "Time_Window").agg(
    min("Heart_Rate").alias("min_rate"),
    max("Heart_Rate").alias("max_rate")
)

df_frozen = df_agg.filter(col("min_rate") == col("max_rate"))

vitals_clean_df = df_windowed.join(
    df_frozen,
    on=["hashed_name", "Time_Window"],
    how="left_anti"
)
```

`left_anti` returns only rows from the left table with no match on the right — the cleanest way to apply an exclusion list.

---

### Step 5 — Broadcast Join

subs table is 10,000 rows (~125 KB) — well within safe broadcast range. No shuffle on the large vitals table.

```python
df_joined = vitals_clean_df.join(
    broadcast(df_subs_upd),
    vitals_clean_df.hashed_name == df_subs_upd.hashed_name,
    how="left"
).select(
    vitals_clean_df.hashed_name,
    "Time_Window", "Heart_Rate", "ville", "Subscription_Type"
)
```

Confirmed in physical plan: `PhotonBroadcastHashJoin BuildRight` with `EXECUTOR_BROADCAST` on subs side. No Exchange on vitals.

---

### Step 6 — Ghost User Isolation

Left join leaves nulls for users with no subscription record. Isolated before building the board report.

```python
df_ghost = df_joined.filter(col("Subscription_Type").isNull())
df_clean  = df_joined.filter(col("Subscription_Type").isNotNull())
```

---

### Step 7 — City Stress Matrix

```python
df_final = df_clean.groupBy("ville").pivot("Subscription_Type").agg(
    max("Heart_Rate").alias("max_Heart_Rate"),
    min("Heart_Rate").alias("min_Heart_Rate")
)
```

Output: one row per city, columns for Free_max, Free_min, Premium_max, Premium_min.

---

### Step 8 — Interactive Widget

Board can toggle between cities without re-running the pipeline. `df_final` is already in memory — only the display cell re-executes.

```python
dbutils.widgets.dropdown("ville", "Paris", ["Paris", "Lyon", "Marseille"])

selected_ville = dbutils.widgets.get("ville")
display(df_final.filter(col("ville") == selected_ville))
```

Same concept as a slicer in Power BI — control filters the already-computed result, doesn't recompute it.

---

## Physical Plan — What This Looked Like in Practice

The explain() output for the broadcast join showed:

- `PhotonBroadcastHashJoin` — broadcast working correctly
- `PhotonShuffledHashJoin LeftAnti` — frozen sensor exclusion (separate shuffle, expected)
- `EXECUTOR_BROADCAST` on subs — small table sent to every executor
- `PushedFilters` on FileScan — predicate pushdown active

See [Shuffle](../concepts/shuffle.md) and [Spark Execution Model](../concepts/spark-execution-model.md) for the concepts behind what appeared in this plan.

---

## Key Decisions

| Decision | Choice | Why |
|----------|--------|-----|
| When to mask | At ingestion | Raw PII never travels through the pipeline |
| Fixed vs per-team salt | Fixed company salt | Enables cross-table joins — teams must share the same secret |
| Frozen sensor approach | Window + left_anti | Preserves real stable readings, only removes stuck sensors |
| Join strategy | Broadcast | subs table at 125 KB — eliminates shuffle on 750k vitals rows |
| Ghost users | Isolate, not delete | Available for investigation; excluded from board output |
