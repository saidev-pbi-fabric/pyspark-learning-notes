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

---

## Discussion Questions

These are the five preparation questions from the bootcamp session. Answers written from Chloé's perspective as the lead data engineer.

---

### Q1 — Privacy & Utility: Salt Coordination Across Teams

**If different teams use different salts, how can they work together? What rules should Chloé suggest to the CPO?**

The problem is straightforward: if Team A hashes `User_ID` with salt `"TeamA_2025"` and Team B hashes the same `User_ID` with salt `"TeamB_2025"`, the resulting hashes are completely different strings. A join between their datasets on `hashed_name` returns zero rows. The data looks clean, the join runs without errors, but the output is empty. That is worse than a visible crash — it silently produces wrong results.

**What Chloé should propose to the CPO:**

1. **One company salt, stored in a secrets vault.** Databricks Secrets (backed by Azure Key Vault) holds the salt as a named secret. No team stores it in a notebook or a config file. Every pipeline that needs to hash `User_ID` reads it from the same vault path at runtime.

    ```python
    salt = dbutils.secrets.get(scope="santeflux", key="user_id_salt")
    ```

2. **Access control on the vault, not on the salt string.** The CPO grants access to the Databricks secret scope by team, not by individual. A team either has permission to read the salt or it doesn't. This replaces informal "share the string over Slack" workflows with a proper access log.

3. **Salt versioning policy.** If the salt ever needs to change (rotation policy or a breach), every existing hash becomes unmatchable. All historical data must be re-hashed before the new salt goes live. This is expensive, so the CPO needs to treat the salt like a primary key — stable, versioned, change only with a migration plan.

4. **Document the dependency.** Every dataset that uses a hashed ID should have a metadata note: "hashed with `santeflux/user_id_salt` v1, applied using SHA-256." Any analyst joining two hashed datasets can verify they used the same version.

**The core trade-off:** Salting adds security (stops rainbow table attacks) but turns `User_ID` into a coordination contract. The salt is the shared secret that makes cross-team joins possible. Manage it like infrastructure, not like a password someone typed in a notebook three months ago.

---

### Q2 — Performance & Architecture: Shuffle Wall from pivot()

**Why does pivot() create more network stress than a simple filter()? How would you explain Shuffle costs to a non-technical boss?**

**The technical reason:**

A `filter()` is a narrow transformation. Each executor looks at its own rows and decides: keep this one or drop it. No data moves between machines. Ten executors work in parallel, independently.

A `pivot()` is a wide transformation. To compute `max(Heart_Rate)` for the combination of `ville = Paris` and `Subscription_Type = Free`, Spark needs **every row** where `ville = Paris` and `Subscription_Type = Free` to land on **the same executor**. Rows currently spread across 10 machines need to be sorted by their group key and sent across the network to the right executor before any aggregation can start. This cross-network movement is the shuffle.

The "Shuffle Wall" in the Spark UI is the stage boundary where all tasks in Stage N must complete before Stage N+1 can begin. Nothing moves forward until the last slow executor finishes sending its data.

**How to explain it to a non-technical boss:**

> "Imagine you have 10 employees, each with a pile of 75,000 health records on their desk. You ask them to remove any record marked 'invalid'. Each person works through their own pile. Fast — no one needs to talk to anyone else.
>
> Now you ask them to calculate the maximum heart rate for Paris users across all piles. Every person who has a Paris record has to walk across the room and hand it to one designated person. That walking is the shuffle. The more Paris records exist, the more walking happens. And everyone else has to wait for the last person to finish walking before the calculation can start.
>
> Pivot is that second task. We reduced the data before the pivot — removed frozen sensors and ghost users first — so fewer records needed to walk across the room."

**The Spark UI confirms this:** the pivot stage shows an Exchange node (shuffle) in the physical plan, a spike in network I/O, and a clear stage boundary. The filter stage before it shows no Exchange node at all.

---

### Q3 — Join Strategy: Ghost Users and When to Broadcast

**Which join type finds Ghost Users? When is broadcast() the right choice?**

**Finding Ghost Users — two valid approaches:**

Ghost users are rows in `vitals` with no matching record in `subs`. Two ways to find them:

**Option A — left_anti join (direct approach):**

```python
df_ghost = vitals_clean_df.join(
    df_subs_upd,
    on="hashed_name",
    how="left_anti"
)
```

`left_anti` returns only left-side rows with no match on the right. One join, no nulls to filter.

**Option B — left join then filter nulls (what Chloé actually did):**

```python
df_joined = vitals_clean_df.join(broadcast(df_subs_upd), ..., how="left")
df_ghost = df_joined.filter(col("Subscription_Type").isNull())
df_clean = df_joined.filter(col("Subscription_Type").isNotNull())
```

This is slightly more work but produces both `df_ghost` and `df_clean` from a single join pass, which is more efficient than running two separate joins. Chloé chose this because she needed both outputs anyway.

**When is broadcast() the right choice?**

Broadcast is correct when one table is small enough to fit in executor memory and the other is large. The rule:

| Condition | Use broadcast? |
|-----------|---------------|
| Small table < 10 MB (Spark default threshold) | Auto-broadcast — Spark does it automatically |
| Small table 10 MB – 200 MB | Manual `broadcast()` hint — safe |
| Small table > 200 MB | Risky — executor memory pressure, may OOM |
| Both tables large | Sort-merge join — no broadcast |

In this case: `subs` is 10,000 rows at ~125 KB. Trivially safe to broadcast. The physical plan confirmed it: `PhotonBroadcastHashJoin BuildRight` with `EXECUTOR_BROADCAST` on the subs side and no Exchange node on the 750,000-row vitals table.

The broadcast eliminates the shuffle entirely on the large table. Without it, Spark shuffles both tables by join key — 750,000 rows crossing the network unnecessarily.

---

### Q4 — Tungsten Advantage: Off-Heap Memory and Cluster Sizing

**How does off-heap memory change how Chloé picks her cluster size? What metrics should she watch?**

**What off-heap means:**

Normal Java programs store objects in JVM heap memory. The JVM's garbage collector periodically scans the heap to free memory from objects no longer in use. During a GC pause, all threads stop — the executor is frozen, tasks stall, and the whole stage slows down. On large datasets with lots of intermediate objects, GC pauses can eat 10–20% of job time.

Tungsten stores data in **off-heap memory** — raw OS memory, outside the JVM. The garbage collector never touches it. Spark manages the memory layout directly using binary format (like a database does), which is also more CPU cache-friendly.

**What this means for cluster sizing:**

With standard JVM heap: you have to over-provision. If you need 16 GB of working data, you size for 24–32 GB because GC pressure grows non-linearly. You're buying headroom against pauses.

With Tungsten off-heap: GC pressure drops sharply. You can size more tightly — provision for what the data actually needs, not what GC-related overhead might consume. Less wasted compute cost.

The off-heap allocation is controlled by:

```python
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "2g")  # per executor
```

If off-heap is too small, Spark spills intermediate data to disk — which is slow. If it's too large, you're wasting memory that the OS could use for file caching.

**Metrics to watch in the Spark UI:**

| Metric | Where | What it tells you |
|--------|-------|-------------------|
| GC Time | Executor tab | Should be near zero; spikes mean heap pressure |
| Spill (Memory) | Stage tab | Data spilling from RAM to disk — add off-heap or reduce partition size |
| Spill (Disk) | Stage tab | Worse than memory spill — serious memory shortage |
| Executor Memory Used | Executor tab | If consistently < 60%, you're over-provisioned |
| Task Duration Variance | Stage tab | High variance = some executors are GC-pausing or spilling while others finish |
| Shuffle Read/Write | Stage tab | High shuffle volume + GC time together = resize the cluster |

For SantéFlux on serverless Databricks, Photon Engine handles much of this automatically — it runs on native code entirely, so Tungsten's off-heap benefit is built in. On a standard cluster, these settings matter more.

---

### Q5 — The Boardroom Pitch: One Chart, Five Minutes

**What one chart shows that Chloé's work keeps the data safe AND delivers the business answer?**

**The chart: the City Stress Matrix filtered to one city via the widget.**

```
| ville  | Free_max_Heart_Rate | Free_min_Heart_Rate | Premium_max_Heart_Rate | Premium_min_Heart_Rate |
|--------|---------------------|---------------------|------------------------|------------------------|
| Paris  | 120                 | 60                  | 98                     | 55                     |
```

**Why this one output proves both things at once:**

- **Data safety:** There are no names, no IDs, no individual records in this table. It is aggregated ranges. No one can reverse-engineer a person from a maximum heart rate. The pipeline stripped every piece of PII before this result was ever computed. The board is looking at the proof of GDPR compliance — not a policy document, the actual output.

- **Business value:** The table answers the question the CPO blocked analytics from answering. Paris Free-tier users hit a max of 120 bpm vs Premium users at 98 bpm. That is a product insight. Are Free users more physically active? Are Premium features attracting a different demographic? The data now supports that conversation.

- **The widget adds one more proof point:** Chloé can toggle between Paris, Lyon, and Marseille live in the room without re-running anything. The pipeline already finished. The board sees that the system is interactive, not a one-time report.

**The five-minute pitch structure:**

1. "750,000 health records. Names masked at ingestion, IDs replaced with cryptographic hashes, stuck sensors removed. Nothing identifiable enters any output." (30 seconds — safety first, because that is what blocked analytics)
2. Show the table. "This is what 750,000 records look like after the pipeline — city-level ranges, subscription tier split." (1 minute)
3. Toggle the widget. "The board can slice by city. No re-run, no IT ticket." (30 seconds)
4. One question back to the room: "Which city do you want to look at first?" (the rest of the five minutes)

The chart does not need annotations or a legend. The absence of names and IDs is the story. A table of numbers with no personal data in it is the deliverable the CPO asked for.
