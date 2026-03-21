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

Five preparation questions from the bootcamp session, answered from Chloé's perspective as the lead data engineer.

---

### Q1 — Privacy & Utility: Salt Coordination Across Teams

**If different teams use different salts, how can they work together? What rules should Chloé suggest to the CPO?**

If Team A hashes `User_ID` with `"TeamA_2025"` and Team B uses `"TeamB_2025"`, the resulting hashes are different strings. A join between their datasets on `hashed_name` returns zero rows. No error. No warning. Just silently empty output. That's worse than a crash — at least a crash is visible.

So Chloé needs to give the CPO a few concrete rules.

One salt, stored in a secrets vault. Databricks Secrets (backed by Azure Key Vault) holds it as a named secret. No team stores it in a notebook or config file. Every pipeline reads it from the same vault path at runtime:

```python
salt = dbutils.secrets.get(scope="santeflux", key="user_id_salt")
```

Access control goes on the vault, not on the string itself. The CPO grants access to the secret scope by team — either a team can read the salt or it can't. This replaces "share it over Slack" with an actual access log.

There's one thing the CPO really needs to understand: if the salt ever changes, every existing hash becomes unmatchable. All historical data needs re-hashing before the new salt goes live. That's expensive. Treat the salt like a primary key — stable, versioned, only change it with a migration plan in place.

And document it. Every dataset using a hashed ID should note which salt version was applied and which function. Salting stops rainbow table attacks, but it also turns `User_ID` into a coordination contract across teams. Manage it like infrastructure.

---

### Q2 — Performance & Architecture: Shuffle Wall from pivot()

**Why does pivot() create more network stress than a simple filter()? How would you explain Shuffle costs to a non-technical boss?**

A `filter()` is a narrow transformation. Each executor looks at its own rows and decides keep or drop — no data crosses the network. Ten executors work in parallel, no one waits for anyone.

A `pivot()` is different. To compute `max(Heart_Rate)` for Paris + Free tier, Spark needs every row matching that combination on the same executor. Rows spread across 10 machines have to be sorted by group key and sent across the network first. That movement is the shuffle. The Shuffle Wall in the Spark UI is the stage boundary where all tasks in Stage N must finish before Stage N+1 can start — everything waits for the last slow executor to finish sending its data.

For the boss:

> "Imagine 10 employees, each with a pile of 75,000 records. You ask them to remove anything marked 'invalid'. Each person works through their own pile — no coordination needed.
>
> Now you ask them to find the highest heart rate across all Paris records. Every person who has a Paris record has to walk across the room and hand it to one designated person. That walking is the shuffle. The more Paris records exist, the more walking. And everyone waits for the last person to finish before any calculation can happen.
>
> Pivot is that second task. We cut the data down before reaching it — frozen sensors and ghost users removed first — so fewer records needed to make that trip."

The physical plan confirms it: the pivot stage has an Exchange node and a spike in network I/O. The filter stage before it has neither.

---

### Q3 — Join Strategy: Ghost Users and When to Broadcast

**Which join type finds Ghost Users? When is broadcast() the right choice?**

Ghost users are in vitals but have no matching record in subs. Two ways to find them.

The direct way — left_anti:

```python
df_ghost = vitals_clean_df.join(
    df_subs_upd,
    on="hashed_name",
    how="left_anti"
)
```

Returns only left-side rows with no match on the right. One join, no nulls to filter afterward.

What Chloé actually did — left join then filter:

```python
df_joined = vitals_clean_df.join(broadcast(df_subs_upd), ..., how="left")
df_ghost = df_joined.filter(col("Subscription_Type").isNull())
df_clean = df_joined.filter(col("Subscription_Type").isNotNull())
```

More code, but it produces both `df_ghost` and `df_clean` from a single join pass. Since both outputs were needed anyway, one join is more efficient than two separate ones.

On broadcast: it sends the small table to every executor so the large table never moves across the network. At ~125 KB, `subs` is an easy call.

| Situation | Decision |
|-----------|----------|
| Small table < 10 MB | Auto-broadcast — Spark handles it |
| Small table 10–200 MB | Manual `broadcast()` hint — safe |
| Small table > 200 MB | Risky — executor memory pressure |
| Both tables large | Sort-merge join |

Without broadcast, Spark shuffles both tables by join key — 750,000 rows crossing the network for no reason. The physical plan confirmed it worked: `PhotonBroadcastHashJoin BuildRight`, `EXECUTOR_BROADCAST` on subs, no Exchange on vitals.

---

### Q4 — Tungsten Advantage: Off-Heap Memory and Cluster Sizing

**How does off-heap memory change how Chloé picks her cluster size? What metrics should she watch?**

Normal Java programs store objects in JVM heap memory. The garbage collector periodically scans the heap to free memory from objects no longer in use. During a GC pause, all threads stop — executor frozen, tasks stalled, whole stage slows. On large datasets, GC pauses can eat 10–20% of job time.

Tungsten stores data in off-heap memory, outside the JVM entirely. The GC never touches it. Spark manages the layout directly in binary format, which is also more CPU cache-friendly.

For cluster sizing, this changes the numbers. Without off-heap you over-provision: if you need 16 GB of working data, you size for 24–32 GB because GC pressure grows non-linearly and you need headroom against pauses. With Tungsten, GC pressure drops and you can size closer to what the data actually needs.

```python
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "2g")  # per executor
```

Too small and Spark spills intermediate data to disk. Too large and you're wasting memory the OS could use for file caching.

Metrics to watch in the Spark UI:

| Metric | Tab | What it tells you |
|--------|-----|-------------------|
| GC Time | Executor | Near zero is normal. Spikes mean heap pressure |
| Spill (Memory) | Stage | Data overflowing from RAM — add off-heap or reduce partitions |
| Spill (Disk) | Stage | Worse than memory spill — serious shortage |
| Executor Memory Used | Executor | Consistently under 60% means over-provisioned |
| Task Duration Variance | Stage | High variance — some executors GC-pausing or spilling while others finish |

For SantéFlux on serverless Databricks, Photon runs on native code and handles most of this automatically. On a standard cluster these settings matter more.

---

### Q5 — The Boardroom Pitch: One Chart, Five Minutes

**What one chart shows that Chloé's work keeps the data safe AND delivers the business answer?**

The City Stress Matrix, filtered to one city via the widget:

```
| ville  | Free_max_Heart_Rate | Free_min_Heart_Rate | Premium_max_Heart_Rate | Premium_min_Heart_Rate |
|--------|---------------------|---------------------|------------------------|------------------------|
| Paris  | 120                 | 60                  | 98                     | 55                     |
```

No names. No IDs. No individual records — just aggregated ranges. The board is looking at actual GDPR-compliant output, not a compliance document. And it answers the question that got analytics blocked: Paris Free-tier users hit 120 bpm max vs Premium at 98. That's a product question the data team can now investigate.

Toggling the widget live in the room adds something: Chloé can switch between Paris, Lyon, and Marseille without re-running anything. The pipeline already finished. It shows the board the system works interactively, not as a one-time dump.

Five minutes:

1. "750,000 health records. Names masked, IDs hashed, stuck sensors removed. Nothing identifiable reaches any output." (30 seconds — lead with safety, that's what blocked analytics)
2. Show the table. "This is what 750,000 records look like after the pipeline." (1 minute)
3. Toggle the widget live. "No re-run, no IT ticket." (30 seconds)
4. Ask: "Which city do you want to look at first?" (fill the rest of the time with the conversation the board actually wants to have)

The table needs no annotations. The absence of personal data is the point.
