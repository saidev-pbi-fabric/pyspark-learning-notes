# Window functions

## Already familiar from SQL

```sql
AVG(Heart_Rate) OVER (PARTITION BY User_ID ORDER BY Timestamp)
```

PySpark's window functions work the same way. They look at rows related to the current row without collapsing them into one. That's what separates them from `groupBy`.

---

## The frozen sensor problem

A sensor freezes when it gets stuck sending the same heart rate over and over. SantéFlux had 150 frozen readings out of 750,000.

A working sensor moves. A frozen one flatlines. Detection: bucket readings into 10-minute windows per user. If `min == max` inside a bucket, the sensor didn't change. Those records skew the city averages and need to be removed before the report runs.

---

## Detecting frozen sensors

```python
from pyspark.sql.functions import window, min, max, col

df_agg = df_hashed.groupBy(
    "hashed_id",
    window("Timestamp", "10 minutes")
).agg(
    min("Heart_Rate").alias("min_rate"),
    max("Heart_Rate").alias("max_rate")
)

# frozen windows
df_frozen = df_agg.filter(col("min_rate") == col("max_rate"))
df_frozen.count()

# clean data for reporting
df_clean = df_agg.filter(col("min_rate") != col("max_rate"))
```

`window("Timestamp", "10 minutes")` creates buckets: 08:00–08:10, 08:10–08:20, and so on. Same user, same bucket, same group. A frozen sensor — same reading ten times in a row — comes out with identical min and max.

---

## One gotcha — use window() directly in groupBy

`window()` creates a struct with `start` and `end` inside it. If you attach it with `withColumn` first and try to reference it by name in `groupBy`, Spark can't find it.

```python
# Fails — CANNOT_RESOLVE_DATAFRAME_COLUMN
df = df.withColumn("tw", window("Timestamp", "10 minutes"))
df.groupBy("hashed_id", "tw")

# Works
df.groupBy("hashed_id", window("Timestamp", "10 minutes"))
```

Put `window()` directly in the `groupBy`. No intermediate column needed.

---

## groupBy vs window()

| | groupBy | window() |
|--|---------|----------|
| Rows returned | One per group | Same count as input |
| Collapses data? | Yes | No |
| Use case | Final aggregation (avg by city) | Pattern detection (frozen per bucket) |

---

## Where this fits in the pipeline

```
Raw ingest → mask names → hash + salt IDs → detect frozen sensors → aggregate → report
```

The frozen sensor check sits between hashing and final aggregation. Clean data only goes into the Health Trends Report.

See [Hashing & Salting](hashing.md) for why `hashed_id` is used instead of `User_ID` directly.
