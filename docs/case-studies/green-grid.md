# Case Study 1 — Green Grid Smart Billing Pipeline

**Domain:** Smart Energy
**Platform:** Databricks · Unity Catalog · Serverless Compute
**Status:** Complete

---

## The Scenario

Green Grid is a smart energy company. They collect electricity usage readings from smart meters installed at customer homes. Each reading records how many kilowatt-hours (kWh) were consumed at a specific time.

**The business problem:** Calculate each customer's bill, applying a peak-hour surcharge (18:00–21:00), while ensuring bad sensor readings don't corrupt the billing data, and customer email addresses are never stored in plain text.

---

## Data Sources

Three raw files, uploaded to a Unity Catalog managed volume:

| File | Format | What It Contains |
|------|--------|-----------------|
| `customer_pii.csv` | CSV | Customer name, email, meter ID, plan ID |
| `meter_readings.json` | JSON | kWh reading, meter ID, status, timestamp |
| `pricing_plans.csv` | CSV | Plan ID, plan name, base price per kWh |

**Unity Catalog path:**
```
workspace
  └── green_grid_case_study (schema)
        └── raw_files (managed volume)
              ├── customer_pii.csv
              ├── meter_readings.json
              └── pricing_plans.csv
```

---

## Pipeline Architecture

```
Raw Files (Volume)
      │
      ▼
  Ingest (3 DataFrames)
      │
      ▼
  Data Quality Split
  ├── Bad records  →  Quarantine_Readings (Delta table)
  └── Clean records
          │
          ▼
      Join (meter readings + PII + pricing plans)
          │
          ▼
      PII Hashing (email → SHA-256, raw email dropped)
          │
          ▼
      Business Logic (hour extraction → peak pricing → bill amount)
          │
          ▼
      Silver_SmartBill (Delta table)
```

---

## Step-by-Step Code

### 1. Import Libraries

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
```

### 2. Ingest Data

**Customer PII (CSV — inferSchema):**
```python
df_pii = spark.read.csv(
    "/Volumes/workspace/green_grid_case_study/raw_files/customer_pii.csv",
    header=True, inferSchema=True
)
display(df_pii)
df_pii.printSchema()
```

**Pricing Plans (CSV — inferSchema):**
```python
df_plans = spark.read.csv(
    "/Volumes/workspace/green_grid_case_study/raw_files/pricing_plans.csv",
    header=True, inferSchema=True
)
display(df_plans)
df_plans.printSchema()
```

**Meter Readings (JSON — explicit schema):**

Explicit schema used here because `timestamp` must be `TimestampType` and `kwh` must be `DoubleType` for downstream calculations to work correctly.

```python
schema_readings = StructType([
    StructField("kwh",       DoubleType(),    True),
    StructField("meter_id",  StringType(),    True),
    StructField("status",    StringType(),    True),
    StructField("timestamp", TimestampType(), True)
])

df_readings = spark.read.json(
    "/Volumes/workspace/green_grid_case_study/raw_files/meter_readings.json",
    multiLine=True,
    schema=schema_readings
)
display(df_readings)
df_readings.printSchema()
```

### 3. Data Quality — Quarantine Bad Records

Valid kWh range: 0 to 99.99. Anything outside this is quarantined.

```python
# Capture bad records
df_quarantine = df_readings.filter(
    (df_readings.kwh < 0) | (df_readings.kwh >= 100)
)

# Write to quarantine table
df_quarantine.write.mode("overwrite").saveAsTable(
    "workspace.green_grid_case_study.Quarantine_Readings"
)

# Validate the write
df_test = spark.read.table("workspace.green_grid_case_study.Quarantine_Readings")
df_quarantine.count() == df_test.count()   # must return True
```

```python
# Keep only clean records
df_clean = df_readings.filter(
    df_readings.kwh.isNotNull() &
    (df_readings.kwh >= 0) &
    (df_readings.kwh < 100)
)
df_clean.write.mode("overwrite").saveAsTable(
    "workspace.green_grid_case_study.clean_readings"
)
```

### 4. Join the Three Datasets

Two left joins chained — clean readings is the primary dataset.

```python
df_joined = df_clean \
    .join(df_pii,   df_clean.meter_id == df_pii.MeterID,   "left") \
    .join(df_plans, df_pii.PlanID     == df_plans.PlanID,  "left") \
    .drop(df_clean.meter_id) \   # resolve ambiguous column
    .drop(df_pii.PlanID)         # resolve ambiguous column

display(df_joined)
```

### 5. Hash PII

Replace raw email with SHA-256 hash. Drop the original immediately.

```python
df_hashed = df_joined.withColumn(
    "CustomerHashed",
    sha2(df_joined.CustomerEmail, 256)
).drop(df_joined.CustomerEmail)

display(df_hashed)
```

### 6. Apply Peak Pricing and Calculate Bill

```python
# Extract hour from timestamp
df_final1 = df_hashed.withColumn("Hour", hour(df_hashed.timestamp))

# Apply 2x rate during peak hours (18:00–21:00)
df_finalrate = df_final1.withColumn(
    "FinalRate",
    when(
        (df_final1.Hour >= 18) & (df_final1.Hour <= 21),
        df_final1.BasePricePerKwh * 2
    ).otherwise(df_final1.BasePricePerKwh)
)

# Calculate bill amount (kWh × rate, rounded to 2dp)
df_billamount = df_finalrate.withColumn(
    "Bill_Amount",
    round(df_finalrate.kwh * df_finalrate.FinalRate, 2)
)

display(df_billamount)
```

### 7. Write Silver Table

```python
df_billamount.write.mode("overwrite").saveAsTable(
    "workspace.green_grid_case_study.Silver_SmartBill"
)

# Read back to confirm
df_silver_tbl = spark.read.table("workspace.green_grid_case_study.Silver_SmartBill")
display(df_silver_tbl)
```

---

## Key Decisions Made

| Decision | Why |
|----------|-----|
| Explicit schema for meter readings JSON | `timestamp` and `kwh` types must be correct for downstream logic to work |
| Left join (not inner) | Keep all clean readings even if no matching customer or plan is found |
| Quarantine before cleaning | Audit trail — bad records must be visible and traceable, not silently dropped |
| Hash email, then drop original | SHA-256 is sufficient for analytics; raw PII must never reach the silver layer |
| Round at final step | Avoids accumulated rounding errors from intermediate calculations |

---

## What We Learned

1. **CSV vs JSON ingestion** — different defaults, timestamps always need explicit handling
2. **Quarantine pattern** — split bad records before transforming; always validate writes
3. **Chained joins** — two left joins sequentially; drop ambiguous columns immediately
4. **PII handling** — hash with `sha2()`, drop original in same step
5. **`when().otherwise()`** — PySpark's conditional column logic
6. **`hour()`** — extracts hour integer from `TimestampType` for time-window logic
7. **Silver layer naming** — `Silver_` prefix signals cleaned, enriched, business-ready data

---

## Concepts Used

- [Unity Catalog](../concepts/unity-catalog.md)
- [Data Ingestion](../concepts/data-ingestion.md)
- [Schema Definition](../concepts/schema-definition.md)
- [Data Quality & Quarantine](../concepts/data-quality.md)
- [PII Handling](../concepts/pii-handling.md)
- [Joins in PySpark](../concepts/joins.md)
- [Business Logic](../concepts/business-logic.md)
