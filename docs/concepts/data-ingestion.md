# Data Ingestion

## What Does "Ingestion" Actually Mean?

It just means **reading a file and loading it into PySpark** so you can work with it.

Think of it like opening a spreadsheet in Excel — except instead of Excel, it's PySpark, and instead of clicking File → Open, you write one line of code.

In SQL terms: it's like doing a bulk import or loading data into a staging table. Once it's loaded, you can query and transform it.

---

## Reading a CSV File

```python
df_pii = spark.read.csv(
    "/Volumes/workspace/green_grid_case_study/raw_files/customer_pii.csv",
    header=True,
    inferSchema=True
)
```

Breaking this down:

| Part | What it means |
|------|--------------|
| `spark.read.csv(...)` | "Open this CSV file" |
| The path in quotes | Where the file lives (in the Volume) |
| `header=True` | "The first row is column names, not data" |
| `inferSchema=True` | "Have a guess at the data types for me" |

After loading, **always run these two lines:**
```python
display(df_pii)       # preview the data — like SELECT TOP 10
df_pii.printSchema()  # check what types Spark assigned to each column
```

`printSchema()` is your best friend. Run it every single time after loading a file.

---

## Reading a JSON File

```python
df_readings = spark.read.json(
    "/Volumes/workspace/green_grid_case_study/raw_files/meter_readings.json",
    multiLine=True
)
```

The only extra thing here is `multiLine=True`.

**Why?** JSON files come in two formats:

**Format 1 — One record per line** (Spark reads this by default):
```json
{"id": 1, "kwh": 45.2}
{"id": 2, "kwh": 30.1}
```

**Format 2 — Standard JSON array** (needs `multiLine=True`):
```json
[
  {"id": 1, "kwh": 45.2},
  {"id": 2, "kwh": 30.1}
]
```

The Green Grid file was a standard JSON array, so we needed `multiLine=True`. If you forget it and the file is an array, Spark will either error or return garbage. Just always add it when reading JSON.

---

## CSV vs JSON — What Gets Inferred Correctly?

This trips people up. Here's the honest truth:

| Data Type | CSV | JSON |
|-----------|-----|------|
| Numbers | Usually correct | Correct |
| Text | Correct | Correct |
| Dates/Timestamps | **Comes in as text** | **Comes in as text** |
| True/False | Comes in as text | Correct |

**The big one to remember:** Timestamps almost always come in as text (string), regardless of the file format. You'll need to either cast them manually or use an explicit schema (see [Schema Definition](schema-definition.md)).

---

## The SQL Equivalent

Loading a file in PySpark is like this in SQL:

```sql
-- SQL
CREATE TABLE staging_readings AS
SELECT * FROM OPENROWSET(BULK 'meter_readings.json', ...)

-- PySpark equivalent
df_readings = spark.read.json("/path/to/meter_readings.json", multiLine=True)
```

The DataFrame (`df_readings`) is your staging table. It lives in memory, not written to disk yet.
