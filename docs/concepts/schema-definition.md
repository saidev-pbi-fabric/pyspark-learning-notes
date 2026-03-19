# Schema definition

## The problem: Spark is guessing

When you use `inferSchema=True`, Spark reads through your entire file and guesses what type each column should be. Sometimes it guesses right. Sometimes it doesn't.

The most common failure: timestamps come in as `string` instead of `TimestampType`. You only find out later when a date calculation fails and you're wondering why.

It also means Spark reads the file twice — once to guess types, once to actually load the data. On large files, that matters.

The fix is to just tell Spark exactly what types you want. No guessing.

---

## Think of it like column definitions in SQL

In SQL, when you create a table you define what type each column is:

```sql
CREATE TABLE meter_readings (
    kwh        FLOAT,
    meter_id   VARCHAR(50),
    status     VARCHAR(20),
    timestamp  DATETIME
)
```

An explicit schema in PySpark is exactly the same thing — you're defining the columns and their types before loading the data.

---

## How to write an explicit schema

You use two things: `StructType` (the whole schema) and `StructField` (one column).

```python
from pyspark.sql.types import *

schema_readings = StructType([
    StructField("kwh",       DoubleType(),    True),
    StructField("meter_id",  StringType(),    True),
    StructField("status",    StringType(),    True),
    StructField("timestamp", TimestampType(), True)
])
```

Each `StructField` has three things:

```
StructField( "column_name" ,  DataType() ,  nullable? )
```

- `"column_name"` — must match exactly what's in the file
- `DataType()` — what type you want
- `True/False` — `True` means nulls are allowed, `False` means they're not

Then pass the schema when you read the file:

```python
df_readings = spark.read.json(
    "/Volumes/workspace/green_grid_case_study/raw_files/meter_readings.json",
    multiLine=True,
    schema=schema_readings   # ← just add this
)
```

Spark now reads the file once, applies your types, done.

---

## Common data types: SQL vs PySpark

| SQL | PySpark | Use for |
|-----|---------|---------|
| `VARCHAR` / `NVARCHAR` | `StringType()` | Text, IDs, codes |
| `INT` | `IntegerType()` | Whole numbers |
| `FLOAT` / `DECIMAL` | `DoubleType()` | kWh, prices, amounts |
| `DATE` | `DateType()` | Dates only (no time) |
| `DATETIME` / `TIMESTAMP` | `TimestampType()` | Date + time |
| `BIT` / `BOOLEAN` | `BooleanType()` | True/False flags |

---

### One thing to watch out for with ID columns

If your data has a `user_id` like `00123`, and you define it as `IntegerType()`, Spark stores it as `123`. The leading zeros are gone — silently, no error.

Same problem with any ID that looks numeric but isn't really a number: account numbers, meter IDs, postal codes, IFSC codes.

Rule: if a column is an ID or code — even if it contains only digits — use `StringType()`. You'll never do maths on a user_id, so there's no reason to store it as a number.

---

## When to use which

| Situation | Use |
|-----------|-----|
| Exploring a new file for the first time | `inferSchema=True` — quick look |
| Any pipeline you're actually building | Explicit schema — every time |
| File has timestamps or specific decimal precision | Explicit schema — always |

Rule of thumb: `inferSchema` for exploration, explicit schema for anything you'll run again.

---

## Why this matters

In Green Grid, `meter_readings.json` had a `timestamp` column. Without an explicit schema, it would have loaded as `string`. Then when we tried to extract the hour with `hour(df.timestamp)`, it would have failed — because `hour()` only works on `TimestampType`, not text.

Defining the schema upfront saved us from a confusing error three steps later.
