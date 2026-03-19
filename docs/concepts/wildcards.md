# Wildcards

## What is a wildcard?

A wildcard is a stand-in character that means "anything can go here."

Think of it like a blank tile in Scrabble — it can represent anything. In data, you use a wildcard when you want to match a pattern rather than an exact value.

---

## You already know this: SQL `LIKE`

```sql
SELECT * FROM customers
WHERE email LIKE '%@gmail.com'
```

The `%` is the wildcard. It means "I don't care what comes before @gmail.com — match anything."

| Pattern | Matches | Doesn't match |
|---------|---------|---------------|
| `'%@gmail.com'` | `john@gmail.com`, `x@gmail.com` | `john@hotmail.com` |
| `'john%'` | `john`, `johnny`, `johnson` | `mr.john` |
| `'%error%'` | `critical_error_log`, `my_error_2` | `warning` |

There's also `_` — exactly one character:

```sql
WHERE code LIKE 'A_C'   -- matches ABC, A1C, ACC — not AC, not ABBC
```

---

## Wildcards in PySpark: two places you'll use them

### 1. When reading files: load multiple files at once

```python
# Load just one specific file
df = spark.read.csv("/Volumes/workspace/project/raw_files/sales_jan.csv")

# Load ALL csv files in the folder
df = spark.read.csv("/Volumes/workspace/project/raw_files/*.csv")

# Load only files that start with "sales_"
df = spark.read.csv("/Volumes/workspace/project/raw_files/sales_*.csv")
```

Think of it like selecting files in Windows Explorer — instead of Ctrl+clicking each one, you just say "give me everything ending in `.csv`."

Spark loads all matching files and combines them into one DataFrame automatically.

### 2. When filtering data: `like()` (same as SQL `LIKE`)

```python
# SQL:     WHERE email LIKE '%@gmail.com'
# PySpark:
df.filter(df.email.like('%@gmail.com'))

# SQL:     WHERE status LIKE 'ERR%'
# PySpark:
df.filter(df.status.like('ERR%'))
```

Same logic as SQL — just written as a method on the column.

---

## Why this matters in real pipelines

Imagine Green Grid delivers a new meter readings file every day:

```
meter_readings_2024_01_01.json
meter_readings_2024_01_02.json
meter_readings_2024_01_03.json
... (365 files a year)
```

Without wildcards you'd write 365 separate read statements. With a wildcard:

```python
df_readings = spark.read.json(
    "/Volumes/workspace/green_grid/raw_files/meter_readings_*.json",
    multiLine=True,
    schema=schema_readings
)
```

Spark finds every matching file and combines them all into one DataFrame. This is how production pipelines actually work — files land daily and the pipeline picks them all up without you changing any code.

---

## The `SELECT *` wildcard

```sql
SELECT * FROM table    -- * means "give me all columns"
```

```python
# PySpark
df.select("*")
# In practice, display(df) already shows all columns
```

---

## Quick reference

| Context | Wildcard | Meaning |
|---------|----------|---------|
| SQL `LIKE` | `%` | Zero or more of any character |
| SQL `LIKE` | `_` | Exactly one character |
| PySpark `like()` | `%` | Same as SQL `%` |
| PySpark `like()` | `_` | Same as SQL `_` |
| File paths | `*` | Any file matching the pattern |
| Column selection | `*` | All columns |

---

## One thing to watch out for

When using `*` to load multiple files, all files must have the same structure — same columns, same types. If one file has an extra column or a different name, Spark will either error or fill missing columns with nulls.

This is another reason explicit schemas matter — when loading 365 files at once, you want Spark enforcing the structure, not guessing it from each file.
