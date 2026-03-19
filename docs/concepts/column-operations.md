# Column Operations

## Two functions you'll use constantly

Once your dataframe is loaded, you'll spend a lot of time either adding/updating columns or renaming them. PySpark has two dedicated functions for this.

---

## withColumn — add or replace a column

```python
df.withColumn("column_name", expression)
```

Think of it like `ALTER TABLE` + `UPDATE` in SQL, but for a dataframe. You give it a column name and an expression. Two things can happen:

- If the column name **doesn't exist** — it creates a new column
- If the column name **already exists** — it replaces that column in place

```python
from pyspark.sql.functions import col, upper

# Create a new column
df = df.withColumn("gst_flag", col("amount") * 0.18)

# Replace an existing column (normalize mobile numbers)
df = df.withColumn("mobile", col("mobile").cast("string"))
```

Both use the exact same syntax. The only difference is whether the name is new or existing — Spark figures it out.

---

## withColumnRenamed — rename a column

```python
df.withColumnRenamed("old_name", "new_name")
```

This just renames — it doesn't change the data. Like `sp_rename` in T-SQL or aliasing a column, except the rename sticks on the dataframe.

```python
# Source file has "cust_id", you want it as "customer_id"
df = df.withColumnRenamed("cust_id", "customer_id")

# Rename Aadhaar field to match downstream naming convention
df = df.withColumnRenamed("aadhaar_no", "national_id")
```

---

## Side by side

| | `withColumn` | `withColumnRenamed` |
|---|---|---|
| What it does | Adds or replaces a column | Renames a column |
| Changes data? | Yes (the expression runs) | No — name only |
| SQL equivalent | `ALTER TABLE` + computed column | `sp_rename` / column alias |
| When to use | Transformations, type casts, derived columns | Standardising names from source files |

---

## One thing to be careful about

`withColumn` runs the expression every time. If you chain many `withColumn` calls, Spark builds up a long execution plan. For bulk transformations (10+ columns), `select` with expressions is faster. For 1-3 columns, `withColumn` is fine.

```python
# Fine for a few columns
df = df.withColumn("tax", col("amount") * 0.18)
df = df.withColumn("total", col("amount") + col("tax"))

# For many columns — use select instead
df = df.select(
    "*",
    (col("amount") * 0.18).alias("tax"),
    (col("amount") * 1.18).alias("total")
)
```
