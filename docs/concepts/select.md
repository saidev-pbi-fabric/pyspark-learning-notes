# select in PySpark

## Already familiar from SQL

```sql
SELECT User_ID, Heart_Rate, ville FROM df_vitals
```

```python
df_selected = df_vitals.select("User_ID", "Heart_Rate", "ville")
```

Same logic. Pass in the columns you want.

---

## Renaming while selecting

```python
from pyspark.sql.functions import col

df_selected = df_vitals.select(
    col("User_ID").alias("patient_id"),
    col("Heart_Rate").alias("bpm"),
    col("ville").alias("city")
)
```

SQL: `SELECT User_ID AS patient_id, Heart_Rate AS bpm, ville AS city`

---

## selectExpr

If you want to write SQL strings instead of chaining `col()` and `alias()`:

```python
df_selected = df_vitals.selectExpr(
    "User_ID as patient_id",
    "Heart_Rate as bpm",
    "Heart_Rate * 1.1 as adjusted_bpm"
)
```

Same result, reads more like a SELECT clause.

---

## Dropping columns

When you want everything except a couple of columns, `drop` beats listing the ones you want.

```python
df_clean = df_vitals.drop("Full_Name", "Status")
```

SQL has no direct equivalent — you'd have to name every column you want to keep. `drop` is one of those small things PySpark handles better.

---

## select vs withColumn

| | select | withColumn |
|--|--------|------------|
| Use when | Choosing which columns to keep | Adding or changing one column |
| Affects column count? | Yes | No |
| Common mistake | Accidentally dropping a column by not listing it | Using it for everything when select would be simpler |
