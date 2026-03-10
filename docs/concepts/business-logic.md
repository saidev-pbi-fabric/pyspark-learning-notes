# Business Logic in PySpark

## `when().otherwise()` = CASE WHEN

If you know `CASE WHEN` in SQL, you already know this. PySpark's version is called `when().otherwise()` — it does the exact same thing.

```sql
-- SQL
CASE
    WHEN hour >= 18 AND hour <= 21 THEN base_price * 2
    ELSE base_price
END AS FinalRate
```

```python
# PySpark — same logic
from pyspark.sql.functions import when

df = df.withColumn(
    "FinalRate",
    when(
        (df.Hour >= 18) & (df.Hour <= 21),
        df.BasePricePerKwh * 2
    ).otherwise(df.BasePricePerKwh)
)
```

Direct mapping:
- `CASE WHEN condition THEN value` → `when(condition, value)`
- `ELSE value` → `.otherwise(value)`
- `END` → nothing needed, `.otherwise()` closes it

---

## Multiple Conditions (Multiple WHEN clauses)

```sql
-- SQL
CASE
    WHEN kwh < 10  THEN 'Low'
    WHEN kwh < 50  THEN 'Medium'
    ELSE 'High'
END
```

```python
# PySpark
df = df.withColumn(
    "Tier",
    when(df.kwh < 10, "Low")
    .when(df.kwh < 50, "Medium")
    .otherwise("High")
)
```

Spark evaluates top to bottom and stops at the first match — same as SQL.

---

## Adding a New Column — `withColumn()`

In SQL, calculated columns live in the SELECT:
```sql
SELECT kwh * rate AS Bill_Amount FROM ...
```

In PySpark, you add columns using `withColumn()`:
```python
df = df.withColumn("Bill_Amount", df.kwh * df.FinalRate)
```

`withColumn` takes two things:
1. The name of the new column (in quotes)
2. The expression to calculate it

It doesn't change the original DataFrame — it returns a new one with the extra column added. That's why you write `df = df.withColumn(...)` — you're reassigning `df` to the new version.

---

## Extracting Parts of a Timestamp

This is like `DATEPART()` or `HOUR()` in SQL:

```sql
-- SQL
DATEPART(HOUR, timestamp) AS Hour
```

```python
# PySpark
from pyspark.sql.functions import hour

df = df.withColumn("Hour", hour(df.timestamp))
```

Other equivalents:

| SQL | PySpark | Returns |
|-----|---------|---------|
| `DATEPART(HOUR, col)` | `hour(col)` | 0–23 |
| `DATEPART(MINUTE, col)` | `minute(col)` | 0–59 |
| `DATEPART(MONTH, col)` | `month(col)` | 1–12 |
| `DATEPART(YEAR, col)` | `year(col)` | e.g. 2024 |
| `DATEPART(WEEKDAY, col)` | `dayofweek(col)` | 1=Sunday |
| `FORMAT(col, 'yyyy-MM-dd')` | `date_format(col, 'yyyy-MM-dd')` | "2024-03-15" |

**Important:** These functions only work if the column is `TimestampType`. If it's a string (which happens when you don't define a schema), they'll fail or return null. This is one of the main reasons we define schemas explicitly.

---

## Rounding Numbers — `round()`

```sql
-- SQL
ROUND(kwh * rate, 2) AS Bill_Amount
```

```python
# PySpark
from pyspark.sql.functions import round

df = df.withColumn(
    "Bill_Amount",
    round(df.kwh * df.FinalRate, 2)   # 2 decimal places
)
```

Exact same behaviour as SQL `ROUND()`. Always round at the final calculation step — not in intermediate steps — to avoid accumulated rounding errors.

---

## The Green Grid Bill Calculation — All Together

```python
from pyspark.sql.functions import hour, when, round

# Step 1: Extract the hour from the timestamp
df_final1 = df_hashed.withColumn("Hour", hour(df_hashed.timestamp))

# Step 2: Determine the rate (double during peak hours)
df_finalrate = df_final1.withColumn(
    "FinalRate",
    when(
        (df_final1.Hour >= 18) & (df_final1.Hour <= 21),
        df_final1.BasePricePerKwh * 2
    ).otherwise(df_final1.BasePricePerKwh)
)

# Step 3: Calculate the bill (kWh × rate, 2 decimal places)
df_billamount = df_finalrate.withColumn(
    "Bill_Amount",
    round(df_finalrate.kwh * df_finalrate.FinalRate, 2)
)
```
