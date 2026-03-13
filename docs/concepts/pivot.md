# Pivot in PySpark

## Rows into columns

You know this from Excel pivot tables and Power Query. Same idea — take unique values from a column and spread them out as separate columns.

Average heart rate by city normally looks like this:

| ville | avg_heart_rate |
|-------|---------------|
| Paris | 79.2 |
| Lyon | 81.4 |
| Marseille | 77.8 |

After pivot:

| Paris | Lyon | Marseille |
|-------|------|-----------|
| 79.2 | 81.4 | 77.8 |

---

## The code

```python
from pyspark.sql.functions import avg, round

df_pivot = df_clean.groupBy().pivot("ville").agg(round(avg("Heart_Rate"), 2))
display(df_pivot)
```

| Part | What it does |
|------|-------------|
| `groupBy()` | No row grouping — whole dataset, one output row |
| `pivot("ville")` | Each city becomes a column |
| `agg(avg(...))` | Average heart rate per city column |

---

## Add a second dimension

Put something in `groupBy()` to get one row per value of that column:

```python
df_pivot = df_clean.groupBy("Status").pivot("ville").agg(round(avg("Heart_Rate"), 2))
```

One row per Status, one column per city.

---

## SQL equivalent

```sql
SELECT *
FROM df_clean
PIVOT (
    AVG(Heart_Rate)
    FOR ville IN ('Paris', 'Lyon', 'Marseille')
)
```

PySpark doesn't need the value list — it reads the distinct values from the data automatically. SQL requires you to hard-code them.
