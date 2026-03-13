# Joins in PySpark

## Good News — You Already Know This

If you know SQL JOINs, you already understand PySpark joins. The logic is identical. The only thing that changes is how you write it.

```sql
-- SQL
SELECT *
FROM clean_readings r
LEFT JOIN customer_pii p ON r.meter_id = p.MeterID
LEFT JOIN pricing_plans pl ON p.PlanID = pl.PlanID
```

```python
# PySpark — same join, different syntax
df_joined = df_clean \
    .join(df_pii,   df_clean.meter_id == df_pii.MeterID,   "left") \
    .join(df_plans, df_pii.PlanID     == df_plans.PlanID,  "left")
```

Same result. Different shape.

---

## Join Syntax Breakdown

```python
df_result = df_left.join(df_right, join_condition, join_type)
```

| Part | Example | What it is |
|------|---------|------------|
| `df_left` | `df_clean` | Your primary table (like the FROM table in SQL) |
| `df_right` | `df_pii` | The table you're joining to |
| `join_condition` | `df_clean.meter_id == df_pii.MeterID` | The ON clause |
| `join_type` | `"left"` | INNER, LEFT, RIGHT, OUTER |

Note: in PySpark the join condition uses `==` (two equals signs), not `=` like SQL.

---

## Join Types

| SQL | PySpark | Returns |
|-----|---------|---------|
| `INNER JOIN` | `"inner"` | Only matching rows from both sides |
| `LEFT JOIN` | `"left"` | All rows from left, matching from right (null if no match) |
| `RIGHT JOIN` | `"right"` | All rows from right, matching from left |
| `FULL OUTER JOIN` | `"outer"` | All rows from both sides |

**In most pipelines, you'll use `"left"`.** You keep all your primary records and attach extra data where it exists.

---

## Chaining Multiple Joins

In SQL you write multiple JOINs in one query. In PySpark you chain them — each `.join()` adds another join:

```python
df_joined = df_clean \
    .join(df_pii,   df_clean.meter_id == df_pii.MeterID,   "left") \
    .join(df_plans, df_pii.PlanID     == df_plans.PlanID,  "left")
```

The `\` at the end of each line is just a line continuation — it tells Python "the code continues on the next line". Same result as writing it all on one line, just more readable.

---

## The Duplicate Column Problem

This is the one thing PySpark handles differently from SQL — and it will catch you out if you're not ready for it.

**The problem:** When you join two DataFrames that both have a column called `meter_id`, both copies end up in the result. PySpark keeps both, and when you try to reference `meter_id` later, it doesn't know which one you mean.

In SQL this would just be an ambiguous column error. In PySpark it's the same — but you have to resolve it yourself by explicitly dropping the duplicate.

```python
df_joined = df_clean \
    .join(df_pii, df_clean.meter_id == df_pii.MeterID, "left") \
    .drop(df_clean.meter_id)    # ← drop the copy from df_clean
```

**Use `df_name.column_name` when dropping, not just the column name as a string.**

```python
# Safe — drops only the meter_id from df_clean
.drop(df_clean.meter_id)

# Risky — drops ALL columns named meter_id (both copies)
.drop("meter_id")
```

---

## Always Check Row Counts After a Join

This is the PySpark equivalent of checking your SQL result set:

```python
print(f"Before join: {df_clean.count()}")
print(f"After join:  {df_joined.count()}")
```

For a LEFT JOIN: the after count should be **equal to or greater than** the before count.
- Equal = every row matched exactly once (ideal)
- Greater = some rows matched multiple times (duplicate keys on the right side — investigate)
- Less = impossible with a left join (if this happens, something went wrong)

---

## Left anti join

Left anti join returns rows from the left table that don't exist in the right table. It's the `NOT IN` of PySpark joins.

```python
# SQL
SELECT * FROM df_all WHERE User_ID NOT IN (SELECT User_ID FROM df_clean)

# PySpark — same result
df_result = df_all.join(df_clean, "User_ID", "left_anti")
```

SantéFlux use case: after cleaning out frozen readings, find users whose every reading was frozen — nothing made it through.

```python
df_all_users = df_hashed.select("hashed_id").distinct()
df_clean_users = df_clean.select("hashed_id").distinct()

df_fully_frozen = df_all_users.join(df_clean_users, "hashed_id", "left_anti")
```

If User 2045 had 150 readings and all 150 were frozen, they show up here. Users with at least one clean reading don't.

| Join type | Returns |
|-----------|---------|
| inner | matching rows from both sides |
| left | all left rows, null where no right match |
| left_anti | left rows with no match on the right side |

---

## Summary

| SQL | PySpark |
|-----|---------|
| `FROM a LEFT JOIN b ON a.id = b.id` | `a.join(b, a.id == b.id, "left")` |
| Multiple JOINs | Chain `.join()` calls |
| Ambiguous columns | Drop duplicates explicitly with `.drop(df.column)` |
| Check row counts | `df.count()` before and after |
