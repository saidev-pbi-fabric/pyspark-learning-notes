# Data Quality & Quarantine

## The Real-World Problem

Imagine a smart meter on someone's wall. It sends a reading every hour. But sometimes the sensor glitches — it sends `-500 kWh` or `9999 kWh`. Those numbers aren't real. If you use them to calculate a bill, the customer gets charged nonsense.

You can't control what comes in from the real world. You CAN control what you let through into your pipeline.

---

## The Quarantine Pattern — The Bouncer Analogy

Think of a nightclub bouncer. They check everyone at the door. People who don't meet the rules don't get in — but they're not disappeared. They go on a list (for the venue's records). The people inside are all legitimate.

That's the quarantine pattern:

```
All incoming records
      │
      ├──  Bad records  →  Quarantine table  (kept, logged, visible)
      │
      └──  Good records  →  Continue the pipeline
```

The bad records don't get deleted. They get written to a separate table so you can audit them later. This is important — someone might need to investigate why 200 records were rejected.

---

## In SQL Terms

This is equivalent to:

```sql
-- Bad records → quarantine table
INSERT INTO quarantine_readings
SELECT * FROM meter_readings
WHERE kwh < 0 OR kwh >= 100;

-- Good records → clean table
INSERT INTO clean_readings
SELECT * FROM meter_readings
WHERE kwh IS NOT NULL
  AND kwh >= 0
  AND kwh < 100;
```

In PySpark, you do the same thing with `.filter()`:

---

## The PySpark Code

```python
# Step 1: Capture the bad records
df_quarantine = df_readings.filter(
    (df_readings.kwh < 0) | (df_readings.kwh >= 100)
)

# Step 2: Write them to a quarantine table (for audit)
df_quarantine.write.mode("overwrite").saveAsTable(
    "workspace.green_grid_case_study.Quarantine_Readings"
)
```

```python
# Step 3: Keep only the clean records
df_clean = df_readings.filter(
    df_readings.kwh.isNotNull() &
    (df_readings.kwh >= 0) &
    (df_readings.kwh < 100)
)
```

**Important — always wrap each condition in parentheses `()`.**
Without them, PySpark's operator order gets confused. This is one of those "just always do it" rules.

---

## The Null Trap

Here's something that surprises people:

```python
# This does NOT filter out nulls
df.filter(df.kwh >= 0)
```

In SQL: `WHERE kwh >= 0` also silently excludes nulls. It's the same behaviour.

A null compared to a number returns null — not True, not False. So the row just disappears silently.

If you want to be explicit about what you're keeping, always add `.isNotNull()`:

```python
df.filter(
    df_readings.kwh.isNotNull() &   # explicitly keep only non-nulls
    (df_readings.kwh >= 0) &
    (df_readings.kwh < 100)
)
```

---

## Always Validate Your Writes

After writing any table, read it back and check the row count matches. This catches cases where the write failed silently.

```python
# Read the table back
df_test = spark.read.table("workspace.green_grid_case_study.Quarantine_Readings")

# Count should match — if False, something went wrong
df_quarantine.count() == df_test.count()
```

This is the equivalent of:
```sql
SELECT COUNT(*) FROM quarantine_readings  -- should match what you inserted
```

Make it a habit. It takes 2 seconds and saves you from debugging a corrupt table later.

---

## Filter Quick Reference

| What you want | PySpark |
|---------------|---------|
| Remove nulls | `df.filter(df.col.isNotNull())` |
| Keep only nulls | `df.filter(df.col.isNull())` |
| Value in range | `df.filter((df.col >= 0) & (df.col < 100))` |
| Either condition | `df.filter((df.col < 0) \| (df.col >= 100))` |

`&` = AND, `|` = OR — same logic as SQL, just written differently.
