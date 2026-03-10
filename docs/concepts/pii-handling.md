# PII Handling

## What Is PII?

**PII = Personally Identifiable Information** — any data that could tell you who a specific real person is.

Examples:
- Email address
- Full name
- Phone number
- Home address
- National ID number
- Date of birth (especially with a name or postcode)

Think of it as: *"If I could use this piece of data to find a specific person, it's PII."*

---

## Why Does It Matter?

Laws like GDPR (UK/EU) say you must protect this data. Storing someone's email address in plain text in a database table that 50 analysts can query is a problem. If that table gets breached — or even just accidentally shared — real people's private information is exposed.

It's not just a technical problem. It's a legal and ethical one.

---

## The Solution — Blur the Face, Not the Person

Think of CCTV footage. If you blur someone's face in a video:
- You can still count how many people walked through a door
- You can still see what they were wearing (other data points)
- But you cannot identify who the person is

That's exactly what **hashing** does to an email address.

You replace `john.smith@gmail.com` with a fixed-length code:

```
john.smith@gmail.com
      ↓ SHA-256 hash
b94f6f125c79e3a5ffaa826f584c10d52ada669e6762051b826b55776d05a3d
```

The code (hash) is always 64 characters. It's always the same for the same input. But you **cannot work backwards** from the hash to get the original email. It's a one-way street.

For analytics purposes, this is fine — you can still count unique customers, group by customer, and track behaviour. You just can't see their real email.

---

## How to Do It in PySpark

```python
from pyspark.sql.functions import sha2

df_hashed = df_joined.withColumn(
    "CustomerHashed",          # name of the new hashed column
    sha2(df_joined.CustomerEmail, 256)   # 256 = SHA-256 algorithm
).drop(df_joined.CustomerEmail)          # immediately drop the real email
```

Two things happening here:
1. **`withColumn`** — creates a new column called `CustomerHashed` with the hash value
2. **`.drop`** — removes the original `CustomerEmail` column in the same step

**Always drop the raw PII column immediately.** Don't carry it forward "just in case". If it reaches the silver table, it's a problem.

---

## SHA-256 — Do I Need to Understand the Maths?

No. All you need to know:

| Property | What it means practically |
|----------|--------------------------|
| One-way | You cannot get the email back from the hash |
| Deterministic | Same email always = same hash (so you can still JOIN on it) |
| No collisions (in practice) | Two different emails will not produce the same hash |
| 256-bit output | Always 64 characters long |

SHA-256 is the industry standard for this use case. It's built into PySpark — no libraries to install.

---

## Hashing vs Encryption — What's the Difference?

People sometimes confuse these. Simple version:

| | Hashing | Encryption |
|--|---------|-----------|
| Can you reverse it? | **No** | Yes, with the right key |
| Use case | Analytics — you don't need the real value | When you need to recover the original later |
| Key management needed? | No | Yes |

For a billing pipeline where you just need to identify unique customers — hashing is correct. You don't need to email them from the pipeline, so you don't need the real address.

---

## The Pattern in One Rule

> Hash the PII. Drop the original. Never let raw PII reach the silver layer.
