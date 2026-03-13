# Hashing, Salting & Data Protection

## One-way means exactly that

You send a WhatsApp message. The receiver reads it, but WhatsApp itself can't. Now imagine there's no receiver — the message turns into a fixed string of characters with no way back. That's what hashing is.

You put a value in, you get a code out. You cannot reverse it.

| Input | SHA-256 Hash |
|-------|-------------|
| `1000` | `a866c6b58d...` |
| `1001` | `9d4b77cc1f...` |
| `1000` | `a866c6b58d...` |

Same input, same hash every time. Change one character anywhere and the entire output changes.

---

## MD5 vs SHA-256 — what you actually heard

"128-bit" and "256-bit" don't come from the same algorithm. That's where the confusion starts.

| Algorithm | Bits | Hash length | Status |
|-----------|------|-------------|--------|
| MD5 | 128-bit | 32 characters | Broken — don't use it |
| SHA-1 | 160-bit | 40 characters | Deprecated — don't use it |
| SHA-256 | 256-bit | 64 characters | Current standard |
| SHA-512 | 512-bit | 128 characters | High-security use cases |

MD5 is not part of SHA-2. It's an older algorithm from a different family, and it was cracked years ago. If you see MD5 in a pipeline today, that's worth a conversation.

SHA-256 is the default because it hasn't been broken in 30 years, runs fast enough for production, and every major regulation — GDPR, HIPAA, PCI-DSS — accepts it. Spark has it built in via `sha2(..., 256)`.

The "2" in SHA-2 is the generation number, not the bit size. SHA-256 is second-generation SHA with 256-bit output.

---

## Why hashing alone isn't enough

Because the same input always gives the same hash, a hacker doesn't need to crack anything.

User_ID is 1000. Before touching your database, the hacker hashes every number from 1 to 1,000,000 and stores the results. When they steal the data, they look up the hash and find 1000 in under a second. The work was done before the attack even started. That's a rainbow table attack.

Brute force is the slower version — no pre-built table, so they hash values one by one until something matches. Same result, just takes longer.

| | Brute force | Rainbow table |
|--|-------------|---------------|
| When does the work happen? | During the attack | Before the attack |
| Speed | Slow | Instant |
| Does salt stop it? | Slows it significantly | Stops it completely |

---

## What salting does

You attach a secret string to the value before hashing it.

Without salt: `Hash(1000)` produces something the hacker's table already has an entry for.

With salt: `Hash("1000santeflux_secret_2025")` produces something that table has never seen. To crack it, they'd need to rebuild the entire table using your exact salt — which they don't know.

Mobile number as an example: there are roughly 10 billion possible 10-digit numbers. That's achievable to pre-hash. Add a salt and now every number has to be combined with every possible secret string before hashing. The problem goes from big to not realistic.

---

## Types of salt

| Type | How it works | Best for |
|------|-------------|----------|
| Static salt | One secret for all records | Small teams, single pipeline |
| Per-record salt | Different salt per row, stored alongside | Maximum security |
| Per-team salt | Each team holds their own secret | Multi-team setups |
| Time-based salt | Salt rotates on a schedule | Regulatory environments |

For SantéFlux on Databricks Free Edition, static salt is the call. The pipeline joins data across Paris, Lyon, and Marseille — per-team salt would break those joins. One shared salt keeps everything connected.

---

## The code

```python
from pyspark.sql.functions import sha2, concat, col, lit

SALT = "santeflux_secret_2025"

df = df_vitals.withColumn(
    "hashed_id",
    sha2(concat(col("User_ID").cast("string"), lit(SALT)), 256)
)
```

What happens to User_ID 1000, step by step:

| Step | Value |
|------|-------|
| Original | `1000` |
| After adding salt | `1000santeflux_secret_2025` |
| After sha2(..., 256) | `f3c92a1148...` (64 characters) |

The `256` in `sha2(..., 256)` picks which SHA-2 variant Spark uses. Always use 256 unless you have a specific reason not to.

---

## Where this sits in the pipeline

```
Raw ingest → mask names → hash + salt IDs → aggregate → report
```

Names get masked at ingestion. IDs get hashed before anything downstream sees them. By the time data hits a report, there's no PII left — just city-level averages Moreau can approve.

See [Data Masking](data-masking.md) for the name masking code and [PII Handling](pii-handling.md) for the full GDPR pipeline setup.
