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

## Fixed salt vs random salt

This is the decision that determines whether your hashed data can be joined across tables.

### Fixed salt (one constant for all rows)

One secret string. Applied the same way to every row. Same input always produces the same hash.

```python
SALT = "myapp_secret_2025"

df = df.withColumn(
    "hashed_id",
    sha2(concat(col("User_ID").cast("string"), lit(SALT)), 256)
)
```

User 1000 hashes to the same value every time this runs — whether in today's pipeline or next month's. That means you can JOIN across DataFrames, match records across tables, and track the same person over time without ever storing their real ID.

**The downside:** if someone discovers your salt, they can rebuild the rainbow table for your exact salt and crack every record at once. One breach, everything exposed.

---

### Random salt (unique salt per row)

A fresh random value is generated for every row at hash time. The salt is stored alongside the hash in the table.

```python
from pyspark.sql.functions import sha2, concat, col, lit, expr

# Generate a random UUID as the salt for each row
df = df.withColumn("salt", expr("uuid()"))

df = df.withColumn(
    "hashed_id",
    sha2(concat(col("User_ID").cast("string"), col("salt")), 256)
)
```

Even if two users have the same User_ID (which shouldn't happen, but if it did), their hashes would differ because each row has its own salt. A breach of one row doesn't help an attacker crack any other row.

**The downside:** User 1000 hashes to a different value every time, because the salt is different every time. You cannot JOIN this hash across tables. You cannot track the same person across pipelines unless you carry the salt forward and re-hash consistently.

---

### The core trade-off

| | Fixed salt | Random salt |
|--|------------|-------------|
| Same input → same hash? | Yes | No |
| Can you JOIN across tables? | Yes | No |
| One breach exposes all records? | Yes | No — each row is independent |
| Where is the salt stored? | In your code / Key Vault | In the table, alongside the hash |
| Typical use case | Data pipelines, cross-table joins | Password storage, user credentials |

---

### How to decide

**Use fixed salt when:**
- You need to join hashed IDs across two DataFrames
- The same entity (person, account) appears in multiple tables and needs to match
- You're building a pipeline where consistency across runs matters

**Use random salt when:**
- You're storing passwords or authentication tokens (you verify by re-hashing with the stored salt, you never need to join)
- Each record is self-contained and never needs to match to another row
- Maximum security per record matters more than joinability

For most data engineering pipelines — fixed salt. For authentication systems — random salt.

SantéFlux uses fixed salt because the Health Trends Report joins patient data across Paris, Lyon, and Marseille. Random salt would break every cross-city join.

---

## Types of salt (reference)

| Type | How it works | Best for |
|------|-------------|----------|
| Fixed / static | One secret for all records, stored in config or Key Vault | Pipelines, cross-table joins |
| Random / per-record | Fresh random value per row, stored in the table | Passwords, credentials, one-way verification |
| Per-team | Each team holds their own secret | Multi-team setups where teams never need to join across each other |
| Time-based / rotating | Salt changes on a schedule | Regulatory environments requiring periodic re-hashing |

For SantéFlux on Databricks Free Edition, fixed salt is the call. The pipeline joins data across Paris, Lyon, and Marseille — per-team or random salt would break those joins. One shared secret keeps everything connected.

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

## col(), lit() and concat()

These three always show up together in the hashing step.

`col("User_ID")` tells Spark to pull a column from the DataFrame. Different value per row. `lit(SALT)` says treat this as a fixed constant — same value every row. Without `lit()`, Spark can't tell if your string is a column name or just text. `concat()` joins them before hashing runs.

```python
sha2(concat(col("User_ID").cast("string"), lit(SALT)), 256)
```

Read it inside out: get the ID as text → attach the salt → hash the result.

SQL equivalent: `SHA2(CONCAT(CAST(User_ID AS STRING), 'SanteFlux_Salt_2025'), 256)`

---

## Hash on the ID, never the name

Full_Name fails as a hash key. Names aren't unique — two users can share one. Hash `Full_Name` and groupBy that hash, and their records land in the same bucket. A frozen sensor for User 2045 goes undetected because User 3891, same name, has normal fluctuating readings. The min and max spread apart and nothing gets flagged.

User_ID is unique by design. One ID, one person, consistent hash across the whole pipeline.

| | Full_Name | User_ID |
|--|-----------|---------|
| Unique? | No | Yes |
| Safe to group by? | No | Yes |
| Changes over time? | Yes (name changes) | No |

Don't combine name and ID in the hash either. If the name changes, the hash changes, and you lose the ability to track the same person across time. The ID alone is enough.

Mask the name for display. Hash the ID for computation. They're solving different problems.

---

## Shared salt vs per-team salt

Chloe's question: one company-wide salt for joins, or one per team to contain a breach?

For SantéFlux: shared salt. The Health Trends Report joins Paris, Lyon and Marseille. Per-team salts mean User 2045 hashes differently in each city's data — the join breaks and the report can't be built.

Shared salt means a single breach exposes everything. That's the real cost. Production answer: Azure Key Vault, centrally managed, rotatable, audited. On Databricks Free Edition, a hardcoded variable is the starting point.

```python
SALT = "SanteFlux_Salt_2025"  # move to Key Vault in production
```

---

## Where this sits in the pipeline

```
Raw ingest → mask names → hash + salt IDs → aggregate → report
```

Names get masked at ingestion. IDs get hashed before anything downstream sees them. By the time data hits a report, there's no PII left — just city-level averages Moreau can approve.

See [Data Masking](data-masking.md) for the name masking code and [PII Handling](pii-handling.md) for the full GDPR pipeline setup.
