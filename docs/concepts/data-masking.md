# Data Masking

## You've Seen This Your Whole Life

Look at an ATM receipt. Your card number doesn't show as `4567 8901 2345 6789`. It shows as `XXXX XXXX XXXX 6789`.

That's data masking. The real value exists in the database — but what gets displayed or stored in a report is partially hidden so anyone who accidentally sees it can't misuse it.

**Indian examples you'll instantly recognise:**

| Real Value | Masked Version | Where You See This |
|------------|---------------|-------------------|
| `4567 8901 2345 6789` | `XXXX XXXX XXXX 6789` | ATM receipts, bank statements |
| `9876 5432 1098` (Aadhaar) | `XXXX XXXX 1098` | DigiLocker, UIDAI documents |
| `ABCDE1234F` (PAN) | `XXXXXXX34F` | Form 26AS, ITR documents |
| `9876543210` (mobile) | `98XXXXXX10` | OTP screens, delivery apps |

UIDAI actually **legally mandates** Aadhaar masking — you are not allowed to display a full Aadhaar number on any printed or digital document. So data masking isn't just good practice, sometimes it's the law.

---

## Masking vs Hashing — What's the Difference?

We covered hashing in the [PII Handling](pii-handling.md) page. Here's how they compare:

| | Hashing | Masking |
|--|---------|---------|
| What happens | Full value replaced with an unreadable code | Part of the value replaced with X |
| Can you read it? | No — completely unrecognisable | Partially — last 4 digits still visible |
| Reversible? | No | Original is still stored safely elsewhere |
| Use case | Analytics pipelines — you never need the real value | Reports and documents — humans need to partially verify it |

**When to use which:**
- **Hash** the email address in your billing pipeline — analysts don't need real emails to do their job
- **Mask** the Aadhaar on a printed report — the person needs to confirm it's their number, but you don't expose all 12 digits

---

## How Masking Works in PySpark

You use `regexp_replace()` — find a pattern in the text, replace it with X's.

**Mask first 8 digits of an Aadhaar number:**

```python
from pyspark.sql.functions import regexp_replace

df_masked = df.withColumn(
    "aadhaar_masked",
    regexp_replace(df.aadhaar, r'^[0-9]{8}', 'XXXXXXXX')
)
```

| Before | After |
|--------|-------|
| `987654321098` | `XXXXXXXX1098` |

**Mask middle digits of a mobile number:**

```python
df_masked = df.withColumn(
    "mobile_masked",
    regexp_replace(df.mobile, r'([0-9]{2})[0-9]{6}([0-9]{2})', '$1XXXXXX$2')
)
```

| Before | After |
|--------|-------|
| `9876543210` | `98XXXXXX10` |

Don't worry about the exact regex pattern yet — that's covered in the [Regex](regex.md) page. The point here is that `regexp_replace` is the PySpark function that does the masking.

---

## The Power Query Equivalent

In Power Query you've used **Replace Values** — type the exact value you want to find, type what to replace it with. Masking in PySpark is the same idea, except instead of an exact value you give it a pattern (a regex). That means it works across millions of rows with different values, not just one specific value.

---

## Why This Matters in a Pipeline

In a real data pipeline, raw data comes in with full PII — full Aadhaar, full mobile numbers, full account numbers. By the time that data reaches a report or a downstream team, masking ensures:

- Analysts can work with the data without seeing full sensitive values
- If a report is accidentally shared, the damage is limited
- You stay compliant with regulations like UIDAI's Aadhaar masking mandate

The rule is simple: **mask at the point where data leaves the secure pipeline and enters a report or shared table.**
