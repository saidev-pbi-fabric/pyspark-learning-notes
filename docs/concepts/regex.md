# Regex (Regular Expressions)

## What Is Regex?

Regex = Regular Expression. Forget the name — it sounds scarier than it is.

**Regex is a way of writing a pattern so a computer can find, validate, or replace text that matches that pattern.**

Think of it like this. If someone asked you to go through 10,000 forms and circle every mobile number — how would you know what's a mobile number? You'd look for something that:

- Is 10 digits long
- Starts with 6, 7, 8, or 9
- Has no letters in it

You just described a pattern in your head. Regex is how you write that same pattern in code so the computer does the job in seconds across millions of rows.

---

## Reading a Regex Pattern — Indian Examples

### Example 1 — Indian Mobile Number

**The rule:** 10 digits, must start with 6, 7, 8, or 9.

```
Plain English:   "starts with 6/7/8/9, then 9 more digits"
Regex:            ^[6-9][0-9]{9}$
```

Breaking it down piece by piece:

| Piece | Meaning |
|-------|---------|
| `^` | Start of the text — nothing before this |
| `[6-9]` | One digit that is 6, 7, 8, or 9 |
| `[0-9]` | Any digit from 0 to 9 |
| `{9}` | Exactly 9 times (applies to the thing before it) |
| `$` | End of the text — nothing after this |

So the full pattern says: *"Start, then one digit between 6 and 9, then exactly 9 more digits, then end."*

```
9876543210  →  ✅ valid
1234567890  →  ❌ starts with 1
98765       →  ❌ only 5 digits
98765432101 →  ❌ 11 digits
```

---

### Example 2 — PAN Card Number

**The rule:** 5 uppercase letters, then 4 digits, then 1 uppercase letter → `ABCDE1234F`

```
Plain English:   "5 uppercase letters, 4 digits, 1 uppercase letter"
Regex:            ^[A-Z]{5}[0-9]{4}[A-Z]{1}$
```

| Piece | Meaning |
|-------|---------|
| `[A-Z]{5}` | Exactly 5 uppercase letters (A through Z) |
| `[0-9]{4}` | Exactly 4 digits |
| `[A-Z]{1}` | Exactly 1 uppercase letter |

```
ABCDE1234F  →  ✅ valid PAN
ABCDE123F   →  ❌ only 3 digits
abcde1234f  →  ❌ lowercase not allowed
```

---

### Example 3 — Aadhaar Number

**The rule:** Exactly 12 digits.

```
Regex:   ^[0-9]{12}$
```

```
987654321098   →  ✅ valid
98765432109    →  ❌ only 11 digits
9876543210AB   →  ❌ contains letters
```

---

## The Symbols You'll See Most Often

You don't need to memorise all of regex. These 6 building blocks cover 90% of what you'll encounter:

| Symbol | Meaning | Example |
|--------|---------|---------|
| `[0-9]` | Any single digit | `[0-9]` matches `4` |
| `[A-Z]` | Any single uppercase letter | `[A-Z]` matches `B` |
| `[a-z]` | Any single lowercase letter | `[a-z]` matches `b` |
| `{n}` | Exactly n times | `[0-9]{4}` matches `1234` |
| `^` | Start of text | `^9` means must start with 9 |
| `$` | End of text | `0$` means must end with 0 |

---

## Why Do We Need Regex in Data Engineering?

Three jobs regex does in every real pipeline:

### 1. Validation — Is this data actually valid?

Before processing a million rows, check that the mobile number column actually contains mobile numbers. Catch junk data before it corrupts your results.

```python
# In PySpark — keep only rows with valid Indian mobile numbers
df_valid = df.filter(df.mobile.rlike(r'^[6-9][0-9]{9}$'))
```

`rlike()` is PySpark's "does this match the regex pattern?" function.

```python
# Keep only rows with valid PAN numbers
df_valid_pan = df.filter(df.pan.rlike(r'^[A-Z]{5}[0-9]{4}[A-Z]{1}$'))
```

In SQL terms, this is equivalent to:
```sql
WHERE mobile REGEXP '^[6-9][0-9]{9}$'
```

### 2. Extraction — Pull a pattern out of messy text

Imagine a freeform notes column: `"Customer called, Aadhaar 987654321098 submitted for KYC"`. Regex can pull the 12-digit Aadhaar number out of that sentence automatically.

```python
from pyspark.sql.functions import regexp_extract

df = df.withColumn(
    "aadhaar_extracted",
    regexp_extract(df.notes, r'[0-9]{12}', 0)
)
```

`regexp_extract` finds the first match of the pattern in the text and returns it as a new column.

### 3. Masking — Hide parts of sensitive values

```python
from pyspark.sql.functions import regexp_replace

# Mask first 8 digits of Aadhaar
df_masked = df.withColumn(
    "aadhaar_masked",
    regexp_replace(df.aadhaar, r'^[0-9]{8}', 'XXXXXXXX')
)
# 987654321098 → XXXXXXXX1098
```

`regexp_replace` finds the pattern and replaces it — like Find & Replace in Excel, but working on a pattern across millions of rows instead of one exact value.

---

## The Power Query Comparison

In Power Query you've used **Text Filters** — "begins with", "contains", "ends with". Those are basic pattern checks done through a UI. Regex is the same idea written as a pattern in code — it's more powerful because you can describe complex rules like "exactly 12 digits" or "5 letters then 4 digits then 1 letter" that no dropdown menu can express.

---

## One Thing to Remember

Regex feels confusing at first because it looks like random symbols. The trick is to **read it left to right, one piece at a time.** Every symbol means something specific. Once you know the 6 building blocks above, you can read most patterns you'll encounter in the real world.

You don't need to memorise regex — you look it up when you need it. What matters is understanding *what it's for* and *where to use it* in a pipeline.
