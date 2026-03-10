# PySpark Learning Notes

Notes from a structured PySpark bootcamp on Databricks — covering real case studies, reusable patterns, and concept explanations.

**Audience:** Anyone learning PySpark, Databricks, and data engineering fundamentals.
**Platform:** Databricks (Serverless compute) · Unity Catalog · Delta Lake

---

## What You'll Find Here

### Concepts
Standalone explanations of core ideas — written to be understood once and revisited quickly.

| Topic | What It Covers |
|-------|---------------|
| [Unity Catalog](concepts/unity-catalog.md) | Catalog → Schema → Volume hierarchy, how Databricks organises data |
| [Data Ingestion](concepts/data-ingestion.md) | Reading CSV and JSON files into PySpark DataFrames |
| [Schema Definition](concepts/schema-definition.md) | `inferSchema` vs explicit `StructType` — when to use which |
| [Data Quality & Quarantine](concepts/data-quality.md) | Filtering bad records, quarantine pattern, validation |
| [PII Handling](concepts/pii-handling.md) | What PII is, SHA-256 hashing with `sha2()`, privacy patterns |
| [Joins in PySpark](concepts/joins.md) | Join types, chaining joins, handling ambiguous columns |
| [Business Logic](concepts/business-logic.md) | `when().otherwise()`, timestamp functions, rounding |
| [Wildcards](concepts/wildcards.md) | Pattern matching with `%`, `*`, and `like()` — in SQL, file paths, and filters |
| [Regex](concepts/regex.md) | What regular expressions are, how to read them, and why pipelines need them — with Aadhaar, PAN, and mobile examples |
| [Data Masking](concepts/data-masking.md) | Hiding sensitive data like Aadhaar and PAN numbers — masking vs hashing, and how to do it in PySpark |

### Case Studies
End-to-end projects worked through in the bootcamp — what was built, what decisions were made, and what was learned.

| Case Study | Domain | Key Skills |
|------------|--------|------------|
| [Green Grid](case-studies/green-grid.md) | Smart Energy | Ingestion · Quarantine · PII Hashing · Joins · Peak Pricing · Delta Table |
| [SantéFlux](case-studies/santeflux.md) | Healthcare | *(In progress)* |

---

## How to Use These Notes

- **Revising a concept?** Go to Concepts — each page is self-contained.
- **Reviewing a project end-to-end?** Go to Case Studies — full pipeline walkthrough with code.
- **Looking for a specific function?** Use the search bar at the top.
