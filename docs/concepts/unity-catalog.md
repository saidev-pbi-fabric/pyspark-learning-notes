# Unity Catalog

## Think of It Like a Filing Cabinet

Imagine a big filing cabinet at work.

- The **cabinet itself** = the Catalog
- Each **drawer** in the cabinet = a Schema
- Each **folder** inside a drawer = a Table or a Volume (for raw files)

In Databricks, this is called the **Unity Catalog** — it's just the organised structure for storing all your data and files.

```
Catalog  (the whole cabinet)
  └── Schema  (one drawer)
        └── Tables, Views, Volumes  (folders inside that drawer)
```

---

## How You Reference Things

Just like a full address — you always write it as three parts:

```
catalog.schema.table_name
```

**Example from Green Grid:**
```
workspace.green_grid_case_study.Silver_SmartBill
```

Read that as: *"In the workspace cabinet, open the green_grid_case_study drawer, find the Silver_SmartBill folder."*

---

## What Is a Volume?

A Volume is just a folder for **raw files** — CSVs, JSONs, Excel files, etc. It is NOT a table. You can't query it with SQL. It's just file storage, like the Files section in a Fabric Lakehouse.

```
workspace
  └── green_grid_case_study
        └── raw_files  ← this is the Volume (a folder)
              ├── customer_pii.csv
              ├── meter_readings.json
              └── pricing_plans.csv
```

To read a file from a Volume, you use its path:
```
/Volumes/workspace/green_grid_case_study/raw_files/customer_pii.csv
```

Think of it as the file path on your laptop — just structured the same way as the catalog hierarchy.

---

## Managed vs External Volume — The Simple Version

| | Managed | External |
|--|---------|----------|
| Where files live | Databricks looks after it | Your own storage (ADLS, S3) |
| When to use | Learning, most projects | Production when you control the storage |

In the bootcamp we use **managed** — Databricks handles everything, you just upload files and use them.

---

## If You Know Microsoft Fabric...

| Databricks | Microsoft Fabric |
|------------|-----------------|
| Catalog | Workspace |
| Schema | Lakehouse |
| Volume | Files section in Lakehouse |
| Managed Table | Delta Table |

Same idea, different names.

---

## The Three Commands You'll Use Most

```python
# Read a file from a volume
spark.read.csv("/Volumes/catalog/schema/volume/file.csv")

# Write data as a table
df.write.saveAsTable("catalog.schema.table_name")

# Read a table back (like SELECT * FROM table)
spark.read.table("catalog.schema.table_name")
```
