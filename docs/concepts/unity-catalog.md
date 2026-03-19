# Unity Catalog

## Think of it like a filing cabinet

Imagine a big filing cabinet at work.

- Cabinet = the Catalog
- Each drawer = a Schema
- Each folder inside a drawer = a Table or a Volume (for raw files)

In Databricks, this is the Unity Catalog. It's the organised structure for storing all your data and files.

```
Catalog  (the whole cabinet)
  └── Schema  (one drawer)
        └── Tables, Views, Volumes  (folders inside that drawer)
```

---

## How you reference things

Always three parts, dot-separated:

```
catalog.schema.table_name
```

Example from Green Grid:
```
workspace.green_grid_case_study.Silver_SmartBill
```

Read that as: "In the workspace cabinet, open the green_grid_case_study drawer, find the Silver_SmartBill folder."

---

## What is a Volume?

A Volume is a folder for raw files — CSVs, JSONs, Excel files, etc. It is not a table. You can't query it with SQL. It's just file storage, same as the Files section in a Fabric Lakehouse.

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

Same structure as the catalog hierarchy. Same logic as a file path on your laptop.

---

## Managed vs External Volume

| | Managed | External |
|--|---------|----------|
| Where files live | Databricks looks after it | Your own storage (ADLS, S3) |
| When to use | Learning, most projects | Production when you control the storage |

In the bootcamp we use managed. Databricks handles everything, you just upload files and use them.

---

## If you know Microsoft Fabric

| Databricks | Microsoft Fabric |
|------------|-----------------|
| Catalog | Workspace |
| Schema | Lakehouse |
| Volume | Files section in Lakehouse |
| Managed Table | Delta Table |

Same idea, different names.

---

## Three commands you'll use constantly

```python
# Read a file from a volume
spark.read.csv("/Volumes/catalog/schema/volume/file.csv")

# Write data as a table
df.write.saveAsTable("catalog.schema.table_name")

# Read a table back (like SELECT * FROM table)
spark.read.table("catalog.schema.table_name")
```
