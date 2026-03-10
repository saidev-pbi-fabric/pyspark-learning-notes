# PySpark Learning Notes

PySpark and Databricks concept notes, patterns, and case study walkthroughs from a structured bootcamp.

Built with [MkDocs Material](https://squidfunk.github.io/mkdocs-material/).

## Viewing Locally

```bash
pip install -r requirements.txt
mkdocs serve
```

Then open `http://localhost:8000` in your browser.

## Structure

```
docs/
├── index.md               # Home page
├── concepts/              # Standalone concept explanations
│   ├── unity-catalog.md
│   ├── data-ingestion.md
│   ├── schema-definition.md
│   ├── data-quality.md
│   ├── pii-handling.md
│   ├── joins.md
│   └── business-logic.md
└── case-studies/          # End-to-end project walkthroughs
    ├── green-grid.md
    └── santeflux.md
```
