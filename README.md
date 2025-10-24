# Data Quality & Validation Framework (Pro Edition)

This repository provides a **production-style** data quality framework with
config-driven rules, reusable validation modules, orchestration, CI, and reporting.
It ships with large synthetic datasets (customers & transactions) to demo scale.

## Highlights
- ~50k customers + ~300k transactions partitioned by `ingest_date`
- **Config-driven** rule definitions (YAML/JSON) for *schema*, *nulls*, *uniqueness*,
  *referential integrity*, *ranges*, *regex*, *custom Python checks*
- **Validation Engine** (Python) with pluggable rules & report writers (JSON/CSV)
- **Airflow DAG** to schedule end-to-end DQ on partitions
- **Redshift** DDL for demo schema; sample SQL queries for KPI rollups
- **Power BI** notes + KPI definitions and incremental refresh guidance
- **CI** (flake8 + pytest) covering rule loaders and core validators
- **Docs**: kickoff notes, framework design, monitoring, runbooks

> Replace placeholder values (buckets, roles, connections) before deployment.
