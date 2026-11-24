# Databricks notebook source
# MAGIC %md
# MAGIC # Day 1-2 · Setup: Unity Catalog objects
# MAGIC
# MAGIC **Goal:** create the catalog, the medallion schemas and a volume that acts as our "landing zone"
# MAGIC (where the MLS vendor would drop files).
# MAGIC
# MAGIC Unity Catalog hierarchy: **metastore → catalog → schema → table / view / volume / function**
# MAGIC
# MAGIC | Schema | Purpose |
# MAGIC |---|---|
# MAGIC | `raw` | volume `landing` holding the raw files exactly as received |
# MAGIC | `bronze` | raw data as Delta tables + ingestion metadata, no business logic |
# MAGIC | `silver` | cleaned, deduplicated, typed, conformed (one row per entity / event) |
# MAGIC | `gold` | star schema + aggregates for BI |
# MAGIC
# MAGIC 📖 Read: [Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/) ·
# MAGIC [Volumes](https://docs.databricks.com/aws/en/volumes/)

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
except Exception as e:
    raise RuntimeError(
        f"Could not create catalog '{CATALOG}'. Edit notebooks/_config.py and set CATALOG = 'workspace', then rerun.\n{e}"
    )

for s in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.landing COMMENT 'Landing zone for MLS feed files'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add documentation as you go (interviewers love governed, documented data)

# COMMAND ----------

spark.sql(f"COMMENT ON SCHEMA {CATALOG}.bronze IS 'Raw MLS feed as Delta, append-only, with ingestion metadata'")
spark.sql(f"COMMENT ON SCHEMA {CATALOG}.silver IS 'Cleaned, deduplicated, conformed listing / sale / agent data'")
spark.sql(f"COMMENT ON SCHEMA {CATALOG}.gold IS 'Star schema and market KPIs for BI'")

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))
display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.raw"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Checkpoint
# MAGIC - Open **Catalog Explorer** and find your catalog, schemas and volume.
# MAGIC - Interview line: *"I organised the lakehouse in Unity Catalog with one schema per medallion layer, and a volume as the governed landing zone, so access, lineage and audit all come from one place."*
