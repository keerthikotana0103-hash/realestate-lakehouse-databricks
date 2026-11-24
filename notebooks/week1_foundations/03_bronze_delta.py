# Databricks notebook source
# MAGIC %md
# MAGIC # Day 4 · Bronze layer + Delta Lake fundamentals
# MAGIC
# MAGIC **Goal:** land raw data as Delta tables and *see* what Delta gives you: ACID writes, a transaction log,
# MAGIC time travel, schema enforcement and schema evolution.
# MAGIC
# MAGIC This is a **batch** load on purpose. In Week 2 you replace it with Auto Loader (incremental),
# MAGIC and that before/after comparison is a great interview story.
# MAGIC
# MAGIC 📖 Read: [What is Delta Lake](https://docs.databricks.com/aws/en/delta/) ·
# MAGIC [Table history / time travel](https://docs.databricks.com/aws/en/tables/history)

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

from pyspark.sql import functions as F


def with_ingest_meta(df):
    return (df.withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.col("_metadata.file_path")))


ref = LANDING["reference"]
reference_tables = {
    "properties": "properties.csv",
    "markets": "markets.csv",
    "offices": "offices.csv",
    "agents": "agents.csv",
    "mortgage_rates_weekly": "mortgage_rates_weekly.csv",
}
for table, file in reference_tables.items():
    df = spark.read.option("header", True).option("inferSchema", True).csv(f"{ref}/{file}")
    with_ingest_meta(df).write.mode("overwrite").saveAsTable(f"{CATALOG}.bronze.{table}")

with_ingest_meta(spark.read.json(LANDING["listing_events"])) \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{CATALOG}.bronze.listing_events")

with_ingest_meta(spark.read.option("header", True).option("inferSchema", True).csv(LANDING["sales"])) \
    .write.mode("overwrite").saveAsTable(f"{CATALOG}.bronze.sales")

with_ingest_meta(spark.read.option("header", True).option("inferSchema", True).csv(LANDING["agent_updates"])) \
    .write.mode("overwrite").saveAsTable(f"{CATALOG}.bronze.agent_updates")

display(spark.sql(f"SHOW TABLES IN {CATALOG}.bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta transaction log: every write is a versioned commit

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {CATALOG}.bronze.listing_events"))
display(spark.sql(f"DESCRIBE DETAIL {CATALOG}.bronze.listing_events"))   # format, location, numFiles, sizeInBytes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time travel: make a bad change, then recover

# COMMAND ----------

tbl = f"{CATALOG}.bronze.markets"
spark.sql(f"UPDATE {tbl} SET region = 'OOPS' WHERE metro = 'Austin'")      # simulate a bad deploy
display(spark.sql(f"SELECT metro, region FROM {tbl} WHERE metro = 'Austin'"))

prev = spark.sql(f"DESCRIBE HISTORY {tbl}").filter("operation != 'UPDATE'").agg(F.max("version")).first()[0]
display(spark.sql(f"SELECT metro, region FROM {tbl} VERSION AS OF {prev} WHERE metro = 'Austin'"))

spark.sql(f"RESTORE TABLE {tbl} TO VERSION AS OF {prev}")                  # undo it
display(spark.sql(f"SELECT DISTINCT metro, region FROM {tbl} ORDER BY region"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema enforcement vs schema evolution
# MAGIC Delta **rejects** writes whose schema doesn't match (protects downstream), unless you explicitly allow evolution.

# COMMAND ----------

extra = spark.table(f"{CATALOG}.bronze.offices").limit(1).withColumn("phone", F.lit("555-0100"))
try:
    extra.write.mode("append").saveAsTable(f"{CATALOG}.bronze.offices")
except Exception as e:
    print("Schema enforcement blocked the write ✅\n", str(e)[:300])

# COMMAND ----------

# Now evolve on purpose (then put it back the way it was)
before = spark.sql(f"DESCRIBE HISTORY {CATALOG}.bronze.offices").agg(F.max("version")).first()[0]
extra.write.mode("append").option("mergeSchema", True).saveAsTable(f"{CATALOG}.bronze.offices")
display(spark.table(f"{CATALOG}.bronze.offices"))
spark.sql(f"RESTORE TABLE {CATALOG}.bronze.offices TO VERSION AS OF {before}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notice the schema evolution already in the feed
# MAGIC From 2026-08-15 NEW events carry `hoa_fee` and `virtual_tour_url`. Land files through September with
# MAGIC `01_land_files`, rerun this notebook, and check the schema. In Week 2, Auto Loader handles this with
# MAGIC `cloudFiles.schemaEvolutionMode`.

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE {CATALOG}.bronze.listing_events"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Checkpoint / interview line
# MAGIC *"Bronze is append-only raw data in Delta with `_ingested_at` and `_source_file`, so any number in a dashboard
# MAGIC can be traced back to the exact file it came from. Delta's transaction log gave us time travel. I've used `RESTORE` to
# MAGIC roll back a bad update, and schema enforcement stops an upstream change from silently breaking downstream tables."*
