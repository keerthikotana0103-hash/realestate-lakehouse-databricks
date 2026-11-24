# Databricks notebook source
# MAGIC %md
# MAGIC # Day 3 · Explore the raw feed with PySpark
# MAGIC
# MAGIC **Goal:** get fluent with DataFrames and *find the data problems* before you build anything.
# MAGIC A data engineer who profiles first is a data engineer who doesn't get paged at 2am.
# MAGIC
# MAGIC 📖 Read: [PySpark overview](https://spark.apache.org/docs/latest/api/python/index.html)

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

from pyspark.sql import functions as F

events = (spark.read.json(LANDING["listing_events"])
          .withColumn("_source_file", F.col("_metadata.file_path")))   # file-level lineage, free with every read
events.printSchema()
display(events.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. What kinds of events arrive?

# COMMAND ----------

display(events.groupBy("event_type", "status").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Profile data quality: these become your **expectations** in Week 2

# COMMAND ----------

dq = events.agg(
    F.count("*").alias("rows"),
    F.countDistinct("event_id").alias("distinct_event_ids"),
    F.sum(F.when(F.col("list_price").isNull(), 1).otherwise(0)).alias("null_price"),
    F.sum(F.when(F.col("list_price") <= 0, 1).otherwise(0)).alias("non_positive_price"),
    F.sum(F.when(F.col("property_id").isNull(), 1).otherwise(0)).alias("missing_property_id"),
)
display(dq.withColumn("duplicate_rows", F.col("rows") - F.col("distinct_event_ids")))

# COMMAND ----------

dupes = events.groupBy("event_id").count().filter("count > 1")
display(events.join(dupes.select("event_id"), "event_id").orderBy("event_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Sales: find the late-arriving rows and the corrections

# COMMAND ----------

sales = (spark.read.option("header", True).option("inferSchema", True).csv(LANDING["sales"])
         .withColumn("_source_file", F.col("_metadata.file_path")))
sales.printSchema()

late = sales.withColumn("lag_days", F.datediff(F.to_date("reported_at"), F.col("close_date"))).filter("lag_days > 0")
display(late.orderBy(F.desc("lag_days")))

display(sales.filter("record_version > 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Join to reference data: list price per sqft by metro

# COMMAND ----------

ref = LANDING["reference"]
properties = spark.read.option("header", True).option("inferSchema", True).csv(f"{ref}/properties.csv")
markets = spark.read.option("header", True).option("inferSchema", True).csv(f"{ref}/markets.csv")

new_listings = events.filter("event_type = 'NEW' AND list_price > 0")
by_metro = (new_listings.join(properties, "property_id")
            .join(markets, "zip_code")
            .groupBy("metro")
            .agg(F.count("*").alias("new_listings"),
                 F.percentile_approx("list_price", 0.5).alias("median_list_price"),
                 F.round(F.avg(F.col("list_price") / F.col("sqft")), 0).alias("avg_price_per_sqft"))
            .orderBy(F.desc("median_list_price")))
display(by_metro)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Checkpoint: write these in `notes/interview_log.md`
# MAGIC 1. How many duplicates / bad prices / missing property ids did you find?
# MAGIC 2. Why is `inferSchema` fine for exploring but **not** for production? (cost of an extra pass, type drift between files → use explicit schemas or Auto Loader schema hints)
# MAGIC 3. What is `_metadata.file_path` and why keep it in bronze? (traceability back to the exact source file)
