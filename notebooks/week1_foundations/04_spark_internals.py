# Databricks notebook source
# MAGIC %md
# MAGIC # Day 5 · Spark internals you must be able to explain
# MAGIC
# MAGIC Lazy evaluation · transformations vs actions · narrow vs wide · shuffles · join strategies · AQE.
# MAGIC
# MAGIC On serverless compute the classic Spark UI is replaced by the **query profile**
# MAGIC (click "See performance" under a cell → open the query profile). Use it for every cell below.
# MAGIC
# MAGIC 📖 Read: [Optimization recommendations](https://docs.databricks.com/aws/en/optimizations/)

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

from pyspark.sql import functions as F

events = spark.table(f"{CATALOG}.bronze.listing_events")
properties = spark.table(f"{CATALOG}.bronze.properties")
markets = spark.table(f"{CATALOG}.bronze.markets")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Lazy evaluation: nothing runs until an action

# COMMAND ----------

plan_only = events.filter("event_type = 'PRICE_CHANGE'").select("listing_id", "list_price", "previous_price")
# ^ instant: just a plan.  The next line is an ACTION, so now Spark executes.
print(plan_only.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Narrow vs wide: look for `Exchange` (= shuffle) in the physical plan

# COMMAND ----------

narrow = events.filter("list_price > 0").withColumn("price_k", F.col("list_price") / 1000)
narrow.explain()          # no Exchange: each partition is processed independently

wide = events.groupBy("listing_id").agg(F.count("*").alias("n_events"))
wide.explain()            # Exchange hashpartitioning(listing_id): rows move across the cluster

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Join strategies
# MAGIC `markets` is tiny (40 rows), so Spark **broadcasts** it to every task: no shuffle of the big side.
# MAGIC Find `BroadcastHashJoin` in the plan. Two big tables would use `SortMergeJoin` (both sides shuffled).

# COMMAND ----------

joined = events.join(properties, "property_id").join(F.broadcast(markets), "zip_code")
joined.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Partitions

# COMMAND ----------

try:
    print("partitions:", events.rdd.getNumPartitions())
except Exception:
    # Serverless / Spark Connect has no RDD API: read task counts from the query profile instead
    print("RDD API not available on serverless; open the query profile to see tasks per stage")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Checkpoint: answer out loud, then write in `notes/interview_log.md`
# MAGIC 1. What triggers a shuffle and why is it expensive? (data moves over the network + disk spill)
# MAGIC 2. When does Spark choose broadcast join, and when is forcing it dangerous? (small side < threshold; forcing a big table → driver/executor OOM)
# MAGIC 3. What does **Adaptive Query Execution** do? (re-plans at runtime: coalesces shuffle partitions, switches to broadcast, splits skewed partitions)
# MAGIC 4. What is data skew, and how would it show up here? (e.g. one metro with 10x listings → one slow task; fix with AQE skew join, salting)
