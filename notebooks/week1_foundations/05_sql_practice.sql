-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Day 6 · SQL practice on the raw MLS feed
-- MAGIC
-- MAGIC Ten questions a real estate analyst or BI engineer actually gets asked. Q1 to Q3 are solved; solve the rest
-- MAGIC yourself, then compare with a friend or with Genie in Week 3.
-- MAGIC Change the catalog below if you set `CATALOG = 'workspace'` in `_config`.

-- COMMAND ----------

USE CATALOG realestate;
USE SCHEMA bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Q1 (solved) · Current status of every listing: latest event wins (`ROW_NUMBER`)

-- COMMAND ----------

WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY listing_id ORDER BY event_ts DESC, event_id DESC) AS rn
  FROM listing_events
  WHERE listing_id IS NOT NULL
)
SELECT status, COUNT(*) AS listings
FROM ranked
WHERE rn = 1
GROUP BY status
ORDER BY listings DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Q2 (solved) · Median list price and price per sqft by metro

-- COMMAND ----------

SELECT m.metro,
       COUNT(*)                                       AS new_listings,
       PERCENTILE_APPROX(e.list_price, 0.5)           AS median_list_price,
       ROUND(AVG(e.list_price / p.sqft), 0)           AS avg_price_per_sqft
FROM listing_events e
JOIN properties p ON e.property_id = p.property_id
JOIN markets    m ON p.zip_code   = m.zip_code
WHERE e.event_type = 'NEW' AND e.list_price > 0
GROUP BY m.metro
ORDER BY median_list_price DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Q3 (solved) · Size of every price cut, and the share of listings that had one (`LAG`)

-- COMMAND ----------

WITH prices AS (
  SELECT DISTINCT event_id, listing_id, event_ts, list_price
  FROM listing_events
  WHERE list_price > 0 AND event_type IN ('NEW', 'PRICE_CHANGE')
),
changes AS (
  SELECT listing_id, event_ts, list_price,
         LAG(list_price) OVER (PARTITION BY listing_id ORDER BY event_ts) AS prev_price
  FROM prices
)
SELECT COUNT(DISTINCT listing_id)                                              AS listings_with_cut,
       ROUND(AVG((prev_price - list_price) / prev_price) * 100, 2)             AS avg_cut_pct,
       ROUND(COUNT(DISTINCT listing_id) * 100.0 /
             (SELECT COUNT(DISTINCT listing_id) FROM listing_events), 1)        AS pct_of_all_listings
FROM changes
WHERE prev_price IS NOT NULL AND list_price < prev_price;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Your turn
-- MAGIC **Q4** Days on market (DOM) for every sold listing: `list_date` of NEW → `close_date` of the sale. Median DOM by metro?
-- MAGIC
-- MAGIC **Q5** List-to-sale ratio by metro (`sale_price / final list_price`). Which metro sells over asking?
-- MAGIC
-- MAGIC **Q6** Do listings that had a price cut take longer to sell? Compare median DOM with vs without a cut.
-- MAGIC
-- MAGIC **Q7** Top 10 agents by closed volume (sum of `sale_price`) counting **both** listing side and buyer side.
-- MAGIC
-- MAGIC **Q8** Weekly new listings and new pendings vs `rate_30yr_fixed`. Does the rate spike show up?
-- MAGIC
-- MAGIC **Q9** Sales: keep only the latest `record_version` per `sale_id`. How many sales were corrected, and by how much?
-- MAGIC
-- MAGIC **Q10** Write a `MERGE INTO` that upserts `sales` into a new table `silver_sales_practice` keyed on `sale_id`,
-- MAGIC updating only when the incoming `record_version` is higher. (This becomes real in Week 2.)
-- MAGIC
-- MAGIC 📖 [MERGE INTO docs](https://docs.databricks.com/aws/en/delta/merge)

-- COMMAND ----------

-- Q4: write your query here
