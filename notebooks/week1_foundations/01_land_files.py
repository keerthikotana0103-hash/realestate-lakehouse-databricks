# Databricks notebook source
# MAGIC %md
# MAGIC # Day 2 · Simulate file arrival into the landing volume
# MAGIC
# MAGIC In real life the MLS vendor drops a file every day. Here we **copy files from the Git repo into the
# MAGIC volume up to a chosen date**. Run it again later with a later `through_date` and only the new days
# MAGIC are copied, which is exactly what makes incremental ingestion (Auto Loader, Week 2) meaningful.
# MAGIC
# MAGIC Data range: 2026-07-01 → 2026-09-28. Suggested: land the first week now (`2026-07-07`).

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

import os
import shutil
from datetime import date

dbutils.widgets.text("through_date", "2026-07-07", "Land files up to (inclusive)")
through = date.fromisoformat(dbutils.widgets.get("through_date"))

# COMMAND ----------

def land(src_dir, dst_dir, prefix=None):
    """Copy files whose date suffix <= through_date (or all files when prefix is None). Skips files already landed."""
    os.makedirs(dst_dir, exist_ok=True)
    copied = 0
    for name in sorted(os.listdir(src_dir)):
        if prefix:
            file_date = date.fromisoformat(name.replace(prefix, "").split(".")[0])
            if file_date > through:
                continue
        dst = os.path.join(dst_dir, name)
        if not os.path.exists(dst):
            shutil.copy(os.path.join(src_dir, name), dst)
            copied += 1
    return copied


summary = {
    "reference": land(os.path.join(REPO_DATA, "reference"), LANDING["reference"]),
    "listing_events": land(os.path.join(REPO_DATA, "landing", "listing_events"), LANDING["listing_events"], "listing_events_"),
    "sales": land(os.path.join(REPO_DATA, "landing", "sales"), LANDING["sales"], "sales_"),
    "agent_updates": land(os.path.join(REPO_DATA, "landing", "agent_updates"), LANDING["agent_updates"], "agent_updates_"),
}
print(f"Landed through {through}: {summary}")

# COMMAND ----------

display(dbutils.fs.ls(LANDING["listing_events"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Checkpoint
# MAGIC - Browse the volume in Catalog Explorer and open one `.jsonl` file.
# MAGIC - Question to think about: *what happens if the vendor re-sends yesterday's file?* (Answer in Week 2: Auto Loader tracks processed files in its checkpoint, and silver dedups on `event_id`.)
