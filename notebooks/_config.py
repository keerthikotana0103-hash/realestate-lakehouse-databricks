# Databricks notebook source
# MAGIC %md
# MAGIC # Shared config
# MAGIC Every notebook starts with `%run ../_config` (or `%run ./_config`).
# MAGIC Change `CATALOG` here if you can't create a catalog in your workspace (use `workspace` on Free Edition then).

# COMMAND ----------

import os

CATALOG = "realestate"          # set to "workspace" if CREATE CATALOG is not allowed for you
SCHEMAS = ["raw", "bronze", "silver", "gold"]
VOLUME = f"/Volumes/{CATALOG}/raw/landing"


def _find_repo_root(start):
    """Walk up from the notebook's folder until we find data/generate_data.py (the repo root)."""
    path = os.path.abspath(start)
    while path != os.path.dirname(path):
        if os.path.exists(os.path.join(path, "data", "generate_data.py")):
            return path
        path = os.path.dirname(path)
    raise FileNotFoundError("Repo root not found - open these notebooks from a Databricks Git folder clone of the repo.")


REPO_ROOT = _find_repo_root(os.getcwd())
REPO_DATA = os.path.join(REPO_ROOT, "data")

# landing sub-folders inside the Unity Catalog volume
LANDING = {
    "listing_events": f"{VOLUME}/listing_events",
    "sales": f"{VOLUME}/sales",
    "agent_updates": f"{VOLUME}/agent_updates",
    "reference": f"{VOLUME}/reference",
}

print(f"CATALOG={CATALOG}\nVOLUME={VOLUME}\nREPO_ROOT={REPO_ROOT}")
