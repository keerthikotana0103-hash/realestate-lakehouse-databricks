# Week 2 · Data engineering (Days 8–14)

| Day | Notebook to create | Build |
| --- | --- | --- |
| 8 | `10_autoloader_bronze.py` | Replace the Week 1 batch load with Auto Loader (`cloudFiles`), `schemaLocation`, `_rescued_data` |
| 9 | (same) | `trigger(availableNow=True)`; land 7 more days with `01_land_files` and prove only new files are read |
| 10 | `11_silver_listings.py` | Explicit types, dedup on `event_id`, quarantine table for bad rows |
| 11 | `12_scd2_agents.py` | SCD Type 2 `dim_agent` from `agent_updates` with `MERGE` (`valid_from`, `valid_to`, `is_current`) |
| 12 | `pipelines/realestate_pipeline.py` | Rebuild bronze→silver as a Lakeflow Declarative Pipeline: expectations + `AUTO CDC` for listing SCD2 |
| 13 | `13_performance.py` | Liquid clustering on `listing_id` / `close_date`, `OPTIMIZE`, before/after query timings |
| 14 | `jobs/` | Lakeflow Job: land → pipeline → gold build, retries + failure email |
