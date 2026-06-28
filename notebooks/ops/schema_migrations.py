# Databricks notebook source
# MAGIC %md
# MAGIC # Schema Migrations
# MAGIC
# MAGIC Run this notebook whenever new columns are added to existing Delta tables.
# MAGIC All cells are idempotent — safe to re-run (uses IF NOT EXISTS pattern via TRY/EXCEPT).
# MAGIC
# MAGIC ## Migration log
# MAGIC | Date | Migration | Branch |
# MAGIC |------|-----------|--------|
# MAGIC | 2026-06-28 | Add instrument_id to bronze tables | feature/phase3-silver |
# MAGIC | 2026-06-28 | Add vendor to control.ingestion_watermark | feature/phase3-silver |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migration 001 — Add `instrument_id` to bronze tables
# MAGIC
# MAGIC **Why:** Silver layer needs to join bronze records to reference tables using
# MAGIC the permanent surrogate key. Without instrument_id on bronze records, Silver
# MAGIC must join via symbol — which is fragile (symbols change, get reused after delistings).
# MAGIC
# MAGIC **Nullable:** Yes — existing records have NULL. New records will have the value
# MAGIC stamped by the ingestion pipeline from reference.instrument.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE tradeanalytics.bronze.market_data_daily
# MAGIC ADD COLUMNS (instrument_id BIGINT COMMENT 'Permanent surrogate key from reference.instrument — NULL for records ingested before Phase 3');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE tradeanalytics.bronze.market_data_rejected
# MAGIC ADD COLUMNS (instrument_id BIGINT COMMENT 'Permanent surrogate key from reference.instrument — NULL for records ingested before Phase 3');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migration 002 — Add `vendor` to `control.ingestion_watermark`
# MAGIC
# MAGIC **Why:** Tracks which data vendor actually wrote the watermark.
# MAGIC Allows detecting when provider switches (ibkr → yahoo fallback or vice versa).
# MAGIC
# MAGIC **Note:** This column was defined in the Phase 3A DDL notebook (04_phase3a_schema_additions.py).
# MAGIC If that notebook was already run, this cell will fail — that is expected. Check result below.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE tradeanalytics.control.ingestion_watermark
# MAGIC ADD COLUMNS (vendor STRING COMMENT 'Data vendor that wrote this watermark — ibkr | yahoo | polygon');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migration 003 — Add `vendor` to `control.job_run_log`
# MAGIC
# MAGIC **Why:** Audit trail should record which vendor was used per run.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE tradeanalytics.control.job_run_log
# MAGIC ADD COLUMNS (vendor STRING COMMENT 'Data vendor used for this run — ibkr | yahoo | polygon');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — confirm all columns exist

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bronze.market_data_daily' AS table_name, column_name, data_type
# MAGIC FROM tradeanalytics.information_schema.columns
# MAGIC WHERE table_schema = 'bronze'
# MAGIC   AND table_name   = 'market_data_daily'
# MAGIC   AND column_name  = 'instrument_id'
# MAGIC UNION ALL
# MAGIC SELECT 'bronze.market_data_rejected', column_name, data_type
# MAGIC FROM tradeanalytics.information_schema.columns
# MAGIC WHERE table_schema = 'bronze'
# MAGIC   AND table_name   = 'market_data_rejected'
# MAGIC   AND column_name  = 'instrument_id'
# MAGIC UNION ALL
# MAGIC SELECT 'control.ingestion_watermark', column_name, data_type
# MAGIC FROM tradeanalytics.information_schema.columns
# MAGIC WHERE table_schema = 'control'
# MAGIC   AND table_name   = 'ingestion_watermark'
# MAGIC   AND column_name  = 'vendor'
# MAGIC UNION ALL
# MAGIC SELECT 'control.job_run_log', column_name, data_type
# MAGIC FROM tradeanalytics.information_schema.columns
# MAGIC WHERE table_schema = 'control'
# MAGIC   AND table_name   = 'job_run_log'
# MAGIC   AND column_name  = 'vendor'
# MAGIC ORDER BY table_name;

# COMMAND ----------

print("✅ Schema migrations complete.")
print("   Expected: 4 rows in verification above — one per column.")
print("   If any row is missing, that ALTER TABLE failed — check cell output above.")
