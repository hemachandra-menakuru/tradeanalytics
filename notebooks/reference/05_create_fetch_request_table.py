# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Create control.fetch_request (Two-Plane Architecture work queue)
# MAGIC
# MAGIC **Purpose:** the work queue between the Databricks planner job (writes intent)
# MAGIC and the EC2 bridge agent (executes fetches against IBKR). Decision record:
# MAGIC CLAUDE.md §14 "PLATFORM ARCHITECTURE — Two-Plane Design (2026-07-04)".
# MAGIC
# MAGIC **Lifecycle:** `PENDING` (planner) → `LANDED` (agent moved manifest to done/,
# MAGIC raw data in S3) → `INGESTED` (ingestion job wrote Bronze) | `FAILED`.
# MAGIC
# MAGIC **Writers:** ONLY Databricks jobs write this table (planner inserts, ingestion
# MAGIC job updates). The EC2 agent signals via S3 manifest moves only — it never
# MAGIC touches Delta. Not append-only: status transitions are UPDATEs (control table,
# MAGIC same as ingestion_watermark).
# MAGIC
# MAGIC Safe to re-run (CREATE TABLE IF NOT EXISTS). Pure %sql — no Python deps.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS tradeanalytics.control;

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tradeanalytics.control.fetch_request (
# MAGIC     request_id        BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     request_key       STRING NOT NULL,          -- deterministic business key; carried in the S3 manifest
# MAGIC     batch_id          STRING NOT NULL,          -- planner run that created this request
# MAGIC
# MAGIC     -- What to fetch (self-contained — agent needs no joins)
# MAGIC     instrument_id     BIGINT NOT NULL,
# MAGIC     symbol            STRING NOT NULL,          -- denormalised for agent convenience
# MAGIC     vendor            STRING NOT NULL,          -- ibkr | polygon | ...
# MAGIC     stream            STRING NOT NULL,          -- daily | intraday | tick
# MAGIC     bar_interval      STRING NOT NULL,          -- 1d | 1h | ... (never name a column 'interval')
# MAGIC     start_date        DATE   NOT NULL,
# MAGIC     end_date          DATE   NOT NULL,
# MAGIC     load_type         STRING NOT NULL,          -- INITIAL_LOAD | INCREMENTAL | GAP_FILL | HISTORY_EXTENSION | FORCE_RELOAD
# MAGIC     task_type         STRING NOT NULL DEFAULT 'FETCH_OHLCV',  -- generic: future agents add PUBLISH_SIGNAL, LLM_ENRICH, PLACE_ORDER
# MAGIC
# MAGIC     -- Lifecycle
# MAGIC     status            STRING NOT NULL DEFAULT 'PENDING',      -- PENDING | LANDED | INGESTED | FAILED
# MAGIC     s3_manifest_path  STRING NOT NULL,          -- control/pending/<request_key>.json at creation
# MAGIC     s3_data_path      STRING,                   -- set on reconcile: where the agent landed raw data
# MAGIC     record_count      BIGINT,                   -- bars landed (from agent's result manifest)
# MAGIC     attempt_count     INT    NOT NULL DEFAULT 0,
# MAGIC     error_message     STRING,
# MAGIC
# MAGIC     requested_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
# MAGIC     landed_at         TIMESTAMP,                -- from agent's result manifest
# MAGIC     ingested_at       TIMESTAMP                 -- when Bronze write completed
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Two-plane work queue: Databricks planner writes intent, EC2 bridge agent executes via S3 manifests, ingestion job reconciles. CLAUDE.md §14.'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed'        = 'true',
# MAGIC     'delta.feature.allowColumnDefaults' = 'supported'
# MAGIC );

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Verify
# MAGIC DESCRIBE TABLE EXTENDED tradeanalytics.control.fetch_request;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Operator queries (day-to-day, single SQL each)
# MAGIC
# MAGIC ```sql
# MAGIC -- What is the agent working on right now?
# MAGIC SELECT status, COUNT(*) FROM tradeanalytics.control.fetch_request GROUP BY status;
# MAGIC
# MAGIC -- Anything stuck? (PENDING older than 2 hours during market week)
# MAGIC SELECT * FROM tradeanalytics.control.fetch_request
# MAGIC WHERE status = 'PENDING' AND requested_at < current_timestamp() - INTERVAL 2 HOURS;
# MAGIC
# MAGIC -- Full history for one instrument
# MAGIC SELECT * FROM tradeanalytics.control.fetch_request
# MAGIC WHERE symbol = 'AAPL' ORDER BY requested_at DESC;
# MAGIC ```
