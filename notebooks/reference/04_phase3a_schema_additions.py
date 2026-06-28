# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 3A — Schema Additions: Vendor-Agnostic + Corporate Actions
# MAGIC
# MAGIC **Run once. Safe to re-run (IF NOT EXISTS everywhere).**
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC ### New tables
# MAGIC | Table | Purpose |
# MAGIC |---|---|
# MAGIC | `reference.instrument_vendor_id` | Vendor-specific IDs per instrument. Replaces `ibkr_con_id` in `reference.instrument`. Supports IBKR, Polygon, Bloomberg, etc. |
# MAGIC | `reference.corporate_actions` | Permanent audit log of all corporate events: splits, reverse splits, dividends, spinoffs. Append-only. |
# MAGIC | `control.corporate_action_candidates` | Saga state machine. Tracks detection → classification → reload → resolved lifecycle so no step is ever lost silently. |
# MAGIC
# MAGIC ### Schema alterations to existing tables
# MAGIC | Table | Change | Why |
# MAGIC |---|---|---|
# MAGIC | `reference.instrument` | Remove `ibkr_con_id` (after migrating data) | Vendor-specific ID does not belong in vendor-neutral master |
# MAGIC | `reference.ticker_feed_config` | Add `preferred_vendor`, `fallback_vendor` | Per-instrument vendor config for 2000+ instrument scale |
# MAGIC | `control.ingestion_watermark` | Add `vendor` | Know which vendor provided each instrument's data |
# MAGIC | `control.job_run_log` | Add `vendor` | Full audit trail of which vendor ran each job |
# MAGIC
# MAGIC ### Data migration
# MAGIC - Existing `ibkr_con_id` values moved to `reference.instrument_vendor_id` before column is dropped.
# MAGIC
# MAGIC **Run order:** top to bottom. All cells are idempotent.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 — New Table: `reference.instrument_vendor_id`
# MAGIC
# MAGIC **Concept:** Every data vendor has its own identifier for the same instrument.
# MAGIC IBKR uses a `conid` (contract ID). Polygon uses a ticker. Bloomberg uses a FIGI.
# MAGIC None of these belong in `reference.instrument` because they are vendor relationships,
# MAGIC not intrinsic properties of the instrument.
# MAGIC
# MAGIC This table is a lookup: given our `instrument_id` and a vendor name, what is
# MAGIC that vendor's identifier for this instrument?
# MAGIC
# MAGIC **SCD Type 2:** vendor IDs can change (e.g. IBKR can change conids on exchange moves).
# MAGIC Old rows get `valid_to` set; new row inserted with `is_current = true`.
# MAGIC This table is append-only — no updates, ever.
# MAGIC
# MAGIC **At 2000+ instruments:** adding a new vendor = INSERT rows for all instruments,
# MAGIC zero schema changes.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tradeanalytics.reference.instrument_vendor_id (
# MAGIC     vendor_mapping_id       BIGINT    GENERATED ALWAYS AS IDENTITY,
# MAGIC     instrument_id           BIGINT    NOT NULL,
# MAGIC     vendor                  STRING    NOT NULL,   -- ibkr | polygon | bloomberg | refinitiv | yahoo
# MAGIC     vendor_instrument_id    STRING    NOT NULL,   -- IBKR conid, Bloomberg ID, Polygon ticker, etc.
# MAGIC     vendor_exchange         STRING,               -- vendor's own exchange code for this instrument
# MAGIC     valid_from              DATE      NOT NULL,
# MAGIC     valid_to                DATE,                 -- NULL = currently valid
# MAGIC     is_current              BOOLEAN   NOT NULL DEFAULT true,
# MAGIC     notes                   STRING,
# MAGIC     created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Vendor-specific instrument identifiers. One row per instrument per vendor. SCD Type 2 — append-only.'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.appendOnly'                   = 'true',
# MAGIC     'delta.feature.allowColumnDefaults'  = 'supported'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 — Migrate `ibkr_con_id` data before dropping the column
# MAGIC
# MAGIC Copy all existing IBKR conids from `reference.instrument` into the new
# MAGIC `reference.instrument_vendor_id` table. Only inserts where the vendor_mapping
# MAGIC does not already exist — safe to re-run.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO tradeanalytics.reference.instrument_vendor_id
# MAGIC     (instrument_id, vendor, vendor_instrument_id, valid_from, is_current)
# MAGIC SELECT
# MAGIC     instrument_id,
# MAGIC     'ibkr'                            AS vendor,
# MAGIC     CAST(ibkr_con_id AS STRING)       AS vendor_instrument_id,
# MAGIC     '2010-01-01'                      AS valid_from,
# MAGIC     true                              AS is_current
# MAGIC FROM tradeanalytics.reference.instrument
# MAGIC WHERE ibkr_con_id IS NOT NULL
# MAGIC   AND NOT EXISTS (
# MAGIC       SELECT 1 FROM tradeanalytics.reference.instrument_vendor_id v
# MAGIC       WHERE v.instrument_id = tradeanalytics.reference.instrument.instrument_id
# MAGIC         AND v.vendor = 'ibkr'
# MAGIC         AND v.is_current = true
# MAGIC   );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 — Verify migration before dropping the column

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     i.instrument_id,
# MAGIC     il.symbol,
# MAGIC     i.ibkr_con_id                      AS old_column,
# MAGIC     v.vendor_instrument_id             AS new_table,
# MAGIC     CASE WHEN CAST(i.ibkr_con_id AS STRING) = v.vendor_instrument_id
# MAGIC          THEN '✅ match' ELSE '❌ mismatch' END AS check
# MAGIC FROM tradeanalytics.reference.instrument i
# MAGIC JOIN tradeanalytics.reference.instrument_listing il
# MAGIC   ON i.instrument_id = il.instrument_id AND il.is_current = true
# MAGIC LEFT JOIN tradeanalytics.reference.instrument_vendor_id v
# MAGIC   ON i.instrument_id = v.instrument_id AND v.vendor = 'ibkr' AND v.is_current = true
# MAGIC WHERE i.ibkr_con_id IS NOT NULL
# MAGIC ORDER BY il.symbol;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 — Drop `ibkr_con_id` from `reference.instrument`
# MAGIC
# MAGIC **Prerequisite:** verify all rows matched in Part 3 before running this cell.
# MAGIC
# MAGIC Delta column dropping requires enabling column mapping mode first.
# MAGIC This is a one-time, one-way operation per table.
# MAGIC Column mapping mode = 'name' means Delta tracks columns by name, not position —
# MAGIC dropped columns are hidden but data is not physically removed until VACUUM.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE tradeanalytics.reference.instrument
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.columnMapping.mode' = 'name',
# MAGIC     'delta.minReaderVersion'   = '2',
# MAGIC     'delta.minWriterVersion'   = '5'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE tradeanalytics.reference.instrument
# MAGIC DROP COLUMN ibkr_con_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5 — Schema alterations to existing tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a — `reference.ticker_feed_config`: add vendor preference columns
# MAGIC
# MAGIC `preferred_vendor = NULL` means use the global default from `config/sources.yml`.
# MAGIC Set a value only when an instrument needs a specific vendor (e.g. a futures contract
# MAGIC only available on Bloomberg, or an FX pair only on Refinitiv).

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE tradeanalytics.reference.ticker_feed_config
# MAGIC ADD COLUMNS (
# MAGIC     preferred_vendor  STRING,   -- NULL = use global default. ibkr | polygon | bloomberg | refinitiv
# MAGIC     fallback_vendor   STRING    -- NULL = use global fallback. ibkr | yahoo
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b — `control.ingestion_watermark`: add vendor column
# MAGIC
# MAGIC Records which vendor provided the ingested data for each instrument.
# MAGIC On vendor switch: if stored vendor != new vendor, IngestionPlanner treats as INITIAL_LOAD.
# MAGIC This prevents cross-vendor price base mismatches.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE tradeanalytics.control.ingestion_watermark
# MAGIC ADD COLUMN vendor STRING;   -- ibkr | polygon | bloomberg | yahoo | NULL (legacy/unknown)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5c — `control.job_run_log`: add vendor column
# MAGIC
# MAGIC Full audit trail: which vendor was used for every fetch.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE tradeanalytics.control.job_run_log
# MAGIC ADD COLUMN vendor STRING;   -- ibkr | polygon | bloomberg | yahoo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6 — New Table: `reference.corporate_actions`
# MAGIC
# MAGIC **Concept:** A permanent, append-only log of every corporate action event ever detected
# MAGIC for any instrument. One row per event.
# MAGIC
# MAGIC **What it is NOT:** a price correction mechanism. IBKR retroactively adjusts all
# MAGIC historical prices on its end. Our correction mechanism is FORCE_RELOAD + Bronze
# MAGIC `record_version`. This table is the AUDIT TRAIL of what events triggered those reloads,
# MAGIC and the source of split-date features in the feature store (split dates are themselves
# MAGIC a signal — they precede index rebalancing, liquidity changes, and retail attention spikes).
# MAGIC
# MAGIC **event_factor:** always means "multiply historical prices by this to align to current base"
# MAGIC - Forward split 4-for-1:    event_factor = 0.25  (ratio_to=1 / ratio_from=4)
# MAGIC - Reverse split 1-for-10:  event_factor = 10.0  (ratio_to=10 / ratio_from=1)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tradeanalytics.reference.corporate_actions (
# MAGIC     action_id               BIGINT    GENERATED ALWAYS AS IDENTITY,
# MAGIC     instrument_id           BIGINT    NOT NULL,
# MAGIC     effective_date          DATE      NOT NULL,   -- first date the new adjustment applies
# MAGIC     event_type              STRING    NOT NULL,   -- SPLIT | REVERSE_SPLIT | DIVIDEND
# MAGIC                                                   -- SPECIAL_DIVIDEND | SPINOFF
# MAGIC     split_ratio_from        INT       NOT NULL DEFAULT 1,   -- e.g. 10 in "10-for-1"
# MAGIC     split_ratio_to          INT       NOT NULL DEFAULT 1,   -- e.g. 1  in "10-for-1"
# MAGIC     event_factor            DOUBLE    NOT NULL,             -- ratio_to / ratio_from
# MAGIC     dividend_amount         DOUBLE,                         -- per-share cash amount
# MAGIC     dividend_currency       STRING,
# MAGIC     event_detail            STRING,                         -- human-readable: "10-for-1 split"
# MAGIC     source_vendor           STRING    NOT NULL,             -- ibkr | yahoo | polygon | manual
# MAGIC     is_verified             BOOLEAN   NOT NULL DEFAULT false,
# MAGIC     verification_sources    STRING,                         -- JSON: '["ibkr","yahoo"]'
# MAGIC     candidate_id            BIGINT,                         -- FK → control.corporate_action_candidates
# MAGIC     reload_triggered        BOOLEAN   NOT NULL DEFAULT false,
# MAGIC     reload_triggered_at     TIMESTAMP,
# MAGIC     reload_completed_at     TIMESTAMP,
# MAGIC     created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Permanent audit log of all corporate action events. Append-only. Source of split-date features.'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.appendOnly'                   = 'true',
# MAGIC     'delta.feature.allowColumnDefaults'  = 'supported'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7 — New Table: `control.corporate_action_candidates`
# MAGIC
# MAGIC **Concept:** The saga state machine. When a corporate action is detected, we write here
# MAGIC FIRST — before doing anything else. Then each subsequent step (classify, trigger reload,
# MAGIC verify resolution) updates the `status` column.
# MAGIC
# MAGIC If any step fails mid-way, this table records exactly where the saga stopped.
# MAGIC The next job run's recovery step reads all non-RESOLVED rows and retries from
# MAGIC the last completed step. Nothing is ever lost silently.
# MAGIC
# MAGIC **Status lifecycle:**
# MAGIC ```
# MAGIC DETECTED → CLASSIFIED → RELOAD_TRIGGERED → RESOLVED
# MAGIC                                           → FALSE_POSITIVE (large earnings move, not a split)
# MAGIC                                           → FAILED (retry_count exceeded max)
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tradeanalytics.control.corporate_action_candidates (
# MAGIC     candidate_id        BIGINT    GENERATED ALWAYS AS IDENTITY,
# MAGIC     instrument_id       BIGINT    NOT NULL,
# MAGIC     detected_on_date    DATE      NOT NULL,   -- date where price discrepancy was found
# MAGIC     price_in_bronze     DOUBLE    NOT NULL,   -- close stored in market_data_daily (old base)
# MAGIC     price_from_vendor   DOUBLE    NOT NULL,   -- close IBKR returned for same date (new base)
# MAGIC     price_ratio         DOUBLE    NOT NULL,   -- price_from_vendor / price_in_bronze
# MAGIC     status              STRING    NOT NULL DEFAULT 'DETECTED',
# MAGIC     event_type          STRING,               -- filled after CLASSIFIED step
# MAGIC     action_id           BIGINT,               -- FK → reference.corporate_actions
# MAGIC     reload_command_id   BIGINT,               -- FK → control.ingestion_command
# MAGIC     resolved_at         TIMESTAMP,
# MAGIC     failure_reason      STRING,
# MAGIC     retry_count         INT       NOT NULL DEFAULT 0,
# MAGIC     detected_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
# MAGIC     updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Saga state machine for corporate action detection pipeline. Tracks every step from detection to resolution.'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.feature.allowColumnDefaults'  = 'supported'
# MAGIC     -- NOT appendOnly: status is updated as saga progresses
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8 — Final Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'reference' AS schema_name, table_name FROM information_schema.tables
# MAGIC WHERE table_catalog = 'tradeanalytics' AND table_schema = 'reference'
# MAGIC UNION ALL
# MAGIC SELECT 'control'   AS schema_name, table_name FROM information_schema.tables
# MAGIC WHERE table_catalog = 'tradeanalytics' AND table_schema = 'control'
# MAGIC ORDER BY schema_name, table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm ibkr_con_id is gone from reference.instrument
# MAGIC DESCRIBE TABLE tradeanalytics.reference.instrument;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm vendor IDs migrated correctly
# MAGIC SELECT vendor, COUNT(*) AS instruments
# MAGIC FROM tradeanalytics.reference.instrument_vendor_id
# MAGIC GROUP BY vendor;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm new columns on ticker_feed_config
# MAGIC DESCRIBE TABLE tradeanalytics.reference.ticker_feed_config;
