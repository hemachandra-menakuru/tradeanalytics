# Databricks notebook source
# MAGIC %md
# MAGIC # Reference & Control Schema Setup
# MAGIC Run once. Creates schemas and tables. Safe to re-run (IF NOT EXISTS everywhere).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS tradeanalytics.reference;
# MAGIC CREATE SCHEMA IF NOT EXISTS tradeanalytics.control;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tradeanalytics.reference.instrument (
# MAGIC     instrument_id   BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     isin            STRING,
# MAGIC     figi            STRING,
# MAGIC     composite_figi  STRING,
# MAGIC     ibkr_con_id     BIGINT,
# MAGIC     asset_class     STRING NOT NULL,
# MAGIC     sector          STRING,
# MAGIC     industry        STRING,
# MAGIC     is_active       BOOLEAN NOT NULL DEFAULT true,
# MAGIC     is_delisted     BOOLEAN NOT NULL DEFAULT false,
# MAGIC     delisted_date   DATE,
# MAGIC     source          STRING,
# MAGIC     created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
# MAGIC     updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed'          = 'true',
# MAGIC     'delta.feature.allowColumnDefaults'   = 'supported'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tradeanalytics.reference.instrument_listing (
# MAGIC     listing_id      BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     instrument_id   BIGINT NOT NULL,
# MAGIC     symbol          STRING NOT NULL,
# MAGIC     exchange        STRING,
# MAGIC     exchange_mic    STRING,
# MAGIC     currency        STRING NOT NULL DEFAULT 'USD',
# MAGIC     company_name    STRING,
# MAGIC     min_tick_size   DOUBLE,
# MAGIC     min_lot_size    INT,
# MAGIC     valid_from      DATE NOT NULL,
# MAGIC     valid_to        DATE,
# MAGIC     is_current      BOOLEAN NOT NULL DEFAULT true,
# MAGIC     change_reason   STRING,
# MAGIC     created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed'          = 'true',
# MAGIC     'delta.feature.allowColumnDefaults'   = 'supported'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tradeanalytics.reference.universe_membership (
# MAGIC     membership_id   BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     instrument_id   BIGINT NOT NULL,
# MAGIC     universe_code   STRING NOT NULL,
# MAGIC     index_weight    DOUBLE,
# MAGIC     source_etf      STRING,
# MAGIC     is_active       BOOLEAN NOT NULL DEFAULT true,
# MAGIC     added_date      DATE NOT NULL,
# MAGIC     removed_date    DATE,
# MAGIC     created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
# MAGIC     updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed'          = 'true',
# MAGIC     'delta.feature.allowColumnDefaults'   = 'supported'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tradeanalytics.reference.ticker_feed_config (
# MAGIC     config_id           BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     instrument_id       BIGINT NOT NULL,
# MAGIC     stream              STRING NOT NULL DEFAULT 'daily',
# MAGIC     target_start_date   DATE NOT NULL,
# MAGIC     target_end_date     DATE,
# MAGIC     run_frequency       STRING NOT NULL DEFAULT 'daily',
# MAGIC     batch_group         STRING NOT NULL DEFAULT 'A',
# MAGIC     priority            INT NOT NULL DEFAULT 5,
# MAGIC     is_active           BOOLEAN NOT NULL DEFAULT true,
# MAGIC     max_lookback_days   INT NOT NULL DEFAULT 365,
# MAGIC     created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
# MAGIC     updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
# MAGIC     created_by          STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed'          = 'true',
# MAGIC     'delta.feature.allowColumnDefaults'   = 'supported'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tradeanalytics.reference.market_calendar (
# MAGIC     calendar_id     BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     exchange_mic    STRING NOT NULL,
# MAGIC     calendar_date   DATE NOT NULL,
# MAGIC     session_type    STRING NOT NULL,
# MAGIC     description     STRING,
# MAGIC     open_time_utc   STRING,
# MAGIC     close_time_utc  STRING,
# MAGIC     valid_year      INT NOT NULL,
# MAGIC     source          STRING NOT NULL DEFAULT 'exchange_calendars',
# MAGIC     created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.appendOnly'                    = 'true',
# MAGIC     'delta.feature.allowColumnDefaults'   = 'supported'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tradeanalytics.control.ingestion_watermark (
# MAGIC     watermark_id            BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     instrument_id           BIGINT NOT NULL,
# MAGIC     stream                  STRING NOT NULL DEFAULT 'daily',
# MAGIC     interval                STRING NOT NULL,
# MAGIC     earliest_date           DATE,
# MAGIC     latest_date             DATE,
# MAGIC     record_count            BIGINT NOT NULL DEFAULT 0,
# MAGIC     last_batch_id           STRING,
# MAGIC     last_run_at             TIMESTAMP,
# MAGIC     last_load_type          STRING,
# MAGIC     status                  STRING NOT NULL DEFAULT 'active',
# MAGIC     consecutive_failures    INT NOT NULL DEFAULT 0,
# MAGIC     last_error              STRING,
# MAGIC     suspended_at            TIMESTAMP,
# MAGIC     suspended_reason        STRING,
# MAGIC     created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
# MAGIC     updated_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.feature.allowColumnDefaults'   = 'supported'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tradeanalytics.control.ingestion_batch_config (
# MAGIC     batch_config_id             BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     job_type                    STRING NOT NULL,
# MAGIC     stream                      STRING NOT NULL,
# MAGIC     batch_groups_included       STRING NOT NULL,
# MAGIC     run_frequencies_included    STRING NOT NULL,
# MAGIC     max_symbols_per_run         INT,
# MAGIC     max_lookback_days           INT NOT NULL DEFAULT 30,
# MAGIC     is_active                   BOOLEAN NOT NULL DEFAULT true,
# MAGIC     updated_at                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
# MAGIC     updated_by                  STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.feature.allowColumnDefaults'   = 'supported'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tradeanalytics.control.ingestion_command (
# MAGIC     command_id          BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     instrument_id       BIGINT NOT NULL,
# MAGIC     stream              STRING NOT NULL DEFAULT 'daily',
# MAGIC     action              STRING NOT NULL,
# MAGIC     override_start_date DATE,
# MAGIC     override_end_date   DATE,
# MAGIC     consumed            BOOLEAN NOT NULL DEFAULT false,
# MAGIC     consumed_at         TIMESTAMP,
# MAGIC     consumed_by_batch   STRING,
# MAGIC     reason              STRING,
# MAGIC     created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
# MAGIC     created_by          STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.feature.allowColumnDefaults'   = 'supported'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tradeanalytics.control.job_run_log (
# MAGIC     log_id              BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     batch_id            STRING NOT NULL,
# MAGIC     job_type            STRING NOT NULL,
# MAGIC     run_started_at      TIMESTAMP NOT NULL,
# MAGIC     run_completed_at    TIMESTAMP,
# MAGIC     duration_seconds    DOUBLE,
# MAGIC     instrument_id       BIGINT,
# MAGIC     stream              STRING,
# MAGIC     interval            STRING,
# MAGIC     load_type           STRING,
# MAGIC     status              STRING NOT NULL,
# MAGIC     records_fetched     BIGINT NOT NULL DEFAULT 0,
# MAGIC     records_new         BIGINT NOT NULL DEFAULT 0,
# MAGIC     records_amended     BIGINT NOT NULL DEFAULT 0,
# MAGIC     records_skipped     BIGINT NOT NULL DEFAULT 0,
# MAGIC     records_rejected    BIGINT NOT NULL DEFAULT 0,
# MAGIC     fetch_start_date    DATE,
# MAGIC     fetch_end_date      DATE,
# MAGIC     error_message       STRING,
# MAGIC     error_type          STRING,
# MAGIC     pipeline_version    STRING,
# MAGIC     cluster_id          STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.appendOnly'                    = 'true',
# MAGIC     'delta.feature.allowColumnDefaults'   = 'supported'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO tradeanalytics.control.ingestion_batch_config
# MAGIC     (job_type, stream, batch_groups_included, run_frequencies_included, max_lookback_days, is_active)
# MAGIC SELECT 'bronze_daily', 'daily', 'A,B,C,D', 'daily', 30, true
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM tradeanalytics.control.ingestion_batch_config
# MAGIC     WHERE job_type = 'bronze_daily'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN tradeanalytics.reference;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN tradeanalytics.control;
