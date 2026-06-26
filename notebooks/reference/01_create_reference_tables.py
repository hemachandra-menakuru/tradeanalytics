# Databricks notebook source
# One-time setup notebook — run on-demand, not scheduled.
# Creates tradeanalytics.reference schema and all reference tables.
#
# Safe to re-run: all DDL uses CREATE IF NOT EXISTS.
# Run this once before the universe sync job runs for the first time.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1 — Create Schemas

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS tradeanalytics.reference")
spark.sql("CREATE SCHEMA IF NOT EXISTS tradeanalytics.control")
print("✓ Schemas ready: tradeanalytics.reference, tradeanalytics.control")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2 — reference.instrument
# MAGIC Permanent master record for every financial instrument we know about.
# MAGIC One row per instrument, forever. Never deleted — only deactivated.

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS tradeanalytics.reference.instrument (
    instrument_id   BIGINT        GENERATED ALWAYS AS IDENTITY,
    isin            STRING,
    figi            STRING,
    ibkr_con_id     BIGINT,
    asset_class     STRING        NOT NULL,
    is_active       BOOLEAN       NOT NULL DEFAULT true,
    created_at      TIMESTAMP     NOT NULL,
    updated_at      TIMESTAMP     NOT NULL,

    CONSTRAINT pk_instrument PRIMARY KEY (instrument_id)
)
USING DELTA
COMMENT 'Permanent master record per financial instrument. Never deleted.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
)
""")
print("✓ reference.instrument")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3 — reference.instrument_listing
# MAGIC Symbol + exchange per instrument. SCD Type 2 — history preserved when
# MAGIC symbols change (e.g. FB → META). is_current=true is the active row.

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS tradeanalytics.reference.instrument_listing (
    listing_id      BIGINT        GENERATED ALWAYS AS IDENTITY,
    instrument_id   BIGINT        NOT NULL,
    symbol          STRING        NOT NULL,
    company_name    STRING,
    exchange        STRING,
    exchange_mic    STRING,
    currency        STRING,
    min_tick_size   DOUBLE,
    min_lot_size    DOUBLE,
    valid_from      DATE          NOT NULL,
    valid_to        DATE,
    is_current      BOOLEAN       NOT NULL DEFAULT true,
    change_reason   STRING,
    created_at      TIMESTAMP     NOT NULL,

    CONSTRAINT pk_instrument_listing PRIMARY KEY (listing_id),
    CONSTRAINT fk_listing_instrument FOREIGN KEY (instrument_id)
        REFERENCES tradeanalytics.reference.instrument(instrument_id)
)
USING DELTA
COMMENT 'Symbol and exchange per instrument. SCD Type 2 — preserves symbol rename history.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
)
""")
print("✓ reference.instrument_listing")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4 — reference.universe_membership
# MAGIC Which instruments belong to which index universe (SP500, NDX100, RUSSELL2000).
# MAGIC Updated monthly by the universe sync job.

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS tradeanalytics.reference.universe_membership (
    membership_id   BIGINT        GENERATED ALWAYS AS IDENTITY,
    instrument_id   BIGINT        NOT NULL,
    universe_code   STRING        NOT NULL,
    source_etf      STRING        NOT NULL,
    index_weight    DOUBLE,
    valid_from      DATE          NOT NULL,
    valid_to        DATE,
    is_current      BOOLEAN       NOT NULL DEFAULT true,
    created_at      TIMESTAMP     NOT NULL,
    updated_at      TIMESTAMP     NOT NULL,

    CONSTRAINT pk_universe_membership PRIMARY KEY (membership_id),
    CONSTRAINT fk_membership_instrument FOREIGN KEY (instrument_id)
        REFERENCES tradeanalytics.reference.instrument(instrument_id)
)
USING DELTA
COMMENT 'Index universe membership per instrument. SP500, NDX100, RUSSELL2000, ETF_CURATED.'
PARTITIONED BY (universe_code)
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
)
""")
print("✓ reference.universe_membership")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5 — reference.instrument_feed_config
# MAGIC DESIRED STATE — controls what the ingestion job fetches.
# MAGIC is_active=false by default. Flip to true to start ingesting an instrument.

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS tradeanalytics.reference.instrument_feed_config (
    config_id           BIGINT      GENERATED ALWAYS AS IDENTITY,
    instrument_id       BIGINT      NOT NULL,
    stream_name         STRING      NOT NULL,
    is_active           BOOLEAN     NOT NULL DEFAULT false,
    target_start_date   DATE,
    target_end_date     DATE,
    run_frequency       STRING      NOT NULL DEFAULT 'daily',
    batch_group         STRING      NOT NULL DEFAULT 'A',
    priority            INT         NOT NULL DEFAULT 5,
    max_lookback_days   INT         NOT NULL DEFAULT 365,
    created_at          TIMESTAMP   NOT NULL,
    updated_at          TIMESTAMP   NOT NULL,

    CONSTRAINT pk_feed_config PRIMARY KEY (config_id),
    CONSTRAINT fk_feed_config_instrument FOREIGN KEY (instrument_id)
        REFERENCES tradeanalytics.reference.instrument(instrument_id),
    CONSTRAINT uq_feed_config UNIQUE (instrument_id, stream_name)
)
USING DELTA
COMMENT 'Desired ingestion state per instrument. is_active=false = known but not ingesting.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
)
""")
print("✓ reference.instrument_feed_config")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

tables = spark.sql("""
    SHOW TABLES IN tradeanalytics.reference
""").toPandas()

print("Tables in tradeanalytics.reference:")
for _, row in tables.iterrows():
    count = spark.sql(
        f"SELECT COUNT(*) as n FROM tradeanalytics.reference.{row['tableName']}"
    ).collect()[0]["n"]
    print(f"  ✓ {row['tableName']} ({count} rows)")
