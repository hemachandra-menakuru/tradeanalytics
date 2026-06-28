# Databricks notebook source
# One-time setup notebook — run on-demand, not scheduled.
# Creates all tables in the tradeanalytics.control schema.
#
# Safe to re-run: all DDL uses CREATE IF NOT EXISTS.
# Run this once after 01_create_reference_tables.py has been run.

# COMMAND ----------

# MAGIC %md
# MAGIC # Control Tables — Setup
# MAGIC
# MAGIC The `control` schema is the **system's own workspace**. It tracks:
# MAGIC - What data has already been fetched (`ingestion_watermark`)
# MAGIC - How each daily job run is scoped (`ingestion_batch_config`)
# MAGIC - One-off operator instructions (`ingestion_command`)
# MAGIC - A full audit trail of every run (`job_run_log`)
# MAGIC
# MAGIC **Operator rule:**
# MAGIC - You may read any table here freely
# MAGIC - You may INSERT into `ingestion_command` to issue instructions
# MAGIC - You must NEVER manually INSERT or UPDATE `ingestion_watermark` — it is system-managed state
# MAGIC
# MAGIC Run all cells top to bottom. Safe to re-run at any time.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 1 — control.ingestion_watermark
# MAGIC
# MAGIC **What it is:** The system's memory of what data it has already fetched per instrument.
# MAGIC
# MAGIC **How it works:** After every successful fetch, the pipeline updates this table with the
# MAGIC earliest and latest date it fetched. On the next run, the planner reads this and only
# MAGIC fetches what is missing — no duplicate downloads, no gaps.
# MAGIC
# MAGIC **Status values:**
# MAGIC - `active`    — fetching normally
# MAGIC - `suspended` — automatically stopped after 3 consecutive failures (resume via ingestion_command)
# MAGIC - `paused`    — manually paused by operator (resume via ingestion_command)
# MAGIC
# MAGIC ⚠️ Never manually INSERT or UPDATE this table. It is written only by the pipeline.

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS tradeanalytics.control.ingestion_watermark (
    watermark_id         BIGINT    GENERATED ALWAYS AS IDENTITY,
    instrument_id        BIGINT    NOT NULL,
    stream               STRING    NOT NULL,
    earliest_date        DATE,
    latest_date          DATE,
    last_successful_run  TIMESTAMP,
    record_count         BIGINT    NOT NULL DEFAULT 0,
    status               STRING    NOT NULL DEFAULT 'active',
    consecutive_failures INT       NOT NULL DEFAULT 0,
    last_error_message   STRING,
    created_at           TIMESTAMP NOT NULL,
    updated_at           TIMESTAMP NOT NULL,

    CONSTRAINT pk_watermark       PRIMARY KEY (watermark_id),
    CONSTRAINT fk_watermark_instr FOREIGN KEY (instrument_id)
                                  REFERENCES tradeanalytics.reference.instrument(instrument_id)
)
USING DELTA
COMMENT 'ACTUAL STATE — what data has been fetched per instrument and stream. System-managed, never touch manually.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
)
""")
# Valid values: active | suspended | paused — enforced in Python (LoadType enum)
print("✓ control.ingestion_watermark")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 2 — control.ingestion_batch_config
# MAGIC
# MAGIC **What it is:** Controls the scope of each job run — which batch groups (A, B, C, D) get
# MAGIC processed and how many symbols are allowed per run.
# MAGIC
# MAGIC **How it works:** When the daily job starts it reads this table for `job_type = 'daily'`
# MAGIC and only processes instruments in those batch groups. This lets you control cost and
# MAGIC runtime without changing any code.
# MAGIC
# MAGIC **Common operator actions:**
# MAGIC ```sql
# MAGIC -- Expand daily job to also run batch group C
# MAGIC UPDATE tradeanalytics.control.ingestion_batch_config
# MAGIC SET batch_groups_included = 'A,B,C'
# MAGIC WHERE job_type = 'daily';
# MAGIC
# MAGIC -- Reduce symbols per run during cluster cost-saving period
# MAGIC UPDATE tradeanalytics.control.ingestion_batch_config
# MAGIC SET max_symbols_per_run = 50
# MAGIC WHERE job_type = 'daily';
# MAGIC ```

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS tradeanalytics.control.ingestion_batch_config (
    config_id             BIGINT    GENERATED ALWAYS AS IDENTITY,
    job_type              STRING    NOT NULL,
    batch_groups_included STRING    NOT NULL,
    max_symbols_per_run   INT       NOT NULL DEFAULT 500,
    is_active             BOOLEAN   NOT NULL DEFAULT true,
    description           STRING,
    created_at            TIMESTAMP NOT NULL,
    updated_at            TIMESTAMP NOT NULL,

    CONSTRAINT pk_batch_config PRIMARY KEY (config_id)
)
USING DELTA
COMMENT 'Controls which batch groups and how many symbols each job type processes per run.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
)
""")
# Valid job_type values: daily | weekly | on_demand — enforced in Python
print("✓ control.ingestion_batch_config")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed ingestion_batch_config
# MAGIC
# MAGIC Insert the initial configuration for each job type.
# MAGIC Only inserts if the table is empty — safe to re-run.
# MAGIC
# MAGIC | job_type   | batch_groups | meaning                              |
# MAGIC |------------|--------------|--------------------------------------|
# MAGIC | daily      | A,B          | priority stocks run every day        |
# MAGIC | weekly     | C,D          | secondary stocks run on weekends     |
# MAGIC | on_demand  | A,B,C,D      | manual runs process everything       |

# COMMAND ----------

existing = spark.sql("SELECT COUNT(*) as n FROM tradeanalytics.control.ingestion_batch_config").collect()[0]["n"]

if existing == 0:
    spark.sql("""
        INSERT INTO tradeanalytics.control.ingestion_batch_config
            (job_type, batch_groups_included, max_symbols_per_run, is_active, created_at, updated_at)
        VALUES
            ('daily',     'A,B',     500, true, current_timestamp(), current_timestamp()),
            ('weekly',    'C,D',     500, true, current_timestamp(), current_timestamp()),
            ('on_demand', 'A,B,C,D', 500, true, current_timestamp(), current_timestamp())
    """)
    print("✓ Seeded 3 rows into control.ingestion_batch_config")
else:
    print(f"✓ control.ingestion_batch_config already has {existing} rows — skipping seed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 3 — control.ingestion_command
# MAGIC
# MAGIC **What it is:** A queue of one-off instructions from you to the pipeline.
# MAGIC
# MAGIC **How it works:** You INSERT a row with an action. The next job run picks it up,
# MAGIC executes it once, then marks it `consumed`. It never fires again.
# MAGIC
# MAGIC **Available actions:**
# MAGIC
# MAGIC | Action        | What it does                                                    |
# MAGIC |---------------|-----------------------------------------------------------------|
# MAGIC | FORCE_RELOAD  | Re-fetch a specific date range, overwriting existing data       |
# MAGIC | HISTORY_LOAD  | Extend history further back than the current earliest_date      |
# MAGIC | PAUSE         | Stop fetching this instrument (until you issue RESUME)          |
# MAGIC | RESUME        | Resume a paused or suspended instrument                         |
# MAGIC | SKIP_ONCE     | Skip this instrument on the next run only                       |
# MAGIC
# MAGIC **Example — re-fetch last 30 days for SPY:**
# MAGIC ```sql
# MAGIC INSERT INTO tradeanalytics.control.ingestion_command
# MAGIC     (instrument_id, action, override_start_date, override_end_date, requested_by, requested_at)
# MAGIC VALUES
# MAGIC     (505, 'FORCE_RELOAD', '2026-05-28', '2026-06-28', 'HC', current_timestamp());
# MAGIC ```
# MAGIC
# MAGIC **Example — pause QQQ:**
# MAGIC ```sql
# MAGIC INSERT INTO tradeanalytics.control.ingestion_command
# MAGIC     (instrument_id, action, requested_by, requested_at)
# MAGIC VALUES
# MAGIC     (506, 'PAUSE', 'HC', current_timestamp());
# MAGIC ```

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS tradeanalytics.control.ingestion_command (
    command_id           BIGINT    GENERATED ALWAYS AS IDENTITY,
    instrument_id        BIGINT    NOT NULL,
    action               STRING    NOT NULL,
    override_start_date  DATE,
    override_end_date    DATE,
    status               STRING    NOT NULL DEFAULT 'pending',
    requested_by         STRING,
    requested_at         TIMESTAMP NOT NULL,
    consumed_at          TIMESTAMP,
    error_message        STRING,

    CONSTRAINT pk_command       PRIMARY KEY (command_id),
    CONSTRAINT fk_command_instr FOREIGN KEY (instrument_id)
                                REFERENCES tradeanalytics.reference.instrument(instrument_id)
)
USING DELTA
COMMENT 'One-off operator instructions to the pipeline. Each command fires exactly once then is marked consumed.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
)
""")
# Valid action values: FORCE_RELOAD | HISTORY_LOAD | PAUSE | RESUME | SKIP_ONCE — enforced in Python
# Valid status values: pending | consumed | failed — enforced in Python
print("✓ control.ingestion_command")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 4 — control.job_run_log
# MAGIC
# MAGIC **What it is:** A permanent audit trail of every fetch the pipeline has ever performed.
# MAGIC
# MAGIC **How it works:** After every instrument fetch (success or failure), the pipeline
# MAGIC appends one row here. You can query it to answer:
# MAGIC - When did we last successfully fetch NVDA?
# MAGIC - How many records were rejected last Tuesday?
# MAGIC - Which instruments failed yesterday and why?
# MAGIC
# MAGIC **This table is append-only** — rows are never updated or deleted.
# MAGIC
# MAGIC **Useful queries:**
# MAGIC ```sql
# MAGIC -- Last successful run per instrument
# MAGIC SELECT instrument_id, MAX(completed_at) as last_success
# MAGIC FROM tradeanalytics.control.job_run_log
# MAGIC WHERE status = 'success'
# MAGIC GROUP BY instrument_id;
# MAGIC
# MAGIC -- All failures in the last 7 days
# MAGIC SELECT * FROM tradeanalytics.control.job_run_log
# MAGIC WHERE status = 'failed'
# MAGIC AND started_at >= current_date() - INTERVAL 7 DAYS;
# MAGIC ```

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS tradeanalytics.control.job_run_log (
    log_id              BIGINT    GENERATED ALWAYS AS IDENTITY,
    job_run_id          STRING    NOT NULL,
    instrument_id       BIGINT    NOT NULL,
    stream              STRING    NOT NULL,
    load_type           STRING    NOT NULL,
    records_new         INT       NOT NULL DEFAULT 0,
    records_amended     INT       NOT NULL DEFAULT 0,
    records_rejected    INT       NOT NULL DEFAULT 0,
    status              STRING    NOT NULL,
    error_message       STRING,
    started_at          TIMESTAMP NOT NULL,
    completed_at        TIMESTAMP,

    CONSTRAINT pk_job_run_log PRIMARY KEY (log_id),
    CONSTRAINT fk_log_instr   FOREIGN KEY (instrument_id)
                              REFERENCES tradeanalytics.reference.instrument(instrument_id)
)
USING DELTA
COMMENT 'Append-only audit trail of every pipeline fetch. Never updated or deleted.'
TBLPROPERTIES (
    'delta.appendOnly'                  = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
)
PARTITIONED BY (stream)
""")
# Valid status values: success | failed | skipped — enforced in Python (LoadType enum)
# Valid load_type values: INITIAL_LOAD | INCREMENTAL | FORCE_RELOAD | GAP_FILL | HISTORY_EXT | NO_OP | SKIP
print("✓ control.job_run_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("  Control Tables — Setup Complete")
print("=" * 60)

tables = spark.sql("SHOW TABLES IN tradeanalytics.control").toPandas()

for _, row in tables.iterrows():
    count = spark.sql(
        f"SELECT COUNT(*) as n FROM tradeanalytics.control.{row['tableName']}"
    ).collect()[0]["n"]
    print(f"  ✓ {row['tableName']:35} ({count} rows)")

print()
print("Batch config seeded:")
spark.sql("""
    SELECT job_type, batch_groups_included, max_symbols_per_run
    FROM tradeanalytics.control.ingestion_batch_config
    ORDER BY job_type
""").show(truncate=False)
