# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — control.planner_run_log (planner run-timing audit)
# MAGIC
# MAGIC One row per FetchPlannerJob run — closes the observability gap that the
# MAGIC planner has no measurable duration (fetch_request.requested_at is a
# MAGIC single-transaction timestamp, so it can't time the planner).
# MAGIC
# MAGIC **Note:** `FetchPlannerJob` also creates this table on first run
# MAGIC (CREATE TABLE IF NOT EXISTS) so it works without running this notebook —
# MAGIC this is the canonical DDL of record (rule: schema lives in a notebook AND
# MAGIC in code). Append-only. Pure %sql, safe to re-run.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tradeanalytics.control.planner_run_log (
# MAGIC     run_id                 BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     batch_id               STRING NOT NULL,
# MAGIC     stream                 STRING NOT NULL,
# MAGIC     vendor                 STRING NOT NULL,
# MAGIC     run_started_at         TIMESTAMP NOT NULL,
# MAGIC     run_completed_at       TIMESTAMP NOT NULL,
# MAGIC     duration_seconds       DOUBLE NOT NULL,
# MAGIC     instruments_evaluated  INT NOT NULL,
# MAGIC     requests_emitted       INT NOT NULL,
# MAGIC     skipped_noop           INT NOT NULL,
# MAGIC     skipped_inflight       INT NOT NULL,
# MAGIC     skipped_unmapped       INT NOT NULL,
# MAGIC     orphans_repaired       INT NOT NULL,
# MAGIC     dry_run                BOOLEAN NOT NULL,
# MAGIC     pipeline_version       STRING
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Append-only audit of FetchPlannerJob runs — one row per run.'
# MAGIC TBLPROPERTIES ('delta.appendOnly'='true');

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Planner run history (most recent first)
# MAGIC SELECT batch_id, run_started_at, duration_seconds,
# MAGIC        instruments_evaluated, requests_emitted,
# MAGIC        skipped_unmapped, orphans_repaired, dry_run
# MAGIC FROM tradeanalytics.control.planner_run_log
# MAGIC ORDER BY run_started_at DESC;
