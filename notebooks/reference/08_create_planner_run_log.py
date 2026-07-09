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
# MAGIC %md
# MAGIC ## Unified run timeline — v_all_runs
# MAGIC One row per job run across ALL jobs, so "what ran, when, how long" is a
# MAGIC single query without knowing which table each job logs to. Planner rows
# MAGIC come from planner_run_log (already run-level); ingest rows are aggregated
# MAGIC from job_run_log (which is per-instrument). Interim until ENH-6 unifies
# MAGIC the underlying tables (CLAUDE.md §3.5).

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW tradeanalytics.control.v_all_runs AS
# MAGIC SELECT 'fetch_planner' AS job_type, batch_id,
# MAGIC        run_started_at, run_completed_at, duration_seconds,
# MAGIC        stream, vendor,
# MAGIC        requests_emitted AS items, instruments_evaluated AS instruments,
# MAGIC        'completed' AS status, dry_run, pipeline_version
# MAGIC FROM tradeanalytics.control.planner_run_log
# MAGIC UNION ALL
# MAGIC SELECT job_type, batch_id,
# MAGIC        MIN(run_started_at), MAX(run_completed_at), SUM(duration_seconds),
# MAGIC        MAX(stream), MAX(vendor),
# MAGIC        SUM(records_new) AS items, COUNT(*) AS instruments,
# MAGIC        CASE WHEN MIN(status) = 'success' THEN 'success' ELSE 'partial' END AS status,
# MAGIC        false AS dry_run, MAX(pipeline_version)
# MAGIC FROM tradeanalytics.control.job_run_log
# MAGIC GROUP BY job_type, batch_id;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Unified timeline — every job run, most recent first
# MAGIC SELECT job_type, batch_id, run_started_at, duration_seconds,
# MAGIC        instruments, items, status
# MAGIC FROM tradeanalytics.control.v_all_runs
# MAGIC ORDER BY run_started_at DESC;
