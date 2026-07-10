# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Readable views over instrument_id-keyed tables
# MAGIC
# MAGIC **Why:** control/reference tables key on `instrument_id` (never symbol —
# MAGIC symbols change, e.g. FB→META, and live in ONE place: `instrument_listing`,
# MAGIC SCD-2). These views join the current symbol/company back in for READ
# MAGIC convenience, WITHOUT denormalizing storage. A rename shows up instantly
# MAGIC via the `is_current=true` join — the view can never go stale.
# MAGIC
# MAGIC Use the `v_*` views for operational queries; write to the base tables.
# MAGIC Pure `%sql`, safe to re-run (CREATE OR REPLACE VIEW).

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Desired-state feed config, with symbol/company/conId joined live
# MAGIC CREATE OR REPLACE VIEW tradeanalytics.reference.v_ticker_feed_config AS
# MAGIC SELECT fc.config_id, fc.instrument_id,
# MAGIC        l.symbol, l.company_name, l.exchange_mic, i.asset_class,
# MAGIC        v.vendor_instrument_id AS ibkr_conid,
# MAGIC        fc.stream, fc.is_active, fc.batch_group, fc.priority,
# MAGIC        fc.target_start_date, fc.run_frequency, fc.max_lookback_days, fc.updated_at
# MAGIC FROM tradeanalytics.reference.ticker_feed_config fc
# MAGIC JOIN tradeanalytics.reference.instrument i
# MAGIC      ON i.instrument_id = fc.instrument_id
# MAGIC LEFT JOIN tradeanalytics.reference.instrument_listing l
# MAGIC      ON l.instrument_id = fc.instrument_id AND l.is_current = true
# MAGIC LEFT JOIN tradeanalytics.reference.instrument_vendor_id v
# MAGIC      ON v.instrument_id = fc.instrument_id AND v.vendor='ibkr' AND v.is_current = true;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Actual-state watermark, with symbol joined live
# MAGIC CREATE OR REPLACE VIEW tradeanalytics.control.v_ingestion_watermark AS
# MAGIC SELECT w.*, l.symbol, l.company_name
# MAGIC FROM tradeanalytics.control.ingestion_watermark w
# MAGIC LEFT JOIN tradeanalytics.reference.instrument_listing l
# MAGIC      ON l.instrument_id = w.instrument_id AND l.is_current = true;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Work queue, with symbol/company joined live (fetch_request already carries
# MAGIC -- a denormalised symbol for the agent, but the view adds company + current listing)
# MAGIC CREATE OR REPLACE VIEW tradeanalytics.control.v_fetch_request AS
# MAGIC SELECT fr.request_key, fr.batch_id, fr.instrument_id,
# MAGIC        l.symbol, l.company_name, fr.vendor, fr.vendor_instrument_id,
# MAGIC        fr.stream, fr.bar_interval, fr.start_date, fr.end_date,
# MAGIC        fr.load_type, fr.status, fr.record_count, fr.error_message, fr.requested_at
# MAGIC FROM tradeanalytics.control.fetch_request fr
# MAGIC LEFT JOIN tradeanalytics.reference.instrument_listing l
# MAGIC      ON l.instrument_id = fr.instrument_id AND l.is_current = true;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Audit log, with symbol joined live
# MAGIC CREATE OR REPLACE VIEW tradeanalytics.control.v_job_run_log AS
# MAGIC SELECT j.*, l.symbol
# MAGIC FROM tradeanalytics.control.job_run_log j
# MAGIC LEFT JOIN tradeanalytics.reference.instrument_listing l
# MAGIC      ON l.instrument_id = j.instrument_id AND l.is_current = true;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Quick sanity: active feed config, human-readable
# MAGIC SELECT symbol, company_name, asset_class, batch_group, target_start_date, ibkr_conid
# MAGIC FROM tradeanalytics.reference.v_ticker_feed_config
# MAGIC WHERE is_active = true ORDER BY symbol;
