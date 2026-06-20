# Databricks notebook source
# TradeAnalytics — Bronze Daily Ingestion
# =========================================
# Entry point for the daily Bronze ingestion job.
# Runs as a Databricks job on schedule: 7:00 PM Mon-Fri Eastern.
#
# Parameters (passed via Databricks job widgets):
#   symbols:    comma-separated list or empty (all active)
#   dry_run:    true/false (plan only, no writes)
#   as_of_date: YYYY-MM-DD or empty (today)
#
# To run manually in Databricks notebook:
#   dbutils.widgets.text("symbols", "")
#   dbutils.widgets.text("dry_run", "false")
#   dbutils.widgets.text("as_of_date", "")

# COMMAND ----------

import sys
import logging
from datetime import date

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("bronze_daily_ingestion")

# COMMAND ----------

# Read job parameters
try:
    symbols_param   = dbutils.widgets.get("symbols").strip()
    dry_run_param   = dbutils.widgets.get("dry_run").strip().lower()
    as_of_date_param = dbutils.widgets.get("as_of_date").strip()
except Exception:
    # Running locally or without widgets
    symbols_param    = ""
    dry_run_param    = "false"
    as_of_date_param = ""

symbols    = [s.strip() for s in symbols_param.split(",") if s.strip()] or None
dry_run    = dry_run_param == "true"
as_of_date = date.fromisoformat(as_of_date_param) if as_of_date_param else None

logger.info(
    f"Job parameters: symbols={symbols}, dry_run={dry_run}, "
    f"as_of_date={as_of_date}"
)

# COMMAND ----------

# Load config
from src.config.config_loader import ConfigLoader

config = ConfigLoader.load(environment="dev")
logger.info(f"Config loaded: catalog={config.databricks.catalog}")

# COMMAND ----------

# Validate daily stream is enabled
if not config.daily.enabled:
    logger.warning("Daily stream is DISABLED in config — exiting")
    dbutils.notebook.exit("Stream disabled")

# COMMAND ----------

# Run ingestion
from src.ingestion.jobs.bronze_ingestion_job import BronzeIngestionJob

job = BronzeIngestionJob(
    config=config,
    stream_name="daily",
    spark=spark,  # Databricks spark session
)

summary = job.run(
    symbols=symbols,
    as_of_date=as_of_date,
    dry_run=dry_run,
)

# COMMAND ----------

# Log summary
logger.info(f"Job completed: {summary}")
logger.info(f"Total records written: {summary.total_records_written}")
logger.info(f"Total records rejected: {summary.total_records_rejected}")
logger.info(f"Symbols skipped (no-op): {len(summary.skipped)}")

if summary.failed:
    logger.error(f"Failed symbols: {summary.failed}")
    for sym, err in summary.errors.items():
        logger.error(f"  {sym}: {err}")

# COMMAND ----------

# Display summary table (Databricks display)
import pandas as pd

results_data = [
    {
        "symbol":          r.symbol,
        "records_written": r.records_written,
        "records_amended": r.records_amended,
        "records_skipped": r.records_skipped,
        "rejected":        r.rejected_written,
        "duration_ms":     r.duration_ms,
    }
    for r in summary.results
]

if results_data:
    display(pd.DataFrame(results_data))

# COMMAND ----------

# Exit with status
if summary.failed:
    raise Exception(
        f"Job completed with {len(summary.failed)} failures: "
        f"{summary.failed}"
    )

logger.info("Bronze daily ingestion completed successfully")
