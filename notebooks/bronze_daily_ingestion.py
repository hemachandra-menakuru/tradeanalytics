# Databricks notebook source
# TradeAnalytics — Bronze Daily Ingestion

# COMMAND ----------
# Install ALL dependencies first — before any other imports
import subprocess
result = subprocess.run(
    ["pip", "install", "python-dotenv", "yfinance", "--quiet", "--no-warn-script-location"],
    capture_output=True, text=True
)
print(result.stdout)
print(result.stderr)
print("✅ Dependencies installed")

# COMMAND ----------
import sys, os, logging
from datetime import date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("bronze_daily_ingestion")

# COMMAND ----------
bundle_path = "/Workspace/Users/handh.stocks@gmail.com/.bundle/tradeanalytics/dev/files"
if bundle_path not in sys.path:
    sys.path.insert(0, bundle_path)
logger.info(f"Bundle path added: {bundle_path}")

# COMMAND ----------
try:
    symbols_param    = dbutils.widgets.get("symbols").strip()
    dry_run_param    = dbutils.widgets.get("dry_run").strip().lower()
    as_of_date_param = dbutils.widgets.get("as_of_date").strip()
    start_date_param = dbutils.widgets.get("start_date").strip()
    end_date_param   = dbutils.widgets.get("end_date").strip()
except Exception:
    symbols_param    = ""
    dry_run_param    = "false"
    as_of_date_param = ""
    start_date_param = ""
    end_date_param   = ""

symbols    = [s.strip() for s in symbols_param.split(",") if s.strip()] or None
dry_run    = dry_run_param == "true"
as_of_date = date.fromisoformat(as_of_date_param) if as_of_date_param else None
start_date = date.fromisoformat(start_date_param) if start_date_param else None
end_date   = date.fromisoformat(end_date_param)   if end_date_param   else None
logger.info(
    f"Parameters: symbols={symbols}, dry_run={dry_run}, "
    f"as_of_date={as_of_date}, start_date={start_date}, end_date={end_date}"
)

# COMMAND ----------
os.environ["IBKR_ACCOUNT_ID"] = dbutils.secrets.get("tradeanalytics", "IBKR_ACCOUNT_ID")
logger.info("Secrets loaded")

# COMMAND ----------
from src.config.config_loader import ConfigLoader
ConfigLoader.reset()
config = ConfigLoader.load(environment=os.getenv("ENVIRONMENT", "dev"))
logger.info(f"Config loaded: catalog={config.databricks.catalog}")

# COMMAND ----------
if not config.daily.enabled:
    logger.warning("Daily stream DISABLED — exiting")
    dbutils.notebook.exit("Stream disabled")

# COMMAND ----------
from src.ingestion.factory.provider_factory import MarketDataFactory
from src.ingestion.providers.yahoo_provider import YahooProvider
from src.ingestion.providers.ibkr_provider import IBKRProvider

MarketDataFactory.register("ibkr",  IBKRProvider)
MarketDataFactory.register("yahoo", YahooProvider)
config._data["sources"]["primary"]  = "yahoo"
config._data["sources"]["fallback"] = "yahoo"
logger.info("Provider set to Yahoo for Databricks")

# COMMAND ----------
from src.ingestion.jobs.bronze_ingestion_job import BronzeIngestionJob

job = BronzeIngestionJob(
    config=config,
    stream_name="daily",
    spark=spark,
)
logger.info(f"Provider: {job._provider.provider_name}")

summary = job.run(
    symbols=symbols,
    as_of_date=as_of_date,
    start_date=start_date,
    end_date=end_date,
    dry_run=dry_run,
)

# COMMAND ----------
logger.info(
    f"Written={summary.total_records_written}, "
    f"Rejected={summary.total_records_rejected}, "
    f"Skipped={len(summary.skipped)}, "
    f"Failed={len(summary.failed)}"
)
if summary.failed:
    for sym, err in summary.errors.items():
        logger.error(f"FAILED {sym}: {err}")

# COMMAND ----------
import pandas as pd
if summary.results:
    display(pd.DataFrame([{
        "symbol":          r.symbol,
        "records_written": r.records_written,
        "records_amended": r.records_amended,
        "records_skipped": r.records_skipped,
        "rejected":        r.rejected_written,
        "duration_ms":     r.duration_ms,
    } for r in summary.results]))

# COMMAND ----------
if summary.failed:
    raise Exception(f"Job failed for: {summary.failed}")

logger.info("✅ Bronze daily ingestion completed successfully")
