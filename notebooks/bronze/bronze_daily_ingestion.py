# Databricks notebook source
# TradeAnalytics — Bronze Daily Ingestion  ⚠ DEV / AD-HOC TOOL, NOT the production path
#
# ─── STATUS (2026-07-05) ────────────────────────────────────────────────────
# SUPERSEDED for production by the Two-Plane trio (CLAUDE.md §14):
#   fetch_planner (serverless) → EC2 fetch agent → raw_to_bronze (serverless)
# This notebook remains ONLY as the LOCAL DEV / AD-HOC path: it fetches
# inline via the provider chain and writes Bronze directly — useful for
# quick experiments and for exercising the provider code end-to-end.
#
# WHERE TO RUN — local Mac ONLY (conda env `tradeanalytics`):
#   cd ~/pr/tradeanalytics
#   INGEST_DRY_RUN=true INGEST_SYMBOLS=SPY \
#     python -c "exec(open('notebooks/bronze/bronze_daily_ingestion.py').read())"
#   Python (incl. IBKR calls) runs on the Mac; Spark writes go through
#   Databricks Connect (serverless) to Unity Catalog.
#
# WHY IT CANNOT RUN AS A CLOUD JOB: Databricks serverless has zero egress —
# ibkr (localhost REST) and ibinsync (EC2, SG-blocked) both fail their health
# checks, and the production guard (sources.production_providers) then raises
# rather than silently ingesting from yahoo. This is intentional.
#
# NOTE: writes are dedup-safe against the production path (Bronze Layer-2
# classify), but ad-hoc runs bypass control.fetch_request / job_run_log —
# there is NO queue audit trail for data ingested this way.
# ────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# ── Environment detection ────────────────────────────────────────────────────
import os, sys, logging
from datetime import date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("bronze_daily_ingestion")

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
IS_SERVERLESS = IS_DATABRICKS and (
    "client." in os.environ.get("DATABRICKS_RUNTIME_VERSION", "")
    or os.environ.get("IS_SERVERLESS", "").lower() == "true"
)

if IS_SERVERLESS:
    EXEC_ENV = "databricks-serverless"
elif IS_DATABRICKS:
    EXEC_ENV = "databricks-cluster"
else:
    EXEC_ENV = "local"

logger.info(f"Execution environment detected: {EXEC_ENV}")

# COMMAND ----------
# ── Import path — no hardcoded workspace paths ──────────────────────────────
# On Databricks, workspace-files notebooks run with CWD = the notebook's folder,
# so the repo root is two levels up (notebooks/bronze → root). Locally, run from
# the repo root or the same relative layout applies.
repo_root = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if os.path.isdir(os.path.join(repo_root, "src")):
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    logger.info(f"Repo root on sys.path: {repo_root}")
else:
    # Already at repo root (local execution) — nothing to add
    logger.info(f"Repo root assumed = CWD: {os.getcwd()}")

# COMMAND ----------
# ── Parameters ───────────────────────────────────────────────────────────────
if IS_DATABRICKS:
    dbutils.widgets.text("symbols",           "",      "Symbols (comma-separated, blank = all active)")
    dbutils.widgets.text("dry_run",           "false", "Dry run (true/false)")
    dbutils.widgets.text("as_of_date",        "",      "As-of date (YYYY-MM-DD, blank = today)")
    dbutils.widgets.text("start_date",        "",      "Start date override (YYYY-MM-DD, blank = planner)")
    dbutils.widgets.text("end_date",          "",      "End date override (YYYY-MM-DD, blank = planner)")
    dbutils.widgets.text("environment",       "dev",   "Deployment environment")
    dbutils.widgets.text("pipeline_version",  "",      "Pipeline version (git SHA)")

    symbols_param     = dbutils.widgets.get("symbols").strip()
    dry_run_param     = dbutils.widgets.get("dry_run").strip().lower()
    as_of_date_param  = dbutils.widgets.get("as_of_date").strip()
    start_date_param  = dbutils.widgets.get("start_date").strip()
    end_date_param    = dbutils.widgets.get("end_date").strip()

    # Serverless has no spark_env_vars — env settings arrive as parameters
    os.environ.setdefault("ENVIRONMENT", dbutils.widgets.get("environment").strip() or "dev")
    pv = dbutils.widgets.get("pipeline_version").strip()
    if pv:
        os.environ["PIPELINE_VERSION"] = pv
else:
    # Local execution — parameters via environment variables (or defaults)
    symbols_param     = os.environ.get("INGEST_SYMBOLS", "")
    dry_run_param     = os.environ.get("INGEST_DRY_RUN", "false").lower()
    as_of_date_param  = os.environ.get("INGEST_AS_OF_DATE", "")
    start_date_param  = os.environ.get("INGEST_START_DATE", "")
    end_date_param    = os.environ.get("INGEST_END_DATE", "")

symbols    = [s.strip() for s in symbols_param.split(",") if s.strip()] or None
dry_run    = dry_run_param == "true"
as_of_date = date.fromisoformat(as_of_date_param) if as_of_date_param else None
start_date = date.fromisoformat(start_date_param) if start_date_param else None
end_date   = date.fromisoformat(end_date_param)   if end_date_param   else None

logger.info(
    f"Parameters: symbols={symbols}, dry_run={dry_run}, as_of_date={as_of_date}, "
    f"start_date={start_date}, end_date={end_date}"
)
# Network diagnostics live in notebooks/ops/connectivity_test.py — not here.

# COMMAND ----------
# ── Secrets ──────────────────────────────────────────────────────────────────
if IS_DATABRICKS:
    os.environ["IBKR_ACCOUNT_ID"] = dbutils.secrets.get("tradeanalytics", "IBKR_ACCOUNT_ID")
    logger.info("Secrets loaded from Databricks secret scope")
# Local: ConfigLoader picks up .env via python-dotenv — nothing to do

# COMMAND ----------
# ── Config ───────────────────────────────────────────────────────────────────
from src.shared.config.config_loader import ConfigLoader
ConfigLoader.reset()
config = ConfigLoader.load(environment=os.getenv("ENVIRONMENT", "dev"))
logger.info(f"Config loaded: catalog={config.databricks.catalog}")

if not config.daily.enabled:
    logger.warning("Daily stream DISABLED — exiting")
    if IS_DATABRICKS:
        dbutils.notebook.exit("Stream disabled")
    raise SystemExit(0)

# COMMAND ----------
# ── Providers — register ALL, let the priority chain + guard decide ─────────
# Local Mac:  ibkr (REST, localhost:5055) resolves first when the gateway is up.
# Databricks: ibkr health check fails (localhost unreachable) → ibinsync (EC2
#             gateway) resolves. yahoo stays registered for tests, but the
#             production guard (sources.production_providers) prevents it from
#             ever silently becoming the production source.
from src.bronze.factory.provider_factory import MarketDataFactory
from src.bronze.providers.ibkr_provider import IBKRProvider
from src.bronze.providers.ibinsync_provider import IBInsyncProvider
from src.bronze.providers.yahoo_provider import YahooProvider

MarketDataFactory.register("ibkr",     IBKRProvider)
MarketDataFactory.register("ibinsync", IBInsyncProvider)
MarketDataFactory.register("yahoo",    YahooProvider)
logger.info(f"Providers registered — priority={list(config.get('sources.priority', default=[]))}")

# COMMAND ----------
# ── Spark session ────────────────────────────────────────────────────────────
# On Databricks, `spark` is provided by the runtime. Locally we create one via
# Databricks Connect targeting SERVERLESS compute (serverless-only decision):
# Python (incl. IBKR calls) runs on the Mac; Spark reads/writes go to Unity Catalog.
if not IS_DATABRICKS:
    from databricks.connect import DatabricksSession
    _profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "handh-trade-aws")
    spark = (
        DatabricksSession.builder
        .profile(_profile)
        .serverless(True)
        .getOrCreate()
    )
    logger.info(f"Databricks Connect session created — profile={_profile}, compute=serverless")

# COMMAND ----------
# ── Universe reader ──────────────────────────────────────────────────────────
from src.reference.readers.delta_universe_reader import DeltaUniverseReader

universe_reader = DeltaUniverseReader(
    mode="spark",
    spark=spark,
    catalog=config.databricks.catalog,
)
logger.info(
    f"Universe reader ready — "
    f"{len(universe_reader.get_active_instruments(symbols=symbols))} active instruments"
)

# COMMAND ----------
# ── Run ingestion ────────────────────────────────────────────────────────────
from src.bronze.jobs.bronze_ingestion_job import BronzeIngestionJob

job = BronzeIngestionJob(
    config=config,
    stream_name="daily",
    spark=spark,
    ticker_reader=universe_reader,
)
logger.info(f"Provider: {job._provider.provider_name} | exec_env: {EXEC_ENV}")

summary = job.run(
    symbols=symbols,
    as_of_date=as_of_date,
    start_date=start_date,
    end_date=end_date,
    dry_run=dry_run,
)

# COMMAND ----------
# ── Results ──────────────────────────────────────────────────────────────────
logger.info(
    f"Written={summary.total_records_written}, "
    f"Rejected={summary.total_records_rejected}, "
    f"Skipped={len(summary.skipped)}, "
    f"Failed={len(summary.failed)}"
)
if summary.failed:
    for sym, err in summary.errors.items():
        logger.error(f"FAILED {sym}: {err}")

import pandas as pd
if summary.results:
    results_df = pd.DataFrame([{
        "symbol":          r.symbol,
        "records_written": r.records_written,
        "records_amended": r.records_amended,
        "records_skipped": r.records_skipped,
        "rejected":        r.rejected_written,
        "duration_ms":     r.duration_ms,
    } for r in summary.results])
    if IS_DATABRICKS:
        display(results_df)
    else:
        print(results_df.to_string(index=False))

if summary.failed:
    raise Exception(f"Job failed for: {summary.failed}")

logger.info("✅ Bronze daily ingestion completed successfully")
