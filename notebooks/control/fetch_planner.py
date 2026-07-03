# Databricks notebook source
# TradeAnalytics — Fetch Planner (Two-Plane Architecture, Plane 1)
#
# Scheduled serverless job (7pm ET Mon–Fri). Derives what each instrument needs
# (desired vs actual state), writes control.fetch_request rows + S3 manifests
# to control/fetch/pending/. The EC2 bridge agent consumes the manifests.
# This job fetches nothing and needs no network egress.

# COMMAND ----------
import os, sys, logging
from datetime import date

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("fetch_planner")

repo_root = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if os.path.isdir(os.path.join(repo_root, "src")) and repo_root not in sys.path:
    sys.path.insert(0, repo_root)

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

# COMMAND ----------
if IS_DATABRICKS:
    dbutils.widgets.text("symbols",     "",      "Symbols (blank = all active)")
    dbutils.widgets.text("dry_run",     "false", "Dry run (true/false)")
    dbutils.widgets.text("as_of_date",  "",      "As-of date (YYYY-MM-DD, blank = today)")
    dbutils.widgets.text("environment", "dev",   "Deployment environment")

    symbols_param    = dbutils.widgets.get("symbols").strip()
    dry_run          = dbutils.widgets.get("dry_run").strip().lower() == "true"
    as_of_date_param = dbutils.widgets.get("as_of_date").strip()
    os.environ.setdefault("ENVIRONMENT", dbutils.widgets.get("environment").strip() or "dev")
else:
    symbols_param    = os.environ.get("PLANNER_SYMBOLS", "")
    dry_run          = os.environ.get("PLANNER_DRY_RUN", "false").lower() == "true"
    as_of_date_param = os.environ.get("PLANNER_AS_OF_DATE", "")

symbols    = [s.strip() for s in symbols_param.split(",") if s.strip()] or None
as_of_date = date.fromisoformat(as_of_date_param) if as_of_date_param else None

# COMMAND ----------
from src.shared.config.config_loader import ConfigLoader
ConfigLoader.reset()
config = ConfigLoader.load(environment=os.getenv("ENVIRONMENT", "dev"))

if not IS_DATABRICKS:
    from databricks.connect import DatabricksSession
    _profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "handh-trade-aws")
    spark = DatabricksSession.builder.profile(_profile).serverless(True).getOrCreate()
    # Local manifest writes go through Spark (no dbutils locally)
    def _fs_put(path: str, contents: str, overwrite: bool = True) -> None:
        from pyspark.sql import Row
        # dbutils unavailable off-Databricks; use spark to write a single-text file
        raise NotImplementedError(
            "Manifest writing from local runs is not supported — run the planner "
            "as a Databricks job, or use dry_run=true locally."
        )
else:
    def _fs_put(path: str, contents: str, overwrite: bool = True) -> None:
        dbutils.fs.put(path, contents, overwrite)

# COMMAND ----------
from src.control.jobs.fetch_planner_job import FetchPlannerJob

job = FetchPlannerJob(config=config, spark=spark, fs_put=_fs_put, stream_name="daily", vendor="ibkr")
summary = job.run(symbols=symbols, as_of_date=as_of_date, dry_run=dry_run)

print("=" * 60)
print("  FETCH PLANNER SUMMARY")
print("=" * 60)
for k, v in summary.items():
    print(f"  {k:<20} {v}")
print("=" * 60)

# COMMAND ----------
if summary["requests_emitted"] == 0 and not summary["skipped_noop"]:
    logger.warning("No requests emitted and nothing was up-to-date — check universe/config")
