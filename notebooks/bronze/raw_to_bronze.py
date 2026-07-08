# Databricks notebook source
# TradeAnalytics — Raw → Bronze Ingestion (Two-Plane Stage 4)
#
# Reconciles agent receipts into control.fetch_request, ingests landed raw
# payloads into Bronze (validate → dedup-append), updates watermarks, writes
# control.job_run_log audit rows, archives receipts.
# Serverless-safe: S3 + Delta only, no egress. Idempotent — safe to re-run.

# COMMAND ----------
# MAGIC %pip install --quiet pyyaml python-dotenv

# COMMAND ----------
import os, sys, logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("raw_to_bronze")

repo_root = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if os.path.isdir(os.path.join(repo_root, "src")) and repo_root not in sys.path:
    sys.path.insert(0, repo_root)

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

# COMMAND ----------
if IS_DATABRICKS:
    dbutils.widgets.text("dry_run",          "false", "Dry run (true/false)")
    dbutils.widgets.text("vendor",           "ibkr",  "Vendor queue")
    dbutils.widgets.text("environment",      "dev",   "Deployment environment")
    dbutils.widgets.text("pipeline_version", "",      "Pipeline version (git SHA)")
    # COST GATE: heavy ingestion runs only when execute=true. The deployed Job
    # sets this in databricks.yml base_parameters. Interactive "Run all" leaves
    # it false → the expensive job.run() is SKIPPED. This exists because a manual
    # interactive run on 2026-07-07 ran for hours (O(N^2), since fixed) on a warm
    # serverless session and billed ~77 DBU. Prefer the JOB; it auto-terminates
    # and enforces the 3600s timeout that interactive sessions do NOT have.
    dbutils.widgets.text("execute",          "false", "Execute ingestion (true=run heavy job)")

    dry_run = dbutils.widgets.get("dry_run").strip().lower() == "true"
    vendor  = dbutils.widgets.get("vendor").strip() or "ibkr"
    execute = dbutils.widgets.get("execute").strip().lower() == "true"
    os.environ.setdefault("ENVIRONMENT", dbutils.widgets.get("environment").strip() or "dev")
    pv = dbutils.widgets.get("pipeline_version").strip()
    if pv:
        os.environ["PIPELINE_VERSION"] = pv
else:
    dry_run = os.environ.get("INGEST_DRY_RUN", "false").lower() == "true"
    vendor  = os.environ.get("INGEST_VENDOR", "ibkr")
    execute = os.environ.get("INGEST_EXECUTE", "false").lower() == "true"

# COMMAND ----------
from src.shared.config.config_loader import ConfigLoader
ConfigLoader.reset()
config = ConfigLoader.load(environment=os.getenv("ENVIRONMENT", "dev"))

if not IS_DATABRICKS:
    from databricks.connect import DatabricksSession
    _profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "handh-trade-aws")
    spark = DatabricksSession.builder.profile(_profile).serverless(True).getOrCreate()
    raise SystemExit("Run this notebook on Databricks (dbutils.fs required for S3 receipts).")

_fs_ls   = dbutils.fs.ls
_fs_head = lambda p: dbutils.fs.head(p, 100_000_000)
_fs_put  = dbutils.fs.put
_fs_rm   = dbutils.fs.rm

# COMMAND ----------
from src.bronze.jobs.raw_to_bronze_job import RawToBronzeJob

if not execute:
    msg = (
        "\n" + "=" * 68 +
        "\n  COST GATE: ingestion NOT executed (execute=false).\n"
        "  This is the safe default so an accidental interactive 'Run all'\n"
        "  never starts a long, billable serverless session.\n\n"
        "  To actually ingest:\n"
        "   • RECOMMENDED — run the deployed Job (auto-terminates, 1h timeout):\n"
        "       databricks bundle run raw_to_bronze\n"
        "       or  Workflows -> [dev] Raw to Bronze Ingestion -> Run now\n"
        "   • Interactive (only for debugging): set the 'execute' widget to true,\n"
        "     and DETACH the serverless session when done.\n" +
        "=" * 68
    )
    print(msg)
    dbutils.notebook.exit("skipped: execute=false (cost gate)")

job = RawToBronzeJob(
    config=config, spark=spark,
    fs_ls=_fs_ls, fs_head=_fs_head, fs_put=_fs_put, fs_rm=_fs_rm,
    stream_name="daily", vendor=vendor,
)
summary = job.run(dry_run=dry_run)

print("=" * 60)
print("  RAW → BRONZE SUMMARY")
print("=" * 60)
for k, v in summary.items():
    print(f"  {k:<22} {v}")
print("=" * 60)

# COMMAND ----------
# Operator verification (also single SQL from anywhere):
display(spark.sql(f"""
    SELECT status, COUNT(*) AS requests
    FROM {config.databricks.catalog}.control.fetch_request GROUP BY status
"""))
display(spark.sql(f"""
    SELECT source, COUNT(*) AS bars, MIN(bar_date) AS earliest, MAX(bar_date) AS latest
    FROM {config.databricks.catalog}.bronze.{config.daily.table}
    GROUP BY source
"""))
