"""
Local smoke test for bronze ingestion via Databricks Connect.

Python (incl. IBKR HTTP calls) runs on your Mac.
Spark writes go to Unity Catalog on the Databricks cluster.

Usage:
    conda activate tradeanalytics
    python scripts/smoke_test_local.py --symbols SPY --start 2026-06-16 --end 2026-06-17
    python scripts/smoke_test_local.py --symbols SPY,QQQ            # last 2 trading days
    python scripts/smoke_test_local.py --symbols SPY --dry-run      # plan only, no writes
"""

import argparse
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

# ── Project root on sys.path ───────────────────────────────────────────────────
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("smoke_test_local")


def main():
    parser = argparse.ArgumentParser(description="Bronze ingestion smoke test (local + IBKR)")
    parser.add_argument("--symbols",   default="SPY", help="Comma-separated symbols (default: SPY)")
    parser.add_argument("--start",     help="start_date YYYY-MM-DD (default: 2 days ago)")
    parser.add_argument("--end",       help="end_date YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--dry-run",   action="store_true", help="Plan only — no writes")
    args = parser.parse_args()

    symbols    = [s.strip() for s in args.symbols.split(",") if s.strip()]
    dry_run    = args.dry_run
    today      = date.today()
    start_date = date.fromisoformat(args.start) if args.start else today - timedelta(days=5)
    end_date   = date.fromisoformat(args.end)   if args.end   else today - timedelta(days=1)

    logger.info(
        f"Smoke test: symbols={symbols}, start={start_date}, end={end_date}, dry_run={dry_run}"
    )

    # ── IBKR account from .env ────────────────────────────────────────────────
    from dotenv import load_dotenv
    load_dotenv()
    ibkr_account = os.getenv("IBKR_ACCOUNT_ID")
    if not ibkr_account:
        raise RuntimeError(
            "IBKR_ACCOUNT_ID not found in .env — "
            "add it with IBKR_ACCOUNT_ID=U5498892"
        )

    # ── Databricks Connect SparkSession ──────────────────────────────────────
    logger.info("Connecting to Databricks cluster via Databricks Connect ...")
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.profile("handh-trade-aws").getOrCreate()
    logger.info("Spark session ready")

    # ── Config ────────────────────────────────────────────────────────────────
    from src.shared.config.config_loader import ConfigLoader
    ConfigLoader.reset()
    config = ConfigLoader.load(environment="dev")
    logger.info(f"Config loaded: catalog={config.databricks.catalog}")

    # ── Provider registration ─────────────────────────────────────────────────
    from src.bronze.factory.provider_factory import MarketDataFactory
    from src.bronze.providers.ibkr_provider import IBKRProvider
    from src.bronze.providers.yahoo_provider import YahooProvider

    MarketDataFactory.register("ibkr",  IBKRProvider)
    MarketDataFactory.register("yahoo", YahooProvider)
    logger.info(f"Providers: primary={config.sources.primary}")

    # ── Universe reader (DeltaUniverseReader — populates instrument_id) ───────
    from src.reference.readers.delta_universe_reader import DeltaUniverseReader

    universe_reader = DeltaUniverseReader(
        mode="spark", spark=spark, catalog=config.databricks.catalog
    )
    active = universe_reader.get_active_instruments(symbols=symbols)
    logger.info(f"Universe: {len(active)} instruments — {[i.symbol for i in active]}")
    for inst in active:
        logger.info(f"  {inst.symbol}: instrument_id={inst.instrument_id}, history_start={inst.effective_history_start}")

    # ── Run ingestion job ─────────────────────────────────────────────────────
    from src.bronze.jobs.bronze_ingestion_job import BronzeIngestionJob

    job = BronzeIngestionJob(
        config=config,
        stream_name="daily",
        spark=spark,
        ticker_reader=universe_reader,
    )
    logger.info(f"Provider: {job._provider.provider_name}")

    summary = job.run(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        dry_run=dry_run,
    )

    # ── Results ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("SMOKE TEST RESULTS")
    print("=" * 60)
    print(f"  Written  : {summary.total_records_written}")
    print(f"  Rejected : {summary.total_records_rejected}")
    print(f"  Skipped  : {len(summary.skipped)}")
    print(f"  Failed   : {len(summary.failed)}")

    for r in summary.results:
        status = "✅" if r.records_written > 0 or r.records_amended > 0 else "⚠️ "
        print(
            f"  {status} {r.symbol}: "
            f"written={r.records_written}, amended={r.records_amended}, "
            f"rejected={r.rejected_written}, duration={r.duration_ms}ms"
        )

    if summary.failed:
        print("\nFAILED:")
        for sym, err in summary.errors.items():
            print(f"  ❌ {sym}: {err}")
        sys.exit(1)

    print("\n✅ Smoke test completed successfully")


if __name__ == "__main__":
    main()
