---
name: tradeanalytics-spark-optimization
description: >
  Spark and Delta Lake performance engineering for the TradeAnalytics medallion pipeline.
  Use this skill whenever the user asks about Spark partitioning strategy, Z-ordering,
  liquid clustering, shuffle optimisation, broadcast joins, Adaptive Query Execution (AQE),
  query plan analysis (EXPLAIN), skew handling, OOM errors, GC pressure, slow Spark jobs,
  Change Data Feed (CDF), Structured Streaming for intraday data, Delta OPTIMIZE/VACUUM,
  file size tuning, or how to make a Silver or Gold job run faster. Also trigger for
  questions about reading Bronze into Silver efficiently, incremental processing patterns,
  how to partition the feature store, or any "why is my Spark job slow" question.
  Covers the full TradeAnalytics pipeline: bronze.market_data_daily → silver.feature_store
  → gold.signal_log. Grounded in Databricks DBR 15.4 / Delta 3.x behaviour.
---

# TradeAnalytics Spark Optimisation

## Workspace context
- Databricks: `dbc-bf0075e6-07aa.cloud.databricks.com`, workspace `handh-dev`
- Unity Catalog: `tradeanalytics`
- Cluster: DBR 15.4 LTS, m5.xlarge, 1 worker, SPOT_WITH_FALLBACK
- Delta version: 3.x (bundled with DBR 15.4)

## Reference files

| File | Contents |
|---|---|
| `references/partitioning.md` | Partition strategy per table, Z-ordering, liquid clustering |
| `references/spark_tuning.md` | AQE, shuffle, joins, broadcast, OOM, GC, skew |
| `references/incremental.md` | CDF, Structured Streaming, watermark-based incremental Silver reads |
| `references/delta_maintenance.md` | OPTIMIZE, VACUUM, file compaction, statistics, time travel cost |

---

## Table-Level Optimisation Decisions (locked)

### `bronze.market_data_daily`
```sql
-- Partition by date only (not ticker — too many partitions for 8 instruments)
-- Z-order by ticker to co-locate same-ticker records within each date partition
PARTITIONED BY (DATE(trade_date))
ZORDER BY (ticker)

-- File target: 128 MB per file after OPTIMIZE
-- OPTIMIZE weekly (low write frequency, append-only)
-- VACUUM retain 30 days (need for time travel debugging)
```

### `silver.feature_store` (Phase 3)
```sql
-- Partition by cluster + date (model training reads one cluster at a time)
-- Z-order by ticker within each partition
PARTITIONED BY (cluster, DATE(as_of_date))
ZORDER BY (ticker)

-- File target: 64 MB (smaller = better for ML training random reads)
-- OPTIMIZE after each daily Silver job run
```

### `gold.signal_log` (Phase 4)
```sql
-- Partition by date only (signal consumers read by date range)
-- Z-order by ticker + cluster for point lookups
PARTITIONED BY (DATE(generated_at))
ZORDER BY (ticker, cluster)

-- OPTIMIZE daily after signal generation job
-- Enable Change Data Feed for downstream consumers
ALTER TABLE tradeanalytics.gold.signal_log
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
```

---

## Bronze → Silver Incremental Read Pattern

Never read the full Bronze table on every Silver run. Use watermarks + CDF.

```python
from delta.tables import DeltaTable

def read_bronze_incremental(spark, catalog: str, last_processed_version: int) -> DataFrame:
    """
    Read only Bronze rows added since last Silver run using CDF.
    last_processed_version: stored in control.ingestion_watermark.
    """
    return (
        spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", last_processed_version + 1)
        .table(f"{catalog}.bronze.market_data_daily")
        .filter("_change_type IN ('insert', 'update_postimage')")
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )


def get_latest_bronze_version(spark, catalog: str) -> int:
    """Get the current Delta version of Bronze for watermarking."""
    return (
        spark.sql(f"DESCRIBE HISTORY {catalog}.bronze.market_data_daily LIMIT 1")
        .collect()[0]["version"]
    )
```

**Why CDF over full re-read:** Bronze grows by ~29 records/ticker/month.
At 50 tickers × 252 trading days = 12,600 records/year. A full re-read is fast now
but degrades as history extends. CDF keeps Silver runs O(new records), not O(total).

---

## Silver Feature Engineering — Partition-Aware Computation

```python
def compute_features_partitioned(spark, catalog: str, cluster: str) -> None:
    """
    Process one cluster at a time to stay within executor memory.
    Each partition = one cluster × one date chunk.
    """
    # Read Bronze for this cluster's tickers only
    tickers = get_cluster_tickers(spark, catalog, cluster)

    bronze_df = (
        spark.table(f"{catalog}.bronze.market_data_daily")
        .filter(col("ticker").isin(tickers))
        .filter(col("data_quality_flag") == False)
        .withColumn("trade_date", col("trade_date").cast("date"))
        .repartition(8, "ticker")          # co-locate same ticker for window functions
        .sortWithinPartitions("trade_date") # ensure chronological order per ticker
    )

    # Window functions for rolling features — MUST be partitioned by ticker
    from pyspark.sql import Window
    w = Window.partitionBy("ticker").orderBy("trade_date")
    w_20 = w.rowsBetween(-19, 0)   # 20-day rolling window

    features_df = bronze_df.select(
        "ticker", "trade_date",
        avg("close").over(w_20).alias("sma_20"),
        stddev("close_return").over(w_20).alias("realised_vol_20d"),
        # ... add all features here
    )

    # Write with partition alignment
    (
        features_df
        .withColumn("cluster", lit(cluster))
        .write
        .format("delta")
        .mode("append")
        .partitionBy("cluster", "trade_date")
        .option("dataChange", "true")
        .saveAsTable(f"{catalog}.silver.feature_store")
    )
```

---

## Broadcast Join Pattern (ticker metadata lookups)

Reference table joins are small — always broadcast them.

```python
from pyspark.sql.functions import broadcast

def enrich_with_instrument_metadata(df: DataFrame, spark) -> DataFrame:
    """
    Join Bronze records with instrument reference data.
    instrument table is small (< 1000 rows) — broadcast to avoid shuffle.
    """
    instrument_ref = spark.table("tradeanalytics.reference.instrument").select(
        "ticker", "asset_class", "exchange_mic"
    )
    return df.join(broadcast(instrument_ref), on="ticker", how="left")
```

**Rule:** any table with < 10 MB should be broadcast. Set threshold explicitly:
```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)  # 10 MB
```

---

## Adaptive Query Execution (AQE) — Required Settings

AQE is on by default in DBR 15.4 but key settings must be tuned for time-series workloads:

```python
# In notebook or job cluster config
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# For time-series with many small tickers: reduce shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "32")  # default 200 is too high for 8 tickers
```

---

## Reading Query Plans

Always check EXPLAIN before running a new heavy query:

```python
# In notebook
df.explain("formatted")   # human-readable with stage boundaries
df.explain("cost")        # includes estimated row counts (needs stats)
```

**Red flags in query plans:**
- `SortMergeJoin` where you expected `BroadcastHashJoin` → table too big to broadcast
- `FileScan` with no partition pruning → missing WHERE on partition column
- `Exchange hashpartitioning` on every stage → too many shuffles, repartition earlier
- `WholeStageCodegen disabled` → UDF in the path (replace with native Spark functions)

---

## OPTIMIZE and VACUUM Schedule

```python
def run_delta_maintenance(spark, catalog: str) -> None:
    """Run after each major job. Put in a weekly maintenance job."""

    tables = [
        f"{catalog}.bronze.market_data_daily",
        f"{catalog}.silver.feature_store",
        f"{catalog}.gold.signal_log",
        f"{catalog}.control.ingestion_watermark",
    ]
    for table in tables:
        spark.sql(f"OPTIMIZE {table}")            # compact small files
        spark.sql(f"ANALYZE TABLE {table} COMPUTE STATISTICS FOR ALL COLUMNS")  # update stats for AQE
        spark.sql(f"VACUUM {table} RETAIN 720 HOURS")   # 30 days retention
```

**Never VACUUM with < 168 hours (7 days) retention** — streaming readers and
time-travel queries fail if files are deleted before they complete.
