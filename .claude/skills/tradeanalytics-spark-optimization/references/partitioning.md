# Partitioning, Z-Ordering & Liquid Clustering

## The TradeAnalytics Partitioning Decision Tree

```
Is the table queried by date range as the PRIMARY filter?
    YES → PARTITION BY date column
    NO  → PARTITION BY the most common filter dimension

After partitioning, are there secondary filter columns (ticker, cluster)?
    YES → ZORDER BY those columns (Delta < 3.1) or CLUSTER BY (Delta 3.1+)
    NO  → No Z-order needed
```

## Why NOT to Over-Partition

The most common mistake: `PARTITION BY (ticker, trade_date)`.

At 8 tickers × 252 trading days × 10 years = 20,160 partition directories.
Spark opens one file handle per partition per task. With 200 shuffle partitions
and 20,160 input partitions: **4M+ file handles**, guaranteed OOM or timeout.

**Rule:** Maximum number of distinct partition values = `(cluster_memory_GB × 1000)`.
For m5.xlarge (16 GB): max ~16,000 partitions. With 8 tickers × 252 days = 2,016 → safe.
Date-only partitioning: 252 partitions/year → always safe.

## Z-Ordering vs Liquid Clustering

| Feature | Z-ORDER BY | CLUSTER BY (liquid) |
|---|---|---|
| Delta version | All versions | Delta 3.1+ (DBR 14.1+) |
| When applied | On OPTIMIZE | Incrementally on write |
| Best for | Infrequent OPTIMIZE | High-frequency writes |
| Columns | Up to 4 | Up to 4 |
| Rewrite on schema change | Full rewrite | Incremental |

**TradeAnalytics decision:** Use Z-ORDER BY (standard). DBR 15.4 supports liquid clustering
but Bronze is append-only (OPTIMIZE weekly) and Silver runs daily → Z-ORDER is fine.

```sql
-- Apply after table creation
OPTIMIZE tradeanalytics.bronze.market_data_daily
ZORDER BY (ticker);

-- Or on Silver feature store (after each daily run)
OPTIMIZE tradeanalytics.silver.feature_store
WHERE cluster = 'momentum'   -- only optimize the partition just written
ZORDER BY (ticker);
```

## Partition Pruning Verification

```python
# Verify partition pruning is working — check FileScan in plan
spark.sql("""
    SELECT ticker, close, trade_date
    FROM tradeanalytics.bronze.market_data_daily
    WHERE trade_date >= '2026-01-01'   -- partition column → pruning fires
    AND ticker = 'SPY'                  -- Z-order column → data skipping fires
""").explain("formatted")
```

Look for: `PartitionFilters: [isnotnull(trade_date#...), (trade_date#... >= 2026-01-01)]`
If missing: the WHERE clause doesn't reference the partition column — full scan.

## File Size Targets

| Table | Target file size | Rationale |
|---|---|---|
| Bronze daily | 128 MB | Sequential scans; large files = fewer opens |
| Silver feature store | 64 MB | ML training reads random rows; smaller = less over-read |
| Gold signal log | 128 MB | Sequential date-range scans |
| Reference tables | No target | Too small to matter |
| Control tables | No target | Too small to matter |

```python
# Set target file size per table
spark.sql("""
    ALTER TABLE tradeanalytics.silver.feature_store
    SET TBLPROPERTIES (
        'delta.targetFileSize' = '67108864',  -- 64 MB in bytes
        'delta.tuneFileSizesForRewrites' = 'true'
    )
""")
```

## Stats Collection for Data Skipping

Delta maintains column-level min/max stats for data skipping. Collect after bulk writes:

```python
spark.sql("""
    ANALYZE TABLE tradeanalytics.bronze.market_data_daily
    COMPUTE STATISTICS FOR COLUMNS ticker, trade_date, close, volume
""")
```

Without stats, data skipping cannot fire on non-partition columns.
With stats on `ticker` + Z-ordering: point lookups for one ticker scan only
the relevant file ranges, not the full partition.
