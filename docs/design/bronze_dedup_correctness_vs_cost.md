# Bronze Deduplication — Correctness vs Cost (design note)

**Status:** decided 2026-07-08. Revises the "three-layer dedup" description in
CLAUDE.md §2 with a cost-aware refinement. Driven by the 2026-07-07 O(N²) stall
and a critical review of *why* Bronze dedup exists.

## The question

Do we need Bronze-write-time deduplication (Layer 2) at all, given that Silver
reads Bronze through a `ROW_NUMBER() OVER (PARTITION BY key ORDER BY
record_version DESC)` window (Layer 3) that already returns exactly one row per
key?

## Finding — Layer 2 is a COST/STORAGE optimization, not a correctness requirement

**Correctness is guaranteed by Layer 3 alone.** If Bronze contains duplicate or
superseded rows, the Silver window discards all but the latest. So duplicates in
Bronze never reach Silver/features.

Layer 2 (BronzeWriter classify new/amend/skip) exists for three NON-correctness
reasons:
1. **Prevent unbounded Bronze bloat.** Bronze is append-only (never self-cleans).
   Incremental runs re-fetch `amendment_buffer_days` (3) every run; without the
   `skip` classification, identical recent bars get re-appended ~3–4× forever.
2. **Keep Silver reads cheap.** Duplicates multiply the rows Silver's window must
   scan+sort — and Silver runs on every feature rebuild. Removing Layer 2 moves
   the dedup cost from once (write) to many (every read).
3. **Explicit `record_version`** for amendment lineage (nice-to-have; `ingested_at
   DESC` ordering could substitute).

## The problem with the ORIGINAL Layer 2 implementation

`BronzeWriter._spark_bulk_fetch` did `spark.table(full_table)` — a **full-table
scan** — on **every** write. Combined with the per-receipt loop, a 15yr backfill
ran that scan 560× against a growing table → **O(N²)** → hours of billed compute.
That is an expensive way to buy a cheap benefit.

## Decision — scope the dedup to when/what it can actually matter

| Load type | Overlap with existing Bronze | Dedup action |
|---|---|---|
| INITIAL_LOAD / HISTORY_EXTENSION / FORCE_RELOAD into empty-for-symbol table | none | **SKIP dedup entirely** — straight append. Nothing exists to collide with. |
| INCREMENTAL / amendment | only the ~3-day buffer window | **Scoped dedup** — check only `symbol IN (batch) AND bar_date >= recent`, never the whole table. |

Layer 3 (Silver window) remains the correctness backstop in all cases.

### Storage layout — Liquid Clustering (complementary, not a substitute)

`CLUSTER BY (symbol, bar_date)` on `bronze.market_data_daily`:
- File-level data skipping → scoped incremental dedup AND all Silver reads touch
  only relevant files (Silver benefit compounds — it runs every feature build).
- Avoids the partition-by-symbol anti-pattern (2k symbols = 2k dirs, small files,
  skew). Liquid clustering handles high cardinality + skew, auto-maintained by
  Predictive Optimization (already enabled at the metastore).
- Apply on a **fresh load** (clean-slate reloads are ideal — data lands clustered,
  no reclustering cost). Keep to 1–2 keys.

Clustering makes remaining scans cheap; skipping makes them free. Use both.

## Implementation sequencing (cost-aware — do NOT gold-plate)

35-ticker scale needs none of this beyond step 1. Order:
1. **NOW (code-only, zero compute):** skip Layer-2 dedup on INITIAL_LOAD /
   HISTORY_EXTENSION / FORCE_RELOAD. Removes ~all the cost actually felt on Jul-7.
2. **Before hundreds of tickers:** scope incremental dedup to symbol + recent-date
   filter in BronzeWriter (line ~430) instead of full-table scan.
3. **Before the ~2k load / next Bronze rebuild:** add `CLUSTER BY (symbol, bar_date)`.

## Scale math (why steps 2–3 matter at 2k tickers)

2,000 tickers × 15yr × ~252 = ~7.5M Bronze rows.
- Full-table scan per instrument (current): 2,000 × ~3.75M avg = ~7.5B row-reads.
- Skip on backfill: **0** reads for the initial load.
- Scoped + clustered incremental: each check touches only recent files for the
  batch symbols — flat per ticker regardless of universe size → true O(N).
