# Databricks notebook source
# MAGIC %md
# MAGIC # spike_native_io — validate two deferred ingestion optimisations
# MAGIC
# MAGIC READ-ONLY probe. Proves (or disproves) two assumptions before we build on
# MAGIC them in `raw_to_bronze_job.py`. Nothing here writes or deletes S3/Delta.
# MAGIC
# MAGIC 1. **boto3 batch delete** — does Databricks *serverless* expose working
# MAGIC    boto3 S3 credentials? (If yes, archive can use `delete_objects` — 1 call
# MAGIC    per 1,000 receipts instead of sequential `dbutils.fs.rm`.)
# MAGIC 2. **`spark.read.text` bulk payload reads** — can we read many landed
# MAGIC    payloads in ONE parallel Spark job (no `dbutils.fs` threads, no nested
# MAGIC    JSON schema inference)? (If yes, the ~1,040 sequential `fs_head` reads
# MAGIC    in ingest collapse into one job.)
# MAGIC
# MAGIC Run cell-by-cell on serverless. Each cell prints PASS / FAIL with the reason.
# MAGIC
# MAGIC ## ⚠️ COST + CORRECTNESS NOTE — read before running
# MAGIC - **Run on a SERVERLESS notebook, NOT local Databricks Connect.** Spike 1
# MAGIC   asks whether *serverless* has boto3 S3 creds; local Connect would test
# MAGIC   your Mac's creds and give a false PASS.
# MAGIC - **This is read-only and tiny** (lists ~3 keys, heads 1 object, reads ~8
# MAGIC   small files) — interactive is the right tool for cell-by-cell verdicts.
# MAGIC - **DETACH / terminate the serverless session the moment you're done.**
# MAGIC   Interactive serverless has no task timeout and bills while warm — a warm
# MAGIC   idle session is what drove the Jul-7 spike, not work like this.
# MAGIC - Touches only S3 + Delta (internal AWS) — does NOT hit the zero-egress
# MAGIC   wall (no IBKR / internet calls here).

# COMMAND ----------
dbutils.widgets.text("raw_bucket", "handh-trade-raw-use1")
dbutils.widgets.text("vendor", "ibkr")
RAW_BUCKET = dbutils.widgets.get("raw_bucket")
VENDOR     = dbutils.widgets.get("vendor")
DONE_PREFIX = f"s3://{RAW_BUCKET}/control/fetch/{VENDOR}/done"
PAYLOAD_PREFIX = f"s3://{RAW_BUCKET}/{VENDOR}"   # where the agent lands raw payloads
print("done prefix   :", DONE_PREFIX)
print("payload prefix:", PAYLOAD_PREFIX)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Spike 1 — boto3 S3 credentials on serverless (read-only: list + head)

# COMMAND ----------
def spike_boto3():
    try:
        import boto3
    except Exception as e:
        return f"FAIL — boto3 not importable on serverless: {e}"
    try:
        s3 = boto3.client("s3", region_name="us-east-1")
        prefix = f"control/fetch/{VENDOR}/done/"
        resp = s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix=prefix, MaxKeys=3)
        keys = [o["Key"] for o in resp.get("Contents", [])]
        if not keys:
            return (f"INCONCLUSIVE — boto3 works but no objects under {prefix}. "
                    f"Credentials resolved (no AccessDenied), which is the thing "
                    f"we needed to confirm.")
        # HEAD one object (read-only) to confirm read auth end to end
        s3.head_object(Bucket=RAW_BUCKET, Key=keys[0])
        return (f"PASS — boto3 S3 creds work on serverless. Listed {len(keys)} key(s), "
                f"head_object OK on {keys[0]}. delete_objects path is viable.")
    except Exception as e:
        name = type(e).__name__
        return (f"FAIL — boto3 present but S3 call failed ({name}): {e}. "
                f"Serverless likely has no usable instance/UC credential for raw "
                f"boto3 → keep sequential dbutils.fs.rm in archive.")

print(spike_boto3())

# COMMAND ----------
# MAGIC %md
# MAGIC ## Spike 2 — spark.read.text bulk payload reads (parallel, no dbutils threads)

# COMMAND ----------
def spike_read_text():
    import json
    # Discover a handful of real payload files (any .json under the payload prefix)
    try:
        found = []
        stack = [PAYLOAD_PREFIX]
        while stack and len(found) < 8:
            cur = stack.pop()
            for f in dbutils.fs.ls(cur):
                if f.path.endswith(".json"):
                    found.append(f.path)
                elif f.path.endswith("/"):
                    stack.append(f.path.rstrip("/"))
                if len(found) >= 8:
                    break
    except Exception as e:
        return f"INCONCLUSIVE — could not list payloads under {PAYLOAD_PREFIX}: {e}"
    if not found:
        return f"INCONCLUSIVE — no .json payloads under {PAYLOAD_PREFIX} to test."
    try:
        # ONE Spark job reads all N files as text (one row = whole file content).
        rows = spark.read.text(found, wholetext=True).collect()
        parsed = 0
        fmts = set()
        for r in rows:
            p = json.loads(r["value"])
            fmts.add(p.get("payload_format"))
            parsed += 1
        ok = fmts == {"ohlcv_json_v1"}
        return (f"{'PASS' if ok else 'CHECK'} — read {len(rows)} payload(s) in one "
                f"spark.read.text job, json.loads OK on all {parsed}. "
                f"payload_format(s) seen: {fmts}. "
                f"{'Bulk-read path is viable.' if ok else 'Unexpected format — inspect.'}")
    except Exception as e:
        return (f"FAIL — spark.read.text/collect failed ({type(e).__name__}): {e}. "
                f"Keep sequential fs_head reads in ingest.")

print(spike_read_text())

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verdict
# MAGIC - Spike 1 PASS → wire boto3 `delete_objects` into `archive_done`.
# MAGIC - Spike 2 PASS → replace ingest's per-group `fs_head` loop with one
# MAGIC   `spark.read.text` over all landed payload paths.
# MAGIC - Either FAIL/INCONCLUSIVE → leave that path sequential (current code);
# MAGIC   the two pure wins already landed stand on their own.
