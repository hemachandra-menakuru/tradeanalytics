---
name: tradeanalytics-aws-platform
description: >
  AWS platform engineering for the TradeAnalytics production infrastructure (account
  311925399625, us-east-1). Use this skill whenever the user asks about S3 bucket
  structure, S3 lifecycle policies, S3 partitioning for Delta Lake, IAM roles and
  least-privilege policies, Secrets Manager for IBKR credentials, CloudWatch alarms
  on Databricks job failures, Lambda event triggers, KMS encryption, VPC security
  groups, AWS cost optimisation, or any "how do I set this up in AWS" question.
  Also trigger for questions about how Databricks connects to S3, Unity Catalog
  external locations, cross-account access, bucket policies, or monitoring production
  job health from the AWS side. This skill is specific to the TradeAnalytics AWS
  account and complements the Databricks-side patterns in databricks-pipeline-advisor.
---

# TradeAnalytics AWS Platform

## Account & Region Reference

| Resource | Value |
|---|---|
| AWS Account | `311925399625` |
| Region | `us-east-1` |
| IAM admin user | `hc-admin` |
| CLI/DABs profile | `handh-trade-aws` |
| Databricks workspace | `handh-dev` |
| Short prefix | `handh-trade` |

## Reference Files

| File | Contents |
|---|---|
| `references/s3_architecture.md` | S3 bucket layout, partitioning, lifecycle, cost optimisation |
| `references/iam_secrets.md` | IAM least-privilege patterns, Secrets Manager, KMS |
| `references/cloudwatch_monitoring.md` | Job failure alarms, cost budget alerts, dashboard |

---

## S3 Bucket Inventory

| Bucket | Purpose | Delta tables? |
|---|---|---|
| `handh-trade-raw-use1` | Raw API responses (JSON), unprocessed | No |
| `handh-trade-refined-use1` | Delta Lake storage (bronze/silver/gold) | Yes |
| `handh-trade-mlflow-use1` | MLflow artifacts (models, plots, metrics) | No |
| `handh-trade-dbx-root-use1` | Databricks workspace root (managed by Databricks) | Managed |

All buckets: versioning enabled, public access blocked, SSE-S3 encryption.

---

## S3 Path Layout for Delta Lake

```
s3://handh-trade-refined-use1/
├── bronze/
│   ├── market_data_daily/          ← Delta table (tradeanalytics.bronze.market_data_daily)
│   ├── market_data_rejected/
│   └── ingestion_watermark_daily/
├── silver/
│   └── feature_store/              ← Delta table (Phase 3)
├── gold/
│   ├── signal_log/                 ← Delta table (Phase 4)
│   └── paper_track/
├── reference/
│   └── [reference tables]/
└── control/
    └── [control tables]/
```

**Never write directly to S3 paths.** All writes go through Unity Catalog table references
(`spark.write.saveAsTable()`). Direct S3 writes bypass Unity Catalog lineage and access control.

---

## IAM Least Privilege (Databricks Storage Credential)

The Databricks storage credential uses an IAM role — not a user key. This is the correct pattern.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DatabricksRefinedBucketAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
        "s3:GetObjectVersion", "s3:ListBucket", "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::handh-trade-refined-use1",
        "arn:aws:s3:::handh-trade-refined-use1/*"
      ]
    },
    {
      "Sid": "DatabricksMLflowBucketAccess",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::handh-trade-mlflow-use1",
        "arn:aws:s3:::handh-trade-mlflow-use1/*"
      ]
    }
  ]
}
```

**Do NOT grant `s3:*` or `arn:aws:s3:::*`.** Scope to specific buckets only.

---

## Secrets Manager — IBKR Credentials

IBKR account ID and gateway credentials are stored in Secrets Manager, not `.env` files.

```python
import boto3
import json

def get_ibkr_credentials(secret_name: str = "handh-trade/ibkr/account") -> dict:
    """
    Retrieve IBKR credentials from AWS Secrets Manager.
    In notebooks: use Databricks secrets (dbutils.secrets) backed by this same secret.
    In local scripts: use boto3 directly.
    """
    client = boto3.client("secretsmanager", region_name="us-east-1")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

# Secret schema (never commit this to git):
# {
#   "account_id": "U5498892",
#   "gateway_url": "https://localhost:5055/v1/api",
#   "databricks_token": "dapi..."
# }
```

**In Databricks notebooks:** use `dbutils.secrets.get(scope="handh-trade", key="ibkr-account-id")`
The Databricks secret scope is backed by the Secrets Manager secret — set up once, used everywhere.

```bash
# One-time setup: link Databricks secret scope to Secrets Manager
databricks secrets create-scope --scope handh-trade --scope-backend-type AWS_SECRETS_MANAGER \
  --initial-manage-principal users \
  --profile handh-trade-aws
```

---

## CloudWatch Alarms — Job Health

Set up alarms on Databricks job runs via CloudWatch metric filters on CloudTrail.

```python
import boto3

def create_job_failure_alarm(job_name: str, sns_topic_arn: str) -> None:
    """
    Alert when a Databricks job fails.
    Databricks publishes job events to CloudWatch when workspace logging is enabled.
    """
    cw = boto3.client("cloudwatch", region_name="us-east-1")
    cw.put_metric_alarm(
        AlarmName=f"databricks-job-failure-{job_name}",
        AlarmDescription=f"Databricks job {job_name} failed",
        MetricName="JobRunFailed",
        Namespace="TradeAnalytics/Databricks",
        Statistic="Sum",
        Period=3600,           # check hourly
        EvaluationPeriods=1,
        Threshold=1,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        AlarmActions=[sns_topic_arn],
        TreatMissingData="notBreaching"
    )
```

**Simpler alternative:** Databricks Workflows natively supports email + webhook
notifications on job failure — set in the job definition before CloudWatch.
Use CloudWatch only when you need cross-service alerting (e.g. trigger Lambda on failure).

---

## S3 Lifecycle Policy — Cost Management

```json
{
  "Rules": [
    {
      "ID": "raw-archive-policy",
      "Filter": {"Prefix": ""},
      "Status": "Enabled",
      "Transitions": [
        {"Days": 30,  "StorageClass": "STANDARD_IA"},
        {"Days": 90,  "StorageClass": "GLACIER_IR"},
        {"Days": 365, "StorageClass": "DEEP_ARCHIVE"}
      ]
    }
  ]
}
```

Apply to `handh-trade-raw-use1` only (raw JSON responses, not Delta tables).
**Never apply lifecycle transitions to Delta table buckets** (`handh-trade-refined-use1`):
Delta reads old Parquet files during time travel — transitioning them to Glacier
breaks RESTORE and time travel queries silently.

---

## Cost Guard Rails

```bash
# Verify billing budget is still in place (set at project start)
aws budgets describe-budgets --account-id 311925399625 --profile handh-trade-aws

# Check current month spend
aws ce get-cost-and-usage \
  --time-period Start=$(date +%Y-%m-01),End=$(date +%Y-%m-%d) \
  --granularity MONTHLY \
  --metrics BlendedCost \
  --profile handh-trade-aws
```

**$50/month budget** was set at Phase 1. Primary cost drivers:
1. Databricks compute (m5.xlarge SPOT, largest cost)
2. S3 storage (grows as Bronze history extends)
3. NAT Gateway (not yet provisioned — Phase 5)
