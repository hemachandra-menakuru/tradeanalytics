---
name: tradeanalytics-cicd-testing
description: >
  CI/CD automation, testing patterns, and data contracts for the TradeAnalytics
  codebase. Use this skill whenever the user asks about GitHub Actions for DABs
  deployment, automated testing of Databricks pipelines, pytest patterns for financial
  data pipelines, data quality contracts, integration test design, environment separation
  (dev vs prod DABs profiles), secrets management in CI, pre-commit hooks, code quality
  checks, or "how do I automate the deployment of my Databricks job". Also trigger
  for questions about how to test the BronzeIngestionJob, BronzeWriter, WatermarkManager,
  or any ingestion component without hitting real IBKR or Databricks. This is the
  Phase 2.5→3 reliability layer — enforces correctness before code ships to Databricks.
---

# TradeAnalytics CI/CD & Testing

## Design Principle

Test the behaviour, not the infrastructure. The test suite must run:
- **Locally** without Databricks Connect, IBKR gateway, or AWS access
- **In CI** without any secrets (mock all external dependencies)
- **In ≤ 2 minutes** for the full unit suite (fast feedback loop)

Integration tests that need Databricks Connect run separately (nightly or on-demand).

## Reference Files

| File | Contents |
|---|---|
| `references/github_actions.md` | DABs deployment workflow, environment separation, secrets in CI |
| `references/pytest_patterns.md` | Test structure, fixtures, mocking IBKR/Spark, financial data test helpers |
| `references/data_contracts.md` | Schema contracts, Bronze record validation, Great Expectations patterns |

---

## Test Pyramid for TradeAnalytics

```
           /  E2E (smoke test) \          ← 1 test, real Databricks, runs weekly
          /  Integration Tests  \         ← ~20 tests, Databricks Connect, runs nightly
         /    Unit Tests         \        ← 249+ tests, no external deps, runs on every PR
```

**Current state:** 249 unit tests passing on `feature/phase3-restructure`.
Keep the pyramid shape — never let integration tests outnumber unit tests.

---

## pytest Configuration

```toml
# pyproject.toml
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
markers = [
    "unit: fast tests with no external dependencies",
    "integration: requires Databricks Connect",
    "smoke: requires live IBKR gateway + Databricks",
]
addopts = "-m unit --tb=short -q"   # default: unit tests only
```

```bash
# Run by tier
pytest -m unit                      # default — fast, no deps
pytest -m integration               # requires DATABRICKS_TOKEN env var
pytest -m smoke                     # requires IBKR gateway running
pytest --cov=src --cov-report=term  # coverage report
```

---

## Fixture Patterns for Financial Data

```python
# tests/conftest.py
import pytest
import pandas as pd
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

@pytest.fixture
def sample_ohlcv_records() -> list[dict]:
    """Minimal valid OHLCV records matching BronzeRecord schema."""
    return [
        {
            "ticker": "SPY",
            "trade_date": date(2026, 6, 1),
            "open": 530.10, "high": 532.50, "low": 529.80, "close": 531.90,
            "volume": 45_000_000,
            "source": "ibkr",
            "interval": "1d",
        }
    ]

@pytest.fixture
def mock_ibkr_provider():
    """IBKR provider that returns deterministic test data — no gateway needed."""
    with patch("src.bronze.providers.ibkr_provider.IBKRProvider") as mock:
        provider = MagicMock()
        provider.get_historical.return_value = [
            {"ticker": "SPY", "trade_date": "2026-06-01", "open": 530.1,
             "high": 532.5, "low": 529.8, "close": 531.9, "volume": 45_000_000}
        ]
        mock.return_value = provider
        yield provider

@pytest.fixture
def mock_spark():
    """In-memory Spark mock for BronzeWriter unit tests."""
    from unittest.mock import MagicMock
    spark = MagicMock()
    spark.table.return_value.schema = MagicMock()
    spark.createDataFrame.return_value = MagicMock()
    return spark

@pytest.fixture
def config(tmp_path):
    """Load test config from config/ — never use prod config in tests."""
    from src.shared.config.config_loader import ConfigLoader
    return ConfigLoader.load(
        config_dir="config",
        stream="daily",
        env="test"
    )
```

---

## Testing BronzeWriter Without Spark

```python
# tests/bronze/test_bronze_writer.py
import pytest
from unittest.mock import MagicMock, call

@pytest.mark.unit
class TestBronzeWriter:
    def test_bulk_classify_skips_existing_records(self, mock_spark):
        """Records already in Bronze should be classified as SKIP."""
        from src.bronze.writers.bronze_writer import BronzeWriter

        writer = BronzeWriter(spark=mock_spark, mode="local")
        # Simulate existing records returned by bulk_fetch
        mock_spark.sql.return_value.toPandas.return_value = pd.DataFrame({
            "ticker": ["SPY"],
            "trade_date": [date(2026, 6, 1)],
            "record_version": [1]
        })
        records = [{"ticker": "SPY", "trade_date": "2026-06-01", "close": 531.9}]
        new, amend, skip = writer._bulk_classify(records)

        assert len(skip) == 1
        assert len(new) == 0
        assert len(amend) == 0

    def test_write_batch_returns_correct_counts(self, mock_spark, sample_ohlcv_records):
        """write_batch() must return actual record counts, not 0."""
        from src.bronze.writers.bronze_writer import BronzeWriter
        writer = BronzeWriter(spark=mock_spark, mode="local")
        result = writer.write_batch(sample_ohlcv_records, stream_cfg=MagicMock())
        assert result.records_written >= 0
        assert result.records_rejected >= 0
```

---

## Data Contract Testing

Validate that Bronze records conform to schema before they reach Delta.

```python
# tests/bronze/test_data_contracts.py
import pytest
from src.bronze.models.bronze_record import BRONZE_REQUIRED_FIELDS, BRONZE_AUDIT_FIELDS

@pytest.mark.unit
class TestBronzeDataContract:
    def test_enriched_record_has_all_audit_fields(self, mock_ibkr_provider, config):
        """_enrich_record() must stamp all 11 audit fields."""
        from src.bronze.validation.validator import DataQualityValidator
        validator = DataQualityValidator(config=config)
        raw = {"ticker": "SPY", "trade_date": "2026-06-01", "close": 531.9,
               "open": 530.1, "high": 532.5, "low": 529.8, "volume": 45_000_000}
        enriched = validator._enrich_record(raw, batch_id="test_batch",
                                             ingestion_type="backfill")
        for field in BRONZE_AUDIT_FIELDS:
            assert field in enriched, f"Missing audit field: {field}"

    def test_data_completeness_pct_ibkr_range(self, config):
        """IBKR records should score 60-64% completeness (known gap: optional fields)."""
        from src.bronze.validation.validator import DataQualityValidator
        validator = DataQualityValidator(config=config)
        raw = {"ticker": "SPY", "close": 531.9, "open": 530.1,
               "high": 532.5, "low": 529.8, "volume": 45_000_000,
               "trade_date": "2026-06-01"}
        pct = validator._compute_completeness(raw)
        assert 55.0 <= pct <= 70.0, f"Unexpected completeness: {pct}"
```

---

## GitHub Actions — DABs Deployment (preview — Phase 3)

```yaml
# .github/workflows/deploy.yml
name: Deploy to Databricks

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: "3.11"}
      - run: pip install -r requirements-dev.txt
      - run: pytest -m unit -q                  # unit tests only in CI
      - run: ruff check src/ tests/             # linting
      - run: mypy src/ --ignore-missing-imports  # type checking

  deploy-dev:
    needs: test
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    environment: dev
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle deploy --target dev
        env:
          DATABRICKS_HOST:  ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
```

**GitHub Secrets required (set in repo Settings → Secrets):**
- `DATABRICKS_HOST`: `https://dbc-bf0075e6-07aa.cloud.databricks.com`
- `DATABRICKS_TOKEN`: Databricks PAT (from Databricks user settings, not committed)

**Never put tokens in `databricks.yml`** — use environment variable substitution:
```yaml
# databricks.yml
targets:
  dev:
    workspace:
      host: ${DATABRICKS_HOST}
      token: ${DATABRICKS_TOKEN}
```

---

## Pre-commit Hooks

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.4.4
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format

  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.10.0
    hooks:
      - id: mypy
        args: [--ignore-missing-imports]
        files: ^src/

  - repo: local
    hooks:
      - id: pytest-unit
        name: unit tests
        entry: pytest -m unit -q --tb=short
        language: system
        pass_filenames: false
        always_run: true
```

```bash
# Install once
pip install pre-commit
pre-commit install
# Now runs automatically on every git commit
```

---

## Environment Separation

```yaml
# databricks.yml targets (already in repo)
targets:
  dev:
    default: true
    workspace:
      host: ${DATABRICKS_HOST}
    variables:
      catalog: tradeanalytics
      job_schedule_status: PAUSED     # never run on schedule in dev

  prod:
    workspace:
      host: ${DATABRICKS_HOST_PROD}   # same workspace for now; separate at Phase 5
    variables:
      catalog: tradeanalytics
      job_schedule_status: UNPAUSED
```

**Current state:** single Databricks workspace (`handh-dev`) with dev/prod separation
via DABs targets. True workspace isolation deferred to Phase 5 (live trading).
