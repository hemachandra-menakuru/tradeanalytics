---
name: tradeanalytics-mlflow-lifecycle
description: >
  MLflow experiment tracking, Unity Catalog model registry, champion/challenger patterns,
  and model promotion workflows for the TradeAnalytics Databricks workspace (handh-dev,
  us-east-1). Use this skill whenever the user mentions MLflow, experiment runs, model
  registry, model versioning, logging metrics, registering a model, promoting a model
  to staging or production, comparing runs, champion/challenger, or model aliases in
  Unity Catalog. Also trigger for questions about experiment naming conventions, run
  comparison across XGBoost/LightGBM clusters, feature importance logging, SHAP logging,
  or how to wire MLflow into the BronzeIngestionJob or a future GoldModelJob. This is
  the authoritative Phase 4 guide for all ML lifecycle decisions in TradeAnalytics.
---

# TradeAnalytics MLflow Lifecycle

## Context

- Databricks workspace: `handh-dev` at `dbc-bf0075e6-07aa.cloud.databricks.com`
- Unity Catalog: `tradeanalytics`
- MLflow is Databricks-managed — no separate server needed
- Model registry lives in Unity Catalog (3-level namespace)

## Experiment Naming Convention (locked)

```
tradeanalytics/{cluster}/{model_type}
```

Examples:
- `tradeanalytics/momentum/xgboost`
- `tradeanalytics/mean_reversion/lightgbm`
- `tradeanalytics/mega_cap/xgboost`
- `tradeanalytics/regime_detector/hmm`

**Why:** Unity Catalog isolates by catalog → schema → model. The experiment path mirrors
the model registry path so a run and its registered model are always obviously paired.

## Model Registry Path (UC 3-level namespace)

```
tradeanalytics.<cluster>.<model_name>
```

Examples:
- `tradeanalytics.momentum.xgboost_daily_signal`
- `tradeanalytics.mean_reversion.lgbm_entry`
- `tradeanalytics.regime_detector.hmm_state`

## Standard Run Logging Pattern

```python
import mlflow
from databricks.sdk import WorkspaceClient

mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(f"tradeanalytics/{cluster}/{model_type}")

with mlflow.start_run(run_name=f"{symbol}_{as_of_date}") as run:
    # Log hyperparameters
    mlflow.log_params(params)

    # Log metrics — always log these four for every model
    mlflow.log_metric("ic_mean", ic_mean)          # Information Coefficient mean
    mlflow.log_metric("ic_ir", ic_ir)              # IC Information Ratio (IC / std(IC))
    mlflow.log_metric("sharpe_oos", sharpe_oos)    # Out-of-sample Sharpe
    mlflow.log_metric("hit_rate", hit_rate)        # Directional accuracy

    # Log model with input example
    mlflow.sklearn.log_model(
        model,
        artifact_path="model",
        registered_model_name=f"tradeanalytics.{cluster}.{model_name}",
        input_example=X_train.iloc[:5]
    )

    # Log feature importance as artifact
    mlflow.log_dict(feature_importance, "feature_importance.json")
```

## Champion/Challenger Pattern

The TradeAnalytics model registry uses UC model aliases — never numeric versions in
production references.

```python
from mlflow import MlflowClient

client = MlflowClient(registry_uri="databricks-uc")

# Promote a version to champion
client.set_registered_model_alias(
    name="tradeanalytics.momentum.xgboost_daily_signal",
    alias="champion",
    version=version_number
)

# Challenger is the version being validated
client.set_registered_model_alias(
    name="tradeanalytics.momentum.xgboost_daily_signal",
    alias="challenger",
    version=new_version_number
)

# Load by alias (never by version number in production)
model = mlflow.sklearn.load_model(
    "models:/tradeanalytics.momentum.xgboost_daily_signal@champion"
)
```

**Promotion gate (locked):** A challenger only replaces the champion after:
1. ≥ 6 months walk-forward out-of-sample period
2. IC_IR > 0.5 on held-out data
3. OOS Sharpe > 0.8 (annualised)
4. No statistically significant decline vs champion on paired t-test (IC series)

## Walk-Forward Run Naming

Each fold in walk-forward validation is its own child run:

```python
with mlflow.start_run(run_name=f"walkforward_{symbol}") as parent:
    for fold_idx, (train_end, test_start, test_end) in enumerate(folds):
        with mlflow.start_run(run_name=f"fold_{fold_idx}", nested=True) as child:
            mlflow.log_params({"train_end": str(train_end), "test_start": str(test_start)})
            mlflow.log_metric("ic_oos", fold_ic)
            mlflow.log_metric("sharpe_oos", fold_sharpe)

    # Aggregate on parent
    mlflow.log_metric("ic_mean_across_folds", np.mean(fold_ics))
    mlflow.log_metric("ic_ir_across_folds", np.mean(fold_ics) / np.std(fold_ics))
```

## SHAP Logging

Log SHAP values as an artifact, not as individual metrics (too many features):

```python
import shap

explainer = shap.TreeExplainer(model)
shap_values = explainer.shap_values(X_test)

# Save summary as artifact
shap_df = pd.DataFrame(shap_values, columns=X_test.columns)
mlflow.log_dict(
    {"mean_abs_shap": dict(shap_df.abs().mean().sort_values(ascending=False))},
    "shap_summary.json"
)
```

## Experiment Comparison Pattern

When comparing runs across hyperparameter sweeps:

```python
runs_df = mlflow.search_runs(
    experiment_names=[f"tradeanalytics/{cluster}/{model_type}"],
    filter_string="metrics.ic_ir > 0.3",
    order_by=["metrics.ic_ir DESC"]
)
```

## What NOT to do

- Do not use numeric version references in production code — use aliases only
- Do not log IC per symbol as separate metric keys — log as artifact dict
- Do not register a model before walk-forward validation is complete
- Do not share a single experiment across multiple clusters (momentum ≠ mean_reversion)
- Do not run MLflow tracking inside `BronzeIngestionJob` — MLflow belongs in Gold layer jobs only
