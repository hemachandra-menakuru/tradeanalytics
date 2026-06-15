# TradeAnalytics — ML Trading Pipeline

End-to-end ML-driven algorithmic trading pipeline built on AWS and Databricks.

## Stack
- **Cloud:** AWS (us-east-1) — S3, IAM, VPC, Secrets Manager
- **Engine:** Databricks — Delta tables, Unity Catalog, MLflow, DLT
- **Data:** Yahoo Finance (dev) · IBKR live account (production)
- **ML:** XGBoost · LSTM · regime classifier · return predictor
- **Execution:** Anthropic LLM agent · IBKR TWS API

## AWS Account
- Account: `handh_tradeanalytics` (311925399625)
- Region: `us-east-1`
- Resource prefix: `handh-trade`

## Project Structure
cat > README.md << 'EOF'
# TradeAnalytics — ML Trading Pipeline

End-to-end ML-driven algorithmic trading pipeline built on AWS and Databricks.

## Stack
- **Cloud:** AWS (us-east-1) — S3, IAM, VPC, Secrets Manager
- **Engine:** Databricks — Delta tables, Unity Catalog, MLflow, DLT
- **Data:** Yahoo Finance (dev) · IBKR live account (production)
- **ML:** XGBoost · LSTM · regime classifier · return predictor
- **Execution:** Anthropic LLM agent · IBKR TWS API

## AWS Account
- Account: `handh_tradeanalytics` (311925399625)
- Region: `us-east-1`
- Resource prefix: `handh-trade`

## Project Structure
tradeanalytics/

├── src/

│   ├── ingestion/     # Data ingestion from Yahoo Finance + IBKR

│   ├── bronze/        # Raw Delta table writes

│   ├── silver/        # Cleaning, dedup, corp action adjustment

│   ├── gold/          # Feature engineering

│   ├── ml/            # ML model training and inference

│   ├── strategy/      # Rule-based Swing Tier Engine

│   ├── fusion/        # Signal fusion and quantified model

│   ├── llm/           # Anthropic LLM decision agent

│   └── broker/        # IBKR TWS API integration

├── pipelines/         # DLT pipeline definitions

├── notebooks/         # Exploration and ML notebooks

├── tests/             # Unit and integration tests

├── config/            # Environment configs (dev/prod)

├── docs/              # Reference documents

└── .github/workflows/ # CI/CD — DABs deploy on push

## Phases
| Phase | Focus | Status |
|---|---|---|
| Phase 1 | AWS + Databricks foundation | In progress |
| Phase 2 | Data ingestion + Bronze/Silver | Pending |
| Phase 3 | ML + feature store | Pending |
| Phase 4 | Strategy + LLM agent | Pending |
| Phase 5 | Live trading + monitoring | Pending |

## Reference
See `docs/TradeAnalytics_Phase1_Infrastructure_v2.docx` for full AWS resource IDs, debug notes and setup history.
