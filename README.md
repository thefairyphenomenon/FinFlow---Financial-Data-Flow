# 🌊 DataFlow — Institutional Financial Data Pipeline

> A production-grade financial data pipeline that ingests, cleans, validates, and serves market and macroeconomic data — built like a real quant infrastructure team would build it.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│   Yahoo Finance (free)        FRED - Federal Reserve (free)     │
└──────────────────┬──────────────────────────┬───────────────────┘
                   │                          │
                   ▼                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    APACHE AIRFLOW (Orchestration)               │
│   DAG: market_data_pipeline          DAG: macro_pipeline        │
│   Schedule: 6PM weekdays             Schedule: 8AM daily        │
└──────────────────────────────┬──────────────────────────────────┘
                                │
                    ┌───────────┼───────────┐
                    ▼           ▼           ▼
              [Ingestor]  [Transformer] [Quality Checker]
                    │                       │
                    ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│              PostgreSQL + TimescaleDB                           │
│   ohlcv (hypertable)    macro_indicators    data_quality_logs   │
└──────────────────────────────┬──────────────────────────────────┘
                                │
                         ┌──────┴──────┐
                         ▼             ▼
                      [Redis]      [FastAPI]
                    (Cache Layer)  (REST API)
                                      │
                                      ▼
                              http://localhost:8000/docs
```

---

## ⚡ Quick Start (No Docker Required)

```bash
# 1. Clone and setup
git clone <your-repo>
cd dataflow
pip install -r requirements.txt

# 2. Run the demo (fetches real data from Yahoo Finance)
python scripts/quickstart_demo.py

# 3. Run tests
pytest tests/ -v
```

---

## 🐳 Full Stack Setup (Docker)

```bash
# 1. Configure environment
cp .env.example .env
# Edit .env — add your FRED API key (free at fred.stlouisfed.org)

# 2. Start all services
docker-compose up -d

# 3. Initialize database
docker exec dataflow_api python -c "
from core.storage.database import init_database
init_database()
"

# 4. Trigger first data load
curl -X POST http://localhost:8000/api/v1/pipeline/trigger/market-data

# 5. View the data
curl http://localhost:8000/api/v1/market/ohlcv/AAPL?limit=5
```

**Services:**
| Service | URL | Credentials |
|---------|-----|-------------|
| DataFlow API | http://localhost:8000/docs | — |
| Airflow UI | http://localhost:8080 | admin / admin |
| PostgreSQL | localhost:5432 | see .env |
| Redis | localhost:6379 | — |

---

## 📁 Project Structure

```
dataflow/
├── api/                        # FastAPI REST API
│   ├── main.py                 # App factory, middleware
│   └── routers/
│       ├── market_data.py      # OHLCV endpoints
│       ├── macro_data.py       # FRED macro endpoints
│       ├── pipeline.py         # Pipeline trigger/monitor
│       └── health.py           # Health checks
│
├── core/                       # Business logic
│   ├── settings.py             # Centralized config
│   ├── logger.py               # Structured JSON logging
│   ├── ingestion/
│   │   ├── yahoo_finance.py    # Yahoo Finance ETL
│   │   └── fred_ingester.py    # FRED macro ETL
│   ├── storage/
│   │   ├── database.py         # SQLAlchemy ORM + TimescaleDB
│   │   └── cache.py            # Redis cache layer
│   └── quality/
│       └── checker.py          # Data quality validation
│
├── airflow/
│   └── dags/
│       └── market_data_pipeline.py  # Orchestration DAG
│
├── infrastructure/
│   ├── docker/Dockerfile.api
│   └── postgres/init.sql
│
├── tests/
│   └── unit/
│       └── test_quality_checker.py
│
├── scripts/
│   └── quickstart_demo.py      # Demo without Docker
│
├── config.yaml                 # App configuration
├── docker-compose.yml          # Full stack setup
├── requirements.txt
└── .env.example                # Environment template
```

---

## 🔌 API Endpoints

### Market Data
```
GET /api/v1/market/tickers
GET /api/v1/market/ohlcv/{ticker}?interval=1d&limit=252
GET /api/v1/market/stats/{ticker}?days=252
```

### Macro Data
```
GET /api/v1/macro/catalog
GET /api/v1/macro/series/FEDFUNDS?limit=100
GET /api/v1/macro/series/T10Y2Y    (yield curve — recession predictor)
```

### Pipeline Control
```
POST /api/v1/pipeline/trigger/market-data
POST /api/v1/pipeline/trigger/macro-data
GET  /api/v1/pipeline/runs
GET  /api/v1/pipeline/runs/{run_id}
```

---

## 🧪 Data Quality Checks

Every ingestion run validates:

| Check | What It Catches |
|-------|----------------|
| `null_check` | Missing values in critical columns |
| `ohlc_consistency` | High < Low (impossible data) |
| `price_gap_check` | >20% single-day price moves (data errors) |
| `volume_check` | Suspiciously low volume |
| `duplicate_check` | Same record ingested twice |

All results are logged to `data_quality_logs` table for audit trail.

---

## 🚀 Roadmap / Upgrades

These additions would make this exceptional:

1. **Feature Store** — Pre-compute rolling returns, volatility, RSI, MACD for ML models
2. **News Sentiment Pipeline** — Ingest financial news via RSS, run NLP sentiment scoring
3. **Anomaly Detection** — ML model to flag unusual market data automatically  
4. **Data Lineage** — Track where every data point came from (OpenLineage)
5. **gRPC API** — Lower latency alternative to REST for internal services
6. **Backfill Job** — Historical data loading for any date range
7. **Alerting** — Slack/email alerts when quality checks fail

---

## 🎯 Why This Impresses Recruiters

- **TimescaleDB** — Knows time-series databases, not just vanilla Postgres
- **Airflow DAGs** — Real orchestration, not cron jobs
- **Data Quality Layer** — Understands that bad data = bad models
- **Cache-aside Pattern** — Thinks about API performance at scale
- **Idempotent Pipelines** — Safe to re-run, no duplicates
- **Structured Logging** — Production observability, not print() statements
- **Docker Compose** — Full stack reproducible locally in one command

---

## 📚 Free Data Sources Used

| Source | Data | Cost |
|--------|------|------|
| Yahoo Finance (yfinance) | OHLCV for all US stocks | Free, no key |
| FRED (St. Louis Fed) | 800,000+ economic series | Free, API key |

---

*Built with Python · FastAPI · PostgreSQL/TimescaleDB · Redis · Apache Airflow · Docker*
