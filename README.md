# 🦊 Foxa — AI-Powered Trading Analytics Lakehouse

> A production-grade **Data Lakehouse** for Indian equities, combining Apache Iceberg, LangGraph multi-agent AI, and the modern data stack.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  INGESTION           │  STORAGE (Lakehouse)  │  SERVING          │
│  ─────────           │  ────────────────────  │  ───────          │
│  Angel One API       │  Bronze → Silver → Gold│  FastAPI          │
│  Screener.in         │  (Apache Iceberg on    │  DuckDB (OLAP)    │
│  FMP API             │   MinIO S3)            │  Airflow DAGs     │
│  Kafka (optional)    │                       │  MLflow Tracking   │
└─────────────────────────────────────────────────────────────────┘
                           │
                    ┌──────┴──────┐
                    │  AI Agents  │
                    │  LangGraph  │
                    │  ChromaDB   │
                    │  OpenRouter │
                    └─────────────┘
```

## Tech Stack

| Layer | Technologies |
|-------|-------------|
| **Storage** | Apache Iceberg · MinIO (S3) · Parquet · SQLite Catalog |
| **Processing** | Polars · PyIceberg · dbt-duckdb |
| **Data Quality** | Great Expectations |
| **AI Agents** | LangGraph · LangChain · ChromaDB · OpenRouter |
| **API** | FastAPI · DuckDB |
| **Orchestration** | Apache Airflow |
| **Tracking** | MLflow · LangSmith |
| **Deploy** | Docker Compose |

## Medallion Architecture

```
 BRONZE (Raw)              SILVER (Cleaned)           GOLD (Analytics)
 ───────────               ────────────────           ────────────────
 • raw OHLCV               • cleaned OHLCV            • trading signals
 • raw fundamentals        • technical indicators     • agent analysis
 • raw news                • sentiment scores         • portfolio metrics
                           • derived features         • market summary
```

## Project Structure

```
foxa/
├── lakehouse/                  # Data Lakehouse (Iceberg + MinIO)
│   ├── minio_client.py         # S3-compatible storage client
│   ├── iceberg_catalog.py      # Table catalog + schemas
│   ├── bronze.py               # Raw data ingestion
│   ├── silver.py               # Cleaning + feature engineering
│   ├── gold.py                 # Analytics-ready aggregations
│   └── pipeline.py             # End-to-end pipeline orchestrator
│
├── agents/                     # AI Agent System
│   ├── langgraph_workflow.py   # LangGraph state machine
│   ├── state.py                # Agent state definitions
│   ├── nodes/                  # Agent nodes (Technical, Fundamental, Risk, Macro, Trader)
│   │   └── data_loader.py
│   ├── fundamental_agent.py    # Fundamental analysis agent
│   ├── multi_agent.py          # Multi-agent coordinator
│   └── tools.py                # Agent tools
│
├── api/                        # REST API
│   └── main.py                 # FastAPI application
│
├── airflow/                    # Workflow Orchestration
│   └── dags/
│       ├── lakehouse_pipeline_dag.py
│       └── signal_generation_dag.py
│
├── data/                       # Data Layer
│   ├── ohlcv_fetcher.py        # Market data fetcher
│   ├── fundamental_fetcher.py  # Fundamental data fetcher
│   ├── market_data.py          # Real-time market data
│   ├── technical_indicators.py # Technical analysis library
│   ├── stock_universe.py       # Stock universe management
│   └── kimi_scanner.py         # Strategy scanner
│
├── data_quality/               # Data Quality
│   └── validation_runner.py    # Great Expectations integration
│
├── great_expectations/         # GE Configuration
│   ├── expectations/           # Validation suites
│   └── checkpoints/            # Validation checkpoints
│
├── dbt_project/                # dbt Transformations
│   ├── models/staging/         # Staging models
│   └── models/marts/           # Business-level marts
│
├── tracking/                   # Experiment Tracking
│   └── mlflow_utils.py         # MLflow integration
│
├── memory/                     # RAG Memory System
├── knowledge/                  # Knowledge Base
├── llm/                        # LLM Provider
├── database/                   # SQLAlchemy models
│
├── docker-compose.yml          # Full stack deployment
├── Dockerfile
├── requirements.txt
└── .env.example
```

## Quick Start

### 1. Clone & Setup

```bash
git clone https://github.com/yourusername/foxa.git
cd foxa
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your API keys
```

### 3. Start Infrastructure

```bash
docker-compose up -d          # MinIO + Airflow
```

### 4. Run the Lakehouse Pipeline

```bash
# Initialize Iceberg tables
python -m lakehouse.iceberg_catalog

# Run Bronze → Silver → Gold pipeline
python -m lakehouse.pipeline --symbols RELIANCE TCS INFY

# Run data quality checks
python -m data_quality.validation_runner
```

### 5. Start the API

```bash
uvicorn api.main:app --reload
# → http://localhost:8000/docs
```

### 6. Run LangGraph Agent Analysis

```bash
python -c "
from agents.langgraph_workflow import TradingAgentWorkflow
wf = TradingAgentWorkflow()
result = wf.analyze('RELIANCE')
print(result['final_recommendation'])
"
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check |
| `GET` | `/signals` | Latest trading signals |
| `GET` | `/stocks/{symbol}/quote` | Real-time stock quote |
| `POST` | `/analyze` | Run multi-agent analysis |

## Key Features

- **Medallion Lakehouse** — Bronze/Silver/Gold zones with Apache Iceberg (ACID, time travel, schema evolution)
- **LangGraph Agents** — 6-node multi-agent graph: Data Loader → Technical Analyst → Fundamental Analyst → Risk Manager → Macro Analyst → Trader
- **500+ Stocks** — NSE/BSE universe with OHLCV history and fundamentals
- **dbt Models** — SQL transformations with lineage tracking
- **Great Expectations** — Automated data quality validation
- **MLflow** — Experiment tracking for agent performance
- **Airflow DAGs** — Scheduled pipeline orchestration

## Skills Demonstrated

| Category | Technologies |
|----------|-------------|
| **Data Lakehouse** | Apache Iceberg, MinIO, Parquet, Medallion Architecture |
| **Modern Data Stack** | dbt-duckdb, Great Expectations, Polars |
| **AI/ML Engineering** | LangGraph, LangChain, ChromaDB, RAG |
| **Data Engineering** | ETL/ELT Pipelines, Schema Evolution, Partitioning |
| **API Development** | FastAPI, Pydantic, REST |
| **MLOps** | MLflow, LangSmith, Experiment Tracking |
| **Orchestration** | Apache Airflow DAGs |
| **DevOps** | Docker, Docker Compose |
| **Domain** | Indian Equities, Technical Analysis, Fundamental Analysis |

## License

MIT
