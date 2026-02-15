# Foxa Analytics - dbt Project

Data transformations for the Foxa Trading Data Lakehouse using dbt-duckdb.

## Project Structure

```
dbt_project/
├── models/
│   ├── staging/          # Light transformations (views)
│   │   ├── stg_ohlcv.sql
│   │   └── stg_indicators.sql
│   ├── marts/            # Business logic (tables)
│   │   ├── fact_signals.sql
│   │   ├── dim_stocks.sql
│   │   └── fact_market_breadth.sql
│   └── sources.yml       # Source definitions
├── tests/                # Custom tests
├── macros/               # Reusable SQL
├── dbt_project.yml       # Project config
└── profiles.yml          # Connection profiles
```

## Models

### Staging Models

| Model | Description | Materialization |
|-------|-------------|-----------------|
| `stg_ohlcv` | Cleaned OHLCV with derived metrics | View |
| `stg_indicators` | Technical indicators with signal interpretations | View |

### Mart Models

| Model | Description | Materialization |
|-------|-------------|-----------------|
| `fact_signals` | Trading signals with indicator context | Table |
| `dim_stocks` | Stock dimension with fundamentals | Table |
| `fact_market_breadth` | Daily market health metrics | Table |

## Usage

```bash
# Navigate to dbt project
cd dbt_project

# Install dependencies
dbt deps

# Run all models
dbt build

# Run specific model
dbt run --select fact_signals

# Run with tests
dbt build --select +fact_signals

# Generate documentation
dbt docs generate
dbt docs serve
```

## Data Lineage

```
bronze.ohlcv → stg_ohlcv ─┬→ fact_signals
                          ├→ dim_stocks
                          └→ fact_market_breadth

silver.indicators → stg_indicators ─┬→ fact_signals
                                    ├→ dim_stocks
                                    └→ fact_market_breadth

gold.signals → fact_signals
```

## Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `lookback_days` | 252 | Trading days for calculations |
| `min_volume_threshold` | 100000 | Minimum volume filter |

## Tests

Built-in tests on source columns:
- `not_null` on symbol, date, close

To add more tests, modify `models/sources.yml`.