# Growth Radar

> Modern data stack pipeline for detecting sector & thematic dynamics on equity markets.

## 🎯 Vision

Growth Radar identifies sectoral and thematic dynamics across equity markets at three
levels of granularity, combining market signals, narrative signals, and emergence signals.

The end goal: a dashboard that answers *"what's moving this week and why?"* at different
zoom levels, with the ability to detect emerging themes that official classifications
don't recognize yet.

## 🔭 Three-level hierarchy

Each company belongs to:

1. **GICS classification** (macro, stable) — 11 sectors, 74 industries, 163 sub-industries
2. **Thematic ETFs** (meso, curated, multi-membership) — ~30 ETFs with weighted holdings
3. **Emerging clusters** (micro, discovered, dynamic) — detected automatically from news
   co-occurrences (Phase 2)

## 🏗️ Architecture (Phase 1)
Sources → Bronze (raw Parquet) → Silver (Spark/DuckDB) → Gold (dbt marts) → Streamlit
↓
MinIO (S3-compatible local)
↓
Orchestrated by Dagster

## 🛠️ Stack

- **Storage**: MinIO (S3-compatible), Parquet partitioned
- **Ingestion**: Python + httpx + Pydantic schemas
- **Transformation**: Apache Spark (heavy workloads) + DuckDB + dbt-core
- **Orchestration**: Dagster (asset-oriented)
- **Serving**: Streamlit
- **Quality**: pytest, dbt tests, ruff, GitHub Actions CI

## 📋 Status

**Phase 1 — Week 1 / 6** : Foundations & first ingestion

## 📚 Documentation

- [Project brief](docs/PROJECT_BRIEF.md) — Full project context and design decisions
- [Architecture Decision Records](docs/decisions/) — Key technical decisions explained

## 🚀 Quick start

See [docs/SETUP.md](docs/SETUP.md) *(coming in Week 1)*.

## 📝 License

MIT
