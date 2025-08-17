# Marketing Data Pipeline (Python · SQL · Docker · Power BI)

A compact portfolio project: generate/ingest marketing data, build **staging → marts** in Postgres, and visualize KPIs (funnel + CAC) in **Power BI**. Orchestrated with **Prefect**, DB served via **Docker**.

## Stack
Docker (Postgres, Adminer) · Python 3.12 · Prefect 3 · SQL models · Power BI

## Quickstart
```bash
# 0) start DB
docker compose up -d
# Adminer: http://localhost:8080  (System: PostgreSQL, Server: db, DB: marketing, user/pass: marketing)

# 1) venv + deps
python -m venv .venv
# Windows: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 2) run pipeline (prefect; auto-fallback if Google Trends blocks)
python src/orchestrate_prefect.py
# (alt without Prefect)
# python src/run_pipeline.py
