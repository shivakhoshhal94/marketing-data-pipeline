from pathlib import Path
from sqlalchemy import text
from utils.db import get_engine

SQL_ORDER = [
    "sql/staging/stg_trends.sql",
    "sql/staging/stg_events.sql",
    "sql/staging/stg_adspend.sql",
    "sql/marts/mart_geo_keyword_daily.sql",
    "sql/marts/mart_funnel_daily.sql",
    "sql/marts/mart_cac_proxy_daily.sql",
]

def run():
    engine = get_engine()
    with engine.begin() as conn:
        for rel in SQL_ORDER:
            path = Path(rel)
            sql = path.read_text(encoding="utf-8")
            print(f"Running {rel} ...")
            conn.exec_driver_sql(sql)
    engine.dispose()
    print("All SQL models executed.")

if __name__ == "__main__":
    run()
