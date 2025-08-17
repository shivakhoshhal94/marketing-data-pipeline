import pandas as pd
from sqlalchemy import text
from utils.db import get_engine
from pathlib import Path

def export(table: str, out_dir: Path):
    engine = get_engine()
    df = pd.read_sql(f"SELECT * FROM {table}", engine)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{table.replace('.', '_')}.csv"
    df.to_csv(out, index=False)
    print(f"Wrote {out}")
    engine.dispose()

if __name__ == "__main__":
    out = Path("out")
    for t in ["marts.funnel_daily", "marts.cac_proxy_daily", "marts.geo_keyword_daily"]:
        export(t, out)
