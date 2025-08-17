import pandas as pd
from sqlalchemy import text
from utils.db import get_engine

def write_df(df: pd.DataFrame, table: str, schema: str | None = None, if_exists: str = "replace"):
    engine = get_engine()
    df.to_sql(table, engine, schema=schema, if_exists=if_exists, index=False)
    engine.dispose()

def init_schemas():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS marts;"))
    engine.dispose()

def write_raw_trends(df: pd.DataFrame):
    write_df(df, "raw_trends", schema=None, if_exists="replace")

def write_raw_events(df: pd.DataFrame):
    write_df(df, "raw_events", schema=None, if_exists="replace")

def write_raw_adspend(df: pd.DataFrame):
    write_df(df, "raw_adspend", schema=None, if_exists="replace")

if __name__ == "__main__":
    init_schemas()
