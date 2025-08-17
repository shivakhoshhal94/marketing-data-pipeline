from prefect import flow, task
from extract_trends import fetch_trends
from generate_events import gen_synthetic
from load_to_db import write_raw_trends, write_raw_events, write_raw_adspend, init_schemas
from run_sql_models import run as run_sql

@task
def t_fetch_trends():
    return fetch_trends(days=90)

@task
def t_gen_synth():
    return gen_synthetic(days=90)

@task
def t_write_trends(df):
    write_raw_trends(df)

@task
def t_write_events(df_events):
    write_raw_events(df_events)

@task
def t_write_adspend(df_adspend):
    write_raw_adspend(df_adspend)

@task
def t_init_schemas():
    init_schemas()

@task
def t_run_sql():
    run_sql()

@flow(name="marketing-demo-pipeline")
def main():
    t_init_schemas()
    trends = t_fetch_trends()
    events, adspend = t_gen_synth()
    t_write_trends(trends)
    t_write_events(events)
    t_write_adspend(adspend)
    t_run_sql()

if __name__ == "__main__":
    main()
