import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

load_dotenv()

def get_engine() -> Engine:
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "marketing")
    user = os.getenv("DB_USER", "marketing")
    pwd  = os.getenv("DB_PASSWORD", "marketing")
    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{name}"
    engine = create_engine(url, future=True)
    return engine
