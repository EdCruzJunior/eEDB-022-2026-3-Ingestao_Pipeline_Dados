import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

BASE_DIR = Path(__file__).resolve().parents[1]
ENV_FILE = BASE_DIR / ".env"

load_dotenv(dotenv_path=ENV_FILE, override=True)

def _get_env(name: str):
    value = os.getenv(name)
    if value is None:
        return None

    value = value.strip()
    if value == "" or value.lower() in {"none", "null"}:
        return None

    return value


DB_HOST = _get_env("DB_HOST")
DB_PORT = _get_env("DB_PORT") or "5432"
DB_NAME = _get_env("DB_NAME")
DB_USER = _get_env("DB_USER")
DB_PASSWORD = _get_env("DB_PASSWORD")

missing = [
    key
    for key, value in {
        "DB_HOST": DB_HOST,
        "DB_NAME": DB_NAME,
        "DB_USER": DB_USER,
        "DB_PASSWORD": DB_PASSWORD,
    }.items()
    if not value
]

if missing:
    raise ValueError(
        "Variaveis de ambiente ausentes para conexao com banco: "
        + ", ".join(missing)
        + ". Configure o arquivo .env antes de executar a ingestao."
    )

DATABASE_URL = URL.create(
    drivername="postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=int(DB_PORT),
    database=DB_NAME,
)

engine = create_engine(DATABASE_URL)