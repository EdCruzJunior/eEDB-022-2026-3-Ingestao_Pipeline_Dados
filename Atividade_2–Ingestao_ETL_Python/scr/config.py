import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

BASE_DIR = Path(__file__).resolve().parents[1]
ENV_FILE = BASE_DIR / ".env"


def _load_env_file():
    candidates = [
        ENV_FILE,
        BASE_DIR / ".env",
        Path.cwd() / ".env",
        Path(__file__).resolve().parent / ".env",
    ]

    for candidate in candidates:
        if candidate and candidate.exists():
            load_dotenv(dotenv_path=str(candidate), override=True)
            return candidate

    return None


_load_env_file()


def _get_env(name: str):
    value = os.getenv(name)
    if value is None:
        env_file = _load_env_file()
        if env_file and env_file.exists():
            with env_file.open("r", encoding="utf-8-sig") as fh:
                for line in fh:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, val = line.split("=", 1)
                    os.environ.setdefault(key.strip(), val.strip())
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

print("DB_HOST     =", repr(DB_HOST))
print("DB_PORT     =", repr(DB_PORT))
print("DB_NAME     =", repr(DB_NAME))
print("DB_USER     =", repr(DB_USER))
print("DB_PASSWORD =", repr(DB_PASSWORD))
print(type(DB_PASSWORD))
print(type(DB_USER))

DATABASE_URL = URL.create(
    drivername="postgresql+psycopg",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=int(DB_PORT),
    database=DB_NAME,
)

engine = create_engine(DATABASE_URL)