import os
from pathlib import Path
from sqlalchemy import create_engine, text

DB_URL = os.getenv(
    "DATA_DB_URL",
    "postgresql+psycopg2://atividade:atividade123@postgres:5432/atividade5"
)
SQL_DIR = Path("/opt/airflow/sql")
engine = create_engine(DB_URL)


def run_sql_file(filename):
    sql = (SQL_DIR / filename).read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(text(sql))
    print(f"Executado: {filename}")


if __name__ == "__main__":
    run_sql_file("create_tables.sql")
    run_sql_file("silver.sql")
    run_sql_file("gold.sql")
    print("Transformação Silver/Gold concluída.")
