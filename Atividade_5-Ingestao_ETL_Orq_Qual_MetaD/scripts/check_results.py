
import os
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(os.getenv(
        "DATA_DB_URL",
        "postgresql+psycopg2://atividade:atividade123@localhost:5432/atividade5"
    ))

for schema, table in [
        ("raw", "reclamacoes"),
        ("raw", "enquadramento"),
        ("raw", "glassdoor_match"),
        ("silver", "reclamacoes"),
        ("gold", "indicadores_reclamacoes"),
        ("gold", "ranking_instituicoes"),
    ]:
        df = pd.read_sql(f'SELECT * FROM "{schema}"."{table}" LIMIT 5', engine)
        count = pd.read_sql(
            f'SELECT COUNT(*) AS n FROM "{schema}"."{table}"', engine
        ).iloc[0]["n"]
        print(f"{schema}.{table}: {count} registros")
        print(df.head(2).to_string(index=False))
