from pathlib import Path

import pandas as pd
from sqlalchemy import text

from config import engine


DELIVERY_FILE = (
    Path("data/delivery") /
    "reclamacoes_glassdoor_final.parquet"
)


def carregar_delivery():

    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE SCHEMA IF NOT EXISTS delivery"
            )
        )

    df = pd.read_parquet(
        DELIVERY_FILE
    )

    print(
        f"Registros: {len(df)}"
    )

    df.to_sql(
        name="reclamacoes_glassdoor_final",
        con=engine,
        schema="delivery",
        if_exists="replace",
        index=False,
        chunksize=10000,
        method="multi"
    )

    print(
        "Tabela delivery.reclamacoes_glassdoor_final "
        "carregada com sucesso."
    )


if __name__ == "__main__":

    carregar_delivery()