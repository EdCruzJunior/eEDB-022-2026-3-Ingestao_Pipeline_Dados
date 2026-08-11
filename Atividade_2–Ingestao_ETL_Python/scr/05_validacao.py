from pathlib import Path

import pandas as pd

from config import engine


def validar():

    print()
    print("=" * 70)
    print("VALIDAÇÃO DO PIPELINE")
    print("=" * 70)

    arquivos_trusted = [
        "reclamacoes.parquet",
        "enquadramento.parquet",
        "glassdoor_match.parquet",
        "glassdoor_match_less.parquet",
    ]

    for nome in arquivos_trusted:

        arquivo = (
            Path("data/trusted") /
            nome
        )

        df = pd.read_parquet(
            arquivo
        )

        print(
            f"Trusted {nome}: "
            f"{len(df)} registros"
        )

    arquivo_delivery = (
        Path("data/delivery") /
        "reclamacoes_glassdoor_final.parquet"
    )

    delivery_parquet = pd.read_parquet(
        arquivo_delivery
    )

    delivery_db = pd.read_sql(
        """
        SELECT *
        FROM delivery.reclamacoes_glassdoor_final
        """,
        engine
    )

    print()
    print(
        "Delivery Parquet:",
        len(delivery_parquet)
    )

    print(
        "Delivery PostgreSQL:",
        len(delivery_db)
    )

    print(
        "Colunas Delivery:",
        len(delivery_db.columns)
    )

    if len(delivery_parquet) == len(delivery_db):

        print(
            "OK - quantidade de registros "
            "consistente."
        )

    else:

        print(
            "ATENÇÃO - quantidade de registros "
            "divergente."
        )


if __name__ == "__main__":

    validar()