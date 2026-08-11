from pathlib import Path

import pandas as pd


TRUSTED_DIR = Path("data/trusted")
DELIVERY_DIR = Path("data/delivery")

DELIVERY_DIR.mkdir(
    parents=True,
    exist_ok=True
)


def criar_delivery():

    reclamacoes = pd.read_parquet(
        TRUSTED_DIR /
        "reclamacoes.parquet"
    )

    enquadramento = pd.read_parquet(
        TRUSTED_DIR /
        "enquadramento.parquet"
    )

    glassdoor = pd.read_parquet(
        TRUSTED_DIR /
        "glassdoor_match_less.parquet"
    )

    # ==================================================
    # JOIN 1 - RECLAMAÇÕES + ENQUADRAMENTO
    # ==================================================

    df = reclamacoes.merge(
        enquadramento,
        how="left",
        left_on="cnpj_if",
        right_on="cnpj",
        suffixes=(
            "",
            "_enquadramento"
        )
    )

    # ==================================================
    # JOIN 2 - DELIVERY + GLASSDOOR
    # ==================================================

    # A base Glassdoor less possui CNPJ.
    df = df.merge(
        glassdoor,
        how="left",
        left_on="cnpj_if",
        right_on="cnpj",
        suffixes=(
            "",
            "_glassdoor"
        )
    )

    # ==================================================
    # CONTROLES
    # ==================================================

    df["data_processamento"] = (
        pd.Timestamp.now()
    )

    df = df.drop_duplicates()

    # ==================================================
    # DELIVERY PARQUET
    # ==================================================

    arquivo = (
        DELIVERY_DIR /
        "reclamacoes_glassdoor_final.parquet"
    )

    df.to_parquet(
        arquivo,
        engine="pyarrow",
        index=False
    )

    print(
        f"Delivery criada: {arquivo}"
    )

    print(
        f"Total de registros: {len(df)}"
    )

    print(
        f"Total de colunas: {len(df.columns)}"
    )

    return df


if __name__ == "__main__":

    criar_delivery()