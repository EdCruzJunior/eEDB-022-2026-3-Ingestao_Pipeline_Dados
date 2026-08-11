from pathlib import Path

import pandas as pd
from sqlalchemy import text

from config import engine


INPUT_DIR = Path("data/input")


def ler_arquivo(arquivo):

    nome = arquivo.name.lower()

    if arquivo.suffix.lower() == ".tsv":

        return pd.read_csv(
            arquivo,
            sep="\t",
            encoding="latin1",
            dtype=str
        )

    if "glassdoor" in nome:

        return pd.read_csv(
            arquivo,
            sep="|",
            encoding="latin1",
            dtype=str
        )

    if arquivo.suffix.lower() == ".csv":

        return pd.read_csv(
            arquivo,
            sep=";",
            encoding="latin1",
            dtype=str
        )

    raise ValueError(
        f"Formato não suportado: {arquivo}"
    )


def nome_tabela(nome_arquivo):

    nome = Path(nome_arquivo).stem

    nome = (
        nome
        .lower()
        .replace("-", "_")
        .replace(" ", "_")
        .replace("(", "")
        .replace(")", "")
    )

    return nome


def ingestao():

    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE SCHEMA IF NOT EXISTS raw"
            )
        )

    arquivos = sorted(
        INPUT_DIR.iterdir()
    )

    if not arquivos:

        raise FileNotFoundError(
            "Nenhum arquivo encontrado em data/input."
        )

    for arquivo in arquivos:

        if arquivo.suffix.lower() not in [
            ".csv",
            ".tsv"
        ]:
            continue

        print("=" * 70)
        print(f"Arquivo: {arquivo.name}")

        df = ler_arquivo(arquivo)

        tabela = nome_tabela(
            arquivo.name
        )

        print(
            f"Registros: {len(df)}"
        )

        print(
            f"Colunas: {len(df.columns)}"
        )

        df.to_sql(
            name=tabela,
            con=engine,
            schema="raw",
            if_exists="replace",
            index=False
        )

        print(
            f"Tabela criada: raw.{tabela}"
        )


if __name__ == "__main__":

    ingestao()