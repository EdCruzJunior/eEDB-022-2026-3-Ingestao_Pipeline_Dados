from pathlib import Path
import re

import pandas as pd

from config import engine


INPUT_DIR = Path("data/input")
TRUSTED_DIR = Path("data/trusted")

TRUSTED_DIR.mkdir(
    parents=True,
    exist_ok=True
)


ARQUIVOS_RECLAMACOES = [
    "2021_tri_01.csv",
    "2021_tri_02.csv",
    "2021_tri_03.csv",
    "2021_tri_04.csv",
    "2022_tri_01.csv",
    "2022_tri_03.csv",
    "2022_tri_04.csv",
]


def resolver_arquivo(*nomes):

    for nome in nomes:
        arquivo = INPUT_DIR / nome
        if arquivo.exists():
            return arquivo

    raise FileNotFoundError(
        "Arquivo nao encontrado em data/input. Tentativas: "
        + ", ".join(nomes)
    )


COLUNAS_RENOMEAR = {
    "Ano": "ano",
    "Trimestre": "trimestre",
    "Categoria": "categoria",
    "Tipo": "tipo",
    "CNPJ IF": "cnpj_if",
    "Instituição financeira": "instituicao_financeira",
    "Índice": "indice",
    "Quantidade de reclamações reguladas procedentes":
        "qtd_reclamacoes_reguladas_procedentes",
    "Quantidade de reclamações reguladas - outras":
        "qtd_reclamacoes_reguladas_outras",
    "Quantidade de reclamações não reguladas":
        "qtd_reclamacoes_nao_reguladas",
    "Quantidade total de reclamações":
        "qtd_total_reclamacoes",
    "Quantidade total de clientes � CCS e SCR":
        "qtd_clientes_ccs_scr",
    "Quantidade de clientes � CCS":
        "qtd_clientes_ccs",
    "Quantidade de clientes � SCR":
        "qtd_clientes_scr",
}


def normalizar_cnpj(valor):

    if pd.isna(valor):

        return None

    valor = re.sub(
        r"\D",
        "",
        str(valor)
    )

    if not valor:

        return None

    return valor


def normalizar_texto(valor):

    if pd.isna(valor):

        return None

    valor = str(valor).strip()

    if not valor:

        return None

    return valor


def converter_decimal(serie):

    return pd.to_numeric(
        serie
        .astype(str)
        .str.replace(
            ",",
            ".",
            regex=False
        ),
        errors="coerce"
    )


def carregar_reclamacoes():

    lista = []

    for nome in ARQUIVOS_RECLAMACOES:

        arquivo = INPUT_DIR / nome

        print(
            f"Lendo {arquivo.name}"
        )

        df = pd.read_csv(
            arquivo,
            sep=";",
            encoding="latin1",
            dtype=str
        )

        # Remove coluna vazia existente nos arquivos
        df = df.loc[
            :,
            ~df.columns.str.startswith(
                "Unnamed:"
            )
        ]

        lista.append(df)

    df = pd.concat(
        lista,
        ignore_index=True
    )

    return df


def tratar_reclamacoes():

    df = carregar_reclamacoes()

    df = df.rename(
        columns=COLUNAS_RENOMEAR
    )

    # Textos
    colunas_texto = [
        "trimestre",
        "categoria",
        "tipo",
        "instituicao_financeira",
    ]

    for coluna in colunas_texto:

        if coluna in df.columns:

            df[coluna] = (
                df[coluna]
                .apply(normalizar_texto)
            )

    # CNPJ
    df["cnpj_if_original"] = df[
        "cnpj_if"
    ]

    df["cnpj_if"] = df[
        "cnpj_if"
    ].apply(normalizar_cnpj)

    # Numéricos
    colunas_numericas = [
        "ano",
        "qtd_reclamacoes_reguladas_procedentes",
        "qtd_reclamacoes_reguladas_outras",
        "qtd_reclamacoes_nao_reguladas",
        "qtd_total_reclamacoes",
        "qtd_clientes_ccs_scr",
        "qtd_clientes_ccs",
        "qtd_clientes_scr",
    ]

    for coluna in colunas_numericas:

        if coluna in df.columns:

            df[coluna] = pd.to_numeric(
                df[coluna],
                errors="coerce"
            )

    # Índice decimal
    if "indice" in df.columns:

        df["indice"] = converter_decimal(
            df["indice"]
        )

    # Remove duplicidades
    df = df.drop_duplicates()

    # Salva Trusted
    arquivo = (
        TRUSTED_DIR /
        "reclamacoes.parquet"
    )

    df.to_parquet(
        arquivo,
        engine="pyarrow",
        index=False
    )

    print(
        f"Trusted reclamações: {arquivo}"
    )

    return df


def tratar_enquadramento():

    arquivo = resolver_arquivo(
        "EnquadramentoInicia_v2.tsv",
        "EnquadramentoInicia_v2(1).tsv",
    )

    df = pd.read_csv(
        arquivo,
        sep="\t",
        encoding="latin1",
        dtype=str
    )

    df.columns = [
        "segmento",
        "cnpj",
        "nome"
    ]

    df["cnpj_original"] = df["cnpj"]

    df["cnpj"] = df[
        "cnpj"
    ].apply(normalizar_cnpj)

    df["segmento"] = df[
        "segmento"
    ].apply(normalizar_texto)

    df["nome"] = df[
        "nome"
    ].apply(normalizar_texto)

    df = df.drop_duplicates()

    arquivo_saida = (
        TRUSTED_DIR /
        "enquadramento.parquet"
    )

    df.to_parquet(
        arquivo_saida,
        engine="pyarrow",
        index=False
    )

    print(
        f"Trusted enquadramento: {arquivo_saida}"
    )

    return df


def tratar_glassdoor():

    arquivo = resolver_arquivo(
        "glassdoor_consolidado_join_match_v2.csv",
        "glassdoor_consolidado_join_match_v2(1).csv",
    )

    df = pd.read_csv(
        arquivo,
        sep="|",
        encoding="latin1",
        dtype=str
    )

    df.columns = [
        str(col).strip().lower()
        for col in df.columns
    ]

    df = df.drop_duplicates()

    arquivo_saida = (
        TRUSTED_DIR /
        "glassdoor_match.parquet"
    )

    df.to_parquet(
        arquivo_saida,
        engine="pyarrow",
        index=False
    )

    print(
        f"Trusted Glassdoor: {arquivo_saida}"
    )

    return df


def tratar_glassdoor_less():

    arquivo = resolver_arquivo(
        "glassdoor_consolidado_join_match_less_v2.csv",
        "glassdoor_consolidado_join_match_less_v2(1).csv",
    )

    df = pd.read_csv(
        arquivo,
        sep="|",
        encoding="latin1",
        dtype=str
    )

    df.columns = [
        str(col).strip().lower()
        for col in df.columns
    ]

    df = df.drop_duplicates()

    arquivo_saida = (
        TRUSTED_DIR /
        "glassdoor_match_less.parquet"
    )

    df.to_parquet(
        arquivo_saida,
        engine="pyarrow",
        index=False
    )

    print(
        f"Trusted Glassdoor less: {arquivo_saida}"
    )

    return df


if __name__ == "__main__":

    tratar_reclamacoes()

    tratar_enquadramento()

    tratar_glassdoor()

    tratar_glassdoor_less()