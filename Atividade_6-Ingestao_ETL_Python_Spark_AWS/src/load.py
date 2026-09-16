"""
load.py

Responsável pela etapa LOAD do pipeline ETL.

Modos disponíveis:

1. LOCAL
   Grava os DataFrames em formato Parquet
   dentro de /opt/project/data/processed.

2. S3
   Grava os DataFrames diretamente no Amazon S3
   utilizando o protocolo s3a://.

As credenciais AWS NÃO são armazenadas neste arquivo.
Elas são obtidas através das variáveis de ambiente:

AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_SESSION_TOKEN       (opcional)
AWS_DEFAULT_REGION

AWS_BUCKET
LOAD_MODE
"""

import os

from pyspark.sql import DataFrame


# ============================================================
# CONFIGURAÇÕES
# ============================================================

LOAD_MODE = os.getenv(
    "LOAD_MODE",
    "LOCAL"
).upper()

BASE_OUTPUT = os.getenv(
    "BASE_OUTPUT",
    "/opt/project/data/processed"
)

AWS_BUCKET = os.getenv(
    "AWS_BUCKET",
    ""
)

AWS_REGION = os.getenv(
    "AWS_DEFAULT_REGION",
    "sa-east-1"
)


# ============================================================
# VALIDAÇÃO
# ============================================================

def validate_configuration():
    """
    Valida as configurações necessárias para execução.
    """

    print("=" * 70)
    print("CONFIGURAÇÃO DO LOAD")
    print("=" * 70)

    print(f"LOAD_MODE   : {LOAD_MODE}")
    print(f"BASE_OUTPUT : {BASE_OUTPUT}")
    print(f"AWS_REGION  : {AWS_REGION}")

    if LOAD_MODE == "S3":

        if not AWS_BUCKET:
            raise ValueError(
                "AWS_BUCKET não foi configurado."
            )

        print(
            f"AWS_BUCKET  : {AWS_BUCKET}"
        )

    elif LOAD_MODE != "LOCAL":

        raise ValueError(
            "LOAD_MODE inválido. "
            "Utilize LOCAL ou S3."
        )

    print("=" * 70)


# ============================================================
# GERAR DESTINO
# ============================================================

def build_output_path(dataset_name: str) -> str:
    """
    Monta o caminho de destino do dataset.

    LOCAL:
        /opt/project/data/processed/reclamacoes

    S3:
        s3a://bucket/processed/reclamacoes
    """

    if LOAD_MODE == "LOCAL":

        return (
            f"{BASE_OUTPUT}/"
            f"{dataset_name}"
        )

    return (
        f"s3a://{AWS_BUCKET}/"
        f"processed/"
        f"{dataset_name}"
    )


# ============================================================
# LOAD GENÉRICO
# ============================================================

def save_parquet(
    df: DataFrame,
    dataset_name: str,
    partition_columns=None
):
    """
    Grava um DataFrame em formato Parquet.

    Parameters
    ----------
    df : DataFrame
        DataFrame Spark.

    dataset_name : str
        Nome lógico do dataset.

    partition_columns : list, optional
        Colunas utilizadas no particionamento.
    """

    output_path = build_output_path(
        dataset_name
    )

    print("")
    print("=" * 70)
    print("LOAD PARQUET")
    print("=" * 70)

    print(
        f"Dataset       : {dataset_name}"
    )

    print(
        f"Modo          : {LOAD_MODE}"
    )

    print(
        f"Destino       : {output_path}"
    )

    if partition_columns:

        print(
            "Particionamento: "
            f"{partition_columns}"
        )

    else:

        print(
            "Particionamento: nenhum"
        )

    # --------------------------------------------------------
    # GRAVAÇÃO
    # --------------------------------------------------------

    writer = (
        df.write
        .mode("overwrite")
    )

    if partition_columns:

        writer = writer.partitionBy(
            *partition_columns
        )

    writer.parquet(
        output_path
    )

    print(
        "Status        : OK"
    )

    print(
        "Formato       : Parquet"
    )

    print("=" * 70)


# ============================================================
# RECLAMAÇÕES
# ============================================================

def load_reclamacoes(
    df: DataFrame
):
    """
    Grava o dataset de reclamações.

    LOCAL:
        data/processed/reclamacoes

    S3:
        s3://bucket/processed/reclamacoes
    """

    save_parquet(
        df=df,
        dataset_name="reclamacoes",
        partition_columns=[
            "Ano",
            "Trimestre"
        ]
    )


# ============================================================
# ENQUADRAMENTO
# ============================================================

def load_enquadramento(
    df: DataFrame
):
    """
    Grava o dataset de enquadramento.
    """

    save_parquet(
        df=df,
        dataset_name="enquadramento"
    )


# ============================================================
# GLASSDOOR
# ============================================================

def load_glassdoor(
    df: DataFrame
):
    """
    Grava o dataset Glassdoor.
    """

    save_parquet(
        df=df,
        dataset_name="glassdoor"
    )


# ============================================================
# CONSOLIDADO
# ============================================================

def load_consolidado(
    df: DataFrame
):
    """
    Grava o dataset consolidado.
    """

    save_parquet(
        df=df,
        dataset_name="consolidado"
    )


# ============================================================
# INDICADORES
# ============================================================

def load_indicadores(
    df: DataFrame
):
    """
    Grava os indicadores analíticos.
    """

    save_parquet(
        df=df,
        dataset_name="indicadores"
    )


# ============================================================
# LOAD COMPLETO
# ============================================================

def load_all(
    df_reclamacoes=None,
    df_enquadramento=None,
    df_glassdoor=None,
    df_consolidado=None,
    df_indicadores=None
):
    """
    Executa o LOAD de todos os datasets disponíveis.
    """

    validate_configuration()

    print("")
    print("=" * 70)
    print("INICIANDO LOAD COMPLETO")
    print("=" * 70)

    # --------------------------------------------------------
    # RECLAMAÇÕES
    # --------------------------------------------------------

    if df_reclamacoes is not None:

        load_reclamacoes(
            df_reclamacoes
        )

    # --------------------------------------------------------
    # ENQUADRAMENTO
    # --------------------------------------------------------

    if df_enquadramento is not None:

        load_enquadramento(
            df_enquadramento
        )

    # --------------------------------------------------------
    # GLASSDOOR
    # --------------------------------------------------------

    if df_glassdoor is not None:

        load_glassdoor(
            df_glassdoor
        )

    # --------------------------------------------------------
    # CONSOLIDADO
    # --------------------------------------------------------

    if df_consolidado is not None:

        load_consolidado(
            df_consolidado
        )

    # --------------------------------------------------------
    # INDICADORES
    # --------------------------------------------------------

    if df_indicadores is not None:

        load_indicadores(
            df_indicadores
        )

    print("")
    print("=" * 70)
    print("LOAD FINALIZADO")
    print("=" * 70)