import os

from pyspark.sql import SparkSession

from extract import extract_trimestrais
from transform import transform_reclamacoes
from load import load_reclamacoes


# ============================================================
# SPARK
# ============================================================

AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")

spark = (
    SparkSession.builder
    .appName(
        "ETL-Reclamacoes-Financeiras"
    )

    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
    )

    .config(
        "spark.hadoop.fs.s3a.endpoint",
        f"s3.{AWS_REGION}.amazonaws.com"
    )

    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "false"
    )

    .getOrCreate()
)


# ============================================================
# INICIO
# ============================================================

print("=" * 70)
print("ETL PYSPARK")
print("=" * 70)


# ============================================================
# EXTRACT
# ============================================================

print("")
print("ETAPA 1 - EXTRACT")


df = extract_trimestrais(
    spark,
    "/opt/project/data/utf8"
)


print(
    f"Registros extraídos: {df.count()}"
)


# ============================================================
# TRANSFORM
# ============================================================

print("")
print("ETAPA 2 - TRANSFORM")


df = transform_reclamacoes(
    df
)


df.printSchema()


# ============================================================
# LOAD
# ============================================================

print("")
print("ETAPA 3 - LOAD")


load_reclamacoes(
    df
)


# ============================================================
# FINAL
# ============================================================

print("")
print("=" * 70)
print("ETL FINALIZADO COM SUCESSO")
print("=" * 70)


spark.stop()