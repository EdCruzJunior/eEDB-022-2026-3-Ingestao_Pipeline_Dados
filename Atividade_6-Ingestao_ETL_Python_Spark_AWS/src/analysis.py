from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg


spark = (
    SparkSession.builder
    .appName("Analise-Reclamacoes")
    .getOrCreate()
)


df = spark.read.parquet(
    "/opt/project/data/processed/reclamacoes"
)


print("Total de registros:")
print(df.count())


print("Reclamações por ano:")

(
    df.groupBy("Ano")
    .agg(
        sum("total_reclamacoes")
        .alias("total_reclamacoes"),

        avg("reclamacoes_por_mil_clientes")
        .alias(
            "media_reclamacoes_1000_clientes"
        )
    )
    .orderBy("Ano")
    .show()
)


spark.stop()