import os
from typing import List

import psycopg2
from pyspark.sql import SparkSession, functions as F, types as T

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "reclamacoes")
CHECKPOINT = os.getenv("CHECKPOINT_DIR", "/app/checkpoint")
OUTPUT = os.getenv("OUTPUT_DIR", "/app/output")
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "streaming")
PG_USER = os.getenv("PG_USER", "streaming")
PG_PASSWORD = os.getenv("PG_PASSWORD", "streaming")

spark = (
    SparkSession.builder
    .appName("PipelineStreamingPySparkKafka")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("source_file", T.StringType()),
    T.StructField("source_row", T.IntegerType()),
    T.StructField("ingestion_ts", T.StringType()),
    T.StructField("data", T.MapType(T.StringType(), T.StringType())),
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

events = (
    raw.select(F.from_json(F.col("value").cast("string"), schema).alias("j"))
       .select("j.*")
)

def normalize_cnpj_col(c):
    return F.regexp_replace(F.coalesce(F.col(c), F.lit("")), r"[^0-9]", "")

def load_db_enrichment(cnpjs: List[str], names: List[str]):
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT cnpj_norm, segmento, nome AS nome_enquadramento
                FROM institution_enrichment
                WHERE cnpj_norm = ANY(%s)
            """, (cnpjs,))
            inst = cur.fetchall()

            cur.execute("""
                SELECT nome_norm, geral, cultura_valores, diversidade_inclusao,
                       qualidade_vida, alta_lideranca, remuneracao_beneficios,
                       oportunidades_carreira, recomendam_pct,
                       perspectiva_positiva_pct
                FROM glassdoor_enrichment
                WHERE nome_norm = ANY(%s)
            """, (names,))
            gd = cur.fetchall()
        return inst, gd
    finally:
        conn.close()

def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    # Converte o mapa JSON em colunas com nomes estáveis.
    d = batch_df.select(
        "event_id", "source_file", "source_row", "ingestion_ts",
        F.col("data").getItem("Ano").alias("ano"),
        F.col("data").getItem("Trimestre").alias("trimestre"),
        F.col("data").getItem("Categoria").alias("categoria"),
        F.col("data").getItem("Tipo").alias("tipo"),
        F.col("data").getItem("CNPJ IF").alias("cnpj_if"),
        F.col("data").getItem("Instituição financeira").alias("instituicao_financeira"),
        F.col("data").getItem("Índice").alias("indice"),
        F.col("data").getItem("Quantidade de reclamações reguladas procedentes").alias("reclamacoes_reguladas_procedentes"),
        F.col("data").getItem("Quantidade de reclamações reguladas - outras").alias("reclamacoes_reguladas_outras"),
        F.col("data").getItem("Quantidade de reclamações não reguladas").alias("reclamacoes_nao_reguladas"),
        F.col("data").getItem("Quantidade total de reclamações").alias("reclamacoes_total"),
        F.col("data").getItem("Quantidade total de clientes \x96 CCS e SCR").alias("clientes_ccs_scr"),
        F.col("data").getItem("Quantidade de clientes \x96 CCS").alias("clientes_ccs"),
        F.col("data").getItem("Quantidade de clientes \x96 SCR").alias("clientes_scr"),
    ).withColumn("cnpj_norm", normalize_cnpj_col("cnpj_if"))

    # Fallback de nome para Glassdoor.
    d = d.withColumn(
        "nome_norm",
        F.upper(F.trim(F.regexp_replace(F.col("instituicao_financeira"), r"\s*\(.*\)$", "")))
    )

    cnpjs = [r["cnpj_norm"] for r in d.select("cnpj_norm").distinct().collect() if r["cnpj_norm"]]
    names = [r["nome_norm"] for r in d.select("nome_norm").distinct().collect() if r["nome_norm"]]

    inst_rows, gd_rows = load_db_enrichment(cnpjs, names)

    inst_schema = T.StructType([
        T.StructField("cnpj_norm", T.StringType()),
        T.StructField("segmento", T.StringType()),
        T.StructField("nome_enquadramento", T.StringType()),
    ])
    gd_schema = T.StructType([
        T.StructField("nome_norm", T.StringType()),
        T.StructField("geral", T.DoubleType()),
        T.StructField("cultura_valores", T.DoubleType()),
        T.StructField("diversidade_inclusao", T.DoubleType()),
        T.StructField("qualidade_vida", T.DoubleType()),
        T.StructField("alta_lideranca", T.DoubleType()),
        T.StructField("remuneracao_beneficios", T.DoubleType()),
        T.StructField("oportunidades_carreira", T.DoubleType()),
        T.StructField("recomendam_pct", T.DoubleType()),
        T.StructField("perspectiva_positiva_pct", T.DoubleType()),
    ])

    inst_df = spark.createDataFrame(inst_rows, inst_schema) if inst_rows else spark.createDataFrame([], inst_schema)
    gd_df = spark.createDataFrame(gd_rows, gd_schema) if gd_rows else spark.createDataFrame([], gd_schema)

    enriched = (
        d.join(inst_df, "cnpj_norm", "left")
         .join(gd_df, "nome_norm", "left")
         .withColumn("batch_id", F.lit(batch_id))
         .withColumn("processed_ts", F.current_timestamp())
         .withColumn(
             "taxa_reclamacoes_por_100k_clientes",
             F.when(
                 F.col("clientes_ccs_scr").cast("double") > 0,
                 F.col("reclamacoes_total").cast("double") * 100000 /
                 F.col("clientes_ccs_scr").cast("double")
             )
         )
         .drop("cnpj_norm", "nome_norm")
    )

    (
        enriched
        .coalesce(1)
        .write
        .mode("append")
        .parquet(OUTPUT)
    )
    print(f"[CONSUMIDOR] batch_id={batch_id} registros={batch_df.count()}")

query = (
    events.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
